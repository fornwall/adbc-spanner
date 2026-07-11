"""ADBC driver for Google Cloud Spanner.

This package bundles the prebuilt Spanner ADBC driver shared library and exposes
a thin Python wrapper around it. The heavy lifting lives in the Rust cdylib; this
module just locates the bundled library and hands it to ``adbc_driver_manager``,
which loads it over the ADBC C ABI.

For a DBAPI 2.0 (PEP 249) connection with pandas/polars/Arrow helpers, use
:func:`adbc_driver_spanner.dbapi.connect` instead of the low-level
:func:`connect` here.
"""

import functools
import pathlib
import typing

import adbc_driver_manager

from ._options import ConnectionOptions, DatabaseOptions, StatementOptions
from ._version import __version__

__all__ = [
    "connect",
    "option_kwargs",
    "ENTRYPOINT",
    "DatabaseOptions",
    "ConnectionOptions",
    "StatementOptions",
    "__version__",
]

#: C entrypoint exported by the shared library (see src/ffi.rs).
ENTRYPOINT = "AdbcSpannerInit"

#: Option keys that name the same underlying setting. ``database=`` populates
#: ``spanner.database``, but the standard ADBC ``uri`` option is a documented alias
#: of it (see :attr:`DatabaseOptions.URI`), so a ``db_kwargs`` entry for either one
#: collides with a ``database=`` argument even though the key strings differ.
_OPTION_ALIASES = {
    "spanner.database": "uri",
    "uri": "spanner.database",
}


def option_kwargs(
    database: typing.Optional[str] = None,
    *,
    endpoint: typing.Optional[str] = None,
    emulator: bool = False,
    keyfile: typing.Optional[str] = None,
    keyfile_json: typing.Optional[str] = None,
    impersonate_target_principal: typing.Optional[str] = None,
    impersonate_delegates: typing.Optional[typing.Union[str, typing.Sequence[str]]] = None,
    impersonate_scopes: typing.Optional[typing.Union[str, typing.Sequence[str]]] = None,
    impersonate_lifetime: typing.Optional[typing.Union[int, str]] = None,
    access_token: typing.Optional[str] = None,
    db_kwargs: typing.Optional[typing.Mapping[str, str]] = None,
) -> typing.Dict[str, str]:
    """Translate the friendly connection kwargs into ``spanner.*`` options.

    Shared by :func:`connect` and :func:`adbc_driver_spanner.dbapi.connect` so the
    two entry points map parameters identically. ``db_kwargs`` is an escape hatch
    for raw option keys that have no friendly keyword argument; it is merged last.

    A ``db_kwargs`` entry that targets a setting an explicit keyword argument already
    populated — the same key, or an alias such as ``uri`` for ``database=`` — raises
    :class:`ValueError` rather than silently overriding one value with the other. Keys
    with no friendly counterpart merge in unchanged.
    """
    options: typing.Dict[str, str] = {}
    # Friendly kwargs -> the driver's option keys (see src/lib.rs).
    if database is not None:
        options["spanner.database"] = database
    if endpoint is not None:
        options["spanner.endpoint"] = endpoint
    if emulator:
        options["spanner.emulator"] = "true"
    if keyfile is not None:
        options["spanner.auth.keyfile"] = keyfile
    if keyfile_json is not None:
        options["spanner.auth.keyfile_json"] = keyfile_json
    # Service-account impersonation (layered on top of the base credentials above);
    # enabled only when a target principal is set. delegates/scopes accept either a
    # comma-separated string or a sequence of strings.
    if impersonate_target_principal is not None:
        options["spanner.auth.impersonate.target_principal"] = impersonate_target_principal
    if impersonate_delegates is not None:
        options["spanner.auth.impersonate.delegates"] = _as_csv(impersonate_delegates)
    if impersonate_scopes is not None:
        options["spanner.auth.impersonate.scopes"] = _as_csv(impersonate_scopes)
    if impersonate_lifetime is not None:
        options["spanner.auth.impersonate.lifetime"] = str(impersonate_lifetime)
    # A caller-supplied OAuth 2.0 bearer token, sent verbatim with no refresh; mutually
    # exclusive with the keyfile/impersonation options above.
    if access_token is not None:
        options["spanner.auth.access_token"] = access_token
    _merge_db_kwargs(options, db_kwargs)
    return options


def _merge_db_kwargs(
    options: typing.Dict[str, str],
    db_kwargs: typing.Optional[typing.Mapping[str, str]],
) -> None:
    """Merge the raw ``db_kwargs`` escape hatch into the friendly-kwarg ``options``.

    Raises :class:`ValueError` if a ``db_kwargs`` key targets a setting a friendly
    keyword argument already populated — either the same key, or an alias of it (see
    :data:`_OPTION_ALIASES`) — so a value is never silently overridden. Keys with no
    friendly counterpart merge in.
    """
    if not db_kwargs:
        return
    friendly = set(options)
    for key, value in db_kwargs.items():
        clash = key if key in friendly else _OPTION_ALIASES.get(key)
        if clash in friendly:
            raise ValueError(
                f"{key!r} in db_kwargs conflicts with a keyword argument that "
                f"already set {clash!r}; pass it only one way"
            )
        options[key] = value


def _as_csv(value: typing.Union[str, typing.Sequence[str]]) -> str:
    """Render a delegates/scopes value as the comma-separated string the driver expects."""
    if isinstance(value, str):
        return value
    return ",".join(value)


def connect(
    database: typing.Optional[str] = None,
    *,
    endpoint: typing.Optional[str] = None,
    emulator: bool = False,
    keyfile: typing.Optional[str] = None,
    keyfile_json: typing.Optional[str] = None,
    impersonate_target_principal: typing.Optional[str] = None,
    impersonate_delegates: typing.Optional[typing.Union[str, typing.Sequence[str]]] = None,
    impersonate_scopes: typing.Optional[typing.Union[str, typing.Sequence[str]]] = None,
    impersonate_lifetime: typing.Optional[typing.Union[int, str]] = None,
    access_token: typing.Optional[str] = None,
    db_kwargs: typing.Optional[typing.Mapping[str, str]] = None,
) -> adbc_driver_manager.AdbcDatabase:
    """Create a low-level ADBC database handle for Spanner.

    Parameters
    ----------
    database:
        Fully-qualified database path,
        ``projects/<p>/instances/<i>/databases/<d>``.
    endpoint:
        Override the Spanner gRPC endpoint (e.g. an emulator ``host:port``).
    emulator:
        Use anonymous credentials and talk to the emulator. When
        ``SPANNER_EMULATOR_HOST`` is set the driver detects the emulator on its
        own, so this is only needed to force it explicitly.
    keyfile / keyfile_json:
        Service-account credentials, as a path or inline JSON. Omit both to use
        Application Default Credentials.
    impersonate_target_principal:
        Service account to impersonate. Setting it enables service-account
        impersonation on top of the base credentials above; leave it unset for no
        impersonation. Follows gcloud's ``--impersonate-service-account`` /
        ``google-cloud-auth``'s ``impersonated`` builder naming.
    impersonate_delegates:
        Optional delegation chain (a comma-separated string or a sequence of
        service-account emails).
    impersonate_scopes:
        Optional OAuth scopes (a comma-separated string or a sequence); defaults to
        the ``cloud-platform`` scope.
    impersonate_lifetime:
        Optional impersonated-token lifetime in seconds; defaults to ``3600``.
    access_token:
        A caller-supplied OAuth 2.0 bearer token, sent verbatim with no refresh.
        Mutually exclusive with the keyfile/impersonation options above.
    db_kwargs:
        Escape hatch for raw ``spanner.*`` option keys with no friendly keyword
        argument, merged last. A key that duplicates a setting an explicit keyword
        argument already populated (the same key, or ``uri`` versus ``database=``)
        raises :class:`ValueError` instead of silently overriding it.

    For a DBAPI 2.0 connection, prefer :func:`adbc_driver_spanner.dbapi.connect`.
    """
    options = option_kwargs(
        database,
        endpoint=endpoint,
        emulator=emulator,
        keyfile=keyfile,
        keyfile_json=keyfile_json,
        impersonate_target_principal=impersonate_target_principal,
        impersonate_delegates=impersonate_delegates,
        impersonate_scopes=impersonate_scopes,
        impersonate_lifetime=impersonate_lifetime,
        access_token=access_token,
        db_kwargs=db_kwargs,
    )
    # ** unpacking accepts the dotted, non-identifier option keys; they land in
    # AdbcDatabase's **kwargs and are forwarded as ADBC options.
    return adbc_driver_manager.AdbcDatabase(
        driver=_driver_path(), entrypoint=ENTRYPOINT, **options
    )


@functools.cache
def _driver_path() -> str:
    """Absolute path to the shared library bundled in this wheel."""
    here = pathlib.Path(__file__).resolve().parent
    for name in ("libadbc_spanner.so", "libadbc_spanner.dylib", "adbc_spanner.dll"):
        candidate = here / name
        if candidate.is_file():
            return str(candidate)
    raise RuntimeError(
        "adbc_driver_spanner: no bundled Spanner driver library found next to "
        f"{here}. This usually means a source/sdist install without a matching "
        "platform wheel; install a prebuilt wheel for your platform instead."
    )

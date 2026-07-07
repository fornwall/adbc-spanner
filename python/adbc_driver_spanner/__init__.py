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

from ._version import __version__

__all__ = ["connect", "ENTRYPOINT", "__version__"]

#: C entrypoint exported by the shared library (see src/ffi.rs).
ENTRYPOINT = "AdbcSpannerInit"


def connect(
    database: typing.Optional[str] = None,
    *,
    endpoint: typing.Optional[str] = None,
    emulator: bool = False,
    keyfile: typing.Optional[str] = None,
    keyfile_json: typing.Optional[str] = None,
    db_kwargs: typing.Optional[typing.Dict[str, str]] = None,
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
    db_kwargs:
        Escape hatch for raw ``adbc.spanner.*`` option keys, merged last.
    """
    kwargs: typing.Dict[str, str] = {
        "driver": _driver_path(),
        "entrypoint": ENTRYPOINT,
    }
    # Friendly kwargs -> the driver's option keys (see src/lib.rs).
    if database is not None:
        kwargs["adbc.spanner.database"] = database
    if endpoint is not None:
        kwargs["adbc.spanner.endpoint"] = endpoint
    if emulator:
        kwargs["adbc.spanner.emulator"] = "true"
    if keyfile is not None:
        kwargs["adbc.spanner.keyfile"] = keyfile
    if keyfile_json is not None:
        kwargs["adbc.spanner.keyfile_json"] = keyfile_json
    if db_kwargs:
        kwargs.update(db_kwargs)

    # ** unpacking accepts the dotted, non-identifier option keys; they land in
    # AdbcDatabase's **kwargs and are forwarded as ADBC options.
    return adbc_driver_manager.AdbcDatabase(**kwargs)


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

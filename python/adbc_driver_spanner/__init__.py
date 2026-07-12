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
    "ENTRYPOINT",
    "DatabaseOptions",
    "ConnectionOptions",
    "StatementOptions",
    "__version__",
]

#: C entrypoint exported by the shared library (see src/ffi.rs).
ENTRYPOINT = "AdbcSpannerInit"


def connect(
    db_kwargs: typing.Optional[typing.Mapping[str, str]] = None,
) -> adbc_driver_manager.AdbcDatabase:
    """Create a low-level ADBC database handle for Spanner.

    Parameters
    ----------
    db_kwargs:
        Raw database-level driver options (see src/lib.rs / docs/options.md),
        e.g. ``{"uri": "spanner:///projects/<p>/instances/<i>/databases/<d>"}``.
        The ``uri`` option requires the ``spanner://`` scheme; a bare database
        path is rejected. Credentials, the emulator, endpoint overrides, and every
        other setting are all passed here as their raw ``spanner.*`` keys — for
        example ``{"uri": "...", "spanner.auth.keyfile": "/path/key.json"}`` or
        ``{"uri": "...", "spanner.emulator": "true"}``.

    For a DBAPI 2.0 connection, prefer :func:`adbc_driver_spanner.dbapi.connect`.
    """
    # ** unpacking accepts the dotted, non-identifier option keys; they land in
    # AdbcDatabase's **kwargs and are forwarded as ADBC options.
    options = dict(db_kwargs) if db_kwargs else {}
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

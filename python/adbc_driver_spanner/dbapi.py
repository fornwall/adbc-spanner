"""DBAPI 2.0 (PEP 249) interface for the Spanner ADBC driver.

This is the layer most users want: it returns a standard DBAPI connection with
cursors, plus the ADBC Arrow extensions (``fetch_arrow_table``, ``fetch_df``,
``adbc_ingest``) that pandas / polars / DuckDB consume directly.

    import adbc_driver_spanner.dbapi as spanner
    with spanner.connect(uri="spanner:///projects/p/instances/i/databases/d") as conn:
        df = conn.cursor().execute("SELECT * FROM Singers").fetch_df()

Note: DBAPI is autocommit-off by default, which puts this driver into its
buffer-and-commit manual-transaction mode — call ``conn.commit()`` to apply DML.
Pass ``autocommit=True`` to keep the driver's default single-statement mode.
"""

import typing

import adbc_driver_manager.dbapi

from . import (
    ENTRYPOINT,
    ConnectionOptions,
    DatabaseOptions,
    StatementOptions,
    _driver_path,
)

__all__ = [
    "connect",
    "DatabaseOptions",
    "ConnectionOptions",
    "StatementOptions",
]


def connect(
    db_kwargs: typing.Optional[typing.Mapping[str, str]] = None,
    conn_kwargs: typing.Optional[typing.Mapping[str, str]] = None,
    autocommit: bool = False,
) -> adbc_driver_manager.dbapi.Connection:
    """Open a DBAPI 2.0 connection to a Spanner database.

    Parameters
    ----------
    db_kwargs:
        Raw database-level driver options (see src/lib.rs / docs/options.md),
        e.g. ``{"uri": "spanner:///projects/<p>/instances/<i>/databases/<d>"}``.
        The ``uri`` option requires the ``spanner://`` scheme; a bare database path
        is rejected. Credentials, the emulator, endpoint overrides, and every other
        setting are passed here as their raw ``spanner.*`` keys.
    conn_kwargs:
        Raw connection-level options (``adbc.connection.*`` / ``spanner.*``).
    autocommit:
        Toggles PEP 249 autocommit; ``False`` (the default) puts the driver into
        its buffer-and-commit manual-transaction mode.
    """
    # The driver manager builds and owns the database/connection handles here and
    # tears them down if the connection fails, so no manual cleanup is needed.
    return adbc_driver_manager.dbapi.connect(
        driver=_driver_path(),
        entrypoint=ENTRYPOINT,
        db_kwargs=dict(db_kwargs) if db_kwargs else None,
        conn_kwargs=conn_kwargs,
        autocommit=autocommit,
    )

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
    option_kwargs,
)

__all__ = [
    "connect",
    "DatabaseOptions",
    "ConnectionOptions",
    "StatementOptions",
]


def connect(
    uri: typing.Optional[str] = None,
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
    conn_kwargs: typing.Optional[typing.Mapping[str, str]] = None,
    autocommit: bool = False,
) -> adbc_driver_manager.dbapi.Connection:
    """Open a DBAPI 2.0 connection to a Spanner database.

    Accepts the same connection parameters as
    :func:`adbc_driver_spanner.connect` (including the ``impersonate_*`` service-account
    impersonation options); ``conn_kwargs`` sets raw ``adbc.connection.*`` options and
    ``autocommit`` toggles PEP 249 autocommit.
    """
    options = option_kwargs(
        uri,
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
    # The driver manager builds and owns the database/connection handles here and
    # tears them down if the connection fails, so no manual cleanup is needed.
    return adbc_driver_manager.dbapi.connect(
        driver=_driver_path(),
        entrypoint=ENTRYPOINT,
        db_kwargs=options,
        conn_kwargs=conn_kwargs,
        autocommit=autocommit,
    )

# adbc-driver-spanner

[![PyPI version](https://img.shields.io/pypi/v/adbc-driver-spanner.svg)](https://pypi.org/project/adbc-driver-spanner/)
[![Python versions](https://img.shields.io/pypi/pyversions/adbc-driver-spanner.svg)](https://pypi.org/project/adbc-driver-spanner/)
[![Wheel](https://img.shields.io/pypi/wheel/adbc-driver-spanner.svg)](https://pypi.org/project/adbc-driver-spanner/#files)
[![License](https://img.shields.io/pypi/l/adbc-driver-spanner.svg)](https://github.com/fornwall/spanner-adbc/blob/main/LICENSE)
[![Build](https://github.com/fornwall/spanner-adbc/actions/workflows/libraries.yml/badge.svg)](https://github.com/fornwall/spanner-adbc/actions/workflows/libraries.yml)

A Python [ADBC](https://arrow.apache.org/adbc/) driver for **Google Cloud Spanner**.

Query Spanner through [DBAPI 2.0](https://peps.python.org/pep-0249/) and fetch
[Apache Arrow](https://arrow.apache.org/) results for pandas, Polars, DuckDB, or PyArrow.

## Install

Requires Python 3.11 or later. Wheels bundle the native driver; see
[Supported platforms](#supported-platforms) for OS requirements.

```sh
pip install "adbc-driver-spanner[dbapi]"  # includes PyArrow and pandas
```

For the low-level ADBC API without DataFrame dependencies, install `adbc-driver-spanner`.

## Quickstart

This example assumes an existing `Singers` table with `SingerId` and `FirstName` columns.

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        df = cur.fetch_df()          # -> pandas.DataFrame
```

Use GoogleSQL `@name` parameters with `cur.execute(...)`; `?` placeholders are unsupported.
`fetchone()` and `fetchall()` return Python rows; Arrow and DataFrame helpers are shown below.

## Driver manifest (`driver="spanner"`)

The package's `spanner.connect()` loads its bundled library directly. To use the generic
`adbc_driver_manager` by driver name, install an [ADBC driver manifest][manifests].
Use a current driver manager; Python manifest support requires
[version 1.8.0 or later](https://arrow.apache.org/blog/2025/09/12/adbc-20-release/).

```sh
pip install --upgrade adbc-driver-manager
python -m adbc_driver_spanner.manifest install
```

The equivalent console command is `adbc-driver-spanner-install-manifest`.

```python docs-test: skip
import adbc_driver_manager.dbapi

with adbc_driver_manager.dbapi.connect(
    driver="spanner",
    uri="spanner:///projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    ...
```

Current driver managers can also infer `spanner` from the URI scheme when `driver` is omitted.

- Re-run the installer after upgrading, reinstalling, or moving the environment: the manifest
  contains the library's absolute path.
- The default directory is `<sys.prefix>/etc/adbc/drivers` inside a virtual environment, or the
  platform's user configuration directory otherwise. `python -m adbc_driver_spanner.manifest path`
  prints the target path.
- Use `install --dir /path/to/drivers` for a custom directory on `ADBC_DRIVER_PATH`.
- For a standalone shared library, edit the `Driver.shared` paths in the repository's
  [spanner.toml][manifest-file].

[manifests]: https://arrow.apache.org/adbc/current/format/driver_manifests.html
[manifest-file]: https://github.com/fornwall/spanner-adbc/blob/main/spanner.toml

## Authentication

With no credential options, the driver uses
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials):
run `gcloud auth application-default login` locally, set `GOOGLE_APPLICATION_CREDENTIALS`, or use
an attached service account on Google Cloud.

Pass explicit credentials in `db_kwargs` alongside `DatabaseOptions.URI.value`:

| Option | Value |
| --- | --- |
| `DatabaseOptions.KEYFILE` | Credential JSON file path |
| `DatabaseOptions.KEYFILE_JSON` | Credential JSON contents |
| `DatabaseOptions.ACCESS_TOKEN` | OAuth bearer token; never refreshed |
| `DatabaseOptions.IMPERSONATE_TARGET_PRINCIPAL` | Service account to impersonate using ADC or explicit base credentials |

Use each enum member's `.value` as the key. `ACCESS_TOKEN` cannot be combined with key-file
credentials or impersonation. See the [option reference][options] for scopes and other auth settings.

For the [Spanner emulator](https://cloud.google.com/spanner/docs/emulator), use anonymous mode:

```python docs-test: skip
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

with spanner.connect(db_kwargs={
    DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
    DatabaseOptions.ENDPOINT.value: "localhost:9010",
    DatabaseOptions.EMULATOR.value: "true",
}) as conn:
    ...
```

Emulator mode rejects explicit credential options; ambient ADC is ignored.

## Options

| `connect()` argument | Purpose |
| --- | --- |
| `db_kwargs` | Database options, including the required `uri` |
| `conn_kwargs` | Connection options |
| `autocommit` | `False` by default; see [Transactions](#transactions) |

Use `DatabaseOptions`, `ConnectionOptions`, and `StatementOptions` enums, or raw option keys.
The [option reference][options] lists types, defaults, and accepted values. Set cursor options with
`conn.cursor(adbc_stmt_kwargs={...})` or `cur.adbc_statement.set_options(**{...})`.

[options]: https://github.com/fornwall/spanner-adbc/blob/main/docs/options.md

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import ConnectionOptions, DatabaseOptions, StatementOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d"},
    conn_kwargs={ConnectionOptions.READ_STALENESS.value: "max:10s"},
    autocommit=True,  # bounded staleness on a single-use read
) as conn:
    with conn.cursor(
        adbc_stmt_kwargs={StatementOptions.ROWS_PER_BATCH.value: "1024"}
    ) as cur:
        cur.execute("SELECT * FROM Singers")
```

In manual query transactions, `max:<duration>` and `min:<timestamp>` bounds become exact
staleness and a fixed read timestamp respectively. Set `ConnectionOptions.READONLY.value` to
`"true"` to reject DML, DDL, and ingest; rollback remains available.

## Transactions

DBAPI defaults to `autocommit=False`. A manual transaction accepts either queries or writes,
chosen by its first query or write; mixing them raises `adbc_driver_manager.ProgrammingError` until
commit or rollback. Queries share a snapshot. DML is buffered until `conn.commit()`, so queries
cannot read buffered writes.

DDL always executes immediately: rollback cannot undo it, and it runs before buffered DML.
Use `autocommit=True` for immediately committed DML, including `THEN RETURN` statements.
See [transactions](https://github.com/fornwall/spanner-adbc/blob/main/docs/transactions.md) for
bulk-ingest and partitioned-DML behavior.

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_manager import ProgrammingError
from adbc_driver_spanner import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        # DDL applies immediately.
        cur.execute("DROP TABLE IF EXISTS Albums")
        cur.execute("CREATE TABLE Albums (Id INT64 NOT NULL) PRIMARY KEY (Id)")

        cur.execute("INSERT INTO Albums (Id) VALUES (1)")  # a DML transaction: buffered
        # Commit before querying the inserted row.
        try:
            cur.execute("SELECT COUNT(*) FROM Albums")
            raise AssertionError("expected the guarded query to raise")
        except ProgrammingError:
            pass
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM Albums")
        assert cur.fetchone()[0] == 1
    conn.rollback()  # end the query snapshot
```

## Working with DataFrames

The quickstart uses pandas. The examples below use the same `Singers` table with `SingerId` and
`FirstName` columns. Install `polars` or `duckdb` separately for those examples.

**pyarrow — results as a native Arrow table:**

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers ORDER BY SingerId")
        table = cur.fetch_arrow_table()      # -> pyarrow.Table
```

**polars — read straight from the connection:**

```python
import polars as pl
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    df = pl.read_database(
        "SELECT SingerId, FirstName FROM Singers ORDER BY SingerId",
        connection=conn,                     # an ADBC connection, not a URI
    )
```

**DuckDB — query the fetched Arrow table in-process:**

```python
import duckdb
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        singers = cur.fetch_arrow_table()

# DuckDB can query the Arrow table by variable name.
top = duckdb.sql("SELECT COUNT(*) AS n, MIN(FirstName) AS first FROM singers").fetchone()
```

## Bulk insert a DataFrame

`cur.adbc_ingest(table, data, mode=...)` inserts Arrow-compatible data in bulk. Convert pandas
DataFrames to Arrow as shown here:

```python
import pandas as pd
import pyarrow as pa
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

frame = pd.DataFrame({"SingerId": [10, 11], "FirstName": ["Carol", "Dave"]})

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
    autocommit=True,                         # apply immediately; returns the row count
) as conn:
    with conn.cursor() as cur:
        # `append` inserts into an existing table (the default mode is `create`).
        rows = cur.adbc_ingest("Singers", pa.Table.from_pandas(frame), mode="append")
```

The `mode` selects how the target table is handled:

- `create` — create the table from the data's Arrow schema first, erroring if it already exists (the default).
- `append` — insert into an existing table.
- `create_append` — create the table only if it is absent, then insert.
- `replace` — drop any existing table, recreate it from the schema, then insert.

Create modes omit a primary key, so Spanner supplies a [hidden `rowid` column][no-pk].
For an explicit primary key, create the table with SQL first and use `mode="append"`.
In manual mode, inserts need `conn.commit()`; table creation or replacement still happens immediately.
Autocommit loads may commit in multiple chunks, so a failed load can leave rows written.

[no-pk]: https://cloud.google.com/spanner/docs/primary-key-default-value#tables-without-primary-keys

## Partitioned reads and Data Boost

A large scan can be split into independent partitions and read in parallel — optionally on Spanner's
serverless [Data Boost] compute, so the work is isolated from your provisioned instance. This uses
the ADBC partitioned-execution extension (`adbc_execute_partitions` / `adbc_read_partition`):

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions, StatementOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        # Optional statement options, set on the underlying ADBC statement:
        cur.adbc_statement.set_options(**{
            StatementOptions.DATA_BOOST.value: "true",  # run on Data Boost
        })
        partitions, schema = cur.adbc_execute_partitions("SELECT SingerId FROM Singers")

    # Each descriptor is opaque bytes; it can be shipped to another worker,
    # process, or connection and read independently.
    for token in partitions:
        with conn.cursor() as cur:
            cur.adbc_read_partition(token)
            table = cur.fetch_arrow_table()
            ...
```

Spanner decides partitionability from the query plan. Simple scans are suitable; joins, ordering,
and aggregation can prevent partitioning. See the [partitionability rules][parallel-reads].

Descriptors contain SQL and transaction identifiers and are unauthenticated. Only read descriptors
from trusted sources: they execute using the receiving connection's credentials.

[Data Boost]: https://cloud.google.com/spanner/docs/databoost/databoost-overview
[parallel-reads]: https://cloud.google.com/spanner/docs/reads#read_data_in_parallel

## Supported platforms

`pip` selects a wheel matching the OS and architecture:

| Platform       | Wheel tag                | Minimum requirement                          |
| -------------- | ------------------------ | -------------------------------------------- |
| Linux x86-64   | `manylinux_2_35_x86_64`  | glibc >= 2.35 |
| Linux aarch64  | `manylinux_2_35_aarch64` | glibc >= 2.35 |
| Linux x86-64 musl | `musllinux_1_2_x86_64` | musl libc >= 1.2 |
| Linux aarch64 musl | `musllinux_1_2_aarch64` | musl libc >= 1.2 |
| macOS arm64    | `macosx_11_0_arm64`      | macOS >= 11.0                                |
| macOS x86-64   | `macosx_10_15_x86_64`    | macOS >= 10.15                               |
| Windows x86-64 | `win_amd64`              | 64-bit Windows                               |
| Windows arm64  | `win_arm64`              | ARM64 Windows                                |

Building for an older OS requires a compatible native driver and Python dependencies.
For local builds and releases, see [CONTRIBUTING.md](https://github.com/fornwall/spanner-adbc/blob/main/CONTRIBUTING.md).

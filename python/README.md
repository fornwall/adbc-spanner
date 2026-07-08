# adbc-driver-spanner

[![PyPI version](https://img.shields.io/pypi/v/adbc-driver-spanner.svg)](https://pypi.org/project/adbc-driver-spanner/)
[![Python versions](https://img.shields.io/pypi/pyversions/adbc-driver-spanner.svg)](https://pypi.org/project/adbc-driver-spanner/)
[![Wheel](https://img.shields.io/pypi/wheel/adbc-driver-spanner.svg)](https://pypi.org/project/adbc-driver-spanner/#files)
[![License](https://img.shields.io/pypi/l/adbc-driver-spanner.svg)](https://github.com/fornwall/adbc-spanner/blob/main/LICENSE)
[![Build](https://github.com/fornwall/adbc-spanner/actions/workflows/libraries.yml/badge.svg)](https://github.com/fornwall/adbc-spanner/actions/workflows/libraries.yml)

A Python [ADBC](https://arrow.apache.org/adbc/) driver for **Google Cloud Spanner**.

Query Spanner through a standard [DBAPI 2.0](https://peps.python.org/pep-0249/) connection
and get results back as [Apache Arrow](https://arrow.apache.org/) — ready to hand straight to
pandas, polars, DuckDB, or PyArrow with no per-row Python conversion.

## Install

```sh
pip install adbc-driver-spanner

# For the DataFrame / Arrow helpers (fetch_df, fetch_arrow_table, adbc_ingest, …):
pip install "adbc-driver-spanner[dbapi]" pandas
```

The wheels ship a prebuilt native library, so there is nothing to compile. Prebuilt wheels are
published for Linux (x86-64, aarch64), macOS (arm64, x86-64), and Windows (x86-64, arm64) — see
[Supported platforms](#supported-platforms) for the minimum OS / glibc each one requires.

## Quickstart

```python
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        df = cur.fetch_df()          # -> pandas.DataFrame
```

`connect()` returns an ordinary DBAPI connection: use `cur.execute(...)` with `?`/`@name`
parameters, `cur.fetchone()` / `cur.fetchall()`, `conn.commit()`, and so on. The `fetch_*`
helpers below add zero-copy Arrow output on top.

## Authentication

By default the driver uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
(ADC) — the same credentials `gcloud auth application-default login` and Google Cloud runtimes
provide. To use a service-account key instead, pass its path or its JSON:

```python
# docs-test: skip
import adbc_driver_spanner.dbapi as spanner

spanner.connect(database="projects/p/instances/i/databases/d",
                keyfile="/path/to/service-account.json")
```

To impersonate another service account on top of your base credentials, set
`impersonate_target_principal=`:

```python
# docs-test: skip
spanner.connect(database="projects/p/instances/i/databases/d",
                impersonate_target_principal="target@p.iam.gserviceaccount.com",
                impersonate_scopes=["https://www.googleapis.com/auth/cloud-platform"])
```

To talk to the [Spanner emulator](https://cloud.google.com/spanner/docs/emulator), point at its
endpoint and pass `emulator=True` (which connects with anonymous credentials):

```python
# docs-test: skip
spanner.connect(database="projects/p/instances/i/databases/d",
                endpoint="localhost:9010", emulator=True)
```

## Connection options

`connect()` accepts these keyword arguments:

| kwarg                             | Description                                                                                     |
| --------------------------------- | ----------------------------------------------------------------------------------------------- |
| `database=`                       | Spanner database path, `projects/<p>/instances/<i>/databases/<d>` (**required**).                |
| `endpoint=`                       | Explicit gRPC endpoint (e.g. an emulator at `localhost:9010`); defaults to production Spanner.   |
| `emulator=`                       | `True` to connect with anonymous credentials for the emulator.                                  |
| `keyfile=`                        | Path to a service-account / credential JSON file (default: Application Default Credentials).     |
| `keyfile_json=`                   | The same credential JSON passed inline as a string instead of a file path.                      |
| `impersonate_target_principal=`   | Service account to impersonate on top of the base credentials.                                  |
| `impersonate_delegates=`          | Delegation chain for impersonation — a comma-separated string or a list of emails.              |
| `impersonate_scopes=`             | OAuth scopes for the impersonated token (comma-separated string or list; default cloud-platform). |
| `impersonate_lifetime=`           | Lifetime of the impersonated token, in seconds (default `3600`).                                |
| `autocommit=`                     | `False` (the DBAPI default) groups writes into a transaction; `True` applies each immediately — see [Transactions](#transactions). |

Less common settings are passed as raw option strings via `db_kwargs=` (database-level),
`conn_kwargs=` (connection-level), or per cursor with `conn.cursor(adbc_stmt_kwargs={...})`. The
complete, authoritative list — every option with its type, default, and behaviour — is in
[docs/options.md](https://github.com/fornwall/adbc-spanner/blob/main/docs/options.md). A few that
are handy from Python:

| Option                     | Level      | Description                                                                                   |
| -------------------------- | ---------- | --------------------------------------------------------------------------------------------- |
| `adbc.connection.readonly` | connection | `"true"` rejects all writes on the connection (see below); queries still run.                 |
| `spanner.read.staleness`   | conn/stmt  | Serve reads from a bounded-stale snapshot, e.g. `"max:10s"` or `"exact:5s"`, for lower latency. |
| `spanner.rows_per_batch`   | statement  | Rows per streamed Arrow batch (default `8192`); lower it to cap peak memory.                   |

### Read-only connections

Pass `conn_kwargs={"adbc.connection.readonly": "true"}` to guarantee a connection can only read —
any `INSERT`/`UPDATE`/`DELETE`, DDL, or bulk ingest raises, while queries still run:

```python
# docs-test: skip
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(database="projects/p/instances/i/databases/d",
                     conn_kwargs={"adbc.connection.readonly": "true"}) as conn:
    conn.cursor().execute("SELECT 1")   # ok
    # any INSERT/UPDATE/DELETE, DDL or adbc_ingest raises
```

### Smaller result batches

Results stream back as Arrow record batches. Lower `spanner.rows_per_batch` on the cursor to cap
peak memory on a wide or large result:

```python
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    with conn.cursor() as cur:
        cur.adbc_statement.set_options(**{"spanner.rows_per_batch": "1024"})
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        reader = cur.fetch_record_batch()    # batches of <= 1024 rows
        table = reader.read_all()
```

## Transactions

A DBAPI connection is **autocommit-off by default**, so writes are grouped into a transaction and
applied together when you call `conn.commit()` (`conn.rollback()` discards them). Two behaviours to
be aware of in this mode:

- **Reads don't see uncommitted writes.** A query always runs against the latest committed data, so
  an `INSERT` followed by a `SELECT COUNT(*)` in the same transaction returns the *pre-insert* count.
  Call `conn.commit()` first if a later statement must see earlier writes.
- **DDL applies immediately.** `CREATE` / `ALTER` / `DROP` are not transactional in Spanner and take
  effect as soon as they run, regardless of `commit()`.

Connect with `autocommit=True` if you want every statement to apply immediately.

```python
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
) as conn:  # DBAPI default: autocommit off => manual transaction
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Albums")  # DDL runs immediately
        cur.execute("CREATE TABLE Albums (Id INT64 NOT NULL) PRIMARY KEY (Id)")
        cur.execute("INSERT INTO Albums (Id) VALUES (1)")  # buffered, not applied yet
        cur.execute("SELECT COUNT(*) FROM Albums")
        assert cur.fetchone()[0] == 0  # pre-insert count: the INSERT is not visible yet
    conn.commit()  # the buffered INSERT is applied here, atomically
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM Albums")
        assert cur.fetchone()[0] == 1  # visible only after commit
```

## Working with DataFrames

Results come back as Apache Arrow, so they flow into the popular DataFrame libraries without a
per-row conversion. The DataFrame / Arrow paths need the `[dbapi]` extra (which pulls in PyArrow).
Remember that **writes need `conn.commit()`** unless you connect with `autocommit=True`.

All examples assume a `Singers(SingerId INT64, FirstName STRING)` table.

**pandas:**

```python
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers ORDER BY SingerId")
        df = cur.fetch_df()                  # -> pandas.DataFrame
```

**pyarrow — results as a native Arrow table:**

```python
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers ORDER BY SingerId")
        table = cur.fetch_arrow_table()      # -> pyarrow.Table
```

**polars — read straight from the connection:**

```python
import polars as pl
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
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

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        singers = cur.fetch_arrow_table()

# `singers` is a pyarrow.Table; DuckDB queries it by variable name, no copy.
top = duckdb.sql("SELECT COUNT(*) AS n, MIN(FirstName) AS first FROM singers").fetchone()
```

## Bulk insert a DataFrame

`cur.adbc_ingest(table, data, mode=...)` inserts an Arrow table (or anything Arrow-convertible, like
a pandas DataFrame) in bulk, without writing SQL:

```python
import pandas as pd
import pyarrow as pa
import adbc_driver_spanner.dbapi as spanner

frame = pd.DataFrame({"SingerId": [10, 11], "FirstName": ["Carol", "Dave"]})

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
    autocommit=True,                         # apply immediately; returns the row count
) as conn:
    with conn.cursor() as cur:
        # `append` (the default) inserts into an existing table.
        rows = cur.adbc_ingest("Singers", pa.Table.from_pandas(frame), mode="append")
```

The `mode` selects how the target table is handled:

- `append` — insert into an existing table (the default).
- `create` — create the table from the data's Arrow schema first, erroring if it already exists.
- `create_append` — create the table only if it is absent, then insert.
- `replace` — drop any existing table, recreate it from the schema, then insert.

Spanner requires a primary key on every table, but an ingested Arrow batch has none, so the three
create modes add a synthetic `adbc_ingest_key` column (a UUID string) as the primary key. It is not
part of your data, but it is a real column and will show up in `SELECT *`.

## Partitioned reads and Data Boost

A large scan can be split into independent partitions and read in parallel — optionally on Spanner's
serverless [Data Boost] compute, so the work is isolated from your provisioned instance. This uses
the ADBC partitioned-execution extension (`adbc_execute_partitions` / `adbc_read_partition`):

```python
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    with conn.cursor() as cur:
        # Optional statement options, set on the underlying ADBC statement:
        cur.adbc_statement.set_options(**{
            "spanner.data_boost_enabled": "true",  # run on Data Boost
            "spanner.max_partitions": "8",          # cap the partition count
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

Only single-table scans are partitionable — queries with an `ORDER BY` or aggregation are not.

[Data Boost]: https://cloud.google.com/spanner/docs/databoost/databoost-overview

## Supported platforms

Each wheel bundles a native library and carries a platform tag with a minimum-OS floor. `pip` picks
the matching wheel automatically:

| Platform       | Wheel tag                | Minimum requirement                          |
| -------------- | ------------------------ | -------------------------------------------- |
| Linux x86-64   | `manylinux_2_35_x86_64`  | glibc >= 2.35 (e.g. Ubuntu 22.04, Debian 12) |
| Linux aarch64  | `manylinux_2_35_aarch64` | glibc >= 2.35 (e.g. Ubuntu 22.04, Debian 12) |
| macOS arm64    | `macosx_11_0_arm64`      | macOS >= 11.0                                |
| macOS x86-64   | `macosx_10_15_x86_64`    | macOS >= 10.15                               |
| Windows x86-64 | `win_amd64`              | 64-bit Windows                               |
| Windows arm64  | `win_arm64`              | ARM64 Windows                                |

Any Python 3 works — the wheels are ABI-agnostic. On an older glibc or macOS than the floor above,
`pip` finds no matching wheel; build the native driver from source instead.

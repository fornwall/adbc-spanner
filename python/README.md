# adbc-driver-spanner

[![PyPI version](https://img.shields.io/pypi/v/adbc-driver-spanner.svg)](https://pypi.org/project/adbc-driver-spanner/)
[![Python versions](https://img.shields.io/pypi/pyversions/adbc-driver-spanner.svg)](https://pypi.org/project/adbc-driver-spanner/)
[![Wheel](https://img.shields.io/pypi/wheel/adbc-driver-spanner.svg)](https://pypi.org/project/adbc-driver-spanner/#files)
[![License](https://img.shields.io/pypi/l/adbc-driver-spanner.svg)](https://github.com/fornwall/adbc-spanner/blob/main/LICENSE)
[![Build](https://github.com/fornwall/adbc-spanner/actions/workflows/libraries.yml/badge.svg)](https://github.com/fornwall/adbc-spanner/actions/workflows/libraries.yml)

A Python [ADBC](https://arrow.apache.org/adbc/) driver for **Google Cloud Spanner**.

It bundles the prebuilt native driver (a Rust cdylib) and exposes it through
[`adbc_driver_manager`](https://pypi.org/project/adbc-driver-manager/), so you
get a standard DBAPI 2.0 connection whose results come back as Apache Arrow —
ready for pandas, polars, DuckDB, or PyArrow without a per-row Python conversion step.

## Install

```sh
pip install adbc-driver-spanner
# for the DataFrame / Arrow helpers used below:
pip install adbc-driver-spanner[dbapi] pandas
```

Prebuilt wheels are published for Linux (x86-64, aarch64), macOS (arm64,
x86-64), and Windows (x86-64, arm64).

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

## Connection options

Options mirror the driver's `spanner.*` keys:

| kwarg          | driver option               |
| -------------- | --------------------------- |
| `database=`    | `spanner.database`     |
| `endpoint=`    | `spanner.endpoint`     |
| `emulator=`    | `spanner.emulator`     |
| `keyfile=`     | `spanner.keyfile`      |
| `keyfile_json=`| `spanner.keyfile_json` |
| `impersonate_target_principal=` | `spanner.impersonate.target_principal` |
| `impersonate_delegates=`        | `spanner.impersonate.delegates`        |
| `impersonate_scopes=`           | `spanner.impersonate.scopes`           |
| `impersonate_lifetime=`         | `spanner.impersonate.lifetime`         |

Standard `adbc.connection.*` options are passed through `conn_kwargs=`. To open a
**read-only connection** — one that rejects DML, DDL and ingest (they raise, while
queries still run) — pass `conn_kwargs={"adbc.connection.readonly": "true"}`:

```python
# docs-test: skip
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(database="projects/p/instances/i/databases/d",
                     conn_kwargs={"adbc.connection.readonly": "true"}) as conn:
    conn.cursor().execute("SELECT 1")   # ok
    # any INSERT/UPDATE/DELETE, DDL or adbc_ingest raises
```

The flag is live: it is checked when a statement executes, so toggling
`adbc.connection.readonly` on an open connection immediately applies to already-open cursors
as well as new ones.

Credentials default to Application Default Credentials; pass `keyfile=` /
`keyfile_json=` for a service account, or point at the emulator:

```python
# docs-test: skip
spanner.connect(database="projects/p/instances/i/databases/d",
                endpoint="localhost:9010", emulator=True)
```

Set `impersonate_target_principal=` to authenticate as another service account on top
of the base credentials (mirrors the BigQuery driver's `impersonate.*` options).
`impersonate_delegates=` / `impersonate_scopes=` accept a comma-separated string or a
list; `impersonate_lifetime=` is the token lifetime in seconds (default `3600`):

```python
# docs-test: skip
spanner.connect(database="projects/p/instances/i/databases/d",
                impersonate_target_principal="target@p.iam.gserviceaccount.com",
                impersonate_scopes=["https://www.googleapis.com/auth/cloud-platform"])
```

## Option reference

The driver accepts options at three levels, each with its own place in the DBAPI:

- **Database options** — the `connect()` kwargs in the table above, or raw keys via
  `db_kwargs={...}`.
- **Connection options** — raw keys via `conn_kwargs={...}` (as in the read-only example above).
- **Statement options** — per cursor, via `conn.cursor(adbc_stmt_kwargs={...})` or
  `cur.adbc_statement.set_options(**{...})` (as in the partitioned-reads example below).

All values are passed as strings. Boolean options accept `"true"`/`"false"` (also
`"1"`/`"0"`/`"yes"`/`"no"`, case-insensitive); integer options accept a numeric string.

**Database options** (`connect()` kwargs or `db_kwargs=`):

| Option                                       | Value                             | Default                           | Description                       |
| -------------------------------------------- | --------------------------------- | --------------------------------- | --------------------------------- |
| `spanner.database`                           | `projects/<p>/instances/<i>/databases/<d>` | — (required)             | The Spanner database path (the `database=` kwarg). |
| `spanner.endpoint`                           | host or URL, e.g. `http://localhost:9010` | production Spanner        | Explicit gRPC endpoint (e.g. an emulator). |
| `spanner.emulator`                           | boolean                           | `false` (`true` when `SPANNER_EMULATOR_HOST` is set) | Connect with anonymous credentials (emulator mode). |
| `spanner.keyfile`                            | file path                         | unset (ADC)                       | Path to a service-account JSON key file. |
| `spanner.keyfile_json`                       | JSON string                       | unset (ADC)                       | Inline service-account JSON key; wins over `spanner.keyfile`. |
| `spanner.impersonate.target_principal`       | service-account email             | unset (no impersonation)          | Service account to impersonate; setting it enables impersonation on top of the base credentials. |
| `spanner.impersonate.delegates`              | comma-separated emails            | unset (no delegation)             | Delegation chain for impersonation. |
| `spanner.impersonate.scopes`                 | comma-separated scopes            | `cloud-platform`                  | OAuth scopes for the impersonated token. |
| `spanner.impersonate.lifetime`               | non-negative integer (seconds)    | `3600`                            | Lifetime of the impersonated token. |

**Connection options** (`conn_kwargs=`):

| Option                                       | Value                             | Default                           | Description                       |
| -------------------------------------------- | --------------------------------- | --------------------------------- | --------------------------------- |
| `adbc.connection.autocommit`                 | boolean                           | `false` in DBAPI (the `autocommit=` kwarg; the driver's own default is `true`) | Autocommit off buffers DML until `conn.commit()` — see the manual-transactions section below. |
| `adbc.connection.readonly`                   | boolean                           | `false`                           | Reject all writes on the connection — DML, DDL and ingest raise; queries still run. |
| `adbc.connection.transaction.isolation_level` | `adbc.connection.transaction.isolation.` + `default` / `serializable` / `repeatable_read` | `…isolation.default` | Isolation level for read/write transactions. The other spec levels are rejected as unsupported. |
| `spanner.read.staleness`                     | `exact:<duration>` or `max:<duration>` (`<duration>`: number + optional `s`/`ms`/`us`/`ns`/`m`/`h`, `s` default) | unset (strong read) | Stale-read bound for read-only queries; becomes the default for the connection's cursors. Mutually exclusive with `spanner.read.timestamp`; set to `""` to unset. |
| `spanner.read.timestamp`                     | RFC 3339 timestamp, optionally prefixed `read:` (exact, default) or `min:` (bounded) | unset (strong read) | Absolute read timestamp for read-only queries; same inheritance and mutual exclusion as above. |

**Statement options** (`conn.cursor(adbc_stmt_kwargs=...)` or `cur.adbc_statement.set_options(...)`):

| Option                                       | Value                             | Default                           | Description                       |
| -------------------------------------------- | --------------------------------- | --------------------------------- | --------------------------------- |
| `spanner.rows_per_batch`                     | positive integer                  | `8192`                            | Rows converted into each Arrow record batch as a result streams; larger batches trade memory for fewer conversions. |
| `spanner.data_boost_enabled`                 | boolean                           | `false`                           | Run partitioned reads on [Data Boost] (see below). |
| `spanner.max_partitions`                     | positive integer                  | unset (Spanner chooses)           | Hint for the maximum partition count from `adbc_execute_partitions`. |
| `spanner.read.staleness`                     | as the connection option          | inherited from the connection     | Per-cursor stale-read override. |
| `spanner.read.timestamp`                     | as the connection option          | inherited from the connection     | Per-cursor read-timestamp override. |
| `adbc.ingest.target_table`                   | table name                        | unset                             | Bulk-ingest target table — normally set for you by `cur.adbc_ingest(table, ...)`. |
| `adbc.ingest.target_db_schema`               | schema name                       | unset (default schema)            | Named schema qualifying the ingest target table (`""` selects Spanner's default, unnamed schema); `cur.adbc_ingest(..., db_schema_name=...)`. |
| `adbc.ingest.target_catalog`                 | `""` only                         | unset                             | Spanner has a single, unnamed catalog, so only the empty catalog is accepted. |
| `adbc.ingest.temporary`                      | `false` only                      | `false`                           | Spanner has no temporary tables; `false` is accepted as a no-op, `true` is rejected. |
| `adbc.ingest.mode`                           | `append` / `create` / `create_append` / `replace` | `append`         | Bulk-ingest mode — normally set for you by `cur.adbc_ingest(..., mode=...)`; see the ingest section below. |

For example, to stream a large result in smaller Arrow batches:

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

## Manual transactions: no read-your-writes

DBAPI connections are **autocommit-off by default**, which puts the driver in manual
transaction mode. Spanner's Rust client only exposes read/write transactions through a
closure-based runner (there is no begin/commit handle yet), so the driver implements a manual
transaction by **buffering** DML and applying the whole batch atomically in a single
read/write transaction on `conn.commit()` (`conn.rollback()` discards it). Two consequences
to be aware of:

- **No read-your-writes.** Queries always run immediately in a fresh read-only snapshot, so a
  query inside an open transaction does not see the DML buffered before it — an `INSERT`
  followed by a `SELECT COUNT(*)` returns the *pre-insert* count.
- **DML and DDL reorder.** DDL (`CREATE`/`ALTER`/…) also runs immediately (Spanner DDL is
  never transactional), so DDL issued after buffered DML actually executes before it.

```python
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
) as conn:  # DBAPI default: autocommit off => manual transaction
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Albums")  # DDL runs immediately
        cur.execute("CREATE TABLE Albums (Id INT64 NOT NULL) PRIMARY KEY (Id)")
        cur.execute("INSERT INTO Albums (Id) VALUES (1)")  # DML: buffered, not applied
        cur.execute("SELECT COUNT(*) FROM Albums")
        assert cur.fetchone()[0] == 0  # pre-insert count: the INSERT is not visible yet
    conn.commit()  # the buffered INSERT is applied here, atomically
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM Albums")
        assert cur.fetchone()[0] == 1  # visible only after commit
```

If a statement needs to see earlier writes, `conn.commit()` first — or connect with
`autocommit=True`, where every DML statement applies immediately. This will be fixed properly
once the client exposes begin/commit transaction handles.

## Cookbook

Every snippet below is executed against the Spanner emulator in CI, so they stay
correct. They assume a `Singers(SingerId INT64, FirstName STRING)` table.

Two things to know:

- **DBAPI is autocommit-off by default**, so **DML and ingest need a
  `conn.commit()`** (or pass `autocommit=True`). Reads need neither — but note the
  manual-transaction caveats above: buffered DML is invisible to queries until commit.
- The DataFrame / Arrow paths need the `[dbapi]` extra (pyarrow).

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

**Insert a DataFrame (bulk ingest):**

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

Four ingest modes are supported, matching the ADBC `mode` values:

- `append` — insert into an existing table (the default).
- `create` — build the table from the Arrow schema first, erroring if it already exists.
- `create_append` — build the table only if it is absent, then insert.
- `replace` — drop any existing table, recreate it from the Arrow schema, then insert.

Spanner requires every table to have a primary key, but an ingested Arrow batch has none, so the
three create modes (`create`/`create_append`/`replace`) add a synthetic `adbc_ingest_key`
`STRING(36)` column (defaulted to `GENERATE_UUID()`) as the primary key. It is not part of your
data, but it is a real column, so it shows up in `SELECT *` on the created table.

## Partitioned reads and Data Boost

A large scan can be split into independent partitions and read in parallel —
optionally on Spanner's serverless [Data Boost] compute, so the work is isolated
from your provisioned instance. This uses the ADBC partitioned-execution
extension (`adbc_execute_partitions` / `adbc_read_partition`):

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

The Data Boost choice is baked into each descriptor, so it is honoured wherever
the partition is read. Only single-table scans are partitionable — queries with
an `ORDER BY` or aggregation are not.

[Data Boost]: https://cloud.google.com/spanner/docs/databoost/databoost-overview

## How this package is built

The wheel is **data-only**: it does not compile anything at install time and
links nothing against Python. CI (`.github/workflows/libraries.yml`) builds the
native library per platform, drops it into `adbc_driver_spanner/`, and packages
a `py3-none-<platform>` wheel. See that workflow for the release wiring.

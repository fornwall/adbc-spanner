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

## Cookbook

Every snippet below is executed against the Spanner emulator in CI, so they stay
correct. They assume a `Singers(SingerId INT64, FirstName STRING)` table.

Two things to know:

- **DBAPI is autocommit-off by default**, so **DML and ingest need a
  `conn.commit()`** (or pass `autocommit=True`). Reads need neither.
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
        # The target table must already exist — only append mode is supported.
        rows = cur.adbc_ingest("Singers", pa.Table.from_pandas(frame), mode="append")
```

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

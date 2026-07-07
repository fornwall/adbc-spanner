# adbc-driver-spanner

A Python [ADBC](https://arrow.apache.org/adbc/) driver for **Google Cloud Spanner**.

It bundles the prebuilt native driver (a Rust cdylib) and exposes it through
[`adbc_driver_manager`](https://pypi.org/project/adbc-driver-manager/), so you
get a standard DBAPI 2.0 connection whose results come back as Apache Arrow —
ready for pandas, polars, DuckDB, or PyArrow with no per-row conversion.

## Install

```sh
pip install adbc-driver-spanner
# for the DataFrame helpers used below:
pip install adbc-driver-spanner[dbapi] pandas
```

Prebuilt wheels are published for Linux (x86-64, aarch64), macOS (arm64,
x86-64), and Windows (x86-64, arm64).

## Usage

```python
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(
    database="projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        df = cur.fetch_df()          # -> pandas.DataFrame
        table = cur.fetch_arrow_table()  # -> pyarrow.Table

    conn.commit()  # DBAPI is autocommit-off; commit applies buffered DML
```

Connection options mirror the driver's `adbc.spanner.*` keys:

| kwarg          | driver option               |
| -------------- | --------------------------- |
| `database=`    | `adbc.spanner.database`     |
| `endpoint=`    | `adbc.spanner.endpoint`     |
| `emulator=`    | `adbc.spanner.emulator`     |
| `keyfile=`     | `adbc.spanner.keyfile`      |
| `keyfile_json=`| `adbc.spanner.keyfile_json` |

Credentials default to Application Default Credentials; pass `keyfile=` /
`keyfile_json=` for a service account, or point at the emulator:

```python
spanner.connect(database="projects/p/instances/i/databases/d",
                endpoint="localhost:9010", emulator=True)
```

## Partitioned reads and Data Boost

A large scan can be split into independent partitions and read in parallel —
optionally on Spanner's serverless [Data Boost] compute, so the work is isolated
from your provisioned instance. This uses the ADBC partitioned-execution
extension (`adbc_execute_partitions` / `adbc_read_partition`):

```python
import adbc_driver_spanner.dbapi as spanner

with spanner.connect(database="projects/p/instances/i/databases/d") as conn:
    with conn.cursor() as cur:
        # Optional statement options, set on the underlying ADBC statement:
        cur.adbc_statement.set_options(**{
            "adbc.spanner.data_boost_enabled": "true",  # run on Data Boost
            "adbc.spanner.max_partitions": "8",          # cap the partition count
        })
        partitions, schema = cur.adbc_execute_partitions("SELECT * FROM Singers")

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

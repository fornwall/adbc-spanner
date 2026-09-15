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

# For the DataFrame / Arrow helpers (fetch_df, fetch_arrow_table, adbc_ingest, …).
# The `dbapi` extra pulls in PyArrow and pandas:
pip install "adbc-driver-spanner[dbapi]"
```

The wheels ship a prebuilt native library, so there is nothing to compile. Prebuilt wheels are
published for Linux (x86-64 glibc + aarch64 glibc, plus x86-64 and aarch64 musl for Alpine), macOS
(arm64, x86-64), and Windows (x86-64, arm64) — see [Supported platforms](#supported-platforms) for
the minimum OS / libc each one requires.

## Quickstart

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

`connect()` returns an ordinary DBAPI connection: use `cur.execute(...)` with GoogleSQL's `@name`
parameters (there is no `?` placeholder in GoogleSQL), `cur.fetchone()` / `cur.fetchall()`, `conn.commit()`, and so on. The `fetch_*`
helpers below add zero-copy Arrow output on top.

## Driver manifest (`driver="spanner"`)

`adbc_driver_spanner.dbapi.connect()` above needs no setup — it hands the bundled library's path
straight to the driver manager. If you would rather go through the generic
[`adbc_driver_manager`][adbc-dm] and name the driver (the way the PostgreSQL and SQLite drivers
work), install an [ADBC *driver manifest*][manifests]:

```sh
python -m adbc_driver_spanner.manifest install
# equivalently, the console script installed by the wheel:
adbc-driver-spanner-install-manifest
```

That writes a `spanner.toml` manifest pointing at this wheel's bundled library into a directory the
driver manager searches (`python -m adbc_driver_spanner.manifest path` prints where). Afterwards
both of these work:

```python docs-test: skip
import adbc_driver_manager.dbapi

# By driver name.
with adbc_driver_manager.dbapi.connect(
    driver="spanner",
    uri="spanner:///projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    ...

# By URI alone: with no `driver` option, the driver manager takes the URI *scheme*
# as the driver name — and this driver's scheme is already `spanner`.
with adbc_driver_manager.dbapi.connect(
    uri="spanner:///projects/my-project/instances/my-instance/databases/my-db",
) as conn:
    ...
```

Notes:

- **Re-run it after upgrading, reinstalling, or moving the environment.** A manifest records the
  *absolute* path of the shared library, which the driver manager passes to the dynamic loader
  verbatim (it is not resolved relative to the manifest). That is also why the manifest cannot just
  be shipped inside the wheel: the path is only known once the wheel is installed.
- Inside a virtual environment the default target is `$VIRTUAL_ENV/etc/adbc/drivers/spanner.toml`,
  which the Python driver manager adds to its search path automatically, so the manifest stays
  scoped to that environment. Outside a venv it goes to the user config directory
  (`~/.config/adbc/drivers` on Linux, `~/Library/Application Support/ADBC/Drivers` on macOS,
  `%LOCALAPPDATA%\ADBC\Drivers` on Windows).
- Use `--dir` to install somewhere else, for example a directory on `ADBC_DRIVER_PATH`:
  `python -m adbc_driver_spanner.manifest install --dir /etc/adbc/drivers`.
- Users of the standalone shared library (the GitHub release archives, not the wheel) can start from
  the [`spanner.toml`][manifest-file] in the repository and edit its `Driver.shared` paths.

[adbc-dm]: https://pypi.org/project/adbc-driver-manager/
[manifests]: https://arrow.apache.org/adbc/current/format/driver_manifests.html
[manifest-file]: https://github.com/fornwall/adbc-spanner/blob/main/spanner.toml

## Authentication

The driver supports several credential sources. When you set *no* credential option it falls back to
Application Default Credentials, so **ADC is the default** — most setups need no credential option
at all.

**[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
(ADC)** — the default. Connect with only the URI and the driver picks up whatever ADC resolves in
this environment:

- `gcloud auth application-default login` for local development,
- a service-account key at the path in the `GOOGLE_APPLICATION_CREDENTIALS` environment variable, or
- the attached service account automatically, on a Google Cloud runtime (GCE, GKE, Cloud Run, Cloud
  Functions) via the metadata server.

```python docs-test: skip
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

# No credential option -> Application Default Credentials.
spanner.connect(db_kwargs={
    DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
})
```

There is no flag to "enable" ADC: you select it by leaving every credential option
(`DatabaseOptions.KEYFILE` / `KEYFILE_JSON` / `ACCESS_TOKEN` / `IMPERSONATE_TARGET_PRINCIPAL`) unset.
Setting any of the options below overrides it. (Ambient ADC does *not* conflict with emulator mode —
only an explicit credential option does; see the emulator note below.)

**Service-account key** — to use a key instead of ADC, pass its path or its JSON as a raw option in
`db_kwargs`:

```python docs-test: skip
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

spanner.connect(db_kwargs={
    DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
    DatabaseOptions.KEYFILE.value: "/path/to/service-account.json",
})
```

**Impersonation** — to impersonate another service account on top of your base credentials, set
`DatabaseOptions.IMPERSONATE_TARGET_PRINCIPAL`:

```python docs-test: skip
spanner.connect(db_kwargs={
    DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
    DatabaseOptions.IMPERSONATE_TARGET_PRINCIPAL.value: "target@p.iam.gserviceaccount.com",
    DatabaseOptions.IMPERSONATE_SCOPES.value: "https://www.googleapis.com/auth/cloud-platform",
})
```

**OAuth access token** — set `DatabaseOptions.ACCESS_TOKEN` to authenticate with an OAuth 2.0 bearer
token you already hold (for example from `gcloud auth print-access-token`). It is sent verbatim with no refresh, and is
mutually exclusive with `DatabaseOptions.KEYFILE` / `DatabaseOptions.KEYFILE_JSON` /
`DatabaseOptions.IMPERSONATE_TARGET_PRINCIPAL`:

```python docs-test: skip
spanner.connect(db_kwargs={
    DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
    DatabaseOptions.ACCESS_TOKEN.value: "ya29.a0Af...",
})
```

**Emulator** — to talk to the [Spanner emulator](https://cloud.google.com/spanner/docs/emulator),
point at its endpoint and set `DatabaseOptions.EMULATOR` to `"true"` (which connects with anonymous
credentials; combining it with an explicit credential option above is refused, but ambient ADC is
fine):

```python docs-test: skip
spanner.connect(db_kwargs={
    DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
    DatabaseOptions.ENDPOINT.value: "localhost:9010",
    DatabaseOptions.EMULATOR.value: "true",
})
```

## Options

`connect()` takes three keyword arguments, and every other driver setting travels as an option key:

| kwarg          | Description                                                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `db_kwargs=`   | Database-level options (credentials, emulator, endpoint, …). A `uri` is required; everything else is optional.                              |
| `conn_kwargs=` | Connection-level options (`adbc.connection.*` / `spanner.*`).                                                                               |
| `autocommit=`  | `False` (the DBAPI default) groups statements into manual transactions; `True` applies each immediately — see [Transactions](#transactions). |

Statement-level options go per cursor, either as `conn.cursor(adbc_stmt_kwargs={...})` or as
`cur.adbc_statement.set_options(...)`.

**Every option in
[docs/options.md](https://github.com/fornwall/adbc-spanner/blob/main/docs/options.md) works here** —
that page is the authoritative reference for each one's type, default, allowed values and
round-trip behaviour. The `DatabaseOptions`, `ConnectionOptions` and `StatementOptions` enums in
`adbc_driver_spanner` mirror those keys one for one: each member's `.value` *is* the raw key
(`DatabaseOptions.KEYFILE.value == "spanner.auth.keyfile"`). Naming options through the enums is the
recommended style — for typo-safety and discoverability, the same convention as the BigQuery ADBC
driver — but a raw string works anywhere an enum value does.

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import ConnectionOptions, DatabaseOptions, StatementOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d"},
    conn_kwargs={ConnectionOptions.READ_STALENESS.value: "max:10s"},
    autocommit=True,  # one-shot reads: bounded staleness lets Spanner pick the freshest replica
) as conn:
    cur = conn.cursor(
        adbc_stmt_kwargs={StatementOptions.ROWS_PER_BATCH.value: "1024"}
    )
    cur.execute("SELECT * FROM Singers")
```

(In the default manual-transaction mode, queries share one multi-use read-only transaction — see
[Transactions](#transactions) — and Spanner accepts the bounded-staleness kinds only on single-use
reads, so a `max:<d>`/`min:<t>` bound is pinned there to its most-stale legal equivalent: exact
staleness `<d>` / read timestamp `<t>`.)

**Read-only connections.** `conn_kwargs={ConnectionOptions.READONLY.value: "true"}` guarantees a
connection can only read: any `INSERT`/`UPDATE`/`DELETE`, DDL or `adbc_ingest` raises, and so does
`conn.commit()` if DML was buffered before the flag went on — the transaction stays open and
replayable, and `conn.rollback()` is never gated. Queries still run.

## Transactions

A DBAPI connection is **autocommit-off by default**, so statements run in manual transactions
ended by `conn.commit()` (or discarded by `conn.rollback()`). A manual transaction is exactly one
kind of work — **queries or DML** — fixed by its *first* statement; a statement of the other kind
raises `adbc_driver_manager.ProgrammingError` (ADBC `InvalidState`) until you commit or roll back.
Queries in such a transaction share one consistent snapshot and ending it costs no round-trip; DML
is **buffered** and applied atomically on `conn.commit()`, so there are no read-your-writes. **DDL
is not transaction-aware**: `CREATE`/`ALTER`/`DROP` always apply immediately, `rollback()` cannot
undo them, and DDL issued after buffered DML executes *before* it.

Connect with `autocommit=True` if you want every statement to apply immediately. The full model is
in
[docs/transactions.md](https://github.com/fornwall/adbc-spanner/blob/main/docs/transactions.md).

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_manager import ProgrammingError
from adbc_driver_spanner import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:  # DBAPI default: autocommit off => manual transactions
    with conn.cursor() as cur:
        # DDL applies immediately — no commit needed, and rollback cannot undo it.
        cur.execute("DROP TABLE IF EXISTS Albums")
        cur.execute("CREATE TABLE Albums (Id INT64 NOT NULL) PRIMARY KEY (Id)")

        cur.execute("INSERT INTO Albums (Id) VALUES (1)")  # a DML transaction: buffered
        # Querying while the INSERT is buffered is rejected (no read-your-writes) instead of
        # silently returning a stale count.
        try:
            cur.execute("SELECT COUNT(*) FROM Albums")
            raise AssertionError("expected the guarded query to raise")
        except ProgrammingError:
            pass  # commit (or roll back) first to see the write
    conn.commit()  # the buffered INSERT is applied here, atomically

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM Albums")  # a query transaction: pins a snapshot
        assert cur.fetchone()[0] == 1  # visible only after the DML commit
    conn.rollback()  # ends the query transaction (its snapshot) without a round-trip
```

## Working with DataFrames

Results come back as Apache Arrow, so they flow into the popular DataFrame libraries without a
per-row conversion. The DataFrame / Arrow paths need the `[dbapi]` extra (which pulls in PyArrow).
Remember that **writes need `conn.commit()`** unless you connect with `autocommit=True`.

All examples assume a `Singers(SingerId INT64, FirstName STRING)` table.

**pandas:**

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT SingerId, FirstName FROM Singers ORDER BY SingerId")
        df = cur.fetch_df()                  # -> pandas.DataFrame
```

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

An ingested Arrow batch carries no primary key, so the three create modes declare none: Spanner
keys such a table on a [hidden `rowid` column][no-pk] of its own, which no `SELECT *` returns. The
created table therefore holds exactly the columns you ingested. A primary key fixes Spanner's
physical row layout, so if you want one, create the table yourself with `CREATE TABLE … PRIMARY KEY
(…)` and ingest with `mode="append"`.

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

Only single-table scans are partitionable — queries with an `ORDER BY` or aggregation are not.

A descriptor is opaque but *executable*: it carries the SQL text plus the session and transaction
identity, so `adbc_read_partition` runs whatever it contains with the connection's credentials, and
it is not authenticated. Ship descriptors only over trusted channels, and never read one from an
untrusted source.

[Data Boost]: https://cloud.google.com/spanner/docs/databoost/databoost-overview

## Supported platforms

Each wheel bundles a native library and carries a platform tag with a minimum-OS floor. `pip` picks
the matching wheel automatically:

| Platform       | Wheel tag                | Minimum requirement                          |
| -------------- | ------------------------ | -------------------------------------------- |
| Linux x86-64   | `manylinux_2_35_x86_64`  | glibc >= 2.35 (e.g. Ubuntu 22.04, Debian 12) |
| Linux aarch64  | `manylinux_2_35_aarch64` | glibc >= 2.35 (e.g. Ubuntu 22.04, Debian 12) |
| Linux x86-64 musl | `musllinux_1_2_x86_64` | musl libc >= 1.2 (e.g. Alpine 3.13+)         |
| Linux aarch64 musl | `musllinux_1_2_aarch64` | musl libc >= 1.2 (e.g. Alpine 3.13+)      |
| macOS arm64    | `macosx_11_0_arm64`      | macOS >= 11.0                                |
| macOS x86-64   | `macosx_10_15_x86_64`    | macOS >= 10.15                               |
| Windows x86-64 | `win_amd64`              | 64-bit Windows                               |
| Windows arm64  | `win_arm64`              | ARM64 Windows                                |

Any Python 3 works — the wheels are ABI-agnostic. On an older glibc or macOS than the floor above,
`pip` finds no matching wheel; build the native driver from source instead.

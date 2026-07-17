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

`connect()` returns an ordinary DBAPI connection: use `cur.execute(...)` with `?`/`@name`
parameters, `cur.fetchone()` / `cur.fetchall()`, `conn.commit()`, and so on. The `fetch_*`
helpers below add zero-copy Arrow output on top.

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

```python
# docs-test: skip
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

```python
# docs-test: skip
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions

spanner.connect(db_kwargs={
    DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
    DatabaseOptions.KEYFILE.value: "/path/to/service-account.json",
})
```

**Impersonation** — to impersonate another service account on top of your base credentials, set
`DatabaseOptions.IMPERSONATE_TARGET_PRINCIPAL`:

```python
# docs-test: skip
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

```python
# docs-test: skip
spanner.connect(db_kwargs={
    DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
    DatabaseOptions.ACCESS_TOKEN.value: "ya29.a0Af...",
})
```

**Emulator** — to talk to the [Spanner emulator](https://cloud.google.com/spanner/docs/emulator),
point at its endpoint and set `DatabaseOptions.EMULATOR` to `"true"` (which connects with anonymous
credentials; combining it with an explicit credential option above is refused, but ambient ADC is
fine):

```python
# docs-test: skip
spanner.connect(db_kwargs={
    DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d",
    DatabaseOptions.ENDPOINT.value: "localhost:9010",
    DatabaseOptions.EMULATOR.value: "true",
})
```

## Connection options

`connect()` takes just three keyword arguments — every driver setting travels as an option key,
best spelled with the `DatabaseOptions` / `ConnectionOptions` / `StatementOptions` constants (see
[Typed option keys](#typed-option-keys)):

| kwarg          | Description                                                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `db_kwargs=`   | Database-level options, keyed with the `DatabaseOptions` constants (credentials, emulator, endpoint, …). See the table below.               |
| `conn_kwargs=` | Connection-level options, keyed with the `ConnectionOptions` constants (`adbc.connection.*` / `spanner.*`), e.g. `ConnectionOptions.READONLY`. |
| `autocommit=`  | `False` (the DBAPI default) groups statements into manual transactions (queries or DML — one kind each; DDL always applies immediately); `True` applies each immediately — see [Transactions](#transactions). |

A database URI is required; everything else is optional. The database-level credential and
endpoint options are:

| `DatabaseOptions` member       | Raw key                                       | Description                                                                                     |
| ------------------------------ | --------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `URI`                          | `uri`                                         | A `spanner://` connection URI whose path is the database path, e.g. `spanner:///projects/<p>/instances/<i>/databases/<d>` (**required**). The scheme is required; a bare path is rejected. Query parameters may name database options, but **not** the secret-holding `KEYFILE_JSON` / `ACCESS_TOKEN` (URIs get logged). |
| `ENDPOINT`                     | `spanner.endpoint`                            | Explicit gRPC endpoint (e.g. an emulator at `localhost:9010`); defaults to production Spanner.   |
| `EMULATOR`                     | `spanner.emulator`                            | `"true"` to connect with anonymous credentials for the emulator.                                |
| `KEYFILE`                      | `spanner.auth.keyfile`                        | Path to a service-account / credential JSON file (default: Application Default Credentials).     |
| `KEYFILE_JSON`                 | `spanner.auth.keyfile_json`                   | The same credential JSON passed inline as a string instead of a file path. Write-only: never readable back via `get_option`, and not accepted as a `URI` query parameter — pass it here instead. |
| `ACCESS_TOKEN`                 | `spanner.auth.access_token`                   | OAuth 2.0 bearer token sent verbatim (no refresh); mutually exclusive with the keyfile / impersonation options. Write-only: never readable back via `get_option`, and not accepted as a `URI` query parameter — pass it here instead. |
| `IMPERSONATE_TARGET_PRINCIPAL` | `spanner.auth.impersonate.target_principal`   | Service account to impersonate on top of the base credentials.                                  |
| `IMPERSONATE_DELEGATES`        | `spanner.auth.impersonate.delegates`          | Delegation chain for impersonation — a comma-separated string of emails.                         |
| `IMPERSONATE_SCOPES`           | `spanner.auth.impersonate.scopes`             | OAuth scopes for the impersonated token (comma-separated; default cloud-platform).              |
| `IMPERSONATE_LIFETIME`         | `spanner.auth.impersonate.lifetime`           | Lifetime of the impersonated token, in seconds (default `3600`).                                |

Every other setting is passed the same way — via `db_kwargs=` (database-level), `conn_kwargs=`
(connection-level), or per cursor with `conn.cursor(adbc_stmt_kwargs={...})`. The complete,
authoritative list — every option with its type, default, and behaviour — is in
[docs/options.md](https://github.com/fornwall/adbc-spanner/blob/main/docs/options.md). A few that
are handy from Python:

| `ConnectionOptions` / `StatementOptions` member | Raw key                    | Level      | Description                                                                                   |
| ----------------------------------------------- | -------------------------- | ---------- | --------------------------------------------------------------------------------------------- |
| `ConnectionOptions.READONLY`                    | `adbc.connection.readonly` | connection | `"true"` rejects all writes on the connection (see below); queries still run.                 |
| `READ_STALENESS`                                | `spanner.read.staleness`   | conn/stmt  | Serve reads from a bounded-stale snapshot, e.g. `"max:10s"` or `"exact:5s"`, for lower latency. |
| `DIRECTED_READ`                                 | `spanner.directed_read`    | conn/stmt  | Steer read-only queries to specific replicas, e.g. `"include:us-east1:read_only"` or `"exclude:us-central1"`. |
| `MAX_COMMIT_DELAY`                              | `spanner.commit.max_delay` | conn/stmt  | Max delay Spanner may add to a read/write commit to batch it with others, e.g. `"100ms"` (a duration in `0..=500ms`) — trades a little latency for throughput. |
| `COMMIT_STATS`                                  | `spanner.commit_stats`     | conn/stmt  | `"true"` requests commit statistics on read/write commits; read the mutation count of the most recent commit back with `get_option_int("spanner.commit_stats.mutation_count")` (on the statement for autocommit DML / bulk ingest, on the connection for a manual-mode commit). |
| `QUERY_OPTIMIZER_VERSION`                       | `spanner.query.optimizer_version` | conn/stmt | Pin the query optimizer version, e.g. `"6"` or `"latest"` (also `QUERY_OPTIMIZER_STATISTICS_PACKAGE`). |
| `StatementOptions.ROWS_PER_BATCH`               | `spanner.rows_per_batch`   | statement  | Rows per streamed Arrow batch (default `8192`); lower it to cap peak memory.                   |

### Typed option keys

The `DatabaseOptions`, `ConnectionOptions`, and `StatementOptions` enums (each member's `.value` is
the raw key) are the recommended way to name options — for typo-safety and discoverability, the same
style as the BigQuery ADBC driver's `DatabaseOptions`:

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

The enums cover the full option surface for the `db_kwargs=` / `conn_kwargs=` /
`adbc_stmt_kwargs=` escape hatches. Every key is documented in
[docs/options.md](https://github.com/fornwall/adbc-spanner/blob/main/docs/options.md).

### Read-only connections

Pass `conn_kwargs={ConnectionOptions.READONLY.value: "true"}` to guarantee a connection can only
read — any `INSERT`/`UPDATE`/`DELETE`, DDL, or bulk ingest raises, while queries still run:

```python
# docs-test: skip
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import ConnectionOptions, DatabaseOptions

with spanner.connect(db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d"},
                     conn_kwargs={ConnectionOptions.READONLY.value: "true"}) as conn:
    conn.cursor().execute("SELECT 1")   # ok
    # any INSERT/UPDATE/DELETE, DDL or adbc_ingest raises
```

The guarantee also covers `conn.commit()`: in the default manual-transaction mode DML buffers
until commit (see [Transactions](#transactions)), so a connection turned read-only *after* some
DML was buffered raises `ProgrammingError` on `conn.commit()` — and on switching the connection to
autocommit, which commits pending work — instead of writing. The transaction stays open and
replayable: clear
the flag and commit again to apply it, or `conn.rollback()` (never gated — discarding buffered work
writes nothing) to discard it. Committing a query transaction is likewise always allowed.

### Smaller result batches

Results stream back as Arrow record batches. Lower `spanner.rows_per_batch` on the cursor to cap
peak memory on a wide or large result:

```python
import adbc_driver_spanner.dbapi as spanner
from adbc_driver_spanner import DatabaseOptions, StatementOptions

with spanner.connect(
    db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/my-project/instances/my-instance/databases/my-db"},
) as conn:
    with conn.cursor() as cur:
        cur.adbc_statement.set_options(**{StatementOptions.ROWS_PER_BATCH.value: "1024"})
        cur.execute("SELECT SingerId, FirstName FROM Singers")
        reader = cur.fetch_record_batch()    # batches of <= 1024 rows
        table = reader.read_all()
```

## Transactions

A DBAPI connection is **autocommit-off by default**, so statements run in manual transactions
ended by `conn.commit()` (or discarded by `conn.rollback()`). A manual transaction is exactly one
kind of work — **queries or DML** — fixed by its *first* statement; a statement of the other kind
raises `adbc_driver_manager.ProgrammingError` (ADBC `InvalidState`) until you commit or roll back:

- **Queries share one snapshot.** The first query opens a Spanner multi-use read-only
  transaction, and every query until `commit()`/`rollback()` reads from that same consistent
  snapshot — rows committed by others in the meantime stay invisible. Ending a query transaction
  is free (Spanner read-only transactions need no commit RPC), so commit or roll back as soon as
  you no longer need the snapshot.
- **DML is buffered — no read-your-writes.** `INSERT`/`UPDATE`/`DELETE` (and bulk ingest) buffer
  and apply atomically on `conn.commit()`. A query inside a DML transaction could not see the
  buffered writes, so it is rejected rather than silently returning a stale (*pre-insert*)
  result.
- **DDL is not transaction-aware.** `CREATE`/`ALTER`/`DROP` always apply **immediately** (Spanner
  DDL runs through the admin API and is never transactional — the same no-special-handling
  approach as the ADBC BigQuery driver), regardless of the transaction: `commit()` is not needed
  and `rollback()` cannot undo them, and DDL issued after buffered DML executes *before* it. A
  `;`-separated DDL batch still applies as one `UpdateDatabaseDdl` call.

Connect with `autocommit=True` if you want every statement to apply immediately.

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

Spanner requires a primary key on every table, but an ingested Arrow batch has none, so the three
create modes add a synthetic `adbc_ingest_key` column (a UUID string) as the primary key. It is not
part of your data, but it is a real column and will show up in `SELECT *`.

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

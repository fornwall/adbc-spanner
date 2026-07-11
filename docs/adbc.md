# A step-by-step guide to the Spanner ADBC driver

This document explains what `adbc-spanner` is and how it works, starting from first principles.
It is written for someone who has **not** used Apache Arrow or ADBC before. It focuses on the big
picture — what the pieces are, how they fit together, and how the standard ADBC interface is
implemented on top of Google Cloud Spanner — rather than on the fine details of the driver's
internals.

If you already know ADBC and just want the exhaustive option list, jump to
[docs/options.md](options.md). If you want to run the driver, see the [README](../README.md).

---

## 1. The problem this solves

Say you have a program and you want it to run SQL against a database and get the results back.
There are two long-standing ways to do this:

- **ODBC / JDBC** — old, ubiquitous standards. They hand results back **one row at a time**, as
  loosely-typed cells. For analytics workloads (millions of rows, column-at-a-time processing) this
  row-by-row copying is slow and wasteful.
- **A database-specific client library** — fast, but now your program is welded to one database's
  API. Switch databases and you rewrite everything.

**ADBC (Arrow Database Connectivity)** is a newer standard that fixes both problems:

1. It is a **single, database-agnostic API**. Your program talks to "an ADBC driver"; swapping
   Spanner for DuckDB or BigQuery means swapping the driver, not your code.
2. Results come back as **Apache Arrow** data, not one row at a time.

`adbc-spanner` is the ADBC **driver for Google Cloud Spanner**: the adapter that makes Spanner
look like a generic ADBC database to any ADBC-speaking program.

### What is Apache Arrow?

Arrow is a standard **in-memory layout for tabular data**. Instead of storing a table row by row,
Arrow stores it **column by column**: all the values of column A contiguously, then all of column
B, and so on. This "columnar" layout is what analytics engines want — it is cache-friendly, it
vectorizes well, and, crucially, it is the *same* layout everywhere. Two programs that both speak
Arrow can share a table with **zero conversion or copying**.

The key Arrow term you will see throughout this driver is the **`RecordBatch`**: a chunk of a
table — a set of columns of equal length, with a **schema** (the column names and types) attached.
A query result is delivered as a **stream of `RecordBatch`es** (`RecordBatchReader`): you pull one
batch, process it, pull the next. This is why a huge result set never has to fit in memory all at
once — the driver converts Spanner rows to Arrow batches on demand as you iterate.

---

## 2. The ADBC object model

ADBC is not one flat API; it is a small hierarchy of four objects. You create them top-down, and
each one is progressively more specific. This driver names them `SpannerX`, one Rust module each:

```
SpannerDriver  ──▶  SpannerDatabase  ──▶  SpannerConnection  ──▶  SpannerStatement
   (the loaded        (which database,        (one session /          (one query /
    driver itself)     credentials, etc.)      transaction scope)       statement to run)
```

| Object | What it represents | You typically have… |
| --- | --- | --- |
| **Driver** | The loaded driver code itself — the entrypoint. | one |
| **Database** | *Configuration*: which Spanner database, which credentials, which endpoint. No connection is opened yet — it only holds options. | one per target database |
| **Connection** | A live handle you run work against. Owns transaction state (autocommit vs. manual) and the metadata-introspection calls. | one per thread / unit of work |
| **Statement** | A single SQL statement (or bulk-ingest operation) to configure and execute. | many, short-lived |

The rule of thumb: **options set higher up become defaults lower down.** A staleness bound set on
the connection is inherited by every statement it creates; the statement can then override it.
Configuration flows down the hierarchy.

In Rust, each of these is a *trait* defined by the `adbc_core` crate (`Driver`, `Database`,
`Connection`, `Statement`). This driver's job is to **implement those four traits** for Spanner.
That is, at heart, the whole driver: four Rust structs implementing four standard traits.

---

## 3. The entrypoint: how the shared library gets loaded

The driver can be used two ways, and it is worth understanding both because they explain the shape
of the code.

### As a Rust crate

A Rust program adds `adbc-spanner` as a dependency and calls `SpannerDriver::try_new()` directly.
The four objects above are just Rust structs; you call their methods. This is the direct path.

### As a loadable shared library (the C ABI)

The more interesting path — and the reason ADBC exists as a standard — is the **shared library**.
When built as a `cdylib`, the crate compiles to `libadbc_spanner.so` (Linux) / `.dylib` (macOS) /
`.dll` (Windows). Any program in any language that has an **ADBC driver manager** can load this
file at runtime and talk to Spanner, without ever compiling against this crate.

How does a driver manager, handed nothing but a path to a `.so` file, find its way in? By a single
**exported C function** — the entrypoint. That is the *entire* contract between the driver manager
and the driver: one symbol.

In this crate that symbol is generated by one line ([`src/ffi.rs`](../src/ffi.rs)):

```rust
adbc_ffi::export_driver!(AdbcSpannerInit, crate::SpannerDriver);
```

This macro exports two C symbols:

- **`AdbcSpannerInit`** — the driver-specific init symbol. ADBC's naming convention derives it from
  the library name: `libadbc_spanner` → `AdbcSpannerInit`.
- **`AdbcDriverInit`** — a generic fallback name the driver manager tries when the caller does not
  name an explicit entrypoint.

You can see the symbol is really there:

```sh
cargo build --release
nm -D --defined-only target/release/libadbc_spanner.so | grep AdbcSpannerInit
```

When a driver manager calls this init function, it receives a **table of C function pointers** —
one per ADBC operation (open database, open connection, set option, execute, get next batch, …).
The macro wires each of those pointers to the corresponding method on the Rust structs. From then
on, every call the manager makes crosses the C boundary into safe Rust. Arrow data itself crosses
this boundary through the **Arrow C Data Interface** — a small, stable C struct layout that lets
two languages share the same columnar buffers *without copying them*. This is the second reason
Arrow matters here: it is not just the in-memory format, it is also the zero-copy wire format
across the language boundary.

So, loading from Python looks like this — no Rust in sight:

```python
import adbc_driver_manager
db = adbc_driver_manager.AdbcDatabase(
    driver="/path/to/libadbc_spanner.so",
    entrypoint="AdbcSpannerInit",
    uri="projects/p/instances/i/databases/d",
)
```

(There is also a published Python package, `adbc-driver-spanner`, that bundles the prebuilt library
and a friendly DBAPI 2.0 interface, so you do not have to do this by hand — see
[python/README.md](../python/README.md).)

> **Note on `unsafe`.** Crossing the C ABI is the only `unsafe` code in the whole crate, and it is
> entirely generated by the `export_driver!` macro. The pure-Rust build (no `ffi` feature) forbids
> `unsafe` outright. Everything you write on top of this driver is safe.

---

## 4. One structural fact that shapes everything: sync over async

Before walking through the interface, one design point explains a lot of the code.

The **ADBC traits are synchronous** — `execute()` returns a result, it does not return a future.
But the underlying Google Cloud Spanner client is **asynchronous** (async Rust, built on Tokio).
So every driver method has to bridge the two: it runs the async Spanner call to completion on a
shared Tokio runtime and blocks until it finishes.

```
ADBC method (sync)  ──▶  runtime.block_on(async Spanner call)  ──▶  result
```

There is **one** shared runtime, created once by the driver and passed by reference (`Arc`) into
every database, connection, and statement. You will see `runtime.block_on(...)` at the boundary of
essentially every operation. That is the whole trick: a synchronous ADBC surface over an
asynchronous client.

---

## 5. Walking through the interface

Now the interesting part: what the standard ADBC operations are, and how each is implemented on
Spanner. This is the "big picture" tour — enough to understand *what* each call does and *how* it
maps onto Spanner, without diving into the line-by-line internals.

### 5.1 Opening a database (configuration)

You start with a `SpannerDriver`, then ask it for a `SpannerDatabase`, passing options. The one
required option is the **database path**:

```
projects/<project>/instances/<instance>/databases/<database>
```

supplied either as the standard `uri` option or the driver-specific `spanner.database` key (they
are the same value). You can also pass a `spanner:`-scheme **connection URI** that packs the path,
endpoint, and options into one string:

```
spanner:///projects/p/instances/i/databases/d?spanner.emulator=true
```

The database object is **pure configuration** — no network happens yet. It just holds the path,
the credentials configuration, the endpoint, and any inherited options. Credentials can come from
Application Default Credentials (the usual GCP path), a service-account key file
(`spanner.auth.keyfile`), an OAuth access token, or impersonation; or, for local development, you point
at a **Spanner emulator** and use anonymous credentials. All of this is option plumbing on the
database object, handled in [`src/driver.rs`](../src/driver.rs).

### 5.2 Opening a connection

`database.new_connection()` builds the actual Spanner client and gives you a `SpannerConnection`.
This is the object you run work against. It owns two things worth knowing about:

- **Transaction mode** (autocommit by default — see §5.5).
- **The metadata / introspection calls** (§5.6).

### 5.3 Running a query — `execute`

This is the core read path. You create a statement, set its SQL, and call `execute()`:

```rust
let mut statement = connection.new_statement()?;
statement.set_sql_query("SELECT SingerId, FirstName FROM Singers")?;
let reader = statement.execute()?;   // a RecordBatchReader
for batch in reader {
    let batch = batch?;              // one Arrow RecordBatch
    println!("{} rows", batch.num_rows());
}
```

What happens under the hood:

1. The driver runs the query against Spanner in a single-use read-only transaction (a cheap,
   lock-free snapshot read).
2. It does **not** pull all the rows. Instead `execute()` returns a **lazy** `RecordBatchReader`.
   Each time you ask the reader for the next batch, the driver pulls the next bounded chunk of rows
   from Spanner (chunk size = `spanner.rows_per_batch`, default 8192) and converts just that chunk
   into an Arrow `RecordBatch`.
3. To keep the pipeline full, a background task **prefetches** the next chunk from Spanner while
   your code is still processing the current one.

The upshot: a result set of any size streams through bounded memory. The row → Arrow type mapping
(Spanner `INT64` → Arrow `Int64`, `TIMESTAMP` → Arrow nanosecond timestamp, `ARRAY`/`STRUCT` →
Arrow `List`/`Struct`, and so on) lives in [`src/conversion.rs`](../src/conversion.rs); the full
table is in the [README type-mapping section](../README.md#type-mapping).

DML with a `THEN RETURN` clause also comes back through `execute()` as an Arrow result, since it
produces rows.

### 5.4 Changing data — `execute_update`

For DML (`INSERT`/`UPDATE`/`DELETE`) and DDL (`CREATE`/`ALTER`/`DROP`/…), you call `execute_update`
instead. It returns the **affected-row count** rather than a result stream.

- **DML** runs in a Spanner read/write transaction. A `;`-separated batch (e.g. `DELETE; INSERT`)
  is sent as one atomic `ExecuteBatchDml`.
- **DDL** is not a normal query in Spanner — it goes through the separate Database Admin API
  (`UpdateDatabaseDdl`), which the driver detects and routes automatically. A `;`-separated DDL
  batch is submitted as a single schema change.

The DML/DDL detection and statement splitting live in [`src/sql.rs`](../src/sql.rs); the execution
in [`src/statement.rs`](../src/statement.rs).

### 5.5 Transactions

By default a connection is in **autocommit** mode: every statement commits on its own. Each query
gets a fresh read snapshot; each DML statement gets its own read/write transaction.

Set the standard `adbc.connection.autocommit` option to `false` to enter **manual** mode, then call
`commit()` or `rollback()` on the connection to end the transaction.

There is one important subtlety, and it comes from the preview Spanner client: that client exposes
read/write transactions only through a **closure-based runner** — there is no "open a transaction,
run statements against it, then commit" handle. To make manual transactions work anyway, the driver
**buffers** the DML you issue and replays the whole batch atomically inside a single read/write
runner call at `commit()` time. Two consequences follow, and both are documented user-facing:

- **No read-your-writes.** A query in a manual transaction runs immediately against a fresh
  read-only snapshot; it does not see DML you have buffered but not yet committed. So `INSERT` then
  `SELECT COUNT(*)` returns the *pre-insert* count.
- **`execute_update` returns `None` in manual mode** — the affected-row count is genuinely unknown
  until the buffered batch commits.

This is a deliberate, documented trade-off; the proper fix waits on the client exposing real
begin/commit handles. For the full model see the [`SpannerConnection`
rustdoc](../src/connection.rs) and the [README transactions bullet](../README.md#status).

### 5.6 Introspection — asking the database about itself

ADBC standardizes a set of **metadata** calls so a generic tool (a data browser, a BI client) can
discover what is in a database without knowing it is Spanner. Each returns its answer as — of
course — an Arrow result:

| ADBC call | Question it answers | How this driver implements it |
| --- | --- | --- |
| `get_info` | "What driver/vendor is this, what version?" | Static metadata ([`src/info.rs`](../src/info.rs)). |
| `get_objects` | "What catalogs / schemas / tables / columns / constraints exist?" | Queries Spanner's `INFORMATION_SCHEMA` ([`src/objects.rs`](../src/objects.rs)). |
| `get_table_schema` | "What is the Arrow schema of table X?" | Reads the table's columns and maps them to an Arrow schema. |
| `get_table_types` | "What kinds of table exist?" (`TABLE`, `VIEW`, …) | A fixed, typed result set. |
| `get_statistics` | "Row counts, distinct counts, null counts." | One aggregate scan per table for exact values ([`src/statistics.rs`](../src/statistics.rs)). |
| `get_parameter_schema` | "What parameters does this statement take?" | Inspects the statement's bound parameters. |

The point of this table is not the details but the *shape*: ADBC turns "tell me about yourself"
into ordinary calls that return Arrow, and this driver answers each by querying Spanner's own
catalog and reshaping the answer into the Arrow layout ADBC expects.

### 5.7 Parameters and bulk ingest — `bind`

You rarely want to paste values into SQL text. ADBC lets you **bind** an Arrow `RecordBatch` of
parameter values to a statement before executing it. Two uses:

- **Parameterized queries / DML.** Bind one batch whose columns supply the `@param` values. By
  default binding is *positional* (the i-th bound column fills the i-th distinct parameter); set
  `adbc.statement.bind_by_name = true` to match by column name instead.
- **Bulk ingest** — the fast bulk-load path. You point a statement at a target table, bind a big
  `RecordBatch` (or a whole stream of them via `bind_stream`), and the driver writes the rows.
  Crucially it ships them as native Spanner **insert mutations**, not one `INSERT` statement per
  row, so nothing is SQL-parsed per row. Ingest can *create* the target table from the incoming
  Arrow schema, *append* to an existing one, or *replace* it. Because Spanner caps how much one
  commit may write, a large ingest is committed **chunk by chunk**.

The Arrow → Spanner value mapping and the ingest table-building logic are in
[`src/bind.rs`](../src/bind.rs).

### 5.8 The rest of the surface

A few more standard ADBC operations, in brief:

- **`execute_schema`** — get a query's result schema *without running it*, via a PLAN-only probe.
- **`execute_partitions` / `read_partition`** — split a large read into independent partitions that
  can be executed in parallel, possibly on different machines. `execute_partitions` produces opaque,
  serializable partition descriptors; `read_partition` executes one and streams its rows. (Security
  note: a descriptor embeds the query and session identity and is *executable* — treat it as a
  credential and only move it over trusted channels.)
- **`cancel`** — interrupt an in-flight operation. The cancel signal is *sticky*: it interrupts the
  current blocking Spanner call and stays latched until the object's next operation, so a cancel
  landing between the chunk fetches of a streamed result still cancels the next fetch.

---

## 6. Configuration, the ADBC way

Everything tunable is an **option** — a string key/value set on one of the four objects with
`set_option` (or via the driver manager's `AdbcDatabaseSetOption` / `…ConnectionSetOption` /
`…StatementSetOption`, or Python `db_kwargs` / `conn_kwargs`). The conventions:

- **Standard, spec-defined options** use the `adbc.*` prefix — e.g. `adbc.connection.autocommit`,
  `adbc.connection.readonly`, `adbc.statement.bind_by_name`. These mean the same thing on every
  ADBC driver.
- **Spanner-specific options** use the `spanner.*` prefix — e.g. `spanner.read.staleness`,
  `spanner.request.priority`, `spanner.max_commit_delay`.
- Options set on a higher object are inherited as **defaults** by lower ones (connection → statement),
  and can be overridden lower down. Setting an option to `""` typically unsets it.
- Most options **round-trip**: `get_option` reads back what you set.
- Setting an unknown option fails with `NotImplemented`; reading an unset one fails with `NotFound`.

The complete, authoritative reference — every option at every level, with exact types, defaults,
and round-trip behaviour — is [docs/options.md](options.md). That is the page to consult when you
actually need a specific knob; this document only explains the *mechanism*.

---

## 7. Errors

ADBC has its own small set of error **status codes** (`InvalidArguments`, `NotFound`,
`AlreadyExists`, `InvalidState`, `NotImplemented`, `Timeout`, …). Spanner speaks gRPC status codes.
The driver's job at the boundary ([`src/error.rs`](../src/error.rs)) is to **translate**: it maps
each gRPC code onto the closest ADBC status, keeps the original numeric gRPC code in a
`vendor_code` field so nothing is lost, and forwards Spanner's structured error details (quota
failures, bad-request field violations, retry hints) into the ADBC error's `details`. So a caller
gets a portable ADBC status *and* the Spanner-specific specifics if they want them.

---

## 8. Putting it together — the mental model

1. A driver manager (or your Rust code) loads the driver via its **one exported entrypoint**.
2. You configure a **Database** (which Spanner database, which credentials) — no network yet.
3. You open a **Connection** — now there is a live Spanner client, and a transaction mode.
4. You create **Statements**, set SQL or bind Arrow data, and execute them.
5. Reads stream back as **Arrow `RecordBatch`es**, pulled from Spanner in bounded, prefetched
   chunks; writes go through DML transactions, DDL through the admin API, and bulk loads through
   native mutations.
6. Every synchronous ADBC call **blocks on** the async Spanner client via one shared runtime.
7. Options flow **down** the hierarchy; errors are **translated** from gRPC to ADBC at the boundary.

That is the whole driver. From here, the natural next reads are the
[README](../README.md) for the feature list and type mapping, [docs/options.md](options.md) for the
configuration reference, and the module-level rustdoc in [`src/`](../src) for any internal you want
to go deeper on.

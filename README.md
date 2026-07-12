# adbc-spanner

[![CI](https://github.com/fornwall/adbc-spanner/actions/workflows/ci.yml/badge.svg)](https://github.com/fornwall/adbc-spanner/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver for
[Google Cloud Spanner](https://cloud.google.com/spanner), available as:

- A [python package](https://pypi.org/project/adbc-driver-spanner/)
- A Rust crate (not yet published to crates.io).
- A [loadable shared library driver](#shared-library-loadable-driver)

## Status

Early but working and tested end-to-end against the Spanner emulator.

## Supported ADBC functionality

- SQL queries (`execute`) are streamed back as typed Arrow `RecordBatch`es. Spanner does not support returning
  columnar results directly - rows are pulled from Spanner and converted to Arrow in bounded chunks (with configurable
  size) as the reader is iterated, so a large result set is never fully materialised in memory.
- DML: A `;`-separated batch (e.g. `DELETE; INSERT`) runs atomically in one read/write transaction using
  [batch DML](https://docs.cloud.google.com/spanner/docs/samples/spanner-dml-batch-update).
- DDL (`CREATE`/`ALTER`/`DROP`/`RENAME`/…): Routed to the Database Admin `UpdateDatabaseDdl` API. A
  `;`-separated batch (e.g. a intermediate-table build then rename swap) is submitted as a single
  [schema change](https://docs.cloud.google.com/spanner/docs/schema-updates) near-atomic (but not
  truly atomic, as Spanner does not support atomic DDL) operation.
- Transactions: In manual mode DML — and any bulk ingest's insert mutations — is buffered
  and applied atomically in one read/write transaction on commit, so `execute_update` returns `None`
  rather than an affected-row count — the count is unknown until the buffered batch commits (queries
  and DDL still run immediately). Because only writes are buffered, a manual transaction has **no
  read-your-writes** — a query runs immediately in a fresh read-only snapshot, so an `INSERT`
  followed by a `SELECT COUNT(*)` in the same open transaction returns the *pre-insert* count — and
  **DML and DDL reorder**: DDL issued after buffered DML executes before it (Spanner DDL is never
  transactional). Commit first if a statement needs to see earlier writes. This follows from the
  preview client exposing read/write transactions only through a closure-based runner; it will be
  fixed properly once the client exposes begin/commit handles. DML with
  a [`THEN RETURN`](https://cloud.google.com/spanner/docs/dml-returning) clause returns its rows:
  `execute()` yields them as an Arrow result (autocommit mode only — buffered manual transactions
  cannot produce them).
- [Bulk ingestion](https://arrow.apache.org/adbc/current/format/specification.html#bulk-ingestion)
  are supported through [insert mutations](https://docs.cloud.google.com/spanner/docs/modify-mutation-api).
  The ingest commits in one transaction when it fits Spanner's
  [per-commit limits](https://docs.cloud.google.com/spanner/quotas#limits-for). A larger ingest is
  automatically split into chunks that fits those limits, in which case the ingestion is
  **not atomic as a whole**: a mid-ingest failure leaves earlier chunks committed. In a manual
  transaction (`adbc.connection.autocommit=false`) the mutations are buffered — unchunked — and
  committed atomically with any buffered DML on `commit`; Spanner applies buffered mutations at
  commit time, after the transaction's DML has executed. All four `adbc.ingest.mode` values are
  supported:
  `create` (the ADBC spec default — create the table first, failing if it exists), `append` (insert
  into an existing table), `create_append` (create if absent, then insert) and `replace` (drop and
  recreate).
  The three create modes build the table from the ingest data's Arrow schema, adding a synthetic
  `adbc_ingest_key` `STRING` primary key populated with a UUID per row, because Spanner requires
  every table to have a primary key and the ingest data carries none. That column is a real column,
  so it shows up in a later `SELECT *` from the table. To key on your own data instead, set
  `spanner.ingest.primary_key` to one or more existing ingest columns (comma-separated for a
  composite key, in key order) — those become the primary key and no synthetic column is added; a
  named column absent from the data fails with `InvalidArguments`. For non-atomic,
  high-throughput ("firehose") loads, set `spanner.ingest.batch_write=true` to route an autocommit
  ingest's per-chunk mutations through Spanner's **BatchWrite** RPC instead of a write-only
  transaction (insert/count/error semantics and chunking preserved; BatchWrite applies its mutation
  groups non-atomically). It only affects autocommit ingests — a manual transaction ignores it and
  still buffers and commits atomically — and, since BatchWrite carries no per-request commit options,
  the priority / request-tag / `commit.max_delay` / `commit_stats` options do not apply on that path.
  TODO: Move BatchWrite to section below. perhaps a dedicated bulk ingestion explaining everything around
  that - supported moves, BatchWrite, transaction splitting, transaction behaviour, etc.
- Parameter binding: `bind`/`bind_stream` an Arrow batch whose columns become Spanner named
  parameters; each bound row runs the statement once. How columns pair with the query's `@name`
  parameters is set by the `adbc.statement.bind_by_name` statement option (the [SQLite reference
  driver's convention](https://github.com/apache/arrow-adbc/issues/3362)), a boolean defaulting to
  `false`: **positional** (the default) binds the *i*-th bound column to the *i*-th distinct
  parameter in query order, ignoring column names — the ADBC ordinal contract that positional
  clients and validation suites rely on; **`true`** is strict by-name (a column `id` binds `@id`,
  order-independent), where a bound column that names no query parameter fails with
  `InvalidArguments` naming the missing parameter — for clients whose column names are authoritative
  and may not match the parameters' textual order. A bound *query*
  over several rows executes all of them in one shared read-only snapshot (a multi-use read-only
  transaction at the configured staleness bound), so the per-row results are mutually consistent,
  and its results stream in `spanner.rows_per_batch` chunks like any other query. Because Spanner
  accepts the bounded-staleness kinds only on single-use transactions, a `max:<d>`/`min:<t>` bound
  is pinned there to its most-stale legal equivalent (exact staleness `<d>` / read timestamp `<t>`).
- Metadata: `get_table_types()`, `get_table_schema()`, and `get_objects()` (catalog/schema/table/
  column introspection from `INFORMATION_SCHEMA`; columns report the Spanner-native type, e.g.
  `STRING(MAX)`, as `xdbc_type_name`).
- Statistics: `get_statistics()` computes exact table/column counts with one aggregate scan per table
  — `ROW_COUNT`, and per column `NULL_COUNT` (plus `DISTINCT_COUNT` for groupable types). Spanner has
  no cheap pre-computed statistics, so an `approximate` request gets the same exact scans (exact
  values always satisfy an approximate request, and each row is flagged as not approximate);
  `get_statistic_names` is empty (Spanner has no custom named statistics).
- `execute_schema()`: a query's result schema without running it (via `QueryMode::Plan`), so tools
  can introspect output columns — including a top-level `WITH` — with no data scan.
- Partitioned execution: `execute_partitions()` splits a query into independently executable
  partitions via Spanner's `PartitionQuery` API, each serialized as a self-contained opaque ADBC
  descriptor, and `Connection::read_partition()` streams one partition's rows back as Arrow.
  `spanner.data_boost` bakes [Data Boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview)
  into the descriptors; `spanner.partition.max_count` hints the partition count.

TODO: Go over these and merge with above:

- Manual transactions — setting adbc.connection.autocommit=false plus commit()/rollback() works (via the buffer-and-commit
  scheme), rather than the driver rejecting non-autocommit mode.
- Transaction isolation level — the adbc.connection.transaction.isolation_level option is honored for serializable,
  repeatable_read, and default. The four levels Spanner does not natively expose are promoted upward to the weakest
  supported level that still satisfies them (read_uncommitted/read_committed → repeatable_read; snapshot/linearizable →
  serializable), which is spec-permitted and safe; get_option reports the effective promoted level, and an unknown
  level string is still rejected.
- Read-only connections — adbc.connection.readonly=true is supported, making the connection reject all writes while still
  allowing queries.
- execute_schema() (ADBC 1.1.0) — returns a query's result schema without executing it, via Spanner's QueryMode::Plan.
- Bulk ingest — the full adbc.ingest.* surface (append/create/create_append/replace modes, plus target
  catalog/db_schema/temporary) is implemented over native Spanner mutations.
- Partitioned execution — execute_partitions() and read_partition() are supported, serializing Spanner batch-read partitions
  into opaque ADBC descriptors.
- Cancellation (ADBC 1.1.0) — both Connection::cancel() and Statement::cancel() interrupt an in-flight operation.
- Statistics (ADBC 1.1.0) — get_statistics() returns exact row/null/distinct counts and get_statistic_names() returns a
  correctly-typed empty result.
- Typed option getters (ADBC 1.1.0) — get_option_int(), get_option_double(), and get_option_bytes() are implemented alongside
  the string getter.
- Parameter schema — get_parameter_schema() describes a parameterized statement's bind parameters.
- get_objects with constraints — catalog/schema/table/column introspection including foreign-key constraint_column_usage, not
  just the minimal object listing.
- Current catalog / schema options (ADBC 1.1.0) — adbc.connection.catalog / adbc.connection.db_schema are accepted, but only
  the default empty value is valid: Spanner has a single unnamed catalog, and although it supports named schemas (addressed by
  qualified name, e.g. sales.Orders, and enumerated by get_objects) it has no settable session/current schema to point at one.
- adbc.statement.bind_by_name — the SQLite-reference-driver bind-by-name convention is honored (a de-facto optional convention
  rather than a formal spec option).

## Unsupported optional ADBC functionality

- [Substrait](https://substrait.io/) plans are unsupported.

## Supported Spanner functionality

- Connecting to production Spanner or a [Spanner emulator](https://docs.cloud.google.com/spanner/docs/emulator).
- [Timestamp bounds](https://cloud.google.com/spanner/docs/timestamp-bounds): Queries read at a
  [strong](https://docs.cloud.google.com/spanner/docs/timestamp-bounds#strong) bound by default.
  Bounded or exact staleness can be achieved through ADBC options.  
- [Request priorities](https://cloud.google.com/blog/topics/developers-practitioners/introducing-request-priorities-cloud-spanner-apis)
  are supported through ADBC options. Default is high priority.
- [Request and transaction tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
  are supported through ADBC options.
- [Directed reads](https://cloud.google.com/spanner/docs/directed-reads) are supported through ADBC options.
- [Commit statistics](https://docs.cloud.google.com/spanner/docs/commit-statistics) are supported
  through ADBC options.
- [Custom timeouts](https://docs.cloud.google.com/spanner/docs/custom-timeout-and-retry) are
  supported through ADBC options.
- [Retry policies](https://docs.cloud.google.com/spanner/docs/custom-timeout-and-retry)
  are supported through ADBC options.
- [Throughput optimized writes](https://docs.cloud.google.com/spanner/docs/throughput-optimized-writes)
  are supported through ADBC options.
- [Change streams](https://cloud.google.com/spanner/docs/change-streams) work through the driver's
  ordinary SQL paths — no dedicated support is needed. `CREATE CHANGE STREAM … FOR <table>` /
  `DROP CHANGE STREAM` run through the DDL path like any other DDL; the stream is introspectable via
  `INFORMATION_SCHEMA.CHANGE_STREAMS` / `CHANGE_STREAM_TABLES`; and its generated
  [`READ_<stream>` table-valued function](https://cloud.google.com/spanner/docs/change-streams/details#change_streams-query-syntax)
  runs as an ordinary query, with the driver mapping its nested `ChangeRecord`
  (`data_change_record` / `heartbeat_record` / `child_partitions_record`) natively to Arrow.
- Error reporting: a Spanner/gRPC failure maps onto the closest ADBC status, keeps the exact numeric
  gRPC code in the ADBC error's `vendor_code` (so a retry loop can detect `ABORTED` = 10 precisely),
  and forwards the response's structured
  [`google.rpc.Status` details](https://cloud.google.com/apis/design/errors) into the ADBC error's
  *details*: each detail becomes a `(key, value)` pair whose key is the lowercased proto type name
  (e.g. `google.rpc.errorinfo`, `google.rpc.retryinfo`) and whose value is the detail's ProtoJSON
  encoding as UTF-8 bytes (self-describing via `"@type"`; no `-bin` key suffix since the value is
  text, not binary protobuf). These let a caller see *why* a call failed beyond the status code —
  for example `google.rpc.QuotaFailure` on `RESOURCE_EXHAUSTED`, `google.rpc.BadRequest` /
  `ErrorInfo` on `INVALID_ARGUMENT`, or `google.rpc.PreconditionFailure` on `FAILED_PRECONDITION`.
  (Spanner's `RetryInfo` on `ABORTED` is forwarded the same way, but rarely reaches a caller: the
  client's read/write transaction runner retries aborted transactions itself — consuming that
  `retryDelay` for its own backoff — so an `ABORTED` normally never surfaces from a DML/commit
  path.) Note this per-detail, type-name-keyed ProtoJSON layout deliberately diverges from the
  Flight SQL ADBC driver, which emits a single `grpc-status-details-bin` detail holding the whole
  `google.rpc.Status` as binary protobuf — so a consumer written to Flight SQL's convention won't
  interoperate. The reason is that the pinned preview client decodes details into serde-modelled
  types whose only supported encoding is ProtoJSON, with no binary-protobuf path.
  On a `PERMISSION_DENIED` (which maps to `Unauthorized`), the driver additionally *appends* a short
  IAM-guidance string to the error message. Spanner's own message already names the missing permission
  (e.g. `spanner.databases.select`), which is preserved verbatim, so the driver does not re-parse it
  or name a specific role; it appends a fixed hint to grant an IAM role that includes the missing
  permission and links <https://cloud.google.com/spanner/docs/iam>. (No predefined role is named —
  matching the ADBC BigQuery driver, whose only fixed auth guidance is a re-authentication hint plus a
  doc link and names no roles either.) The guidance only augments the message; the status,
  `vendor_code` and forwarded details are unchanged.

Not supported (returns `NotImplemented`, by nature of Spanner): **Substrait** — Spanner executes
GoogleSQL/PostgreSQL text and has no Substrait support.

## Shared library (loadable driver)

Besides the Rust crate, this builds a C-ABI **shared library** that any ADBC driver manager can load
(`libadbc_spanner.so` on Linux, `libadbc_spanner.dylib` on macOS, `adbc_spanner.dll` on Windows). It
exports the standard `AdbcSpannerInit` entrypoint (plus an `AdbcDriverInit` fallback).

Prebuilt libraries for Linux, macOS and Windows are
attached to every CI run and to each tagged [release](https://github.com/fornwall/adbc-spanner/releases).
To build one yourself: `cargo build --release` → `target/release/libadbc_spanner.so`.

### Configuration options

Options exist at three levels — **database**, **connection** and **statement** — matching the ADBC
object they are set on (`new_database_with_opts` or `set_option` on the database, `set_option` on
the connection, `set_option` on the statement). Driver-specific options use the bare `spanner.*`
prefix; the standard `adbc.*` (spec) options the driver honours — autocommit, read-only, isolation
level, bulk ingest, and so on — are accepted alongside them.

**[docs/options.md](docs/options.md) is the complete, authoritative reference**: every option, at
each level, with its exact type and allowed values, default, and `get_option` round-trip behaviour.

The Spanner database is set with the standard `uri` database option, a **connection URI** with the
`spanner://` scheme: its path is the database path, and its query parameters are database-level
options (see [docs/options.md](docs/options.md#connection-uris)):

```text
spanner:///projects/p/instances/i/databases/d?spanner.endpoint=http://localhost:9010&spanner.emulator=true
spanner://localhost:9010/projects/p/instances/i/databases/d
```

The `spanner://` scheme is **required** — a bare database path is rejected (this matches the ADBC
BigQuery driver, whose `uri` likewise requires the `bigquery://` scheme). The URI path is the
database path; an optional `//host:port` authority becomes `spanner.endpoint`
(write `spanner:///projects/…`, with three slashes, when no
endpoint host is intended). Query parameters must be database-level option names (unknown
keys are rejected); values are percent-decoded per RFC 3986 (`+` is a literal plus, not a space).
The URI is expanded into the individual options immediately when it is set, so precedence is
plain last-writer-wins: an option set after the URI overrides it, and a URI set after an option
overwrites only the fields the URI actually carries. `get_option("uri")` returns the stored
database path, not the original URI.

### Authentication

Credentials are resolved in this order:

1. **Emulator** — if `SPANNER_EMULATOR_HOST` is set (or `spanner.emulator` is `true`), anonymous
   credentials are used and the endpoint is taken from the environment. Combining emulator mode
   with explicit credentials (`spanner.auth.keyfile`, `spanner.auth.keyfile_json`,
   `spanner.auth.impersonate.target_principal`, or `spanner.auth.access_token`) is refused at connect time
   rather than silently ignoring them; ambient ADC (e.g. `GOOGLE_APPLICATION_CREDENTIALS`) does not
   conflict.
2. **Access token** — a caller-supplied OAuth 2.0 bearer token via `spanner.auth.access_token` (see
   below).
3. **Service account** — a key supplied inline via `spanner.auth.keyfile_json` or read from the path
   in `spanner.auth.keyfile`.
4. **[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)**
   otherwise (e.g. `GOOGLE_APPLICATION_CREDENTIALS`, gcloud login, or the metadata server).

#### OAuth access token

Setting `spanner.auth.access_token` authenticates with a bearer token you have already obtained out of
band — for example from `gcloud auth print-access-token`, a Workload Identity exchange, or another
auth library. The token is sent verbatim as the `Authorization: Bearer <token>` header on every
request and is **never refreshed**, so you are responsible for supplying a valid, unexpired token
(and re-connecting once it expires). Because it is a complete credential on its own, it is mutually
exclusive with `spanner.auth.keyfile`, `spanner.auth.keyfile_json`, and
`spanner.auth.impersonate.target_principal` — combining them is refused at connect time.

#### Service-account impersonation

Setting `spanner.auth.impersonate.target_principal` layers
[service-account impersonation](https://cloud.google.com/iam/docs/service-account-impersonation) on
top of whichever base credentials above are in effect: the base credentials call the IAM Credentials
`generateAccessToken` API to mint a short-lived token for the target service account, and the driver
authenticates as that target. The option group follows gcloud's `--impersonate-service-account`
(and `google-cloud-auth`'s `impersonated` builder) naming:

- `spanner.auth.impersonate.target_principal` — the target service-account email (**required** to enable
  impersonation; when unset, authentication is unchanged).
- `spanner.auth.impersonate.delegates` — an optional delegation chain (comma-separated), where each
  service account has the *Token Creator* role on the next and the last on the target.
- `spanner.auth.impersonate.scopes` — optional OAuth scopes (comma-separated); defaults to the
  `cloud-platform` scope.
- `spanner.auth.impersonate.lifetime` — optional token lifetime in seconds; defaults to `3600` (one hour).

#### Quota / billing project

Setting `spanner.auth.quota_project` charges the named project for Spanner API quota — sent as the
`x-goog-user-project` request header — while the data stays owned by whatever project the database
path names. This is needed when the credential's home project differs from the target project, or in
resource-sharing setups; the caller must hold `serviceusage.services.use` on the quota project. It
mirrors the BigQuery ADBC driver's `bigquery.auth.quota_project` (and gcloud's `--billing-project`).

The value is attached to whichever credentials are in effect (Application Default Credentials,
`spanner.auth.keyfile`/`spanner.auth.keyfile_json`, impersonation, or `spanner.auth.access_token`), so it composes
with every credential path. It is a bare project id — not a secret — so it round-trips through
`get_option`, and `""` unsets it. It is refused in emulator mode (which forces anonymous credentials
and ignores billing), like the credential options. If the `GOOGLE_CLOUD_QUOTA_PROJECT` environment
variable is set, the underlying auth library gives it precedence over this option. End-to-end billing
behaviour can only be observed against a real project, not the emulator.

## Type mapping

| Spanner type                                | Arrow type                        |
| ------------------------------------------- | --------------------------------- |
| `BOOL`                                      | `Boolean`                         |
| `INT64`                                     | `Int64`                           |
| `FLOAT64`                                   | `Float64`                         |
| `FLOAT32`                                   | `Float32`                         |
| `DATE`                                      | `Date32`                          |
| `TIMESTAMP`                                 | `Timestamp(Nanosecond, "UTC")` (default) or `Timestamp(Microsecond, "UTC")` — see below |
| `NUMERIC`                                   | `Decimal128(38, 9)`               |
| `BYTES`                                     | `Binary`                          |
| `STRING` / `UUID` / `INTERVAL`               | `Utf8`                            |
| `JSON`                                      | `Utf8` + `arrow.json` extension   |
| `ARRAY<T>`                                  | `List<T>` (recursive)             |
| `STRUCT<..>`                                | `Struct<..>` (recursive)          |
| `ENUM`                                      | `Int64` (the integer ordinal)     |
| `PROTO`                                     | `Binary` (the raw serialized bytes) |

`NULL`s are represented as null slots in the corresponding Arrow array. Decoding is strict: a
present (non-`NULL`) wire value that cannot be decoded as its column's type surfaces an
`InvalidData` error naming the type and the offending value — it is never silently mapped to a
null slot the caller could mistake for a genuine SQL `NULL`. `ARRAY` and `STRUCT` map to
native Arrow `List`/`Struct` recursively, so nested shapes like `ARRAY<STRUCT<..>>` round-trip with
full type fidelity.

`ENUM` and `PROTO` columns map to lossless primitives: `ENUM` → `Int64` (the enum's integer
ordinal, delivered as a decimal string like `INT64`) and `PROTO` → `Binary` (the message's raw
serialized proto2 wire bytes, delivered base64-encoded like `BYTES`). `ARRAY<ENUM>` and
`ARRAY<PROTO>` map to `List<Int64>` / `List<Binary>` the same way, recursively.

Neither type's *structure* — the enum's member names, or the proto's field layout — travels in the
query result metadata; it lives only in the database's proto descriptor bundle (reachable via the
admin `GetDatabaseDdl` RPC, not the data-plane read). So the driver hands back the faithful
primitive (the ordinal / the serialized bytes) rather than a decoded `Dictionary` or `Struct`, and
you decode a `PROTO` value with your own compiled `.proto`. If you want the decoded form directly,
`CAST(col AS STRING)` in your query and Spanner returns it server-side (the enum member name, or the
proto text format) as a `STRING` → `Utf8` column.

`JSON` columns keep `Utf8` storage (the value bytes are the JSON text) but carry the canonical
[`arrow.json`](https://arrow.apache.org/docs/format/CanonicalExtensions.html#json) extension type as
field metadata (`ARROW:extension:name` = `arrow.json`), so Arrow consumers that understand the
extension recognize the logical JSON type while others still read plain strings. The extension is
attached to the Arrow `Field`, not the storage `DataType`; for `ARRAY<JSON>` it sits on the list's
child (`item`) field. The tag also works in the **bind** direction: a string parameter column
carrying `arrow.json` binds as a Spanner `JSON`-typed parameter (a list of tagged strings as
`ARRAY<JSON>`), which is required for inserting into a `JSON` column — Spanner does not coerce
`STRING` parameters to `JSON` (without the tag, wrap the parameter in `PARSE_JSON(@p)` instead).
Bulk-ingest create modes likewise create a `JSON` column for a tagged field. So JSON values
round-trip: what `execute` reads from a `JSON` column can be bound straight back into one.

`TIMESTAMP` is read at full nanosecond precision by default (matching the bind/write path). Arrow
stores `Timestamp(Nanosecond)` as an `i64` count of nanoseconds since the Unix epoch, which spans
only ~1677-09-21 to 2262-04-11 — a narrower window than Spanner's year 1–9999 range. A Spanner
timestamp outside that window cannot be represented, so reading one surfaces an `InvalidArguments`
error naming the column and the offending value rather than silently truncating or wrapping it. To
read tables holding such timestamps, set `spanner.max_timestamp_precision=microseconds` (connection
or statement level): `TIMESTAMP` then maps to `Timestamp(Microsecond, "UTC")`, which covers
Spanner's entire range, at the cost of truncating any sub-microsecond digits toward negative
infinity. Those are the only two modes — there is deliberately no silently-wrapping nanosecond mode
— see [docs/options.md § Timestamp precision](docs/options.md#timestamp-precision).

> **Note:** native `STRUCT` mapping needs `Type::struct_type()`, which is on `google-cloud-rust`
> `main` but not yet in a crates.io release. Until it ships, `Cargo.toml` pins the `google-cloud-*`
> crates to a git revision. `adbc_core`/`adbc_ffi` are likewise pinned to an
> [`apache/arrow-adbc`](https://github.com/apache/arrow-adbc) `main` revision carrying FFI fixes not
> yet in the `0.23` release. Either git pin means `adbc-spanner` cannot itself be published to crates.io in the
> meantime, and downstream crates must take `adbc_core` from the same `arrow-adbc` git revision (see
> the notes in `Cargo.toml`).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing, and release instructions, and
[docs/testing.md](docs/testing.md) for the full testing overview.

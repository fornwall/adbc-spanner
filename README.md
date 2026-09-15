# adbc-spanner

[![CI](https://github.com/fornwall/adbc-spanner/actions/workflows/ci.yml/badge.svg)](https://github.com/fornwall/adbc-spanner/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver for
[Google Cloud Spanner](https://cloud.google.com/spanner), available as:

- A [python package](https://pypi.org/project/adbc-driver-spanner/)
- A Rust crate (not yet published to crates.io).
- A [loadable shared library driver](#shared-library-loadable-driver)

## Status

Early, tested end-to-end against the Spanner emulator.

## Spanner ADBC quirks

- Spanner returns rows, not columns: rows are pulled in bounded chunks and converted to Arrow on
  demand (`spanner.rows_per_batch`, default 8192), with a background task prefetching the next chunk.
- DML: a `;`-separated batch (e.g. `DELETE; INSERT`) runs atomically in one read/write transaction
  using [batch DML](https://docs.cloud.google.com/spanner/docs/samples/spanner-dml-batch-update). A
  batch must be all-DML: mixing in a query or DDL is rejected up front with `InvalidArguments`.
- DDL (`CREATE`/`ALTER`/`DROP`/`RENAME`/…): routed to the Database Admin `UpdateDatabaseDdl` API. A
  `;`-separated batch is submitted as a single
  [schema change](https://docs.cloud.google.com/spanner/docs/schema-updates) — near-atomic, but not
  truly atomic, since Spanner has no atomic DDL.

## Supported optional ADBC functionality

Every option named here is specified in full in **[docs/options.md](docs/options.md)**.

- **Streaming queries** — `execute()` returns a lazy Arrow `RecordBatchReader`.
- **DML and DDL** — `execute_update()`, including `;`-separated batches and
  [`THEN RETURN`](https://cloud.google.com/spanner/docs/dml-returning) rows (autocommit only).
- **Manual transactions** — `adbc.connection.autocommit=false` plus `commit()`/`rollback()`. A
  transaction is exactly one kind of work, queries *or* DML, fixed by its first statement; DDL is
  never transaction-aware. See **[docs/transactions.md](docs/transactions.md)**.
- **Isolation levels** — `adbc.connection.transaction.isolation_level`, honoured for read/write
  transactions and inert on queries.
- **Read-only connections** — `adbc.connection.readonly=true` rejects every write, including the
  commit of buffered work, while queries still run.
- **Parameter binding** — `bind`/`bind_stream` an Arrow batch whose columns become Spanner `@name`
  parameters; each bound row runs the statement once, and a multi-row bound *query* shares one
  read-only snapshot. Positional by default; `adbc.statement.bind_by_name=true` switches to strict
  by-name.
- **Bulk ingest** — the `adbc.ingest.*` surface, shipped as native
  [insert mutations](https://docs.cloud.google.com/spanner/docs/modify-mutation-api) rather than
  per-row `INSERT` DML. All four `adbc.ingest.mode` values work; the three table-building modes
  declare **no primary key**, so Spanner keys the table on a
  [hidden `rowid`](https://cloud.google.com/spanner/docs/primary-key-default-value#tables-without-primary-keys)
  of its own and the created table holds exactly the columns you ingested. An autocommit ingest too
  large for one commit is split into chunks, so it is **not atomic as a whole**.
- **Partitioned execution** — `execute_partitions()` / `read_partition()`, optionally on
  [Data Boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview). **A partition
  descriptor is opaque but *executable*** — it carries the SQL text plus session and transaction
  identity, is **not** authenticated, and `read_partition()` runs it with the connection's
  credentials, so transport descriptors only over trusted channels.
- **`execute_schema()`** — a query's result schema without running it, via `QueryMode::Plan`.
- **`get_parameter_schema()`** — bind parameters typed by a PLAN probe from the surrounding SQL; a
  parameter the probe cannot type is reported as Arrow `Null`, ADBC's "type undetermined".
- **Metadata** — `get_info()`, `get_objects()` (including foreign-key `constraint_column_usage`;
  columns report the Spanner-native type, e.g. `STRING(MAX)`, as `xdbc_type_name`),
  `get_table_types()`, `get_table_schema()`.
- **Statistics** — `get_statistics()` computes exact `ROW_COUNT` / `NULL_COUNT` / `DISTINCT_COUNT`
  with one aggregate scan per table; Spanner has no cheaper pre-computed source, so an
  `approximate` request gets the same exact scans (each row flagged as not approximate).
  `get_statistic_names()` is an empty, correctly-typed result set.
- **Cancellation** — `Connection::get_cancel_handle()` / `Statement::get_cancel_handle()`. The
  signal is sticky: it interrupts the in-flight call and stays latched until the object's next
  operation, so a cancel between chunk fetches still cancels the next fetch.
- **Typed option getters** — `get_option_int()`, `get_option_double()` and `get_option_bytes()`.
- **Current catalog / schema** — `adbc.connection.catalog` and `adbc.connection.db_schema` are
  accepted at their empty default only (see below).

## Unsupported optional ADBC functionality

- [Substrait](https://substrait.io/) plans — Spanner executes GoogleSQL/PostgreSQL text.
- Incremental `execute_partitions` — `adbc.statement.exec.incremental` accepts only the spec default
  `false`; `true` fails with `NotImplemented`.
- Temporary ingest tables — `adbc.ingest.temporary=true` fails with `NotImplemented` (Spanner has
  none).
- Named catalogs — a non-empty `adbc.connection.catalog` or `adbc.ingest.target_catalog` fails with
  `NotImplemented`; Spanner has a single, unnamed catalog.
- A settable current schema — a non-empty `adbc.connection.db_schema` fails with `NotImplemented`.
  Spanner has named schemas, but no session-level schema to select one.

## Supported Spanner functionality

- [Transactions](https://cloud.google.com/spanner/docs/transactions): locking read-write, read-only
  snapshot, and write-only/mutation commits.
  **[docs/transactions.md](docs/transactions.md)** documents Spanner's transaction model, every gRPC
  call the driver makes to read or write data — with its transaction semantics, batching limits and
  the driver's call sites — and what the driver deliberately does not use.
- Connecting to production Spanner or a [Spanner emulator](https://docs.cloud.google.com/spanner/docs/emulator).
- [Timestamp bounds](https://cloud.google.com/spanner/docs/timestamp-bounds): queries read at a
  [strong](https://docs.cloud.google.com/spanner/docs/timestamp-bounds#strong) bound by default;
  bounded or exact staleness through ADBC options.
- [Request priorities](https://cloud.google.com/blog/topics/developers-practitioners/introducing-request-priorities-cloud-spanner-apis)
  (default high),
  [request and transaction tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags),
  [directed reads](https://cloud.google.com/spanner/docs/directed-reads),
  [commit statistics](https://docs.cloud.google.com/spanner/docs/commit-statistics),
  [custom timeouts and retry policies](https://docs.cloud.google.com/spanner/docs/custom-timeout-and-retry),
  [throughput optimized writes](https://docs.cloud.google.com/spanner/docs/throughput-optimized-writes)
  and [partitioned DML](https://docs.cloud.google.com/spanner/docs/dml-partitioned) — all through
  ADBC options.
- [Change streams](https://cloud.google.com/spanner/docs/change-streams) ride the ordinary SQL
  paths: `CREATE`/`DROP CHANGE STREAM` through the DDL path, `INFORMATION_SCHEMA.CHANGE_STREAMS` /
  `CHANGE_STREAM_TABLES` as plain queries, and the generated
  [`READ_<stream>` table-valued function](https://cloud.google.com/spanner/docs/change-streams/details#change_streams-query-syntax)
  as an ordinary query whose nested `ChangeRecord` maps natively to Arrow.
- Error reporting: a Spanner/gRPC failure maps onto the closest ADBC status, keeps the exact numeric
  gRPC code in the ADBC error's `vendor_code` (so a retry loop can detect `ABORTED` = 10 precisely),
  and forwards the response's structured
  [`google.rpc.Status` details](https://cloud.google.com/apis/design/errors) into the ADBC error's
  *details* — each keyed by the lowercased proto type name (`google.rpc.errorinfo`,
  `google.rpc.quotafailure`, …) with the detail's ProtoJSON encoding as the value. On a
  `PERMISSION_DENIED` (→ `Unauthorized`) the driver appends a short IAM hint to the message;
  Spanner's own message already names the missing permission and is preserved verbatim.
  See [docs/adbc.md § Errors](docs/adbc.md#6-errors) for the C-ABI 1.1.0 wrinkle, and
  `src/error.rs` for the full contract.

## Shared library (loadable driver)

Besides the Rust crate, this builds a C-ABI **shared library** that any ADBC driver manager can load
(`libadbc_spanner.so` on Linux, `libadbc_spanner.dylib` on macOS, `adbc_spanner.dll` on Windows). It
exports the standard `AdbcSpannerInit` entrypoint (plus an `AdbcDriverInit` fallback).

Prebuilt libraries for Linux, macOS and Windows are
attached to every CI run and to each tagged [release](https://github.com/fornwall/adbc-spanner/releases).
To build one yourself: `cargo build --release` → `target/release/libadbc_spanner.so`.

### Configuration options

Options exist at three levels — **database**, **connection** and **statement** — matching the ADBC
object they are set on. Driver-specific options use the bare `spanner.*` prefix; the standard
`adbc.*` (spec) options the driver honours are accepted alongside them.

**[docs/options.md](docs/options.md) is the complete, authoritative reference**: every option, at
each level, with its exact type and allowed values, default, and `get_option` round-trip behaviour.

The Spanner database is set with the standard `uri` database option, a **connection URI** with the
`spanner://` scheme — its path is the database path, its query parameters are database-level
options (grammar and rules in
[docs/options.md § Connection URIs](docs/options.md#connection-uris)):

```text
spanner:///projects/p/instances/i/databases/d?spanner.endpoint=http://localhost:9010&spanner.emulator=true
spanner://localhost:9010/projects/p/instances/i/databases/d
```

### Authentication

Credentials are resolved in this order:

1. **Emulator** — if `SPANNER_EMULATOR_HOST` is set (or `spanner.emulator` is `true`), anonymous
   credentials are used and the endpoint is taken from the environment. Combining emulator mode
   with explicit credentials (`spanner.auth.keyfile`, `spanner.auth.keyfile_json`,
   `spanner.auth.impersonate.target_principal`, or `spanner.auth.access_token`) is refused at connect time
   rather than silently ignoring them; ambient ADC (e.g. `GOOGLE_APPLICATION_CREDENTIALS`) does not
   conflict.
2. **Access token** — a caller-supplied OAuth 2.0 bearer token via `spanner.auth.access_token`, sent
   verbatim with no refresh. Mutually exclusive with the keyfile and impersonation options.
3. **Service account** — a key supplied inline via `spanner.auth.keyfile_json` or read from the path
   in `spanner.auth.keyfile`.
4. **[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)**
   otherwise (e.g. `GOOGLE_APPLICATION_CREDENTIALS`, gcloud login, or the metadata server).

[Service-account impersonation](https://cloud.google.com/iam/docs/service-account-impersonation)
(`spanner.auth.impersonate.*`) layers on top of whichever of those is in effect, and
`spanner.auth.quota_project` decouples the project billed for API quota from the one owning the
data; both groups are specified in
[docs/options.md § Database options](docs/options.md#database-options).

The two secret-holding options — `spanner.auth.keyfile_json` (a live private key) and
`spanner.auth.access_token` (a live bearer token) — are **write-only**: reading either back via
`get_option` always fails with `NotFound`, whether set or not, so tooling that dumps connection
options never prints a usable credential. They likewise cannot be passed as `uri` query parameters —
a connection URI is the most-logged configuration artifact there is. `spanner.auth.keyfile` is a
filesystem path, not a secret: it stays readable and remains valid in a URI.

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
full type fidelity. Struct fields are matched **positionally**, not by name, so a `STRUCT` with
duplicate or empty field names — both legal in Spanner, e.g. `STRUCT(1 AS x, 2 AS x)` or an unnamed
`SELECT`ed expression — keeps every field's own value.

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

`ENUM`, `PROTO`, `INTERVAL` and `UUID` have **no** such bind-side tag, so they do not round-trip
through DML parameters automatically: a value read back binds as its Arrow storage type, and Spanner
infers the parameter as `INT64` (`ENUM`), `BYTES` (`PROTO`) or `STRING` (`INTERVAL`/`UUID`) and will
not coerce it into the column's type. To insert or filter one of these via a bound parameter, wrap
it in an explicit `CAST(@p AS ENUM<…> | PROTO<…> | INTERVAL | UUID)` in the SQL. (Bulk ingest is
unaffected — it ships native mutations, not DML parameters — though its create modes still make
`INT64`/`BYTES`/`STRING` columns for these, not `ENUM`/`PROTO`/`INTERVAL`/`UUID`.)

On the bind / bulk-ingest side, unsigned Arrow integers that fit `i64` losslessly —
`UInt8`/`UInt16`/`UInt32` — widen to `INT64` like the signed widths; `UInt64` is unsupported
(`u64::MAX` exceeds `i64::MAX`). `Float16` widens to `FLOAT32` (Spanner has no 16-bit float, but
every `f16` is exactly representable in `f32`). `FixedSizeBinary` binds as `BYTES` like the other
binary layouts.

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
> crates to a git revision. `adbc_core` is likewise pinned to an
> [`apache/arrow-adbc`](https://github.com/apache/arrow-adbc) `main` revision, for the
> `InfoCode::Other(u32)` catch-all that is not yet in the `0.23` release (the C ABI itself is this
> driver's own `src/ffi/` layer, so `adbc_ffi` is only a dev-dependency). Either git pin means
> `adbc-spanner` cannot itself be published to crates.io in the
> meantime, and downstream crates must take `adbc_core` from the same `arrow-adbc` git revision (see
> the notes in `Cargo.toml`).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing, and release instructions, and
[docs/testing.md](docs/testing.md) for the full testing overview.

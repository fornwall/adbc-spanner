# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases are cut with [`cargo-release`](https://github.com/crate-ci/cargo-release); see
[CONTRIBUTING.md](CONTRIBUTING.md) for the release and versioning process.

## [Unreleased]

## [0.7.0] - 2026-07-16

This release settles the driver's option-key layout: credentials moved under `spanner.auth.*`, the
database path moved to the standard `uri` option, and the remaining flat keys were regrouped under
subsystem namespaces. **The renames ship with no backward-compatible aliases**, so a 0.6.0
configuration will not load as-is. The `adbc-driver-spanner` wheel is published, so wheel users must
update their option keys when upgrading:

| 0.6.0 key | 0.7.0 replacement |
| --- | --- |
| `spanner.database` | `uri` — and it now requires a `spanner://` URI |
| `spanner.keyfile` | `spanner.auth.keyfile` |
| `spanner.keyfile_json` | `spanner.auth.keyfile_json` |
| `spanner.impersonate.target_principal` | `spanner.auth.impersonate.target_principal` |
| `spanner.impersonate.delegates` | `spanner.auth.impersonate.delegates` |
| `spanner.impersonate.scopes` | `spanner.auth.impersonate.scopes` |
| `spanner.impersonate.lifetime` | `spanner.auth.impersonate.lifetime` |
| `spanner.data_boost_enabled` | `spanner.data_boost` |
| `spanner.max_partitions` | `spanner.partition.max_count` |
| `spanner.read.timestamp` | merged into `spanner.read.staleness` (`read:<rfc3339>` / `min:<rfc3339>`) |

Python callers are additionally affected by the `connect()` signature change below.

### Added

- Directed reads: `spanner.directed_read` at connection and statement level selects which replicas a
  read-only query runs against, with the grammar
  `<mode>[:<sel>,...][;auto_failover_disabled]`.
- Commit statistics: `spanner.commit_stats` requests them at every commit site, and the read-only
  `spanner.commit_stats.mutation_count` reports the last commit's mutation count.
- Query optimizer options: `spanner.query.optimizer_version` and
  `spanner.query.optimizer_statistics_package`.
- Commit batching: `spanner.commit.max_delay`, a duration in `0..=500ms`.
- OAuth access-token auth: `spanner.auth.access_token` sends a caller-supplied bearer token verbatim
  with no refresh; mutually exclusive with the keyfile/impersonation options and refused in emulator
  mode.
- Quota / billing project: `spanner.auth.quota_project` decouples the project charged for API quota
  (the `x-goog-user-project` header) from the data-owning project.
- Retry tuning: `spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds` bound the client's
  retry policy, and `spanner.retry.backoff.{initial_seconds,max_seconds,multiplier}` tune the
  exponential backoff. Both families are opt-in and independent; unset means the client's own policy.
  The two caps mean different things on the streaming query path than on unary RPCs — see
  [docs/options.md](docs/options.md).
- Bulk-ingest firehose transport: `spanner.ingest.batch_write` routes each autocommit ingest chunk
  through Spanner's BatchWrite RPC, shipping one mutation group per row (non-atomic per group). It is
  ignored in manual mode.
- `spanner.ingest.primary_key` keys a created ingest table on existing columns, in the given order,
  instead of adding the synthetic `adbc_ingest_key` column.
- `spanner.max_timestamp_precision` for full-range TIMESTAMP reads.
- `adbc.statement.bind_by_name` forces strict by-name parameter binding; the default stays
  positional.
- Native type mapping for Spanner `PROTO` (to Arrow `Binary`) and `ENUM` (to an Arrow `Int64`
  ordinal), which previously errored.
- A bulk-ingest chunk that Spanner rejects for exceeding the mutation limit is now bisected and
  retried down to a single row, covering the secondary-index entries the driver's own
  rows × columns estimate cannot see. Only that specific `INVALID_ARGUMENT` is retried.
- `PERMISSION_DENIED` errors get a fixed IAM-guidance hint and the Spanner IAM doc link appended to
  Spanner's own message, which already names the missing permission.

### Changed

- **Breaking:** the option keys in the migration table above moved to their final namespaces with no
  backward-compatible aliases. `spanner.database` is gone in favour of the standard `uri` option,
  which now requires a `spanner://` URI and rejects a bare `projects/…/databases/…` path.
- **Breaking:** the Python `connect()` entry points (both `adbc_driver_spanner.connect` and
  `adbc_driver_spanner.dbapi.connect`) drop their friendly keyword arguments (`uri`, `endpoint`,
  `emulator`, `keyfile`, `keyfile_json`, `impersonate_*`, `access_token`) and the `option_kwargs`
  helper. Pass every driver setting as its raw option key through `db_kwargs` instead — e.g.
  `dbapi.connect(db_kwargs={"uri": "spanner://…", "spanner.auth.keyfile": "…"})`. `dbapi.connect`
  keeps `conn_kwargs` and `autocommit`.
- **Breaking:** option *values* must now be exact lowercase everywhere, matching the ADBC spec and
  every surveyed driver. The previous case-insensitive leniency on the `spanner.read.staleness`
  `exact:`/`max:` prefixes is gone, so `MAX:1m` is now rejected (REVIEW.md COR-7).
- **Breaking:** `adbc.connection.readonly` is enforced on the commit paths too — committing a manual
  transaction's buffered DML or mutations while the flag is set fails with `InvalidState`. A
  query-only or empty transaction still commits, and `rollback` is never gated; the rejection changes
  no state, so the buffer stays replayable (REVIEW.md COR-10).
- **Breaking:** `adbc.connection.transaction.isolation_level` = `snapshot` now maps to Spanner's
  `REPEATABLE_READ` instead of `SERIALIZABLE`, and `get_option` reports it back as
  `repeatable_read` rather than `serializable`. Spanner implements `REPEATABLE_READ` *as* snapshot
  isolation — its proto definition matches ADBC's `snapshot` almost verbatim — so this is an exact
  native mapping, not the promotion the driver previously treated it as. Transactions that ask for
  `snapshot` keep the isolation they requested while shedding serializable's pessimistic read locks
  and higher abort rate; note that `REPEATABLE_READ` detects write-write conflicts only, so DML
  that reads rows it does not write (a subquery guard, a join, `INSERT … SELECT`) can now be
  exposed to write skew where serializable would have aborted it. Callers who want the old
  behaviour should set `serializable` explicitly. `serializable`, `repeatable_read`, `default`, and
  the `read_uncommitted`/`read_committed`/`linearizable` promotions are unchanged (REVIEW.md
  SPEC-7).
- **Breaking:** a `spanner://` connection URI no longer accepts the two secret-holding database
  options — `spanner.auth.keyfile_json` and `spanner.auth.access_token` — as query parameters; a URI
  carrying either is rejected with `InvalidArguments` naming the key. A URI is the most-logged
  configuration artifact there is (shell history, process listings, tracing spans), so a secret in
  one leaks far beyond the driver. Set them as database options instead, or use
  `spanner.auth.keyfile` (a path, not a secret), which a URI may still carry. This matches the
  write-only `get_option` treatment of the same two keys (REVIEW.md SEC-2).
- MSRV raised to 1.97.
- `get_parameter_schema` now reports real Spanner-inferred parameter types instead of typing every
  parameter `Null`: a `QueryMode: PLAN` probe returns the statement's undeclared parameters typed
  from the surrounding SQL (queries plan in a read-only transaction; DML plans in a read/write
  transaction that executes nothing and commits empty). A parameter the probe cannot type — DDL,
  DML on a read-only connection, or a type the SQL context doesn't pin down — is still reported as
  Arrow `Null`, ADBC's convention for an undetermined parameter type.
- Manual transactions are kind-exclusive: a transaction is either queries or DML, fixed by its first
  statement, and work of the other kind is rejected with `InvalidState`. A query transaction opens
  one shared multi-use read-only snapshot, so every read in it sees one timestamp. A query inside a
  DML transaction now errors instead of silently returning a result that misses the buffered DML.
  DDL remains non-transactional and executes immediately.
- Bulk ingest defaults to `create` mode when `adbc.ingest.mode` is unset, matching the ADBC spec.
- Unsupported `adbc.connection.transaction.isolation_level` values are promoted to the weakest
  supported level that still satisfies their guarantees (`read_uncommitted` / `read_committed` to
  `repeatable_read`, `linearizable` to `serializable`) instead of being rejected. `get_option`
  reports the effective level; a genuinely unknown level is still `InvalidArguments`.
- Credentials no longer leak through introspection: `get_option` refuses to return
  `spanner.auth.keyfile_json` / `spanner.auth.access_token`, and `SpannerDatabase`'s `Debug`
  redacts credential fields (REVIEW.md SEC-1).
- Performance: one Spanner client stack is shared across a database's connections, the Database Admin
  client is cached across DDL statements, a mutations-only manual transaction commits via the
  replay-protected write-only path, DATE/TIMESTAMP/NUMERIC decode through direct scanners, and
  several per-row and per-cell allocations were removed from the conversion and metadata paths.
- The `google-cloud-*` git pins moved from a personal fork to upstream
  `googleapis/google-cloud-rust` `main`.

### Fixed

- Binding a `Null`-typed Arrow column (the shape a client builds from a `Null`-typed
  `get_parameter_schema` field, and what pyarrow infers for an all-`None` parameter set) now binds
  NULL per row instead of failing with "unsupported Arrow type Null" (REVIEW.md CONV-1).
- Binding a dictionary-encoded column (Arrow `Dictionary` of any key type — what pandas
  categorical columns produce over the C data interface) now binds each cell's decoded value
  instead of failing with "unsupported Arrow type Dictionary(…)". The encoding is transparent
  end to end: any bindable value type is accepted (scalars and `ARRAY<...>` alike),
  mutation-based bulk ingest decodes the same way, and the ingest create modes map such a column
  to its value type's Spanner column type (REVIEW.md CONV-2).
- An `arrow.json`-tagged string column keeps its `JSON` typing through dictionary encoding.
- The BatchWrite ingest path forwards a failed mutation group's `google.rpc.Status` details into
  `Error.details`, like the main error path (REVIEW.md COR-8).
- gRPC `UNIMPLEMENTED` maps to ADBC `NotImplemented` instead of a generic status.
- `get_info(None)` returns every recognised info code (REVIEW.md SPEC-5).
- `execute_partitions` rejects DML cleanly (REVIEW.md COR-11) and consumes bound data consistently
  with `execute` (REVIEW.md SPEC-3); `execute_schema` / `execute_partitions` strip trailing statement
  terminators.
- Non-DML SQL passed to `execute_update` runs through the read-only query path instead of failing.
- `cancel` is per-operation: a new operation can no longer un-cancel a live streamed reader
  (REVIEW.md CON-2).
- Manual-mode ingest mutations buffer all-or-nothing (REVIEW.md COR-2).
- Conversion hardening: `build_list` uses checked offset arithmetic (CONV-4), `parse_numeric_i128`
  rejects non-canonical NUMERIC strings (CONV-7), and `get_statistics` treats `INTERVAL` as
  non-groupable (CONV-3).
- `parse_duration` rejects oversized durations instead of panicking; boolean options reject
  int-typed sets (REVIEW.md COR-4); the spec-default `incremental=false` option is accepted
  (REVIEW.md SPEC-2).
- Bound rows are cleared after a DDL `execute`; an ingest `create_append` schema mismatch maps to
  `AlreadyExists`; partition descriptors round-trip large floats stably.
- Python: the `access_token` path no longer raises `NameError`.

## [0.6.0] - 2026-07-08

> **Note:** this section was written about 19 hours after the `v0.6.0` tag was cut, and it describes
> the repository state at the time of writing rather than the contents of that release. Several items
> below — request priority / request tags / transaction tags, the RPC timeouts, the
> `adbc.connection.transaction.isolation_level` and `adbc.connection.readonly` options, and the
> `adbc.ingest.target_db_schema` / `target_catalog` support — landed after the tag and actually ship
> in 0.7.0. It is left as published, for the historical record.

The bulk of this release lands the fixes from the multi-aspect repo review (`REVIEW.md`).

### Added

- Stale reads: a single `spanner.read.staleness` option at connection and statement level, whose
  value is one of four prefixes — `exact:<duration>` / `max:<duration>` / `read:<rfc3339>` /
  `min:<rfc3339>`.
- Request priority / request tags / transaction tags
  (`spanner.request.priority`, `spanner.request.tag`, `spanner.transaction.tag`).
- RPC timeouts: `spanner.rpc.timeout_seconds.{query,update,fetch}` as overall per-operation
  deadlines mapped to ADBC `Timeout`.
- Standard `adbc.connection.transaction.isolation_level` (`serializable`/`repeatable_read`) and
  a live `adbc.connection.readonly` flag that statements re-check at execution time.
- Ingest: `adbc.ingest.target_db_schema` / `target_catalog` support; ingest now also triggers
  through `execute()`; `arrow.json`-tagged string params bind as Spanner `JSON`.
- Consolidated option-reference tables in both READMEs; `#![warn(missing_docs)]`; criterion
  benchmarks for the rows-to-Arrow path.
- `google.rpc.Status` error details (e.g. `RetryInfo`) forwarded into `Error.details`.

### Changed

- Bulk ingest is chunked under Spanner's per-commit limits (autocommit); manual mode still buffers
  and commits atomically.
- Streamed chunks are capped by an approximate byte budget in addition to the row count.
- `get_objects` groups INFORMATION_SCHEMA batches into hash maps (O(N) instead of O(N²)) and skips
  all queries at `Catalogs` depth; `get_statistics` per-table scans run with bounded concurrency and
  serve exact values for `approximate=true`.
- Ingest-append failures map to the spec-mandated `NotFound`/`AlreadyExists` statuses;
  `get_table_types` aligns with the `get_objects` vocabulary; `get_table_schema` honors its catalog
  argument.
- Emulator mode combined with explicit credentials is now refused.
- Migrated to the Rust 2024 edition; MSRV raised to 1.96.
- Release/publish is gated on green CI and the local checks via a `cargo-release` pre-release hook
  and a `ci-gate`/`version-gate` in the tag workflow.

### Fixed

- A failed manual-transaction `commit()` keeps the buffered DML for a genuine retry.
- Present-but-undecodable ARRAY/STRUCT wire values error loudly instead of becoming NULL;
  `parse_int64` drops its lossy f64 fallback; `is_dml_returning` no longer false-positives on
  `CASE … THEN return`.
- Comment-only segments are dropped from statement batches; DML is rejected in `execute_schema`.
- Numerous packaging/CI robustness fixes (wheel inspection before publish, graceful `build.rs`
  without `Cargo.lock`, and more).

## [0.5.0] - 2026-07-07

### Added

- Partitioned execution (`execute_partitions` / `read_partition`) with Data Boost.
- A separate Python package, `adbc-driver-spanner`, published to PyPI via trusted publishing, with a
  CI-executed cookbook (pyarrow / polars / duckdb / ingest).

## [0.4.0] - 2026-07-07

### Added

- Streaming query results (lazy, bounded-chunk `RecordBatchReader`) instead of eager materialization.
- Real `get_statistics`; `get_info`; `get_parameter_schema`; foreign-key `constraint_column_usage`
  in `get_objects`; best-effort statement/connection cancellation.
- Binding DATE / TIMESTAMP / NUMERIC parameters; buffered parameterized DML and bulk ingest in
  manual transactions.
- The canonical ADBC C++ validation suite, property-based round-trip tests, and fuzz targets in CI.

### Changed

- Pinned `adbc_core` / `adbc_ffi` to a fork with two fixes; bumped Arrow.

## [0.3.9] - 2026-07-05

### Added

- `get_objects` implementation; manual-transaction DML committed via `ExecuteBatchDml`.

## [0.2.0] - 2026-07-05

### Added

- DDL via the admin `UpdateDatabaseDdl` API; atomic `;`-separated DML batches via `ExecuteBatchDml`.
- Manual multi-statement transactions; `execute_schema`; `get_table_schema`.
- Parameter binding and bulk ingest; native Arrow mapping for DATE / TIMESTAMP / NUMERIC and for
  ARRAY / STRUCT.
- Service-account keyfile / keyfile_json authentication.

## [0.1.0] - 2026-07-05

### Added

- Initial release: an ADBC driver for Google Cloud Spanner returning Arrow record batches, building
  both an rlib and a loadable C-ABI cdylib. SQL queries and CI (clippy, unit tests, Spanner emulator
  integration).

<!-- Releases before this changelog was introduced (0.1.0 – 0.6.0) are summarized above at a high
     level from the git history; see the git tags for the exact commit ranges. -->

[Unreleased]: https://github.com/fornwall/adbc-spanner/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/fornwall/adbc-spanner/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/fornwall/adbc-spanner/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/fornwall/adbc-spanner/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/fornwall/adbc-spanner/compare/v0.3.9...v0.4.0
[0.3.9]: https://github.com/fornwall/adbc-spanner/compare/v0.2.0...v0.3.9
[0.2.0]: https://github.com/fornwall/adbc-spanner/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/fornwall/adbc-spanner/releases/tag/v0.1.0

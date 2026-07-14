# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases are cut with [`cargo-release`](https://github.com/crate-ci/cargo-release); see
[CONTRIBUTING.md](CONTRIBUTING.md) for the release and versioning process.

## [Unreleased]

### Changed

- MSRV raised to 1.97.
- `get_parameter_schema` now reports real Spanner-inferred parameter types instead of typing every
  parameter `Null`: a `QueryMode: PLAN` probe returns the statement's undeclared parameters typed
  from the surrounding SQL (queries plan in a read-only transaction; DML plans in a read/write
  transaction that executes nothing and commits empty). A parameter the probe cannot type — DDL,
  DML on a read-only connection, or a type the SQL context doesn't pin down — is still reported as
  Arrow `Null`, ADBC's convention for an undetermined parameter type.

## [0.6.0] - 2026-07-08

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

[Unreleased]: https://github.com/fornwall/adbc-spanner/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/fornwall/adbc-spanner/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/fornwall/adbc-spanner/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/fornwall/adbc-spanner/compare/v0.3.9...v0.4.0
[0.3.9]: https://github.com/fornwall/adbc-spanner/compare/v0.2.0...v0.3.9
[0.2.0]: https://github.com/fornwall/adbc-spanner/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/fornwall/adbc-spanner/releases/tag/v0.1.0

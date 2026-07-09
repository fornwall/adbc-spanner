# ADBC C++ validation suite

> Part of the testing overview in [docs/testing.md](../docs/testing.md).

Runs the canonical [Apache Arrow ADBC validation suite][suite] against
`adbc-spanner`. The suite is the driver-agnostic conformance test battery that
the in-tree ADBC drivers (SQLite, PostgreSQL, Snowflake, …) use. It exercises the
driver through its **C ABI** — the built `libadbc_spanner` cdylib, loaded by the
ADBC driver manager exactly as a real language binding would — so it complements
the Rust trait-level tests in `tests/integration.rs`.

[suite]: https://github.com/apache/arrow-adbc/tree/main/c/validation

## Running

```sh
# Throwaway emulator, gated (CI) subset — the same thing CI runs:
scripts/run-adbc-validation.sh

# Throwaway emulator, every test (local exploration; expect failures/skips):
scripts/run-adbc-validation.sh --full

# Against an already-running emulator or a real Cloud Spanner database:
SPANNER_EMULATOR_HOST=localhost:9010 scripts/run-adbc-validation.sh
SPANNER_GCP_DATABASE=my-project.my-instance.my-db scripts/run-adbc-validation.sh
```

The script builds the cdylib and the harness, creates the emulator
instance/database when needed, and runs the suite. Requirements beyond the Rust
toolchain: a C++17 compiler, CMake (≥ 3.20) and git. Everything else — the
arrow-adbc validation library, the ADBC driver manager, fmt, nanoarrow and
GoogleTest — is fetched and built from source at a pinned arrow-adbc release
(`ARROW_ADBC_TAG` in `CMakeLists.txt`). No system packages are required.

The **driver** (cdylib) links the `adbc_core` / `adbc_ffi` crates from a git pin
(an `apache/arrow-adbc` `main` revision — see `Cargo.toml`) that carries three FFI
fixes not yet in a crates.io release: an idempotent `AdbcError` release (no
double-free on a second release), `AdbcStatementExecuteQuery` writing
`rows_affected = -1` on the query path (upstream
[apache/arrow-adbc#4469](https://github.com/apache/arrow-adbc/pull/4469)), and the
exporter preserving the caller's `AdbcError.private_data` on the ADBC 1.0.0 path
(upstream [apache/arrow-adbc#4473](https://github.com/apache/arrow-adbc/pull/4473)).
The **C++** validation library and driver manager come from `ARROW_ADBC_TAG`;
they interoperate with the driver over the C ABI. That tag is temporarily pinned to
the [`fornwall/arrow-adbc#7`](https://github.com/fornwall/arrow-adbc/pull/7) fork
branch (based on the same apache `main` rev), which adds the `FloatCastTypeName`
`DriverQuirks` hook so `SELECT CAST(1.5 AS FLOAT64)` runs against Spanner; revert to
a versioned release once that quirk lands upstream.

`SpannerQuirks` (in `spanner_validation.cc`) describes Spanner's capabilities to
the suite — named `@p` parameters, DDL via the admin API, all four ingest modes,
mandatory (NULL-permitting) primary keys — so tests that do not apply to
Spanner's model self-skip rather than fail.

## What CI gates on

- **`SpannerDatabaseTest` + `SpannerConnectionTest`** in full — lifecycle +
  metadata: `get_info`, `get_objects` (table columns, primary-key/constraint
  metadata, and foreign-key `constraint_column_usage`), `get_table_types`,
  `get_table_schema` (a plain table, `NOT_FOUND` for missing and query-shaped
  table names, and a named-schema-qualified table — Spanner has `CREATE SCHEMA`),
  autocommit/transaction options.
- **The `SpannerStatementTest` cases that pass cleanly** — `execute` and
  `execute_schema` for int/string columns and their error paths, `prepare` /
  `get_parameter_schema` / parameter-count / no-query validation, query error
  handling, trailing-semicolon queries (`SELECT current_date;;;` — the driver
  strips trailing statement terminators on the query path, which Spanner's
  single-use query API otherwise rejects), query cancellation, concurrent
  statements, result independence/invalidation, and `AdbcError` compatibility
  (the exporter preserves a 1.0.0 caller's `private_data` — apache/arrow-adbc#4473,
  in the pinned `adbc_ffi` rev), and the ingest **error paths** (`SqlIngestErrors`:
  ingest-without-bind → `INVALID_STATE`, append to a nonexistent table → error,
  create over an existing table → error, incompatible-schema append → error — the
  one `SqlIngest*` case with no non-Spanner DDL or `SELECT *` readback to block it),
  and floating-point `execute`/`execute_schema` (`SqlQueryFloats` / `SqlSchemaFloats`:
  `SELECT CAST(1.5 AS FLOAT64)` via the `FloatCastTypeName` quirk, since GoogleSQL has
  no `FLOAT` keyword).

**All 46 gated tests pass, 0 self-skip.** `DatabaseTest` and `ConnectionTest` run in
full; the `StatementTest` cases are an explicit allowlist in
`scripts/run-adbc-validation.sh`. `SpannerQuirks::supports_bulk_ingest` declares
all four ingest modes (append, create, create_append, replace — the create
modes build the table from the ingest data's Arrow schema with a synthetic
`adbc_ingest_key` UUID primary key satisfying Spanner's mandatory-key rule),
which un-skipped the last two `ConnectionTest` cases, `MetadataGetTableSchema`
and `MetadataGetTableSchemaEscaping` (both gated upstream on
`supports_bulk_ingest(CREATE)`, though their fixtures only use plain DDL).

(`MetadataGetStatisticNames` is gated too: `get_statistic_names` returns a
valid empty catalog — Spanner has no per-column statistics to name, which the
suite accepts.)

## Follow-up work: the remaining `StatementTest` cases

The rest of `StatementTest` (runnable via `--full`) is gated incrementally as
each case is understood. Every remaining case now fails **cleanly** (no aborts —
see the note below); they fall into the following buckets, none of them
incremental driver work:

- **Suite-internal non-Spanner DDL** — several cases issue hardcoded
  `CREATE TABLE x (foo INT)` / `TEXT` / `FLOAT` with no primary key and
  double-quoted identifiers. There is no quirks hook for these, and they are not
  valid Spanner DDL (which needs `INT64`/`FLOAT64`, a `PRIMARY KEY`, backtick
  quoting). Covers e.g. `SqlBind`, `SqlQueryEmpty`, `SqlQueryFloats`,
  `SqlSchemaFloats`, `SqlQueryRowsAffectedDelete`, `Transactions`.
- **Ingest readback** — the driver supports create-mode ingest (with a
  synthetic `adbc_ingest_key` UUID primary key, since Spanner mandates one),
  and since the quirks declare it the `SqlIngest*` cases now *run* under
  `--full` instead of skipping — but the readback cases remain blocked (the
  error-path-only `SqlIngestErrors`, which needs no readback, is gated): they read
  the data
  back via a hardcoded double-quoted `SELECT * FROM "bulk_ingest" ORDER BY "col" …`
  (not valid GoogleSQL, no quirks hook), and `SELECT *` would also surface the
  synthetic key column, breaking the single-column result assertions. Where the
  value is separable — the ingest **identifier-escaping** tests (reserved-word
  table/column names) — it is captured natively instead: the driver quotes
  ingest identifiers (`bind::insert_sql`) and `tests/integration.rs` exercises
  an append-mode ingest into a table `create` with a column `index`.
- **`ECANCELED` through the C stream** — `SqlQueryCancel` requires the result
  stream's `get_next` to return exactly `ECANCELED` (125) after a cancel, but
  arrow-rs's `FFI_ArrowArrayStream` exporter (which `adbc_ffi` uses to export
  the driver's `RecordBatchReader`) maps every error to
  `ENOSYS`/`ENOMEM`/`EIO`/`EINVAL` — there is no `ArrowError` variant that
  reaches 125, so no Rust driver behind `adbc_ffi` can satisfy the case today.
  The driver's cancellation itself works and is sticky (a cancel landing
  between two chunk fetches cancels the next one); it surfaces through the C
  stream as `EINVAL` with the message `Cancelled: operation cancelled`. The
  case previously "passed" only because a between-chunk cancel was silently
  lost and the stream ran to completion. Covered natively by
  `cancel_between_stream_chunks_cancels_the_next_fetch` in
  `tests/integration.rs`; fixing the errno needs a status-aware stream export
  in `adbc_ffi` (the git-pinned fork), at which point the case can be re-gated.
- **Rigid single-partition assumption** — `SqlPartitionedInts` is now *runnable*
  (the driver implements `execute_partitions`/`read_partition`, and the
  `supports_partitioned_data` quirk is `true`), but the upstream case hardcodes
  `ASSERT_EQ(1, num_partitions)` ("Assume only 1 partition") for `SELECT 42`.
  Spanner's `partitionQuery` is free to return more — the emulator returns 2 — so
  the case cannot be gated. The driver's own partitioned round-trip
  (`execute_partitions` → `read_partition`, union of rows) is covered natively by
  `execute_partitions_round_trip` in `tests/integration.rs`.

The three `adbc_ffi` issues that previously blocked a whole swath of these — a
non-idempotent error release (which aborted the process), a missing
`rows_affected` on the query path, and a clobbered 1.0.0 `AdbcError.private_data` —
are **fixed** in the git-pinned `adbc_ffi` (see the top of this file), which is
what unblocked `SqlQueryInts` / `SqlQueryStrings` / `SqlPrepareSelectNoParams` /
`SqlPrepareErrorNoQuery` and `ErrorCompatibility`.

## A note on `--full` process isolation

`--full` runs each test in its own process via `ctest`. This is no longer
required for safety — the git-pinned `adbc_ffi` makes the driver's `AdbcError`
release idempotent, so a *failed* assertion now reports cleanly instead of
double-freeing and aborting the process. Per-test isolation is kept because it
still gives the cleanest independent pass/fail report.

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
# Throwaway emulator, the CI checks (gate + expected-failure + stale guards):
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
GoogleTest — is fetched and built from source at a pinned arrow-adbc `main`
revision (`ARROW_ADBC_TAG` in `CMakeLists.txt`). No system packages are required.

The **driver** (cdylib) links the `adbc_core` / `adbc_ffi` crates from a git pin
(an `apache/arrow-adbc` `main` revision — see `Cargo.toml`) that carries three FFI
fixes not yet in a crates.io release: an idempotent `AdbcError` release (no
double-free on a second release), `AdbcStatementExecuteQuery` writing
`rows_affected = -1` on the query path (upstream
[apache/arrow-adbc#4469](https://github.com/apache/arrow-adbc/pull/4469)), and the
exporter preserving the caller's `AdbcError.private_data` on the ADBC 1.0.0 path
(upstream [apache/arrow-adbc#4473](https://github.com/apache/arrow-adbc/pull/4473)).
The **C++** validation library and driver manager come from `ARROW_ADBC_TAG`;
they interoperate with the driver over the C ABI.

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
  one `SqlIngest*` case with no non-Spanner DDL or `SELECT *` readback to block it).

The gate runs **every case except the documented `EXCLUDED` expected-failures**
(see the next section), and they all pass or self-skip — today **45 cases, 0
self-skip**. `DatabaseTest` and `ConnectionTest` pass in full; from `StatementTest`
everything but the `EXCLUDED` list in `scripts/run-adbc-validation.sh` runs here.
`SpannerQuirks::supports_bulk_ingest` declares
all four ingest modes (append, create, create_append, replace — the create
modes build the table from the ingest data's Arrow schema with a synthetic
`adbc_ingest_key` UUID primary key satisfying Spanner's mandatory-key rule),
which un-skipped the last two `ConnectionTest` cases, `MetadataGetTableSchema`
and `MetadataGetTableSchemaEscaping` (both gated upstream on
`supports_bulk_ingest(CREATE)`, though their fixtures only use plain DDL).

(`MetadataGetStatisticNames` is gated too: `get_statistic_names` returns a
valid empty catalog — Spanner has no per-column statistics to name, which the
suite accepts.)

## How the gate stays honest: one `EXCLUDED` list, three checks

There is no allowlist of cases to run. Instead `scripts/run-adbc-validation.sh`
carries a single `EXCLUDED` array — the cases that are known-not-passing or
not-applicable to Spanner's model, each tagged with a reason grouped by the
buckets below — and derives everything from it. On the default (CI) run it makes
three assertions:

1. **Gate (negative filter).** It runs `spanner_validation --gtest_filter=-<EXCLUDED>`,
   i.e. *every case except* the excluded ones, and requires them all to pass or
   self-skip. This is what **auto-enrolls new upstream tests**: a case added by a
   bump of `ARROW_ADBC_TAG` is not in `EXCLUDED`, so it runs in the gate. If it
   fails, CI goes red and you triage it — fix the driver, or add it to `EXCLUDED`
   with a reason. Nothing new can silently escape the suite.
2. **Expected-failure guard (xfail-strict).** It then runs *only* the excluded
   cases (`--gtest_filter=<EXCLUDED> --gtest_output=xml:…`), captures the
   (deliberately non-zero) exit, and parses the JUnit XML per test case. If any
   excluded case actually **passed** (ran with no failure — a *skip* does not
   count), CI fails: an expected-failure that started passing must be removed from
   `EXCLUDED` so the gate enforces it. This keeps the list from silently
   accumulating cases the driver has since grown to satisfy.
3. **Stale guard.** It enumerates the available cases via `--gtest_list_tests`
   (no database needed) and fails if any `EXCLUDED` entry no longer exists
   upstream (renamed/removed), so the list can't rot.

So the manual periodic `--full` triage is gone: new cases run automatically, a
regressing exclusion or a fixed exclusion both turn CI red, and CI just calls this
script (see `.github/workflows/adbc-validation.yml`). Run only the static stale
guard — no emulator required — with:

```sh
scripts/run-adbc-validation.sh --check-drift
```

Today `EXCLUDED` holds **55** cases (45 non-excluded cases — 44 passing plus
`Transactions`, which self-skips — + 55 excluded = 100 upstream cases total).

`Transactions` is deliberately **not** excluded: it creates a table inside an
uncommitted transaction and expects it hidden from other connections and removed
on rollback, which requires transactional DDL. Spanner has none — DDL goes through
the admin `UpdateDatabaseDdl` API, auto-commits immediately, and cannot be rolled
back — so the case can never apply. The `ddl_implicit_commit_txn` quirk (set in
`spanner_validation.cc`) makes it **self-skip** via the case's own guard, which the
gate tolerates. That is cleaner than carrying it as an expected failure: an
inapplicable case can never "start passing", so it needs no expected-failure
bookkeeping, and a skip states the truth ("not applicable to Spanner") rather than
implying the driver got it wrong.

## The `EXCLUDED` cases, by bucket

Every excluded case (runnable individually via `--full`) fails **cleanly** (no
aborts — see the note below) or self-skips; they fall into the following buckets,
none of them incremental driver work:

- **Suite-internal non-Spanner DDL** — several cases issue hardcoded
  `CREATE TABLE x (foo INT)` / `TEXT` / `FLOAT` with no primary key and
  double-quoted identifiers. There is no quirks hook for these, and they are not
  valid Spanner DDL (which needs `INT64`/`FLOAT64`, a `PRIMARY KEY`, backtick
  quoting). Covers e.g. `SqlBind`, `SqlQueryEmpty`, `SqlQueryFloats`,
  `SqlSchemaFloats`, `SqlQueryRowsAffectedDelete`.
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

`SqlPartitionedInts` was formerly a fourth bucket ("rigid single-partition
assumption"): the upstream case hardcoded `ASSERT_EQ(1, num_partitions)` for
`SELECT 42`, but Spanner's `partitionQuery` is free to return more — the emulator
returns 2. apache/arrow-adbc#4493 relaxed it to allow `>= 1` partitions and assert
on the *union* of all of them, so with `ARROW_ADBC_TAG` on a `main` revision that
carries the fix the case now passes and is **gate-enforced** (the driver
implements `execute_partitions`/`read_partition` and the `supports_partitioned_data`
quirk is `true`). Its round-trip is additionally covered natively by
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

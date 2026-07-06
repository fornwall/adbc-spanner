# ADBC C++ validation suite

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
(`ARROW_ADBC_TAG` in `CMakeLists.txt`, kept in step with the `adbc_core` /
`adbc_ffi` crate versions). No system packages are required.

`SpannerQuirks` (in `spanner_validation.cc`) describes Spanner's capabilities to
the suite — named `@p` parameters, DDL via the admin API, append-only ingest,
mandatory (NULL-permitting) primary keys — so tests that do not apply to
Spanner's model self-skip rather than fail.

## What CI gates on

- **`SpannerDatabaseTest` + `SpannerConnectionTest`** in full — lifecycle +
  metadata: `get_info`, `get_objects` (including table columns and
  primary-key/constraint metadata), `get_table_types`, `get_table_schema`
  (`NOT_FOUND` for a missing table), autocommit/transaction options.
- **The `SpannerStatementTest` cases that pass cleanly** — `execute_schema` for
  int/string columns and its error path, `prepare` / `get_parameter_schema` /
  parameter-count validation, query error handling, concurrent statements, and
  result independence/invalidation.

**31 tests pass, 6 self-skip** (features Spanner does not expose — temp tables,
views, statistics, current-catalog metadata). The `StatementTest` cases are an
explicit allowlist in `scripts/run-adbc-validation.sh` (see the next section for
why it is an allowlist, not an exclude list).

## Follow-up work: the remaining `StatementTest` cases

The rest of `StatementTest` (runnable via `--full`) is gated incrementally as
each case is understood. The remaining ones fall into four buckets:

- **Blocked upstream by `adbc_ffi`** — the error-release function is not
  idempotent (see the note below), so any test that surfaces and releases an
  error aborts the process instead of failing cleanly. Also, `ExecuteQuery` on
  the query path never writes `rows_affected`, so tests asserting it is `1`/`-1`
  get the caller's initial `0`. Not fixable from this crate.
- **Suite-internal non-Spanner DDL** — several cases issue hardcoded
  `CREATE TABLE x (foo INT)` / `TEXT` / `FLOAT` with no primary key and
  double-quoted identifiers. There is no quirks hook for these, and they are not
  valid Spanner DDL (which needs `INT64`, a `PRIMARY KEY`, backtick quoting).
- **Create-mode ingest** — Spanner requires a primary key, so the driver has no
  create-mode ingest; the suite's ingest tests create the target table. Where the
  value is separable from create-mode — e.g. the ingest **identifier-escaping**
  tests (reserved-word table/column names) — it is captured natively instead: the
  driver quotes ingest identifiers (`bind::insert_sql`) and
  `tests/integration.rs` exercises an append-mode ingest into a table `create`
  with a column `index`.
- **Genuine driver gaps** — closed as found. `prepare()` now returns
  `INVALID_STATE` when no query is set (the suite's `SqlPrepareErrorNoQuery`;
  covered by a Rust test since the C++ one trips the upstream release bug).

Each remaining case is a self-contained increment: tune `SpannerQuirks`, fix a
driver gap if one is real (and cover it with a native test when the C++ case is
blocked upstream), then add the newly-green test to the allowlist.

## A note on failures aborting the process

The upstream `adbc_ffi` error-release function is not idempotent, and the
validation matchers release an error once on a *failed* assertion without
clearing it — so a failing assertion double-frees and aborts the process rather
than reporting a clean failure. Passing assertions are unaffected. Because of
this, `--full` runs each test in its own process via `ctest`, so one aborting
test does not hide the rest. The gated subset contains only passing tests, so it
never trips this.

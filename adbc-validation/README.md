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

CI runs the **`SpannerDatabaseTest` and `SpannerConnectionTest`** suites in full
— lifecycle + metadata: `get_info`, `get_objects` (including table columns and
primary-key/constraint metadata), `get_table_types`, `get_table_schema`
(returning `NOT_FOUND` for a missing table), autocommit/transaction options. **19
tests pass and 6 self-skip** (features Spanner does not expose, e.g. temp tables,
views, statistics, current-catalog metadata).

## Follow-up work: gating `StatementTest`

`StatementTest` is compiled and runnable via `--full` but **not gated yet**.
Bringing it into the gate is natural follow-up work the suite now makes easy to
tackle incrementally — one quirk or driver fix at a time. Much of it needs
per-test adaptation to Spanner's model:

- **Mandatory primary keys** — Spanner tables must declare a primary key, so the
  suite's key-less sample tables need quirks-provided DDL.
- **Append-only ingest** — the driver ingests into an existing table only; the
  suite's default create-mode ingest tests self-skip (`supports_bulk_ingest`
  returns true only for `append`).
- **Named parameters** — Spanner uses `@name`, not positional `?`, so parameter
  binding tests bind by column name.
- **`rows_affected` under buffer-and-commit** — manual-mode DML is buffered and
  the affected-row count is unknown until commit, unlike the suite's assumption.

Each is a self-contained increment: tune `SpannerQuirks`, fix a driver gap if one
is real, then move the newly-green tests into the gated filter in
`scripts/run-adbc-validation.sh`.

## A note on failures aborting the process

The upstream `adbc_ffi` error-release function is not idempotent, and the
validation matchers release an error once on a *failed* assertion without
clearing it — so a failing assertion double-frees and aborts the process rather
than reporting a clean failure. Passing assertions are unaffected. Because of
this, `--full` runs each test in its own process via `ctest`, so one aborting
test does not hide the rest. The gated subset contains only passing tests, so it
never trips this.

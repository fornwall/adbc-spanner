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

`StatementTest` is compiled and runnable via `--full` but **not gated yet**. A
`--full` run currently shows **12 pass, the majority self-skip, and 18 fail** (of
the ~79 `StatementTest` cases). The 18 failures fall into three buckets, and only
the first is driver-side:

**1. Driver gaps (fixable here).** For example, `prepare()` used to return `OK`
with no query set instead of `INVALID_STATE` — now fixed, so
`SqlPrepareErrorNoQuery`'s assertion passes (the test still aborts afterwards on
the upstream error-release bug below, but the driver behaviour is now correct).

**2. Upstream `adbc_ffi` (0.23) limitations — not reachable from the driver.**
- `AdbcStatementExecuteQuery` never writes `rows_affected` when a result stream is
  requested (only the no-stream branch does), so every test asserting
  `rows_affected ∈ {1, -1}` sees `0`. The Rust `Statement::execute` trait returns
  only a reader, with no channel to report the count on this path.
- The non-idempotent `AdbcError` release (see the note below) turns any error-path
  test into a `Subprocess aborted` — so even a *correct* driver error aborts
  rather than reporting a clean pass.

**3. Suite portability — hard-coded non-Spanner DDL.** Several tests issue fixed
DDL that Spanner rejects: no `PRIMARY KEY` (`CREATE TABLE queryempty (foo INT)`),
non-Spanner type names (`INTEGER` / `TEXT` / `INT`), and double-quoted
identifiers. This DDL does not route through `SpannerQuirks`, so those tests
cannot pass on Spanner without upstream suite changes.

Net: gating the whole suite is blocked more by upstream (`adbc_ffi`) and suite
portability than by incremental driver work. The realistic path is to gate the
specific green subset that avoids both — tune `SpannerQuirks`, fix real driver
gaps where they exist, then move the newly-green tests into the gated filter in
`scripts/run-adbc-validation.sh`. Spanner model quirks that stay relevant:

- **Mandatory primary keys** — Spanner tables must declare a primary key, so the
  suite's key-less sample tables need quirks-provided DDL.
- **Append-only ingest** — the driver ingests into an existing table only; the
  suite's default create-mode ingest tests self-skip (`supports_bulk_ingest`
  returns true only for `append`).
- **Named parameters** — Spanner uses `@name`, not positional `?`, so parameter
  binding tests bind by column name.
- **`rows_affected` under buffer-and-commit** — manual-mode DML is buffered and
  the affected-row count is unknown until commit, unlike the suite's assumption.

## A note on failures aborting the process

The upstream `adbc_ffi` error-release function is not idempotent, and the
validation matchers release an error once on a *failed* assertion without
clearing it — so a failing assertion double-frees and aborts the process rather
than reporting a clean failure. Passing assertions are unaffected. Because of
this, `--full` runs each test in its own process via `ctest`, so one aborting
test does not hide the rest. The gated subset contains only passing tests, so it
never trips this.

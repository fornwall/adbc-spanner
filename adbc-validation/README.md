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
(`ARROW_ADBC_TAG` in `CMakeLists.txt`). No system packages are required.

The **driver** (cdylib) links the `adbc_core` / `adbc_ffi` crates from a git pin
(see `Cargo.toml`) that carries two fixes not yet in a crates.io release: an
idempotent `AdbcError` release (no double-free on a second release) and
`AdbcStatementExecuteQuery` writing `rows_affected = -1` on the query path
(upstream [apache/arrow-adbc#4469](https://github.com/apache/arrow-adbc/pull/4469)).
The **C++** validation library and driver manager come from `ARROW_ADBC_TAG`;
they interoperate with the driver over the C ABI.

`SpannerQuirks` (in `spanner_validation.cc`) describes Spanner's capabilities to
the suite — named `@p` parameters, DDL via the admin API, append-only ingest,
mandatory (NULL-permitting) primary keys — so tests that do not apply to
Spanner's model self-skip rather than fail.

## What CI gates on

- **`SpannerDatabaseTest` + `SpannerConnectionTest`** in full — lifecycle +
  metadata: `get_info`, `get_objects` (table columns, primary-key/constraint
  metadata, and foreign-key `constraint_column_usage`), `get_table_types`,
  `get_table_schema` (`NOT_FOUND` for a missing table, and a named-schema-qualified
  table — Spanner has `CREATE SCHEMA`), autocommit/transaction options.
- **The `SpannerStatementTest` cases that pass cleanly** — `execute` and
  `execute_schema` for int/string columns and their error paths, `prepare` /
  `get_parameter_schema` / parameter-count / no-query validation, query error
  handling, query cancellation, concurrent statements, and result
  independence/invalidation.

**39 tests pass, 3 self-skip.** The `StatementTest` cases are an explicit
allowlist in `scripts/run-adbc-validation.sh`. The 3 remaining `ConnectionTest`
skips are features Spanner does not expose:

| Skipped test | Why | Enable-able? |
|---|---|---|
| `MetadataGetTableSchema`, `…Escaping` | gate on create-mode ingest | No — Spanner's mandatory primary key rules out create-mode ingest |
| `MetadataGetStatisticNames` | `supports_statistics()` | No — Spanner exposes no portable per-table statistics |

## Follow-up work: the remaining `StatementTest` cases

The rest of `StatementTest` (runnable via `--full`) is gated incrementally as
each case is understood. Every remaining case now fails **cleanly** (no aborts —
see the note below); they fall into three buckets, none of them incremental
driver work:

- **Suite-internal non-Spanner DDL** — several cases issue hardcoded
  `CREATE TABLE x (foo INT)` / `TEXT` / `FLOAT` with no primary key and
  double-quoted identifiers. There is no quirks hook for these, and they are not
  valid Spanner DDL (which needs `INT64`/`FLOAT64`, a `PRIMARY KEY`, backtick
  quoting). Covers e.g. `SqlBind`, `SqlQueryEmpty`, `SqlQueryFloats`,
  `SqlSchemaFloats`, `SqlQueryRowsAffectedDelete`, `Transactions`.
- **Create-mode ingest** — Spanner requires a primary key, so the driver has no
  create-mode ingest; the suite's ingest tests create the target table
  (`SqlIngest*`). Where the value is separable from create-mode — the ingest
  **identifier-escaping** tests (reserved-word table/column names) — it is
  captured natively instead: the driver quotes ingest identifiers
  (`bind::insert_sql`) and `tests/integration.rs` exercises an append-mode ingest
  into a table `create` with a column `index`.
- **One upstream `adbc_ffi` gap** — `ErrorCompatibility` checks that the driver
  preserves the caller's `AdbcError.private_data` / `private_driver`; the FFI
  exporter does not. Not fixable from this crate.

The two `adbc_ffi` issues that previously blocked a whole swath of these — a
non-idempotent error release (which aborted the process) and a missing
`rows_affected` on the query path — are **fixed** in the git-pinned `adbc_ffi`
(see the top of this file), which is what unblocked `SqlQueryInts` /
`SqlQueryStrings` / `SqlPrepareSelectNoParams` / `SqlPrepareErrorNoQuery`.

## A note on `--full` process isolation

`--full` runs each test in its own process via `ctest`. This is no longer
required for safety — the git-pinned `adbc_ffi` makes the driver's `AdbcError`
release idempotent, so a *failed* assertion now reports cleanly instead of
double-freeing and aborting the process. Per-test isolation is kept because it
still gives the cleanest independent pass/fail report.

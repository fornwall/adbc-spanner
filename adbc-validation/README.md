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

CI runs the **`SpannerDatabaseTest` and `SpannerConnectionTest`** suites
(lifecycle + metadata: `get_info`, `get_objects`, `get_table_types`,
`get_table_schema`, autocommit/transaction options), minus the small set of
known gaps below. Of that set 16 tests pass and 6 self-skip (features Spanner
does not expose). The exclusion list lives in `scripts/run-adbc-validation.sh`.

### Known gaps (excluded from the gate)

These are real driver conformance gaps the suite surfaced — the backlog it exists
to drive, not suite bugs:

| Test | Gap |
|------|-----|
| `ConnectionTest.MetadataGetTableSchemaNotFound` | `get_table_schema` on a missing table returns `INTERNAL`; the spec wants `NOT_FOUND`. The driver flattens every backend error to `INTERNAL` (Spanner reports a missing table as `INVALID_ARGUMENT`). |
| `ConnectionTest.MetadataGetObjectsColumns` | `get_objects` leaves `table_constraints` null; the suite expects it populated. |
| `ConnectionTest.MetadataGetObjectsPrimaryKey` | `get_objects` does not emit primary-key constraints. |

### `StatementTest`

`StatementTest` is compiled and runnable (`--full`) but **not gated**. Much of it
does not fit Spanner's model — create-mode ingest (the driver is append-only),
mandatory primary keys, named parameters, `rows_affected` in the buffer-and-commit
manual-transaction model — so it is a mix of legitimate skips, model mismatches
and a few genuine gaps. Bringing it into the gate is future work, one quirk /
driver fix at a time.

## A note on failures aborting the process

The upstream `adbc_ffi` error-release function is not idempotent, and the
validation matchers release an error once on a *failed* assertion without
clearing it — so a failing assertion double-frees and aborts the process rather
than reporting a clean failure. Passing assertions are unaffected. Because of
this, `--full` runs each test in its own process via `ctest`, so one aborting
test does not hide the rest. The gated subset contains only passing tests, so it
never trips this.

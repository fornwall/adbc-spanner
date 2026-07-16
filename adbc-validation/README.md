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
GoogleTest — is fetched and built from source at a pinned arrow-adbc revision
(`ARROW_ADBC_TAG` in `CMakeLists.txt` — an `apache/arrow-adbc` `main` revision
carrying [apache/arrow-adbc#4514](https://github.com/apache/arrow-adbc/pull/4514),
which routes the suite's hardcoded dialect-sensitive SQL through the
`DriverQuirks::RewriteSql` hook). No system packages are required.

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
the suite — named `@p` parameters, backtick identifier quoting, DDL via the admin
API, all four ingest modes, mandatory (NULL-permitting) primary keys, the Arrow
types Spanner widens on readback (`IngestSelectRoundTripType`) — so tests that do
not apply to Spanner's model self-skip rather than fail. Its `RewriteSql`
override substitutes GoogleSQL for the suite's dialect-sensitive default SQL per
stable query id: Spanner-valid `CREATE TABLE`s (INT64/STRING(MAX) + PRIMARY
KEY), `INSERT`s with the column list Spanner requires, ingest readbacks that
select the ingested columns explicitly (dodging the synthetic `adbc_ingest_key`
column) and drop the `NULLS FIRST`/`NULLS LAST` the emulator rejects (GoogleSQL's
defaults match them anyway), and CASTs that give Spanner's parameter-type
inference the context a bare `SELECT @p0, @p1` lacks. Every rewrite pins the
upstream default SQL, so an `ARROW_ADBC_TAG` bump that changes a query fails
loudly instead of silently rewriting the wrong thing.

## Sanitizers (ASan / UBSan)

Set `ADBC_VALIDATION_SANITIZE` to build the **C++ side** of the suite (this harness, the
fetched arrow-adbc driver manager + `adbc_validation`, and the vendored nanoarrow/fmt) with
`-fsanitize=…`, then run the same gate against it:

```sh
ADBC_VALIDATION_SANITIZE=address,undefined scripts/run-adbc-validation.sh
```

By default the **cdylib stays uninstrumented** — it is built by `cargo` (stable) and `dlopen`-loaded
by the driver manager, exactly as in production. This still finds the memory bugs that matter at the C
ABI boundary: AddressSanitizer's `malloc`/`free`/`memcpy` interceptors are process-wide, and
Rust's default allocator *is* libc `malloc`, so a double-free / heap-overflow / use-after-free on
the C-ABI structs the driver hands across the boundary — `ArrowArray`/`ArrowSchema` release
callbacks, `AdbcError` `private_data` lifecycle — is reported **with symbolized Rust frames**,
even though the cdylib itself carries no instrumentation. UBSan additionally covers UB in the
C/C++ code. A memory error that never reaches an interceptor — e.g. a plain out-of-bounds store
*inside* Rust — is invisible this way; that gap is closed by the instrumented-cdylib mode below
(and `cargo-fuzz` additionally runs the offline parser paths under ASan).

### Instrumenting the Rust cdylib itself

Set `ADBC_VALIDATION_RUST_SANITIZE=address` (alongside `ADBC_VALIDATION_SANITIZE=address`) to build
the **cdylib itself** with nightly Rust's `-Zsanitizer=address`, catching memory bugs *inside* Rust
that the C-side interceptors above cannot see:

```sh
ADBC_VALIDATION_SANITIZE=address ADBC_VALIDATION_RUST_SANITIZE=address \
  scripts/run-adbc-validation.sh
```

This needs the **nightly** toolchain with the `rust-src` component (the script uses `-Zbuild-std`
so the standard library is instrumented too — otherwise ASan misreports std allocations), and it
compiles the **C++ side with clang**: Rust's `-Zsanitizer=address` links LLVM **compiler-rt** ASan,
which must be the *same* runtime the main test executable links, so mixing it with gcc's `libasan`
is unsound and aborts at startup. The script therefore defaults `CC`/`CXX` to `clang`/`clang++`
when this is set (export your own to override). The instrumented artifact lands under
`target/<host-triple>/debug/` (an explicit `--target` is required by `-Zbuild-std`), which the
script resolves for you. Only `address` is supported — Rust has no `-Zsanitizer=undefined` cdylib
equivalent, so this leg's C++ side carries just `address` (the `asan-ubsan` leg keeps full C-side
UBSan coverage).

**Canary (positive control).** A passing `rust-asan` leg on its own does not prove the Rust
instrumentation is actually armed — a silent `-Zsanitizer=address` / `-Zbuild-std` regression would
leave the suite green and give false confidence. So this mode also builds the cdylib with
`--cfg asan_canary` (enabling `adbc_spanner_asan_canary`, an intentionally-out-of-bounds symbol in
`src/asan_canary.rs`, present in no other build — a bare `--cfg` is not set by `cargo build` or even
`--all-features`) and, before running the suite, compiles `asan_canary.cc` with clang
`-fsanitize=address` and calls that symbol against a **C++-allocated** buffer: instrumented Rust
writing one byte past C++-allocated heap. ASan must report a cross-boundary `heap-buffer-overflow`
attributed to the Rust frame; if it does **not**, the cdylib is not ASan-armed and the run script
**fails loudly** rather than passing as a no-op. The check runs right after the cdylib build (before
the slow arrow-adbc build), so a disarmed leg fails fast.

Notes:
- **LeakSanitizer is disabled** (`ASAN_OPTIONS=detect_leaks=0`, set by the script): the driver's
  shared Tokio runtime, gRPC connection pools and lazy globals are intentionally process-lifetime
  and would otherwise drown the run in non-actionable "leaks" — even more so with the Rust side
  instrumented. Memory-error (ASan) and UB (UBSan) checks stay fatal.
- aws-lc-rs' C/assembly TLS code is not instrumented (the `cc`-built C keeps its own flags and
  assembly is never instrumented); ASan tolerates this. This is also why MSan is not offered.
- Each mode uses a separate build tree (`.adbc-validation-build-san` for the C-side-only mode,
  `-rustsan` for the instrumented-cdylib mode, which also differs by compiler) so switching does
  not force an arrow-adbc rebuild.

CI runs three legs — `plain`, `asan-ubsan` and `rust-asan` — (see
`.github/workflows/adbc-validation.yml`) over the identical gate/expected-failure/stale checks. The
`asan-ubsan` leg is a strict superset of `plain`; `rust-asan` adds the in-Rust memory-error coverage
the C-side-only legs cannot provide, guarded by the cross-boundary ASan canary above so a
silently-disarmed `rust-asan` leg goes red instead of green.

## What CI gates on

- **`SpannerDatabaseTest` + `SpannerConnectionTest`** in full — lifecycle +
  metadata: `get_info`, `get_objects` (table columns, primary-key/constraint
  metadata, and foreign-key `constraint_column_usage`), `get_table_types`,
  `get_table_schema` (a plain table, `NOT_FOUND` for missing and query-shaped
  table names, and a named-schema-qualified table — Spanner has `CREATE SCHEMA`),
  autocommit/transaction options.
- **The `SpannerStatementTest` cases that pass cleanly** — `execute` and
  `execute_schema` for int/string columns and their error paths, `prepare` /
  `get_parameter_schema` / parameter-count / no-query validation, parameter
  binding end-to-end (`SqlBind`, `SqlPrepareSelectParams`), query error
  handling, trailing-semicolon queries (`SELECT current_date;;;` — the driver
  strips trailing statement terminators on the query path, which Spanner's
  single-use query API otherwise rejects), query cancellation, DML row counts
  (`SqlQueryRowsAffectedDelete{,Stream}`), manual-transaction rollback of
  buffered DML (`SqlQueryInsertRollback`), concurrent statements, result
  independence/invalidation, `AdbcError` compatibility
  (the exporter preserves a 1.0.0 caller's `private_data` — apache/arrow-adbc#4473,
  in the pinned `adbc_ffi` rev), the whole ingest **round-trip family** —
  bool/int/float/string/binary/date/timestamp columns (including the
  large/view Arrow layouts, dictionary-encoded strings and `List` columns) plus
  the append/replace/create-append modes, multi-connection visibility and the
  sample table — and the ingest **error paths** (`SqlIngestErrors`:
  ingest-without-bind → `INVALID_STATE`, append to a nonexistent table → error,
  create over an existing table → error, incompatible-schema append → error).

The gate runs **every case except the documented `EXCLUDED` expected-failures**
(see the next section), and they all pass or self-skip — today **89 cases: 82
pass, 7 self-skip**. `DatabaseTest` and `ConnectionTest` pass in full; from `StatementTest`
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

Today `EXCLUDED` holds **11** cases (89 non-excluded cases — 82 passing plus 7 that
self-skip: `Transactions`, `SqlIngestFloat16`, `SqlIngestPrimaryKey`, and the four
`SqlIngestTemporary*` — + 11 excluded = 100 upstream cases total).

Seven cases are deliberately **not** excluded because they **self-skip**, which the
gate tolerates — so they need no expected-failure bookkeeping. Each is inapplicable
to Spanner's model, so it can never "start passing", and a skip states the truth
("not applicable to Spanner") rather than implying the driver got it wrong:

- `Transactions` creates a table inside an uncommitted transaction and expects it
  hidden from other connections and removed on rollback — i.e. transactional DDL.
  Spanner has none (DDL goes through the admin `UpdateDatabaseDdl` API, auto-commits
  immediately, and cannot be rolled back), so the `ddl_implicit_commit_txn` quirk
  makes the case self-skip via its own guard.
- `SqlIngestFloat16` — Spanner has no 16-bit float type (`supports_ingest_float16`
  is `false`).
- `SqlIngestPrimaryKey` — the case append-ingests rows *omitting* the primary-key
  column and expects the database to auto-assign ascending key values. Spanner has
  no ordered auto-increment (a keyless insert mutation writes NULL, and a second
  NULL key is a duplicate-PK error; SEQUENCEs are bit-reversed, so even a DEFAULT
  key could not satisfy the case's `ORDER BY id` insertion-order readback), so
  `PrimaryKeyIngestTableDdl` returns `nullopt` — the quirk's sanctioned way to
  declare the feature absent.
- `SqlIngestTemporary{,Append,Replace,Exclusive}` — Spanner has no temporary tables
  (`supports_bulk_ingest_temporary` is `false`; the driver also rejects
  `adbc.ingest.temporary=true` outright).

The view-type and target-catalog/db-schema ingest variants are the *opposite* call:
the driver genuinely supports those inputs, so their quirks are declared `true`
rather than hiding the cases behind a false quirk — and with the `RewriteSql`
readbacks all five (`SqlIngestBinaryView`, `SqlIngestStringView`,
`SqlIngestTargetCatalog`, `SqlIngestTargetSchema`, `SqlIngestTargetCatalogSchema`)
pass cleanly and are gate-enforced.

## The `EXCLUDED` cases, by bucket

Every excluded case (runnable individually via `--full`) fails **cleanly** (no
aborts — see the note below) or self-skips; they fall into the following buckets,
none of them fixable by rewriting SQL:

- **Insertion-order readbacks** — `SqlPrepareUpdate` / `SqlPrepareUpdateStream`
  create `bulk_ingest` via create-mode ingest, `INSERT` more rows via bound
  parameters, and assert the readback returns *all* rows in insertion order.
  The `RewriteSql` overrides fix their SQL (the `INSERT` gets its required
  column list, the readback dodges the synthetic key column), but no SQL can
  recover insertion order from a Spanner table: rows come back in primary-key
  order and the synthetic `adbc_ingest_key` is a random UUID. Only the final
  row-order assertion still fails.
- **Arrow types with no Spanner column mapping** — `SqlIngestUInt8/16/32/64`,
  `SqlIngestDuration`, `SqlIngestInterval`, `SqlIngestFixedSizeBinary` fail at
  ingest time with "cannot create a Spanner column for Arrow type …". UInt64
  cannot fit INT64; UInt8/16/32 *could* widen losslessly but the driver has no
  unsigned mapping yet; Duration/Interval(MonthDayNano)/FixedSizeBinary have no
  bind/create mapping (Spanner INTERVAL is not wired up). Expected failures that
  flip to gate-enforced passes as the driver grows each mapping.
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

`SqlQueryFloats` / `SqlSchemaFloats` were formerly part of a "suite-internal
non-Spanner DDL" bucket: both hardcoded `SELECT CAST(1.5 AS FLOAT)`, and GoogleSQL
has no bare `FLOAT` type. apache/arrow-adbc#4496 added the generic
`DriverQuirks::RewriteSql(query_id, default_sql)` hook (a per-query override in the
spirit of the existing `BindParameter` dialect hook), which `SpannerQuirks`
overrides to rewrite that query to `SELECT CAST(1.5 AS FLOAT64)`. With
`ARROW_ADBC_TAG` on a revision that carries the hook, both cases now pass
and are **gate-enforced**.

That hook then dissolved the two biggest former buckets wholesale.
[apache/arrow-adbc#4514](https://github.com/apache/arrow-adbc/pull/4514) routes the
rest of the statement tests' hardcoded SQL through `RewriteSql` under stable query
ids, and the `SpannerQuirks` override substitutes GoogleSQL per id
(see the table in `spanner_validation.cc`):

- The **"suite-internal non-Spanner DDL"** bucket (`SqlBind`, `SqlQueryEmpty`,
  `SqlQueryInsertRollback`, `SqlQueryRowsAffectedDelete{,Stream}`,
  `SqlPrepareSelectParams`) — Spanner-valid `CREATE TABLE`s (`INT64` /
  `STRING(MAX)` + `PRIMARY KEY`), `INSERT`s with the column list GoogleSQL
  requires, and CASTs that give the parameter-type inference the context a bare
  `SELECT @p0, @p1` lacks. All six now pass, gate-enforced.
- The **"ingest readback"** bucket (the whole `SqlIngest*` type family plus
  `Append`/`Replace`/`CreateAppend`/`MultipleConnections`/`Sample`) — the
  `SELECT * FROM bulk_ingest … NULLS FIRST` readbacks used to trip over the
  emulator's `NULLS FIRST`/`NULLS LAST` rejection first and the synthetic
  `adbc_ingest_key` column second. The rewrites select the ingested column(s)
  explicitly and drop the NULLS clause (GoogleSQL's ASC/DESC defaults are
  exactly NULLS FIRST/NULLS LAST, so the semantics are unchanged); the
  `TestSqlIngestType` query id carries the ingested Arrow type as a suffix, which
  lets the two `List` cases order by `` `col`[SAFE_OFFSET(0)] `` instead (GoogleSQL
  cannot `ORDER BY` an ARRAY column). Alongside, `IngestSelectRoundTripType`
  declares Spanner's readback widenings (small/unsigned ints → `INT64`,
  large/view strings → `STRING`, binary variants → `BYTES`) and the fixture's
  `ValidateIngestedTemporalData` checks the timestamp values the driver returns
  (any-unit Arrow timestamps → Spanner `TIMESTAMP` → `Timestamp(Nanosecond,
  "UTC")`). Twenty-four ingest cases moved from excluded to gate-enforced (and
  `SqlIngestPrimaryKey` to a quirk-sanctioned self-skip) — with the six DDL/DML
  cases above, thirty newly enforced cases in all.

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

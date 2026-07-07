# Repo review — multi-aspect, prioritized findings

*Review date: 2026-07-07, against main at `eb17ef2` (v0.5.0 + 48 commits). Produced by eight parallel
specialized reviews — correctness, performance, security, testing, documentation, ADBC conformance,
maintainability, and features / CI-release — each of which read the relevant sources in full and
cross-checked claims against the pinned `google-cloud-rust` client checkout and the `adbc_core` /
`adbc_ffi` sources. Findings that two reviews reached independently are marked as such.*

**Overall:** the driver is in strong shape — canonical ADBC schemas used verbatim, a strict
decode-or-error read path, a single well-fuzzed GoogleSQL lexer, centralized error mapping with
vendor codes preserved, panic-free FFI discipline, differential-oracle tests against the official
Python client, and a hardened release pipeline (SHA-pinned actions, OIDC trusted publishing,
version gates). The findings below are what stands between the current state and a driver that is
safe under retries, conformant for metadata round-trips, and scalable for its headline ingest use
case.

---

## P1 — fix first

### ~~1. A failed `commit()` discards the buffered DML; a retried `commit()` falsely reports success~~ (fixed)

**Fixed.** `commit()` and the autocommit-enable path now apply from a clone of the buffer and
drain it only after a successful apply, so a failed commit keeps the transaction open for a
genuine retry (or `rollback`). Covered by the failed-commit section of the manual-transaction
integration test (SQL-level failure: fail → retry fails again, not vacuous success → rollback →
clean commit) and by `commit_under_transport_fault_never_loses_the_write` in
`tests/resilience.rs`.

### ~~2. `get_table_types` and `get_objects` disagree on table-type vocabulary; the spec round-trip returns zero tables~~ (fixed)

**Fixed.** `get_table_types` now returns `["BASE TABLE", "VIEW"]` — Spanner's own
`INFORMATION_SCHEMA.TABLES.TABLE_TYPE` vocabulary, which is what `get_objects` reports per table
and what the statistics query filters on — so every reported value round-trips as a `get_objects`
`table_type` filter. Covered by a round-trip assertion in the main integration test (filter
`get_objects` by the types `get_table_types` reports; the known base table must come back).

### 3. Bulk ingest is one DML statement per row in one unchunked transaction (performance + features, found independently by both)

`src/statement.rs:155-170` (`build_ingest_statements`) + `src/connection.rs:449-481`
(`run_batch_dml`). An N-row ingest materialises N `INSERT` statements up front (O(N × row width)
memory before anything is sent, doubled by the retry closure's `statements.clone()`), then ships
them all in a **single** `ExecuteBatchDml` transaction with no chunking against Spanner's hard
limits (~80k mutations — DML counts roughly rows × columns — and the ~100MB commit cap). An ingest
of 10k rows × 10 columns is already at the cliff: it fails outright and the user has no recourse
but manual slicing. The Mutation API (`Mutation::new_insert_builder`, `apply`,
`batch_write_transaction` — all exposed by the pinned client) is entirely unused (`grep -rni
mutation src/` is empty). Fix, staged: (a) chunk the DML batch under the commit limits now — this
is a correctness-at-scale fix, not just speed; (b) move ingest to mutations (append/replace map
naturally), optionally BatchWrite behind an option for non-atomic firehose loads.

### 4. Nothing in the tag→publish path runs a single test (CI/release)

`ci.yml` triggers on pushes to main and PRs only — not tags. `libraries.yml` (the only workflow
tags trigger) contains zero test steps, and `python-publish` depends only on compile+repackage
jobs; `cargo-release` has no `pre-release-hook`. Since `cargo release --execute` pushes commit and
tag together, the pipeline will publish to PyPI (irreversible) and attach GitHub Release assets
even if that commit is red on CI or CI hasn't finished. Fix (any of): a smoke test in the `build`
job on tag refs (install the built lib into `python/`, run the emulator e2e suite on linux
x86-64), make `python-publish` wait for the commit's `ci.yml` check to succeed, or minimally add
`pre-release-hook = ["cargo", "test"]`.

### 5. The emulator test suites can all silently skip: one YAML typo turns CI green with zero behavioral coverage (testing)

`tests/integration.rs:85-115`, `tests/resilience.rs:65-75`, `python/tests/conftest.py:75-77`.
Every functional suite self-skips when `SPANNER_EMULATOR_HOST` / `TOXIPROXY_URL` is unset. CI
guards emulator *liveness* but not the *env wiring*: drop or misspell the `env:` line in a
workflow refactor and all 13 integration tests, the Python e2e/oracle suites, and both resilience
tests pass vacuously — and these suites are the only coverage for `statement.rs`, transactions,
ingest, partitions and cancellation. Fix: an `ADBC_TEST_REQUIRE_TARGET=1` variable set in CI that
makes the gates `panic!`/`pytest.fail` instead of skipping (also apply it to the silently-optional
cdylib-path skip in the FFI tests, `tests/integration.rs:1469-1485`).

### 6. The README's Rust quickstart no longer compiles; the Python README documents the wrong ingest capability (docs)

- `README.md` (Usage) tells users `adbc_core = "0.23"` from crates.io, but `Cargo.toml` now pins
  `adbc_core`/`adbc_ffi` to a git fork (`fornwall/arrow-adbc`, rev `786e7f3…`). Cargo does not
  unify a git source with a registry source, so the quickstart's trait imports are different types
  from the ones `SpannerDriver` implements — it cannot compile, and the crate doesn't re-export
  `adbc_core` as a workaround. Fix: pin the snippet to the same git rev or re-export `adbc_core`;
  mention the arrow-adbc pin alongside the google-cloud one in the "not on crates.io" note.
- `python/README.md:147` says "only append mode is supported" — stale; the driver supports
  `append`/`create`/`create_append`/`replace` with a synthetic `adbc_ingest_key` UUID primary key
  in the create modes (`src/statement.rs:407-420`, `src/bind.rs:655-712`). Document all four modes
  and the synthetic-key caveat (it appears in `SELECT *`).

### 7. Stale reads / timestamp bounds are unexposed (features)

Every query uses `client.single_use()` with strong reads (`src/statement.rs:330`, `:677`,
`src/connection.rs:485`); no staleness option exists. Stale reads are one of Spanner's signature
features — cheap, lock-free reads for exactly the analytics audience an Arrow driver serves — and
they pair naturally with the already-shipped Data Boost partitioned reads. The pinned client fully
exposes `TimestampBound::{strong, exact_staleness, max_staleness, read_timestamp,
min_read_timestamp}` on single-use, multi-use and batch read-only builders. Fix: add
`spanner.read.staleness` / `spanner.read.timestamp` connection+statement options and plumb them
into the three `single_use()` call sites plus the partition path. Low–medium effort, high value.

---

## P2 — should fix

### Correctness

- **`split_statements` emits comment-only segments as statements** (`src/ddl.rs:268-274`).
  `execute_update("DELETE FROM t1; DELETE FROM t2; -- cleanup")` sends `"-- cleanup"` as a third
  DML statement → the whole batch fails with `INVALID_ARGUMENT` (in manual mode it is buffered
  silently and only fails at commit). Same for DDL batches, and `"SELECT 1; -- done"` fails where
  `"SELECT 1;"` works because the trailing-terminator strip sees two statements. Fix: drop
  segments that are only whitespace/comments (lex-aware `push_statement`).
- **Manual transactions have no read-your-writes, and DML/DDL reorder** (`src/connection.rs:8-23`,
  `src/statement.rs:200-217`; also flagged by the conformance review). DML is buffered while
  queries run immediately in a fresh read-only snapshot, so `INSERT` → `SELECT COUNT(*)` inside one
  "transaction" silently returns the pre-insert count, and DDL issued after buffered DML executes
  before it. This is a deliberate, documented consequence of the preview client's closure-only
  read/write API — but users reaching the cdylib via Python DBAPI/dbt never see the rustdoc.
  Mitigate now: document at the Python/README level, and consider rejecting (or warning on)
  queries while DML is buffered. Fix properly when the client exposes begin/commit handles.

### ADBC conformance

- **Ingest append doesn't use the spec-mandated error statuses** (`src/statement.rs:155-170`).
  Missing table should be `Status::NotFound`, schema mismatch `Status::AlreadyExists`; today both
  surface as generic mapped Spanner errors. The C++ validation quirks file already admits this
  (`supports_error_on_incompatible_schema() { return false; }`), and `get_table_schema` already
  implements the needed probe-and-remap pattern (`src/connection.rs:879-894`) — reuse it with
  `table_exists` on the ingest error path.
- **Bulk ingest only triggers through `execute_update()`, not `execute()`**
  (`src/statement.rs:513-516`). An FFI caller doing ingest with a non-null stream out-pointer gets
  `InvalidState` ("no SQL query set") instead of an ingest + empty stream. Mirror the
  `execute_update` ingest branch in `execute()` and return `Self::empty_reader()`.
- **`adbc.ingest.target_db_schema` is rejected although the driver supports named schemas
  everywhere else** (`src/statement.rs:405-437`). Accept it and qualify the ingest/CREATE TABLE
  statements via `qualified_table`; accept `target_catalog` when it names the `""` catalog.
- ~~**Standard isolation-level option rejected despite client support**
  (`adbc.connection.transaction.isolation.*` falls into the unknown-key arm,
  `src/connection.rs:745-750`). Spanner supports `REPEATABLE_READ` GA alongside `SERIALIZABLE`,
  and the client exposes `TransactionRunnerBuilder::set_isolation_level`. Low effort.~~
  **Fixed.** `adbc.connection.transaction.isolation_level` is now accepted: `serializable` and
  `repeatable_read` map to the client's `IsolationLevel` and are applied via
  `TransactionRunnerBuilder::set_isolation_level` at every read/write runner site (autocommit DML,
  `THEN RETURN` DML, and manual-mode commit); `default` leaves the database default; the other
  spec levels (read_uncommitted / read_committed / snapshot / linearizable) are rejected with
  `NotImplemented`. The stored level round-trips through `get_option`.

### Performance

- **`bind_params` re-lexes the entire SQL text once per bound row** (`src/statement.rs:113-122` →
  `src/bind.rs:73-109`). A 50k-row bound DML lexes the same SQL 50k times — O(rows × |sql|) CPU
  before the first RPC. Resolve the parameter-name mapping once per (sql, schema) and pass it into
  a per-row loop.
- **String columns pay an extra allocation + copy per value in the read hot path**
  (`src/conversion.rs:450-456`). The `Utf8` arm builds an owned `String` per value, then
  `StringArray::from_iter` copies again — one avoidable malloc+memcpy per non-null string (~8k per
  column per default batch), on Spanner's most common type. Use a `StringBuilder` and
  `append_value(&str)`, mirroring the `BinaryBuilder` arm above it.
- **`rows_per_batch` bounds rows, not bytes** (`src/conversion.rs:98-108`). 8192 rows of
  `STRING(MAX)`/`BYTES(MAX)` (up to 10MB each) can be tens of GB per chunk, held roughly twice
  during conversion. Track approximate cumulative bytes in `pull_chunk` and cut the chunk early at
  a byte budget (16–64MB) in addition to the row cap.
- **`get_objects` assembles the hierarchy with quadratic rescans** (`src/connection.rs:218-267`,
  `:507-590`, `:598-665`). The RPC side is a fixed 6 queries (good), but per table the full
  COLUMNS/TABLE_CONSTRAINTS batches are rescanned and per constraint the full KEY_COLUMN_USAGE
  batch — O(10⁸) string comparisons for a 2k-table schema. Group each batch once into `HashMap`s,
  exactly the pattern `collect_statistics` already uses (`connection.rs:329-335`).

### Security

- **`read_partition` executes an unauthenticated, attacker-suppliable request blob**
  (`src/connection.rs:979-1000`; descriptor from `src/statement.rs:699-715`). The descriptor is
  plain serde-JSON of the client's `Partition`, whose inner `ExecuteSqlRequest` carries the SQL
  text itself — a crafted blob runs arbitrary SQL with the connection's credentials. This is
  inherent to the upstream serde format and to ADBC's portable-descriptor design, so the realistic
  fix is documentation (descriptors are executable request blobs; transport them only over trusted
  channels), optionally an HMAC envelope for stronger guarantees.

### Testing

- **`adbc.connection.readonly` has zero tests** anywhere (four enforcement branches at
  `src/statement.rs:352, 529, 582, 606`). A regression silently allowing writes on a read-only
  connection would ship. Add an integration case covering allow/deny/toggle/round-trip.
- **`create_append` ingest mode is never executed end-to-end**, nor are ingest error paths
  (`create` on existing table, `append` on missing table, schema mismatch)
  (`tests/integration.rs:770-775`).
- **Resilience suite misses some likely production failures** (`tests/resilience.rs`):
  mid-stream disconnect after batches were consumed, latency/timeout toxics (nothing bounds the
  wait — building the commit-fault test confirmed the client marks all data-plane RPCs idempotent
  and retries transport faults with no attempt cap, so a commit under a persistent fault blocks
  unboundedly; see the P3 timeout/retry-tuning feature), and truncated streams. (Faults during a
  manual-transaction commit are now covered by
  `commit_under_transport_fault_never_loses_the_write`.)
- **`statement.rs` has zero unit tests**; the option-coercion error paths (`rows_per_batch=0`,
  bad bools, negative `max_partitions`, unknown-option statuses) are pure functions guarding the
  C-ABI boundary and are unit-testable offline.

### CI / release / packaging

- **`manylinux_2_35` floor excludes RHEL 9, Amazon Linux 2023, Debian 11, Ubuntu 20.04**
  (`libraries.yml:254-255`; the matrix comment already names the fix). Build in a
  `manylinux_2_28` container and lower the tag + verify step together.
- **No musl (Alpine) build/wheel** — `pip install` fails on Alpine-based data-service images.
- **Wheels are published without ever being installed or inspected** — no `twine check`, no
  unzip-and-assert-the-lib-is-inside, no `pip install` + import smoke test; the CI python job
  installs from the source tree, a different packaging path. All cheap on the same runner.
- **The tag-vs-version gate lives only in `python-wheels`**; the `release` job (GitHub assets)
  has no gate, and `release` / `python-publish` are unordered siblings — a partial failure yields
  a PyPI version with no GitHub assets or vice versa. Hoist the check into a `version-gate` job
  both need; run `python-publish` (the irreversible step) last.
- **No CI ever touches real Cloud Spanner** — `SPANNER_GCP_DATABASE` support exists but appears in
  no workflow; auth paths (ADC, impersonation) are untested in CI since the emulator is anonymous.
  A scheduled non-gating workflow via GitHub OIDC → Workload Identity Federation would cover it.

### Maintainability

- **Bool/int/string option parsing is copy-pasted three times** with slightly different error text
  (`src/driver.rs:506-522`, `src/connection.rs:1003-1015`, `src/statement.rs:789-826`); only the
  database copy is fuzzed. Extract a shared `src/options.rs`.
- **`build.rs` panics without `Cargo.lock`, which is excluded from the published package**
  (`build.rs:14-15`, `Cargo.toml:17`) — a landmine wired to the planned "re-enable publish" step.
  Fall back gracefully, or leave a loud comment next to `publish = false`.
- **CLAUDE.md has drifted from ground truth** (also found by the docs review): arrow is now
  `>=58, <60` not `>=53.1, <59`; `adbc_core`/`adbc_ffi` are git-pinned to `fornwall/arrow-adbc`
  (a second publish blocker the "temporary git pin" section doesn't mention); the library matrix
  is six targets, not four, and macOS x86-64 is cross-compiled.
- **The two git-pin revs are spread across ~9 dependency lines** plus `deny.toml` plus docs, with
  no single anchor for the scheduled "revert when upstream releases" edit.
- **`connection.rs` (1050 lines) owns the query half of `get_objects`/`get_statistics`** while
  `objects.rs`/`statistics.rs` hold only the Arrow-assembly half — split along the wrong axis.
  Move the INFORMATION_SCHEMA collectors into their feature modules. (`bind.rs` at 1359 lines is
  fine — ~600 lines are tests and the rest is one cohesive concern.)

### Docs

- **`adbc.connection.readonly` is accepted but documented nowhere** (README, python/README,
  CLAUDE.md, `src/lib.rs` — all silent).
- **Ingest modes + synthetic `adbc_ingest_key` PK missing from the main README**; the emulator
  port-9010 requirement lives only in CLAUDE.md (users on another port get working queries and
  cryptic DDL failures); manual-mode `execute_update` returning `None` is in CLAUDE.md but not
  the README.

---

## P3 — nice to have

**Correctness.** List/Struct columns map present-but-undecodable wire values to NULL,
contradicting the strict-decode policy of the scalar arms (`src/conversion.rs:466-474`,
`:493-513`); `parse_int64`'s f64 fallback loses precision above 2^53 (`conversion.rs:527-532` —
better removed); `is_dml_returning`'s documented false positive (`CASE WHEN c THEN return …`)
hard-errors valid DML in manual mode (`src/ddl.rs:49` + `src/statement.rs:289-295`); `read_only`
is snapshotted into statements at creation, so flipping the connection option leaves existing
statements writable (`src/connection.rs:792-801` — flagged independently by correctness,
conformance and maintainability; share via `SharedTxn`/`AtomicBool`); the autocommit-enable path
reads/takes/flips in separate lock acquisitions, so a concurrently-buffering statement can strand
DML (`connection.rs:734-742`); `get_statistics` breaks for the whole database if any table has a
`TOKENLIST`/`PROTO` column (`is_groupable` only excludes ARRAY/STRUCT/JSON,
`connection.rs:439-442`); `str_col`'s `RecordBatch::column(i)` can panic instead of erroring on a
zero-column metadata batch (`connection.rs:494-505`); `execute_bound_query` runs each bound row in
its own snapshot (mutually inconsistent results) and materialises everything ignoring
`rows_per_batch` (`src/statement.rs:319-345`).

**Performance.** `get_statistics` per-table scans run strictly sequentially — a small
`buffer_unordered(4..8)` would cut wall-clock near-linearly (`connection.rs:337-359`);
`execute_partitions` pays an extra PLAN round trip for the schema (unavoidable until the client
surfaces partition metadata); binary parameters take a forced `to_vec()` copy (upstream `ToValue`
limitation).

**Security.** Emulator mode (`SPANNER_EMULATOR_HOST`) silently forces anonymous credentials +
plaintext `http://`, overriding configured keyfiles — an env-controlled downgrade footgun;
consider refusing (or warning) when explicit credentials were also configured
(`src/driver.rs:150-187`). Credential-building errors interpolate the auth crate's `Display`
output, which is outside this crate's control (`driver.rs:436-473`); scrub or verify it never
embeds key material.

**Conformance.** `adbc.ingest.temporary="false"` (the default value) is rejected instead of
no-op'd; `get_info` could report a vendor version instead of null; the upstream `adbc_ffi` shim
rejects 1.0.0 driver managers and errors on unknown `get_info` codes (both stricter than the C
spec — upstream issues, worth tracking); no `sqlstate` on errors (a coarse mapping would help
ODBC bridges); `get_table_schema` ignores the catalog argument entirely; `get_objects` at
`Catalogs` depth still runs the SCHEMATA query; `execute_schema` lets DML through to a PLAN probe
whose read-only-transaction error is surfaced raw.

**Testing.** Doctests never run in CI (`cargo test --doc` is absent; CLAUDE.md implies otherwise —
also flagged by the CI review, which notes `cargo doc` runs without `--all-features` unlike
docs.rs); keyfile/impersonation auth is offline-unit-tested only, never exercised end-to-end;
weak assertions (`AdbcDdl` note value unchecked, `replace`-ingest values unchecked, only row
counts); untested small surfaces: `rollback()` without a transaction, `get_statistic_names`,
`read_partition` with a garbage descriptor (also a natural fuzz target), `Connection::cancel`;
fuzz gaps: statement-hint parsing (`first_keyword` — this exact code had a real bug),
`strip_trailing_terminators`, parameter-name extraction, `quote_ident`, partition-descriptor
deserialization; `tests/resilience.rs` mutates process env in setup — safe only under
`--test-threads=1`, worth asserting.

**Features** (all exposed by the pinned client unless noted): request priority / request tags /
transaction tags (`StatementBuilder::set_priority/set_request_tag`,
`set_transaction_tag`); request/attempt timeouts and retry tuning (today a hung network path
blocks `block_on` indefinitely unless a second thread cancels); PostgreSQL-dialect databases are
unsupported *and undetected* — minimum viable is probing the dialect once and failing fast with a
clear error; OAuth access-token auth (needs a small custom credentials impl — the auth crate has
no static-token builder); query options (optimizer version), directed reads, commit stats,
`max_commit_delay`, `last_statement` optimization (free RPC saving for single-statement
autocommit DML); proto/enum columns (verify clean failure today); change streams and GQL graph
queries may already work through plain SQL — one emulator test each would let the README claim
them; telemetry/tracing hooks as a backlog entry.

**CI/misc.** No dependabot/renovate (SHA-pinned actions and the two git-pin families drift
unmonitored); no `concurrency:` groups (rapid PR pushes re-run the full 6-platform matrix);
nightly fuzz/resilience failures surface only as email and scheduled workflows auto-disable after
60 days of inactivity — a `failure()` step opening a tracking issue would help;
`foundry-validation` ends in `|| true`, making harness breakage indistinguishable from expected
dialect failures; the Windows import-lib copy is `|| true`-optional (`libraries.yml:138`); the
wheel version parse greps `Cargo.toml` positionally — `cargo metadata | jq` is robust;
`adbc-validation.yml` rebuilds arrow-adbc C++ + GoogleTest from source every run (cache it); the
adbc-validation allowlist means new upstream tests never auto-enter the gate (periodic `--full`
triage is manual); dead crates.io/docs.rs badges in the README; no consolidated
connection/statement option tables in either README (`spanner.rows_per_batch` is missing from the
Python one entirely); wheel docs omit the platform floors (glibc ≥ 2.35, macOS ≥ 11/10.15); no
CHANGELOG/CONTRIBUTING/versioning policy; `#![warn(missing_docs)]` absent; assorted maintainability
polish: the four direction-specific copies of the type mapping (`bind_one` vs `bind_list` is a
genuine 100-line duplication — fold via an element visitor, and add an "adding a type touches
these N sites" checklist), three hand-rolled comment-skipping lexer walkers that could share one
token iterator, ingest-mode strings matched in two places (make it an enum), positional column
indices into INFORMATION_SCHEMA batches, `get_option_int` inconsistencies (database vs statement,
and "not an integer" reported as `NotFound`), SQL-text helpers scattered across three modules.

---

## Strengths (keep doing this)

- **Strict decode policy**: present-but-undecodable wire values are loud `InvalidData` errors,
  never silent NULLs, with out-of-range TIMESTAMP distinguished from malformed input, and NUMERIC
  handled exactly over the full `i128` range.
- **One well-tested GoogleSQL lexer** (`ddl.rs::consume_quoted`) shared by splitting, `THEN
  RETURN` detection and `@param` extraction — triple-quoted/raw strings, all comment forms,
  statement hints — backed by fuzz targets with independent oracles (regex oracle for `like`,
  parse→render→re-parse for values) and a differential oracle against the official Python client.
- **Conformance discipline**: canonical `adbc_core` schemas used verbatim and asserted in tests;
  correct option-status semantics (NotImplemented on unknown set, NotFound on unset get) at all
  three levels, unit-tested and fuzzed through the C-ABI-shaped path; autocommit defaults on;
  cancel is a carefully-latched, reset-on-next-op signal that leaves objects usable.
- **Centralized, tested error mapping** with the gRPC code preserved in `vendor_code` (including
  the thoughtful `ABORTED → IO` choice) and no ad-hoc error construction outside `error.rs`.
- **Panic-free FFI**: no crate-local `unsafe`; upstream `adbc_ffi` catches panics at every extern
  entry point; `nested.rs` converts shape errors to `Status::Internal` instead of panicking.
- **Hardened supply chain and release pipeline**: full-SHA git pins enforced by `deny.toml`
  (`unknown-git = "deny"`), top-level `permissions: contents: read` with single-job elevations, no
  `pull_request_target`, SHA-pinned actions in the artifact-shipping path, artifact-poisoning
  guarded by scoped download patterns, OIDC trusted publishing, and a wheel that loads only its
  own bundled library.
- **The right streaming architecture**: lazy `SpannerBatchReader`, bounded chunks, schema settled
  from the first data chunk with zero extra RPCs, one `block_on` per chunk (never per row), locks
  never held across `block_on`.
- **"Why" comments at every non-obvious constraint** (buffer-and-commit model, sticky cancel,
  decode policy, ABORTED mapping) — the single biggest maintainability asset in the repo.
- **Honest scoping**: RESILIENCE.md and the validation allowlist document what is *not* covered
  and why; the README's type-mapping section is exemplary; the Python cookbook is executed in CI
  so it cannot rot.

## Verified non-issues

- **SQL identifier injection is closed**: every identifier interpolation goes through
  `quote_ident` (backtick + backslash escaping, tested); catalog queries are static SQL with
  filters applied in Rust (`like_match`, which is iterative and DoS-safe); values go through bound
  params. No injection vector found.
- **Partition descriptor lifetime holds**: the descriptor carries its own session name and the
  client permits any `DatabaseClient` on the same database to execute it — the cross-statement /
  cross-connection assumption is sound (the *trust* caveat is the P2 above).
- **`ExecuteBatchDml` partial failure surfaces as `Err`** — no half-applied batch can be reported
  as success; the client's `ResultSet::next` is fused, so re-polling after exhaustion or
  mid-stream error is safe; stream resumption via resume tokens is handled inside the client.
- **`CancelSignal`'s store-before-notify + enable-then-recheck pattern** correctly closes the
  lost-wakeup race.
- **No heavyweight duplicate dependencies** in the lockfile; single versions of arrow, tonic,
  aws-lc-rs; `ring` in `Cargo.lock` is unreferenced residue. Feature hygiene and `pub(crate)`
  discipline are clean; shell scripts all use `set -euo pipefail` with real readiness gates.
- **Keyfile JSON bodies never appear in error messages** (only the path and detected type);
  `SpannerDatabase` has no `Debug` impl that could print secrets.
- **PyPI trusted publishing, the tag-vs-version wheel gate, and the build→wheels artifact
  handoff** were re-verified and remain correct (carried over from the previous review).

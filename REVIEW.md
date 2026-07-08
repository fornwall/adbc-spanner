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

### ~~3. Bulk ingest is one DML statement per row in one unchunked transaction~~ (fixed — stage (a); stage (b), the Mutation API, remains open) (performance + features, found independently by both)

**Fixed (stage (a): chunking).** Autocommit bulk ingest now builds and ships its `INSERT`s **chunk
by chunk** (`SpannerStatement::run_ingest_dml` in `src/statement.rs`), each chunk applied in its own
read/write `ExecuteBatchDml` transaction via the shared `run_batch_dml`. Chunk boundaries come from
`IngestChunkBudget`, a pure offline-tested accumulator that cuts when the next row would exceed
either a mutation budget (`INGEST_CHUNK_MUTATION_LIMIT` = 20,000, counted as rows × columns — a
quarter of Spanner's ~80k commit cap, leaving headroom for the secondary-index entries the driver
cannot see) or an approximate byte budget (`INGEST_CHUNK_BYTE_BUDGET` = 4 MiB, estimated from the
Arrow batch's memory footprint per row — well under the ~100MB commit cap and gRPC request limits);
a single row larger than the whole budget still forms its own one-row chunk. This also removes the
up-front O(N) materialisation: only one chunk of statements exists (and is cloned by the runner's
retry closure) at a time. The affected-row count is summed across chunks. An ingest that fits one
chunk — the common case, and anything that could have committed before — is still a single atomic
transaction; a multi-chunk ingest is **not atomic as a whole** (a mid-ingest failure leaves earlier
chunks committed), documented in the rustdoc, README and CLAUDE.md. Manual-transaction mode is
deliberately unchunked and unchanged: it buffers the rows for `commit`, which applies the user's
whole transaction atomically (chunking there would break the transaction contract), and read-only
enforcement and the append-mode NotFound/AlreadyExists remap are untouched. Covered by chunk-
boundary unit tests in `src/statement.rs` (mutation-limit cut, byte-budget cut, oversized-row
never-starves) and by `bulk_ingest_chunks_past_the_byte_budget` in `tests/integration.rs` (six
~1 MiB rows cross the byte budget → multiple transactions; count sums and every row lands exactly
once). **Still open — stage (b):** move ingest to the Mutation API (`Mutation::new_insert_builder`,
`apply` — append/replace map naturally; mutations are cheaper than DML and raise the effective
per-commit capacity), optionally `batch_write_transaction` (BatchWrite) behind an option for
non-atomic firehose loads; all exposed by the pinned client and still unused.

### ~~4. Nothing in the tag→publish path runs a single test (CI/release)~~ (fixed)

**Fixed.** Two independent guards now stand between a red commit and an irreversible publish. (1) A
`pre-release-hook` in `[package.metadata.release]` (`Cargo.toml`) runs `cargo fmt --all --check &&
cargo clippy --all-targets --all-features -- -D warnings && cargo test` before cargo-release mints
the tag, so `cargo release --execute` refuses to commit/tag/push if the local checks CI enforces
fail (the emulator-gated integration/resilience suites self-skip offline, so this covers the unit
tests + doctests locally). (2) A new `ci-gate` job in `libraries.yml` — the only tag-triggered
workflow — polls `ci.yml`'s run for the tagged `github.sha` (the branch push cargo-release makes
alongside the tag triggers `ci.yml` on that same commit) and only succeeds once it has concluded
`success`, timing out (and failing) after 30 minutes otherwise. Both `release` (GitHub Release
assets) and `python-publish` (PyPI) now `needs: [..., ci-gate]`, so neither the asset upload nor
the irreversible PyPI publish can proceed until the full CI run — emulator suites included — is
green on that exact commit. The gate uses only `gh api` + `jq` with a job-scoped `actions: read`
permission (no new third-party action in the publish path).

### ~~5. The emulator test suites can all silently skip: one YAML typo turns CI green with zero behavioral coverage~~ (fixed) (testing)

**Fixed.** A new opt-in env var, `ADBC_TEST_REQUIRE_TARGET`, flips every skip gate from
self-skip to fail-loud. When it is truthy (`1`/`true`/`yes`) and the target env wiring is missing,
the gates `panic!` (Rust) / `pytest.fail` (Python) with a clear message instead of returning; when
it is unset the behavior is unchanged, so a plain `cargo test` / `pytest` stays green everywhere.
Kept DRY by putting the check in the shared resolvers: `tests/integration.rs`'s `test_target()`
(the single gate all 15+ integration tests funnel through) and `tests/resilience.rs`'s `toxi()` now
delegate to an inner `resolve_*` and panic when it returns `None` under the flag; the FFI
cdylib-path skip is covered by a new `required_cdylib_path()` wrapper used by both driver-manager
tests; and `python/tests/conftest.py`'s `emulator_database` fixture calls `pytest.fail` under the
flag. CI sets `ADBC_TEST_REQUIRE_TARGET: "1"` on the `test` and `python` jobs in `ci.yml` and on the
`resilience` job in `resilience.yml`, so a dropped/misspelled `SPANNER_EMULATOR_HOST` / `TOXIPROXY_URL`
now fails the run instead of passing vacuously.

### ~~6. The README's Rust quickstart no longer compiles; the Python README documents the wrong ingest capability~~ (fixed) (docs)

**Fixed.** The `README.md` quickstart now pins `adbc_core` to the same `fornwall/arrow-adbc` git
revision (`786e7f3…`) as `Cargo.toml`, with an inline comment explaining that Cargo will not unify a
git source with the crates.io `= "0.23"` release, so downstream crates must take `adbc_core` from
that same git rev; the "not on crates.io" note (Usage) and the *Type mapping* note now mention the
arrow-adbc pin alongside the google-cloud one. `python/README.md` no longer claims "only append mode
is supported": it documents all four modes (`append`/`create`/`create_append`/`replace`) and the
synthetic `adbc_ingest_key` UUID primary key the create modes add (and that it appears in
`SELECT *`).

### ~~7. Stale reads / timestamp bounds are unexposed~~ (fixed)

**Fixed.** Two options — `spanner.read.staleness` and `spanner.read.timestamp` — are now honoured at
both connection and statement level (a statement inherits the connection's bound and may override
it). `spanner.read.staleness` is `exact:<duration>` / `max:<duration>` (duration with an optional
`s`/`ms`/`us`/`ns`/`m`/`h` suffix) → `TimestampBound::{exact,max}_staleness`; `spanner.read.timestamp`
is an RFC 3339 timestamp, optionally prefixed `read:` (default) or `min:` →
`TimestampBound::{read,min_read}_timestamp`. The two are mutually exclusive — setting one while the
other is active is rejected with `InvalidArgument` (unset the other with an empty value first) — and
both round-trip through `get_option` (NotFound when unset). The bound is applied at the read-only
query `single_use()` sites (`execute`, bound queries, the `execute_partitions` PLAN probe, and the
connection's `get_table_schema` / statistics table scans) and, for partitioned reads, baked into the
batch read-only transaction so every partition executes at that bound. Parsing lives in
`src/staleness.rs` (`ReadBound` / `ReadStaleness`) with offline unit tests
(`parses_exact_and_max_staleness_with_units`, `parses_read_and_min_timestamp`,
`rejects_bad_staleness`, `rejects_bad_timestamp`, `mutually_exclusive_options_conflict_and_can_be_switched`).

---

## P2 — should fix

### Correctness

- ~~**`split_statements` emits comment-only segments as statements** (`src/ddl.rs:268-274`).
  `execute_update("DELETE FROM t1; DELETE FROM t2; -- cleanup")` sends `"-- cleanup"` as a third
  DML statement → the whole batch fails with `INVALID_ARGUMENT` (in manual mode it is buffered
  silently and only fails at commit). Same for DDL batches, and `"SELECT 1; -- done"` fails where
  `"SELECT 1;"` works because the trailing-terminator strip sees two statements. Fix: drop
  segments that are only whitespace/comments (lex-aware `push_statement`).~~ (fixed)
  **Fixed.** `push_statement` now runs each segment through a shared
  `skip_leading_whitespace_and_comments` lexer helper (extracted from `first_keyword`) and drops it
  when nothing but whitespace and `--`/`#`/`/* … */` comments remains, so a trailing/interleaved
  comment-only segment produces no statement while `SELECT 1 -- done` (real SQL then a comment) is
  kept. `strip_trailing_terminators("SELECT 1; -- done")` now yields `"SELECT 1"` like
  `"SELECT 1;"`. Covered by the `split_drops_comment_only_segments` unit test in `src/ddl.rs`.
- ~~**Manual transactions have no read-your-writes, and DML/DDL reorder** (`src/connection.rs:8-23`,
  `src/statement.rs:200-217`; also flagged by the conformance review). DML is buffered while
  queries run immediately in a fresh read-only snapshot, so `INSERT` → `SELECT COUNT(*)` inside one
  "transaction" silently returns the pre-insert count, and DDL issued after buffered DML executes
  before it. This is a deliberate, documented consequence of the preview client's closure-only
  read/write API — but users reaching the cdylib via Python DBAPI/dbt never see the rustdoc.
  Mitigate now: document at the Python/README level, and consider rejecting (or warning on)
  queries while DML is buffered. Fix properly when the client exposes begin/commit handles.~~ (fixed)
  **Fixed** (the documentation mitigation; no behavior change). Both consequences are now spelled
  out everywhere a cdylib/DBAPI user would look: `python/README.md` gained a "Manual transactions:
  no read-your-writes" section — right where the autocommit-off DBAPI default is introduced — with
  a CI-executed snippet (via `test_readme_cookbook.py`) showing `INSERT` → `SELECT COUNT(*)`
  asserting the *pre-insert* count until `conn.commit()`, plus the DML/DDL-reordering note and a
  cross-reference from the Cookbook's "Two things to know"; the `README.md` Transactions bullet
  names both consequences; and since `mod connection` is private (its module docs never render on
  docs.rs), the rustdoc now also lives on the public `SpannerConnection` struct and in the `lib.rs`
  crate docs (new *Transactions* section), with the `connection.rs` module-doc bullet extended to
  cover the DDL reordering. Rejecting (or warning on) queries while DML is buffered was considered
  and deliberately **not** implemented — it would break legitimate read-then-buffered-write use;
  the proper fix still waits on the client exposing begin/commit handles.

### ADBC conformance

- ~~**Ingest append doesn't use the spec-mandated error statuses**~~ (`src/statement.rs`).
  **Fixed.** On an `append`-mode ingest failure the driver now probes the target table (via the
  shared `connection::table_exists` helper, extracted from the `get_table_schema` probe-and-remap so
  the query is not duplicated) and remaps: a missing table → `Status::NotFound`, an existing table
  (so the insert failed on an incompatible schema) → `Status::AlreadyExists`. A probe that itself
  errors is surfaced rather than masked, and only the `append` path is remapped
  (`create`/`create_append`/`replace` are untouched). The C++ validation quirks
  `supports_error_on_incompatible_schema()` is flipped to `true`, and the upstream
  `SpannerStatementTest.SqlIngestErrors` case now passes end-to-end against the emulator;
  `tests/integration.rs` also covers append-on-missing-table (NotFound) and append-with-mismatched-
  schema (AlreadyExists).
- ~~**Bulk ingest only triggers through `execute_update()`, not `execute()`**
  (`src/statement.rs:513-516`). An FFI caller doing ingest with a non-null stream out-pointer gets
  `InvalidState` ("no SQL query set") instead of an ingest + empty stream. Mirror the
  `execute_update` ingest branch in `execute()` and return `Self::empty_reader()`.~~
  **Fixed.** The ingest branch is now a shared `SpannerStatement::run_ingest` helper (extracted from
  `execute_update`, so the two paths cannot drift), and `execute` dispatches to it before parsing
  SQL: when a `target_table` is configured — and no SQL query has been set — it runs the ingest and
  returns `Self::empty_reader()` (an empty Arrow stream) instead of erroring with `InvalidState`.
  Both entry points gate the ingest branch on `sql.is_none()`. A SQL query and an ingest target are
  kept **mutually exclusive** — `set_sql_query` clears any ingest target and `set_option(TargetTable)`
  clears any SQL query — so a reused statement handle runs whichever was configured most recently,
  in both directions: a query after an ingest runs the query, and (the pattern the Python DBAPI
  `Cursor` produces, one statement per cursor) an ingest after a `CREATE TABLE`/query runs the ingest
  rather than re-running the stale query. Read-only / no-data-bound guards and the `append`-mode
  NotFound/AlreadyExists error remapping are unchanged, and `execute_update` still reports the
  affected-row count. Covered by an ingest-via-`execute()` assertion and a query-then-ingest reuse
  assertion in `tests/integration.rs`.
- ~~**`adbc.ingest.target_db_schema` is rejected although the driver supports named schemas
  everywhere else** (`src/statement.rs:405-437`). Accept it and qualify the ingest/CREATE TABLE
  statements via `qualified_table`; accept `target_catalog` when it names the `""` catalog.~~
  **Fixed.** `adbc.ingest.target_db_schema` is now stored and threaded into the ingest INSERT,
  CREATE TABLE and DROP TABLE via the shared `qualified_table` helper (moved to `src/bind.rs`
  alongside `quote_ident`), so the target/created table is `schema.table`. The append-error probe
  now checks the table in its target schema too. `adbc.ingest.target_catalog` is accepted when it
  names Spanner's only (empty `""`) catalog and rejected with `NotImplemented` otherwise. Both
  options round-trip through `get_option`/`set_option` like the other ingest options.
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

- ~~**`bind_params` re-lexes the entire SQL text once per bound row** (`src/statement.rs:113-122` →
  `src/bind.rs:73-109`). A 50k-row bound DML lexes the same SQL 50k times — O(rows × |sql|) CPU
  before the first RPC. Resolve the parameter-name mapping once per (sql, schema) and pass it into
  a per-row loop.~~ **Fixed.** `resolve_parameter_names` is now `pub(crate)` and called once per
  batch by `build_bound_statements`; `bind_params` takes the precomputed `names: &[String]` and only
  binds values per row, so the SQL is lexed once per (sql, batch) rather than once per row.
- ~~**String columns pay an extra allocation + copy per value in the read hot path**~~
  (`src/conversion.rs:450-456`). **Fixed.** The `Utf8`/fallback arm now builds with a `StringBuilder`
  and `append_value(&str)` (mirroring the `BinaryBuilder` arm), appending the wire string slice
  directly — no per-value owned `String` and no second copy from `StringArray::from_iter`. Only the
  non-string JSON-render fallback still allocates. Behavior is unchanged (null/empty handling
  preserved); covered by `string_array_round_trips_values_and_nulls`.
- ~~**`rows_per_batch` bounds rows, not bytes** (`src/conversion.rs:98-108`). 8192 rows of
  `STRING(MAX)`/`BYTES(MAX)` (up to 10MB each) can be tens of GB per chunk, held roughly twice
  during conversion. Track approximate cumulative bytes in `pull_chunk` and cut the chunk early at
  a byte budget (16–64MB) in addition to the row cap.~~ **Fixed.** `pull_chunk` now tracks an
  approximate cumulative byte size of the rows it has buffered and cuts the chunk once it crosses a
  new `CHUNK_BYTE_BUDGET` constant (32 MiB, mid-range of the 16–64 MB guidance), in addition to the
  existing `rows_per_batch` row cap. The per-row size is estimated cheaply from the wire values
  already in hand via `approx_row_bytes`/`approx_value_bytes` — sum of string lengths (Spanner ships
  `STRING`/`BYTES`(base64)/`INT64`/`NUMERIC`/`DATE`/`TIMESTAMP`/`JSON` as strings), recursing through
  lists and structs, small fixed sizes for other scalars — so it is a rough (slightly conservative,
  since base64 over-estimates decoded `BYTES`) upper estimate, not exact. The budget check runs
  *after* the row is buffered and uses `saturating_add`, so a single row larger than the whole
  budget still forms its own one-row chunk (never an infinite loop or empty chunk), and the
  schema-settling / first-chunk semantics are otherwise unchanged. Covered by the
  `approx_value_bytes_sums_string_lengths` unit test.
- ~~**`get_objects` assembles the hierarchy with quadratic rescans** (`src/connection.rs:218-267`,
  `:507-590`, `:598-665`). The RPC side is a fixed 6 queries (good), but per table the full
  COLUMNS/TABLE_CONSTRAINTS batches are rescanned and per constraint the full KEY_COLUMN_USAGE
  batch — O(10⁸) string comparisons for a 2k-table schema. Group each batch once into `HashMap`s,
  exactly the pattern `collect_statistics` already uses (`connection.rs:329-335`).~~ **Fixed.**
  `collect_objects` now groups each of the six `INFORMATION_SCHEMA` batches ONCE (`group_tables` by
  schema; `group_columns`/`group_constraints` by (schema, table); `group_key_columns`/
  `group_referential` by (constraint_schema, constraint_name)) into `HashMap`s, then assembles the
  hierarchy by keyed lookup instead of rescanning. The per-group `Vec`s keep batch (`ORDER BY`)
  order, so column and key-column ordering — and every catalog/schema/table/column filter — is
  preserved exactly; this is a pure O(N²)→O(N) change with no behavioral difference.

### Security

- ~~**`read_partition` executes an unauthenticated, attacker-suppliable request blob**
  (`src/connection.rs:979-1000`; descriptor from `src/statement.rs:699-715`). The descriptor is
  plain serde-JSON of the client's `Partition`, whose inner `ExecuteSqlRequest` carries the SQL
  text itself — a crafted blob runs arbitrary SQL with the connection's credentials. This is
  inherent to the upstream serde format and to ADBC's portable-descriptor design, so the realistic
  fix is documentation (descriptors are executable request blobs; transport them only over trusted
  channels), optionally an HMAC envelope for stronger guarantees.~~
  **Fixed.** The trust caveat is now documented where it is actionable: a `# Security` rustdoc
  section on `Connection::read_partition` (`src/connection.rs`) and on
  `Statement::execute_partitions` (`src/statement.rs`) spells out that a partition descriptor is
  opaque but *executable* — serde JSON carrying the SQL text plus session/transaction identity — so
  `read_partition` runs whatever it contains with the connection's credentials, and therefore
  descriptors must travel only over trusted channels and never be accepted from an untrusted source.
  The CLAUDE.md Partitioned-execution bullet carries the same caveat as ground truth. Per the
  review's own recommendation the optional HMAC envelope was deliberately **not** implemented: it is
  a larger design change, and the realistic fix for an inherent portable-descriptor property is
  documentation.

### Testing

- ~~**`adbc.connection.readonly` has zero tests** anywhere (four enforcement branches at
  `src/statement.rs:352, 529, 582, 606`). A regression silently allowing writes on a read-only
  connection would ship. Add an integration case covering allow/deny/toggle/round-trip.~~
  **Fixed.** The `readonly_connection_rejects_writes` integration test (in `tests/integration.rs`,
  self-skipping like the others) covers all four dimensions: **round-trip** (the option defaults to
  `false` and set `true`/`false` values read back through `get_option`), **allow** (a `SELECT` still
  runs on a read-only connection), **deny** (a DML `execute_update`, a DDL, and a bulk ingest each
  fail with `InvalidState`), and **toggle/snapshot** (the flag is captured into each statement at
  creation — a statement made while read-only stays read-only after the connection flips back to
  writable, while one created afterwards can write).
- ~~**`create_append` ingest mode is never executed end-to-end**, nor is the `create`-on-existing-table
  error path (`tests/integration.rs`). (The `append`-on-missing-table and schema-mismatch error paths
  are now covered — see the ADBC-conformance fix above.)~~
  **Fixed.** The create-mode ingest section of `query_and_dml_round_trip` (in `tests/integration.rs`,
  self-skipping like the others) now covers both. **`create_append`** is exercised end-to-end against
  a fresh `AdbcCreateAppend` table: the first ingest (table absent) builds the table from the bound
  Arrow schema and inserts 2 rows, and a second ingest (table now present) **appends** 2 more without
  erroring — asserting counts of 2 then 4 and reading the data columns back through the synthetic
  key. The **`create`-on-existing-table** error path re-runs a `create`-mode ingest against the
  already-existing `AdbcCreate` table and asserts it fails (create-mode failures are not remapped, so
  the underlying `CREATE TABLE` DDL error surfaces) and that the failed ingest left the table's row
  count unchanged. The shared `ingest_into(table, mode)` helper (a small refactor of the former
  `ingest_create`) backs all three cases.
- **Resilience suite misses some likely production failures** (`tests/resilience.rs`):
  mid-stream disconnect after batches were consumed, latency/timeout toxics (by default nothing
  bounds the wait — building the commit-fault test confirmed the client marks all data-plane RPCs
  idempotent and retries transport faults with no attempt cap, so a commit under a persistent
  fault blocks unboundedly; the `spanner.rpc.timeout_seconds.*` options now let callers bound
  these operations, and the harness drives the update deadline under a `reset_peer` toxic in
  `update_timeout_bounds_a_faulted_write_then_recovers_when_unset` — proving a bounded write fails
  with `Timeout` in-deadline rather than hanging, then recovers once the deadline is unset), and
  truncated streams. (Faults during a
  manual-transaction commit are now covered by
  `commit_under_transport_fault_never_loses_the_write`.)
- ~~**`statement.rs` has zero unit tests**; the option-coercion error paths (`rows_per_batch=0`,
  bad bools, negative `max_partitions`, unknown-option statuses) are pure functions guarding the
  C-ABI boundary and are unit-testable offline.~~
  **Fixed.** `src/statement.rs`'s `#[cfg(test)] mod tests` now covers the pure option-coercion
  helpers offline (no live connection/runtime): `string_option` (accepts strings, rejects other
  value kinds → `InvalidArguments`), `bool_option` (all truthy/falsy string spellings + int
  0/non-zero; rejects unrecognised strings and non-bool value kinds), `max_partitions_option`
  (positive ints/strings; rejects zero, negatives, non-numeric/float strings and wrong value kinds),
  and `rows_per_batch_option` (positive ints/strings; rejects `0`, negatives, malformed strings and
  wrong value kinds) — asserting the `InvalidArguments` status on every error path. The
  unknown-option statuses (`NotImplemented` on `set_option`, `NotFound` on an unset `get_option`) are
  only reachable through `SpannerStatement`, which needs a live `DatabaseClient`, so they stay
  covered by the emulator integration tests rather than fabricating a fake connection.

### CI / release / packaging

- **`manylinux_2_35` floor excludes RHEL 9, Amazon Linux 2023, Debian 11, Ubuntu 20.04**
  (`libraries.yml:254-255`; the matrix comment already names the fix). Build in a
  `manylinux_2_28` container and lower the tag + verify step together. **(wontfix — we don't care
  about those old distributions.)**
- **No musl (Alpine) build/wheel** — `pip install` fails on Alpine-based data-service images.
- ~~**Wheels are published without ever being installed or inspected** — no `twine check`, no
  unzip-and-assert-the-lib-is-inside, no `pip install` + import smoke test; the CI python job
  installs from the source tree, a different packaging path. All cheap on the same runner.~~
  **Fixed.** The `python-wheels` job now inspects the freshly built wheels before the irreversible
  `python-publish` step (which already `needs: python-wheels`, so a failed inspection blocks the
  release). Two steps were added after "Build wheels": (1) `twine check wheelhouse/*.whl` plus a
  per-wheel assertion that the platform shared library (`libadbc_spanner.so`/`.dylib` /
  `adbc_spanner.dll`, selected from the wheel's platform tag) is actually inside the archive
  (`python -m zipfile -l | grep -qF`), failing loudly if a data-only wheel ships empty; and (2) a
  `pip install` of the actual built `manylinux_2_35_x86_64` wheel (the only tag installable on the
  x86_64 Linux runner) followed by an import smoke test from `$RUNNER_TEMP`
  (`import adbc_driver_spanner; print(adbc_driver_spanner._driver_path()); import
  adbc_driver_spanner.dbapi`) — exercising the *installed* package, not the source tree, and
  `_driver_path()` raises unless the bundled library resolves.
- ~~**The tag-vs-version gate lives only in `python-wheels`**; the `release` job (GitHub assets)
  has no gate, and `release` / `python-publish` are unordered siblings — a partial failure yields
  a PyPI version with no GitHub assets or vice versa. Hoist the check into a `version-gate` job
  both need; run `python-publish` (the irreversible step) last.~~ **Fixed.** A standalone,
  tag-only `version-gate` job now performs the tag-vs-crate-version check once — deriving the crate
  version with `cargo metadata --no-deps --format-version 1 | jq` (robust manifest parsing rather
  than a positional Cargo.toml grep) and failing if the pushed `vX.Y.Z` tag disagrees. Both
  `release` (GitHub Release assets) and `python-publish` (PyPI) now `needs:` it, so a mismatched tag
  publishes nothing anywhere. Ordering is fixed too: `python-publish` — the irreversible PyPI step —
  now also `needs: release`, so it runs strictly *after* the GitHub-Release assets are attached, and
  a partial failure can no longer leave a PyPI version with no matching GitHub assets. The redundant
  inline gate was removed from `python-wheels`, whose remaining "Sync wheel version to the crate"
  step only stamps `_version.py` (no tag comparison), so the check is no longer duplicated.
- **No CI ever touches real Cloud Spanner** — `SPANNER_GCP_DATABASE` support exists but appears in
  no workflow; auth paths (ADC, impersonation) are untested in CI since the emulator is anonymous.
  A scheduled non-gating workflow via GitHub OIDC → Workload Identity Federation would cover it.

### Maintainability

- ~~**Bool/int/string option parsing is copy-pasted three times** with slightly different error text
  (`src/driver.rs:506-522`, `src/connection.rs:1003-1015`, `src/statement.rs:789-826`); only the
  database copy is fuzzed. Extract a shared `src/options.rs`.~~ **Fixed.** A new `src/options.rs`
  holds the shared coercions — `bool_option` (bool-ish strings + integer 0/non-zero), `string_option`,
  and `positive_i64`/`positive_usize` (integer or numeric string, strictly positive) — each taking a
  `what` label so the `InvalidArguments` message names the offending option. The three sites now
  delegate: `driver.rs`'s `bool_value`/`string_value` pass `option {name}` (preserving their exact
  messages, which are the fuzzed copy), `connection.rs`'s `parse_bool`, and `statement.rs`'s
  `bool_option`/`string_option`/`max_partitions_option`/`rows_per_batch_option` (kept as thin
  same-signature wrappers so the existing `statement.rs` unit tests still exercise them by name).
  Behavior — accepted spellings, positive-int semantics, and the `InvalidArguments` status on every
  error path — is unchanged; the driver-options fuzz target still builds and drives the same code.
- ~~**`build.rs` panics without `Cargo.lock`, which is excluded from the published package**
  (`build.rs:14-15`, `Cargo.toml:17`) — a landmine wired to the planned "re-enable publish" step.
  Fall back gracefully, or leave a loud comment next to `publish = false`.~~ **Fixed.** `build.rs`
  now `match`es on `fs::read_to_string(&lockfile)`: a present lockfile behaves exactly as before
  (including the hard error on a surprising/duplicate/empty `arrow-array` version), but a missing or
  unreadable `Cargo.lock` no longer panics — it emits `cargo:warning=...` and embeds
  `ADBC_SPANNER_ARROW_VERSION="unknown"` (so `src/info.rs`'s `env!` still compiles and
  `DriverArrowVersion` reports `vunknown`). This makes a source build from the published package —
  which `exclude`s `Cargo.lock` — succeed instead of failing outright.
- ~~**CLAUDE.md has drifted from ground truth** (also found by the docs review): arrow is now
  `>=58, <60` not `>=53.1, <59`; `adbc_core`/`adbc_ffi` are git-pinned to `fornwall/arrow-adbc`
  (a second publish blocker the "temporary git pin" section doesn't mention); the library matrix
  is six targets, not four, and macOS x86-64 is cross-compiled.~~
  **Fixed.** CLAUDE.md now matches ground truth on all three: the "Arrow version" design point and
  the arrow line read `>=58, <60` (from `Cargo.toml`, which also pins `arrow-buffer` to the same
  range); the "Temporary git pin" section is rewritten as *two* pin families, calling out that
  `adbc_core`/`adbc_ffi`/`adbc_driver_manager` are pinned to a `fornwall/arrow-adbc` fork rev
  (`786e7f3…`, carrying the idempotent-`release_ffi_error` and `rows_affected = -1` fixes) and that
  **each** pin independently blocks `cargo publish`; and the "Shared library" section now lists the
  full six-target matrix (linux x86-64/aarch64, macOS arm64, macOS x86-64 **cross-compiled** on the
  arm64 runner, windows x86-64, windows aarch64 native). The Releasing / Python / ground-truth-source
  notes and the aws-lc build-per-arch/NASM sentence were corrected to match while in there.
- ~~**The two git-pin revs are spread across ~9 dependency lines** plus `deny.toml` plus docs, with
  no single anchor for the scheduled "revert when upstream releases" edit.~~ **Fixed.** CLAUDE.md's
  "Temporary git pins" section now carries a *Revert checklist — the single anchor* subsection: the
  one place that lists both current rev SHAs (`google-cloud-rust` `3872d28…`, `fornwall/arrow-adbc`
  `786e7f3…`), enumerates every location to edit in lockstep when reverting a family to a versioned
  crates.io release (each `Cargo.toml` `[dependencies]`/`[dev-dependencies]` line grouped by family,
  `deny.toml`'s `allow-git`, the `README.md` quickstart + notes, and the `publish = false` release
  gate), and states the invariant that the three arrow-adbc crates (`adbc_core`/`adbc_ffi`/
  `adbc_driver_manager`) must share ONE rev (as the six `google-cloud-*` crates do). A one-line
  pointer comment at the top of `Cargo.toml`'s `[dependencies]`, next to the first pinned dependency,
  directs the maintainer to that checklist, keeping a single source of truth.
- ~~**`connection.rs` (1050 lines) owns the query half of `get_objects`/`get_statistics`** while
  `objects.rs`/`statistics.rs` hold only the Arrow-assembly half — split along the wrong axis.
  Move the INFORMATION_SCHEMA collectors into their feature modules. (`bind.rs` at 1359 lines is
  fine — ~600 lines are tests and the rest is one cohesive concern.)~~ **Fixed.** The
  INFORMATION_SCHEMA collectors now live in the feature modules alongside their Arrow-assembly half,
  so each module owns both halves. `collect_objects` and all its grouping/assembly helpers
  (`group_tables`/`group_columns`/`group_constraints`/`group_key_columns`/`group_referential`,
  `collect_columns`/`collect_constraints`/`foreign_key_usages`, plus the `TableRow`/`ColumnRow`/
  `ConstraintRow`/`KeyColumnRow`/`ReferentialMap` row types) moved to `src/objects.rs`;
  `collect_statistics`, `table_statistics` and `is_groupable` moved to `src/statistics.rs`. Both
  were converted from `SpannerConnection` methods to `pub(crate)` free functions taking the inputs
  they used (`runtime`/`client`/`cancel`, plus `read_staleness` for statistics), and `get_objects`/
  `get_statistics` now call `crate::objects::collect_objects` / `crate::statistics::collect_statistics`.
  The two low-level INFORMATION_SCHEMA primitives shared by both features — `query_batch` and
  `str_col` — stay in `connection.rs` (now `pub(crate)`), as does `like_match` (also used by the
  catalog-filter fast paths). Identical SQL, identical grouping/ordering (`ORDER BY`-preserving
  per-group `Vec`s) and identical output — a pure no-behavior structural move, verified by the full
  unit + emulator integration suites (including `get_statistics_reports_real_counts` and the
  `get_objects` conformance test).

### Docs

- ~~**`adbc.connection.readonly` is accepted but documented nowhere** (README, python/README,
  CLAUDE.md, `src/lib.rs` — all silent).~~
  **Fixed.** The read-only connection option is now documented in all four places: `README.md`
  (a feature bullet), `python/README.md` (a `conn_kwargs={"adbc.connection.readonly": "true"}`
  example — it is a spec `adbc.connection.*` option, so the DBAPI passes it via `conn_kwargs`, not
  `db_kwargs`), `src/lib.rs` (the module-level *Configuration* docs, linking
  `OptionConnection::ReadOnly`), and the CLAUDE.md Transactions design point. All four state the
  confirmed behavior: default `false`; `true` rejects DML/DDL/ingest with `InvalidState` while
  queries still run; it round-trips through `get_option`; and the flag is snapshotted into each
  statement at creation, so a runtime toggle applies to statements created afterwards.
- ~~**Ingest modes + synthetic `adbc_ingest_key` PK missing from the main README**; the emulator
  port-9010 requirement lives only in CLAUDE.md (users on another port get working queries and
  cryptic DDL failures); manual-mode `execute_update` returning `None` is in CLAUDE.md but not
  the README.~~
  **Fixed.** All three are now in `README.md`. The bulk-ingest feature bullet documents the four
  `adbc.ingest.mode` values (`append`/`create`/`create_append`/`replace`) and that the three create
  modes build the table from the ingest data's Arrow schema with a synthetic `adbc_ingest_key`
  `STRING` UUID primary key (Spanner requires a PK) that shows up in a later `SELECT *`. The
  Testing section's `SPANNER_EMULATOR_HOST` bullet spells out the port-`9010` requirement — the
  pinned client derives the admin/REST endpoint by literally substituting `9010`→`9020`, so any
  other gRPC port sends admin (DDL/`create_database`) requests to the gRPC port and fails cryptically
  (`error sending request … /ddl`) while queries keep working; move the host freely but keep the port
  `9010`. The Transactions feature bullet now states that manual-mode `execute_update` returns `None`
  (row count unknown until the buffered batch commits; queries and DDL still run immediately).

---

## P3 — nice to have

**Correctness.** ~~List/Struct columns map present-but-undecodable wire values to NULL,
contradicting the strict-decode policy of the scalar arms (`src/conversion.rs:466-474`,
`:493-513`)~~ (**Fixed.** `build_list` and `build_struct` now apply the scalar arms' strict-decode
policy: a *present* column value that is not a wire list (for `ARRAY`) or neither a wire list nor a
keyed struct (for `STRUCT`) is a loud `InvalidData` decode error via the same `decode_error` helper,
never a silent NULL; genuine SQL NULLs (and absent slots) still map to null slots, and
elements/fields recurse through `build_array` so undecodable nested values error at any depth.
Unit-tested by `list_with_undecodable_element_errors`, `struct_with_undecodable_field_errors`
(incl. an undecodable struct nested inside a list) and `list_and_struct_nulls_round_trip_as_nulls`);
~~`parse_int64`'s f64 fallback loses precision above 2^53 (`conversion.rs:527-532` —
better removed)~~ (**Fixed.** the f64 fallback is gone — `parse_int64` now only accepts the JSON
string encoding Spanner actually uses for `INT64`, so every `i64` round-trips exactly and a JSON
number is a loud decode error instead of a silently-rounded value); ~~`is_dml_returning`'s documented
false positive (`CASE WHEN c THEN return …`)
hard-errors valid DML in manual mode (`src/ddl.rs:49` + `src/statement.rs:289-295`)~~ (**Fixed.**
the scan now tracks `CASE`/`END` nesting depth — using the same quote/comment-aware lexer — and
only recognises `THEN` followed by `RETURN` at depth zero, since GoogleSQL's `THEN RETURN` clause
only appears at the top level at the end of a DML statement; a CASE branch expression that is a
column literally named `return` no longer trips the check, while a genuine top-level `THEN RETURN`
after a CASE expression is still detected. Unit-tested for nested CASE, mixed case, comments,
`THEN RETURN`/`CASE` inside literals and quoted identifiers, and unbalanced `END`); ~~`read_only`
is snapshotted into statements at creation, so flipping the connection option leaves existing
statements writable (`src/connection.rs:792-801` — flagged independently by correctness,
conformance and maintainability; share via `SharedTxn`/`AtomicBool`)~~ (**Fixed.** the connection's
flag is now an `Arc<AtomicBool>` shared into every statement, and all four enforcement branches in
`src/statement.rs` (DML via `execute`, DML via `execute_update`, DDL, bulk ingest) load it at
execution time through one `is_read_only()` helper instead of a creation-time snapshot — so
toggling `adbc.connection.readonly` immediately locks or frees existing statements in both
directions. The `readonly_connection_rejects_writes` integration test's toggle section now asserts
the live semantics both ways (clearing the flag lets a pre-existing statement write; re-enabling it
immediately rejects the same statement with `InvalidState`), and the README.md / python/README.md /
lib.rs / CLAUDE.md docs describe the live-toggle behavior); ~~the autocommit-enable path
reads/takes/flips in separate lock acquisitions, so a concurrently-buffering statement can strand
DML (`connection.rs:734-742`)~~ (**Fixed.** the enable path now flips the mode and takes the buffer
in *one* lock acquisition (`TxnState::enter_autocommit`) — `run_or_buffer` checks-and-buffers under
the same mutex, so once the mode reads autocommit no statement can buffer DML behind the flip — then
applies the taken batch outside the lock (from a clone; locks are never held across `block_on`), and
a failed apply restores manual mode with the batch re-buffered (`restore_manual`), preserving the
P1 #1 guarantee that a failed implicit commit stays retryable/rollbackable; covered by new offline
unit tests of the two helpers plus the existing enable-autocommit success/failure sections of the
manual-transaction integration test); ~~`get_statistics` breaks for the whole database if any table has a
`TOKENLIST`/`PROTO` column (`is_groupable` only excludes ARRAY/STRUCT/JSON,
`connection.rs:439-442`)~~ **Fixed.** `is_groupable` (now in `src/statistics.rs`) also excludes
`TOKENLIST` and `PROTO<...>`, so distinct counts are skipped (not errored) for them, matching the
existing ARRAY/STRUCT/JSON handling; ~~`str_col`'s `RecordBatch::column(i)` can panic instead of erroring on a
zero-column metadata batch (`connection.rs:494-505`)~~ (**Fixed.** `str_col` now bounds-checks the
column index against `batch.num_columns()` and returns a `Status::Internal` error for an
out-of-range/zero-column batch instead of letting `RecordBatch::column` panic; covered by a new
offline unit test); ~~`execute_bound_query` runs each bound row in
its own snapshot (mutually inconsistent results) and materialises everything ignoring
`rows_per_batch` (`src/statement.rs:319-345`)~~ (**Fixed**, both halves. *Materialisation*: bound
queries now stream through the same bounded-chunk machinery as `execute` — a single bound row goes
through `stream_query`, and several bound rows through the new `BoundQueryBatchReader`
(`stream_bound_query` in `src/conversion.rs`), which executes each per-row statement lazily and
converts rows in chunks of `spanner.rows_per_batch` plus the shared `CHUNK_BYTE_BUDGET`, so the
concatenated result is never materialised whole. *Snapshot consistency*: with several bound rows
every statement now executes inside **one** multi-use read-only transaction
(`DatabaseClient::read_only_transaction()`), pinned at the statement's configured read bound, so
the per-row results are mutually consistent; a single bound row keeps the cheap single-use
transaction, which is one snapshot already. One documented semantic nuance remains: Spanner only
accepts the bounded-staleness kinds (`max:<d>` / `min:<t>`) on single-use transactions, so for the
multi-row case they are pinned to the most stale timestamp their window allows — `max:<d>` → exact
staleness `<d>`, `min:<t>` → read timestamp `<t>` (`ReadBound::pinned_for_multi_use` /
`ReadStaleness::multi_use_timestamp_bound` in `src/staleness.rs`, unit-tested offline); always a
legal choice under the original bound. Emulator-covered by `bound_query_streams_in_batches` in
`tests/integration.rs`: three bound rows × 500 rows each at `rows_per_batch=200` yield exactly
nine 200/200/100-row batches in bound-row order).

**Performance.** ~~`get_statistics` per-table scans run strictly sequentially — a small
`buffer_unordered(4..8)` would cut wall-clock near-linearly (`connection.rs:337-359`)~~ **Fixed.**
The per-table aggregate scans now run with bounded concurrency (`buffer_unordered(8)`,
`STATISTICS_SCAN_CONCURRENCY` in `src/statistics.rs`) inside the one shared runtime's single
`block_on_cancellable`, cutting `get_statistics` wall-clock near-linearly on a many-table database.
Output is unchanged: each table's query is prepared in deterministic `table_batch` order, and since
`buffer_unordered` yields out of order the results are index-tagged and slotted back into that order
before parsing, so the tables, schemas and statistics — and their ordering — match the old
sequential loop. Cancellation (the shared `CancelSignal` interrupts the in-flight batch) and error
propagation (any scan error surfaces as an overall `Err`) are preserved. `execute_partitions` pays
an extra PLAN round trip for the schema (unavoidable until the client surfaces partition metadata);
binary parameters take a forced `to_vec()` copy (upstream `ToValue` limitation).

**Security.** ~~Emulator mode (`SPANNER_EMULATOR_HOST`) silently forces anonymous credentials +
plaintext `http://`, overriding configured keyfiles — an env-controlled downgrade footgun;
consider refusing (or warning) when explicit credentials were also configured
(`src/driver.rs:150-187`)~~ (**Fixed.** `connect()` now refuses with `InvalidState` when emulator
mode — via `SPANNER_EMULATOR_HOST` *or* `spanner.emulator` — is combined with explicit driver
credentials (`spanner.keyfile`, `spanner.keyfile_json`, or
`spanner.impersonate.target_principal`); the error names the offending option and what enabled
emulator mode. Ambient ADC (e.g. `GOOGLE_APPLICATION_CREDENTIALS`) and inert `spanner.impersonate.*`
options without a target do not trip the guard, so the plain emulator path — the integration-test
path — is unchanged. Offline unit tests in `src/driver.rs`
(`emulator_mode_with_an_explicit_keyfile_is_refused` and siblings) cover keyfile / keyfile_json /
impersonation-target refusal plus the ambient-ADC negative case; documented in README's
Authentication list and the `OPTION_EMULATOR` rustdoc.) ~~Credential-building errors interpolate the auth crate's `Display`
output, which is outside this crate's control (`driver.rs:436-473`); scrub or verify it never
embeds key material.~~ (**Fixed.** The three credential-build sites in `src/driver.rs`
(`build_credentials_from_json`, the ADC-source and impersonated builders) no longer interpolate the
`google-cloud-auth` builder error's `Display` — whose `Parsing`/`Loading` variants wrap a
`serde_json` error that can echo fragments of the credential JSON body it was deserializing (private
key / refresh token). A new `scrub_credential_error` classifies the error with the auth crate's own
public `is_missing_field`/`is_parsing`/`is_unknown_type`/`is_not_supported`/`is_loading` predicates
and surfaces only one of a handful of fixed, secret-free phrases, so no key material can reach an
ADBC error message regardless of what the auth crate puts in its `Display` now or after a dependency
bump; the credential *type* and (keyfile path) *path* — user-supplied config, not secrets — are
still reported. Offline unit tests in `src/driver.rs`
(`credential_build_failure_never_leaks_key_material` drives a failing `service_account` build whose
body carries a recognizable fake secret and asserts it is absent from the message;
`scrub_credential_error_returns_fixed_phrase` pins the scrubbed phrase against a real builder
error).)

**Conformance.** ~~`adbc.ingest.temporary="false"` (the default value) is rejected instead of
no-op'd~~ (**Fixed.** `set_option("adbc.ingest.temporary", …)` now accepts any falsy spelling of
the shared bool coercion (`false`/`0`/`no`/int 0) as a no-op — the spec default — and rejects only
truthy values with `NotImplemented` ("Spanner has no temporary tables"); `get_option` round-trips
it as `"false"`, which is always the driver's state. Unit-tested offline
(`ingest_temporary_accepts_false_and_rejects_true` in `src/statement.rs`) plus
set-false/round-trip/set-true-fails assertions in the ingest section of `tests/integration.rs`);
`get_info` could report a vendor version instead of null; the upstream `adbc_ffi` shim
rejects 1.0.0 driver managers and errors on unknown `get_info` codes (both stricter than the C
spec — upstream issues, worth tracking); ~~no `sqlstate` on errors (a coarse mapping would help
ODBC bridges)~~ (**Won't fix.** Spanner does not surface a SQLSTATE on its errors, and the driver
follows the same convention as the other ADBC drivers — it does not synthesize a SQLSTATE the
database itself does not provide, since a guessed coarse mapping would be misleading rather than
helpful); ~~`get_table_schema` ignores the catalog argument entirely~~ (**Fixed.**
`get_table_schema` now validates its catalog argument via a pure `check_lookup_catalog` helper in
`src/connection.rs`: `None` and `Some("")` — Spanner's single, unnamed catalog — behave as before,
while any other catalog name fails with `NotFound` (nothing can exist in a catalog Spanner doesn't
have, matching the missing-table status). Covered by an offline unit test
(`lookup_catalog_accepts_only_the_default_empty_catalog`) plus new `Some("")`-still-works /
bogus-catalog-is-NotFound assertions in the `get_table_schema` section of
`tests/integration.rs`); ~~`get_objects` at
`Catalogs` depth still runs the SCHEMATA query~~ (**Fixed.** `collect_objects` in `src/objects.rs`
now short-circuits at `Catalogs` depth before any RPC — the result at that depth is just the single
unnamed catalog with a NULL `catalog_db_schemas` list, which `build` produces without looking at the
collected schemas, so no INFORMATION_SCHEMA query is needed at all; the catalog `LIKE` filter was
already applied RPC-free in `get_objects`. The other depths already fetched only what they populate
(DBSchemas → SCHEMATA only; Tables adds TABLES; All/Columns add COLUMNS + the constraint tables).
Byte-identity of the short-circuit is unit-tested offline
(`build_catalogs_depth_ignores_collected_schemas` in `src/objects.rs`), and the emulator conformance
test grew Catalogs-depth (NULL schemas list, excluding-catalog-filter → zero rows) and
DBSchemas-depth (schemas populated, NULL table lists) shape assertions in `tests/integration.rs`);
~~`execute_schema` lets DML through to a PLAN probe
whose read-only-transaction error is surfaced raw~~ (**Fixed.** `execute_schema` now classifies the
SQL up front via the shared `ddl::is_dml` lexer (`check_schema_query` in `src/statement.rs`) and
rejects DML — including hinted and `THEN RETURN` forms — with a clear `InvalidArguments`
"only supports queries" error instead of Spanner's raw read-only-transaction error; DDL keeps its
existing `InvalidState` rejection. Unit-tested offline (`execute_schema_guard_rejects_ddl_and_dml`
in `src/statement.rs`) plus a DML-rejection assertion in the execute_schema section of
`tests/integration.rs`).

**Testing.** ~~Doctests never run in CI (`cargo test --doc` is absent; CLAUDE.md implies otherwise —
also flagged by the CI review, which notes `cargo doc` runs without `--all-features` unlike
docs.rs)~~ (**Fixed.** `ci.yml` now has a dedicated `cargo test --doc --all-features` step after
the unit tests, and the docs step runs `cargo doc --no-deps --all-features` to match docs.rs);
keyfile/impersonation auth is offline-unit-tested only, never exercised end-to-end;
~~weak assertions (`AdbcDdl` note value unchecked, `replace`-ingest values unchecked, only row
counts)~~ (**Fixed.** The `AdbcDdl` round-trip now downcasts the read-back `Note` column and asserts
it equals the inserted `"hello"`, not just that one row came back; the create-mode ingest test now
reads the `Id`/`Label` columns back after `replace` and asserts the exact `[(10,"x"),(20,"y")]`
rows, distinguishing a real `replace` (one copy) from the duplicated four rows an `append` would
leave — both in `tests/integration.rs`); ~~untested small surfaces: `rollback()` without a transaction, `get_statistic_names`,
`read_partition` with a garbage descriptor (also a natural fuzz target), `Connection::cancel`~~
(**Fixed.** All four are covered. `rollback()` in autocommit mode asserts `InvalidState`, and in
manual mode with nothing buffered it is a no-op that keeps the connection in manual mode
(`rollback_without_a_transaction_is_invalid_state`). `get_statistic_names` asserts the canonical
`GET_STATISTIC_NAMES_SCHEMA` — with the field names/types also spelled out — and zero batches
(`get_statistic_names_is_empty_and_correctly_typed`). The partition-descriptor decode was factored
into a pure `decode_partition` helper in `src/connection.rs` (behavior unchanged) so the rejection
path is unit-tested offline against empty / non-JSON / truncated / wrong-shape-JSON inputs
(`garbage_partition_descriptors_error_cleanly`), and the same inputs go through
`Connection::read_partition` end-to-end (`read_partition_rejects_garbage_descriptors`) — all
`InvalidArguments`, no panic, no RPC. `Connection::cancel` is covered deterministically by
`connection_cancel_is_sticky_until_the_next_operation`, mirroring the statement-level test: a
cancel latched *between* `read_partition` chunk fetches cancels the next fetch, statements on the
same connection are unaffected (own signal), and the connection's next operation resets the latch.
All but the offline decode test live in `tests/integration.rs`.);
~~fuzz gaps: statement-hint parsing (`first_keyword` — this exact code had a real bug),
`strip_trailing_terminators`, parameter-name extraction, `quote_ident`, partition-descriptor
deserialization~~ (**Fixed.** Three new fuzz targets, registered in `fuzz/Cargo.toml` and the
nightly `fuzz.yml` matrix with seed corpora, each with independent oracles rather than
panic-only: `keyword` covers `first_keyword` + `strip_trailing_terminators` (keyword is an
uppercase word occurring verbatim in the input; `is_ddl`/`is_dml` agree with a re-stated copy of
the keyword lists; metamorphic check that whitespace/comment/`@{…}`-hint prefixes — the exact
surface of the real bug — never change the keyword; the strip is a verbatim substring, idempotent,
and `split_statements`-preserving), `params` covers `named_parameters` + `resolve_parameter_names`
+ `quote_ident` (extracted names are distinct well-formed identifiers occurring verbatim as
`@name`; the pairing follows the documented name-mode/positional contract with `InvalidArguments`
exactly on count mismatch; quoting unquotes back to the input and embeds as one opaque token under
the crate's own lexer), and `partition` covers `decode_partition` over arbitrary bytes (rejections
are clean `InvalidArguments`, accepted descriptors survive a serialize → decode → serialize round
trip unchanged).); ~~`tests/resilience.rs` mutates process env in setup — safe only under
`--test-threads=1`, worth asserting.~~ (**Fixed.** The single-threaded contract is now enforced at
runtime: a module-level `SerialGuard` (RAII over a `static AtomicUsize ACTIVE_TESTS`) is constructed
at the start of each test body — after the self-skip check, so a plain multi-threaded `cargo test`
with the env unset still skips cleanly — and its `new()` asserts no other test body is running,
panicking with an actionable "re-run with `--test-threads=1`" message rather than racing the env
swap silently; `drop` releases the slot. `ensure_setup`'s existing `DONE` mutex still serializes the
one-time env swap, and the guard documents/enforces that no concurrent driver connection reads the
transiently-repointed `SPANNER_EMULATOR_HOST`. Both `scripts/with-toxiproxy.sh` and `resilience.yml`
already pass `--test-threads=1`.)

**Features** (all exposed by the pinned client unless noted): request priority / request tags /
transaction tags (`StatementBuilder::set_priority/set_request_tag`,
`set_transaction_tag`); ~~request/attempt timeouts~~ **done** —
`spanner.rpc.timeout_seconds.{query,update,fetch}` (overall per-operation deadlines mapped to
ADBC `Timeout`; see `docs/options.md`); ~~per-attempt retry *tuning* (policies)~~ **done** —
`spanner.retry.{max_attempts,max_elapsed_seconds}` (connection + statement level; bound the pinned
client's gax retry policy via the builders' `with_retry_policy` / `with_begin_retry_policy` /
`with_commit_retry_policy`, layering `RetryPolicyExt::{with_attempt_limit,with_time_limit}` on a
driver-local copy of the client's `SpannerRetryPolicy` so the transport-on-idempotent retry is
preserved — `src/retry.rs`; `RetryConfig` mirrors `RpcTimeouts`/`RequestConfig`). Custom *backoff*
(the gax `BackoffPolicy` / `ExponentialBackoff`, settable via the same builders'
`with_backoff_policy`) is a possible follow-up but was left out to keep the surface focused.
PostgreSQL-dialect databases are
unsupported *and undetected* — minimum viable is probing the dialect once and failing fast with a
clear error; OAuth access-token auth (needs a small custom credentials impl — the auth crate has
no static-token builder); query options (optimizer version), directed reads, commit stats,
`max_commit_delay`, `last_statement` optimization (free RPC saving for single-statement
autocommit DML); proto/enum columns (verify clean failure today); change streams and GQL graph
queries may already work through plain SQL — one emulator test each would let the README claim
them; telemetry/tracing hooks as a backlog entry.

**CI/misc.** ~~No dependabot/renovate (SHA-pinned actions and the two git-pin families drift
unmonitored)~~ (**Fixed.** `.github/dependabot.yml` now monitors the `github-actions` and `cargo`
ecosystems weekly; the two git-pinned families stay out of scope, since Dependabot does not bump
git-revision deps and they are reverted manually per the CLAUDE.md checklist); ~~no `concurrency:`
groups (rapid PR pushes re-run the full 6-platform matrix)~~
(**Fixed.** A release-safe top-level `concurrency:` block (`group: ${{ github.workflow }}-${{
github.ref }}`, `cancel-in-progress: ${{ github.event_name == 'pull_request' }}`) is now on
`ci.yml`, `libraries.yml`, `adbc-validation.yml` and `foundry-validation.yml` — every workflow that
runs on PRs. A rapid re-push cancels the superseded in-flight run for the same PR ref, but the guard
evaluates to `false` for `push`/tag events, so main pushes and `v*` release runs — the
`libraries.yml` build+publish path especially — are never cancelled mid-flight. `fuzz.yml` and
`resilience.yml` have no `pull_request` trigger and were left unchanged.);
~~nightly fuzz/resilience failures surface only as email and scheduled workflows auto-disable after
60 days of inactivity — a `failure()` step opening a tracking issue would help~~ (**Fixed.** Both
`fuzz.yml` and `resilience.yml` gained a `report-failure` job (`needs:` the harness job) guarded by
`if: failure() && github.event_name == 'schedule'` — so it fires only on a failed *nightly* run,
never on manual `workflow_dispatch`. It uses the built-in `gh` CLI with the default `GITHUB_TOKEN`
and a job-scoped `permissions: issues: write` to open a tracking issue (per-workflow label
`nightly-fuzz-failure` / `nightly-resilience-failure`), or, if one is already open, comment on it —
idempotent, so a repeated failure never spams new issues. No third-party action added.);
`foundry-validation` ends in `|| true`, making harness breakage indistinguishable from expected
dialect failures; ~~the Windows import-lib copy is `|| true`-optional (`libraries.yml:138`)~~ (**Fixed.** The Package
step's Windows branch (already gated on `runner.os == Windows`) no longer copies the
`adbc_spanner.dll.lib` with `2>/dev/null || true`; it now asserts the import library exists and
`exit 1`s with a `::error::` message if not, so a build regression that drops it fails the job loudly
instead of shipping a `.zip` missing the import lib.); the
wheel version parse greps `Cargo.toml` positionally — `cargo metadata | jq` is robust;
~~`adbc-validation.yml` rebuilds arrow-adbc C++ + GoogleTest from source every run (cache it)~~
(**Fixed.** `adbc-validation.yml` now caches the `adbc-validation/build` tree (the FetchContent-cloned
+ compiled arrow-adbc driver-manager/validation harness and GoogleTest, plus the harness objects) via
`actions/cache@v6` — the same version the repo already pins that action to in `fuzz.yml`. The key is
`adbc-validation-${{ runner.os }}-${{ hashFiles('adbc-validation/CMakeLists.txt') }}`: keying on the
CMakeLists hash means a bump of the pinned `ARROW_ADBC_TAG` (or any CMake/toolchain flag, all of which
live in that file) invalidates the cache, while a `restore-keys: adbc-validation-${{ runner.os }}-`
fallback seeds a same-OS build tree for an incremental rebuild. The build step still always runs, so on
a hit it is an incremental no-op and on a rev change FetchContent re-fetches the new tag — a stale cache
can never mask a real rev change.); the
adbc-validation allowlist means new upstream tests never auto-enter the gate (periodic `--full`
triage is manual); ~~dead crates.io/docs.rs badges in the README~~ (**Fixed.** The crate is unpublished
(`publish = false`), so its crates.io/docs.rs badges pointed at nonexistent pages and rendered
broken — both removed from `README.md`; the working GitHub Actions CI badge (and the License
badge) are kept.); ~~no consolidated
connection/statement option tables in either README (`spanner.rows_per_batch` is missing from the
Python one entirely)~~ (**Fixed.** `README.md`'s *Configuration options* section now has three
consolidated tables — database, connection and statement level — listing every option the driver
accepts (spec `adbc.*` and vendor `spanner.*`) with value format, default and description,
cross-checked against `src/driver.rs`/`src/connection.rs`/`src/statement.rs`/`src/lib.rs`;
`python/README.md` gained a matching *Option reference* section that also maps each level to its
DBAPI home (`connect()` kwargs / `db_kwargs` vs `conn_kwargs` vs `adbc_stmt_kwargs` /
`cur.adbc_statement.set_options`), documents `spanner.rows_per_batch`, and adds a CI-executed
cookbook snippet exercising it); ~~wheel docs omit the platform floors (glibc ≥ 2.35, macOS ≥ 11/10.15)~~
(**Fixed.** `python/README.md` gained a *Supported platforms* section — a table mapping each wheel
to its floor, cross-checked against `.github/workflows/libraries.yml`: Linux x86-64/aarch64
`manylinux_2_35` (glibc >= 2.35), macOS arm64 `macosx_11_0` (macOS >= 11.0), macOS x86-64
`macosx_10_15` (macOS >= 10.15), and Windows `win_amd64`/`win_arm64`; the Install line now points at
it, and it notes the `py3-none-<platform>` ABI-agnostic tagging); ~~no
CHANGELOG/CONTRIBUTING/versioning policy~~ (**Fixed.** A `CHANGELOG.md` (Keep a Changelog format,
`## [Unreleased]` on top, per-version entries for `0.1.0`–`0.6.0` derived from the git tags) and a
`CONTRIBUTING.md` (build/test/lint commands, the emulator integration via `scripts/with-emulator.sh`,
what CI enforces, the `cargo-release`-only release process, and the SemVer versioning policy —
pointing at the CLAUDE.md revert checklist as the source of truth for the two git-pin families rather
than duplicating it) were added, and both name Semantic Versioning as the versioning policy);
~~`#![warn(missing_docs)]` absent~~ (**Fixed.**
`src/lib.rs` now carries `#![warn(missing_docs)]`; the public surface — the four exported types, the
option/metadata constants and the `bench_support`/`fuzzing` helper modules — was already fully
documented, so the lint gates future additions without needing any new docs, and
`clippy --all-targets --all-features -- -D warnings` plus `cargo doc --no-deps --all-features` stay
clean); assorted maintainability
polish: the four direction-specific copies of the type mapping (`bind_one` vs `bind_list` is a
genuine 100-line duplication — fold via an element visitor, and add an "adding a type touches
these N sites" checklist), ~~three hand-rolled comment-skipping lexer walkers that could share one
token iterator~~ (**Fixed.** `split_statements`, `is_dml_returning` (both `src/ddl.rs`) and
`named_parameters` (`src/bind.rs`) each hand-rolled the same GoogleSQL whitespace/comment/quote walk;
they now share one `Lexer` (`src/ddl.rs::lex`) that tokenizes into `Lexeme::{Word, Quoted, Comment,
Other}` — partitioning the input byte-for-byte so the copying consumer (`split_statements`) rebuilds
the text while the skipping consumers ignore the pieces they don't need, with the raw-literal-prefix
(`r'…'`) tracking centralized in the lexer. All three comment forms, triple-quoted/raw strings and
`@{…}` handling are preserved; the `copy_line_comment`/`skip_line_comment` duplicates were removed,
and a `lexer_partitions_input_byte_for_byte` unit test guards the shared helper), ~~ingest-mode
strings matched in two places (make it an enum)~~ (**Fixed.** The
statement now stores `adbc_core::options::IngestMode` (the spec enum), parsed once at `set_option`
time by `ingest_mode_option` in `src/statement.rs` — which accepts both the canonical
`adbc.ingest.mode.*` spellings (via the `adbc_core::constants` values) and the bare short forms,
and rejects unknown modes with the same `NotImplemented` error as before; `build_ingest_table_ddl`
matches the enum exhaustively with no fallback arm, and `get_option` still reports the canonical
spelling via `String::from(IngestMode)` — unit-tested offline by
`ingest_mode_parses_both_spellings_and_rejects_unknown`), positional column
indices into INFORMATION_SCHEMA batches, ~~`get_option_int` inconsistencies (database vs statement,
and "not an integer" reported as `NotFound`)~~ (**Fixed.** All three levels' `get_option_int`/
`get_option_double` now delegate to `get_option_string` and parse via shared helpers in
`src/options.rs` (`int_from_stored_string` / `double_from_stored_string`): unset/unknown options
keep the string getter's `NotFound` unchanged, while a set-but-non-integer value is
`InvalidArguments` naming the option and value — `NotFound` again means only "option unset". As a
bonus, integer-valued options (`spanner.impersonate.lifetime`, `spanner.rows_per_batch`,
`spanner.max_partitions`) are now gettable as ints at every level that stores them; covered by
offline unit tests in `src/options.rs` and `src/driver.rs`), SQL-text helpers scattered across three modules.

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

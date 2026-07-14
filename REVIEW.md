# Project review — adbc-spanner

A full-project review run on 2026-07-13 by eight parallel review passes (correctness &
error handling, concurrency & the sync-over-async bridge, ADBC spec compliance, type
conversion & the data path, performance, Spanner utilization, security, testing, and
idiomatic code/clarity). Every finding was verified against the actual source (and,
where relevant, the pinned `arrow-adbc` / `google-cloud-rust` git checkouts) before
being listed.

Each finding is a checkbox — tick it when fixed (or explicitly decided against, noting
why next to the item). IDs are stable for cross-referencing.

**Severity counts:** High 4 · Medium 17 · Low ~55 · Upstream 12.

**Overall shape:** the codebase is in unusually good health — centralized identifier
quoting with hostile-name tests, bound-parameter metadata filters, exact ADBC result
schemas, redacted `Debug`, SHA-pinned OIDC release pipeline, and a layered test stack
(emulator + in-process gRPC mock + Toxiproxy + C++ validation + ASan + fuzzing) with
anti-vacuous-skip guards. The real issues cluster in four places: two reachable panics
/ state-poisoning bugs (COR-1, COR-2), `execute_update`'s handling of non-DML text
(COR-3), a cancel-signal reset race (CON-2), and a family of options that are
parse-tested but never verified to reach the wire (TEST-1..5).

---

## 1. Correctness & error handling

- [x] **COR-1 (High)** — Panic on oversized duration values in two user-facing options — `src/staleness.rs:228`, `src/request.rs:310`
  `parse_duration` calls `Duration::from_secs_f64(seconds)` without an upper-bound check; anything above ~1.8e19s **panics** (verified by repro). Reachable from plain option strings: `spanner.read.staleness = "exact:1e20"` (also `max:`, or `exact:1e19h` since the unit multiplies first) and `spanner.commit.max_delay = "1e30"` (the panic fires before the 500 ms cap check). A malformed option must be `InvalidArguments`; instead the panic crosses into the FFI exporter's poison latch and bricks the driver handle. Sibling modules (`timeout.rs:143`, `retry.rs`) already use `Duration::try_from_secs_f64`. **Fix:** `Duration::try_from_secs_f64(...).map_err(|_| bad())`.

- [x] **COR-2 (High)** — Manual-mode bulk ingest partially poisons the transaction buffer on a mid-row conversion failure — `src/statement.rs:567-574`
  The manual branch of `run_ingest_mutations` buffers row-by-row: `txn.buffer_mutation(bind::insert_mutation(...)?)`. `insert_mutation` can fail on a specific row (out-of-range Date32/Date64, out-of-range non-nanosecond timestamp), leaving rows `0..k` already in `TxnState::pending_mutations`. A later `commit` — or a re-ingest of fixed data then commit — silently applies duplicate/partial rows atomically with the rest of the transaction. Breaks the module's own "commit applies the user's whole transaction atomically" contract. **Fix:** build all mutations into a `Vec<Mutation>` first (or reuse `build_range_mutations` over the full range), buffer only after the whole batch converts. (Also resolves the lock-hold half of CON-4.)

- [x] **COR-3 (Medium)** — `execute_update` mis-routes non-DML SQL (SELECT/WITH); in manual mode it silently "succeeds" and poisons the eventual commit — `src/statement.rs:1472-1508`
  `execute_update` never checks `is_dml` (unlike `execute` at `statement.rs:1397`): anything not DDL goes to the DML pipeline. Autocommit: a SELECT fails with a raw, misleading Spanner `ExecuteBatchDml` error. Manual mode: `execute_update("SELECT 1")` returns `Ok(None)` and buffers the SELECT as pending DML — subsequent queries are rejected by the read-your-writes guard, and `commit()` fails the whole batch; only `rollback` (discarding legitimate DML) recovers. Same for mixed `;`-batches (`"DELETE FROM t; SELECT 1"`). Spec angle: `adbc.h` allows `ExecuteQuery(out=NULL)` for *any* statement, so a hard error on SELECT is itself debatable (see UP-11). **Fix:** either run non-DDL/non-DML through the read-only query machinery and discard rows (returning `None`), or reject each non-DML split statement with `InvalidArguments` before buffering.

- [ ] **COR-4 (Medium)** — Boolean options: set-as-int succeeds, get-as-int fails (round-trip lie) — `src/options.rs:20-31,70-75`
  Every boolean option accepts `OptionValue::Int` on set, but `get_option_int` routes through the canonical `"true"`/`"false"` string and errors `InvalidArguments: value "true" is not an integer`. A C caller doing `SetOptionInt(k,1)` then `GetOptionInt(k)` gets an error instead of `1`. **Fix:** map `"true"`/`"false"` → `1`/`0` in `int_from_stored_string` (or the boolean getters).

- [ ] **COR-5 (Low)** — BatchWrite ingest error under-reports rows already applied — `src/statement.rs:794-811`
  On a failed mutation group, `applied` (groups that *did* commit within the same chunk) is discarded; the "N row(s) already committed" annotation counts only earlier chunks. A mid-stream transport error likewise drops the count. **Fix:** fold `applied` into the error annotation.

- [ ] **COR-6 (Low)** — Mutation-build failures in a later chunk bypass the committed-rows annotation — `src/statement.rs:663,694`
  `self.build_range_mutations(...)?` propagates conversion errors before the `note_rows_already_committed` arms run, contradicting the `run_ingest` doc ("the error reports their exact count") when earlier chunks already committed. **Fix:** wrap the build error with `note_rows_already_committed(e, prior_total)` too.

- [ ] **COR-7 (Low)** — `read:`/`min:` staleness prefixes are case-sensitive while `exact:`/`max:` are not — `src/staleness.rs:175-190`
  `"MAX:1m"` parses; `"READ:2026-07-07T00:00:00Z"` falls through to the bare-RFC3339 parse and yields the generic grammar error. **Fix:** case-insensitive prefix match for the absolute forms as well.

- [ ] **COR-8 (Low)** — `from_status_parts` path drops `google.rpc.Status.details` on BatchWrite errors — `src/error.rs:171-183`, `src/statement.rs:804`
  `BatchWriteResponse.status` carries full details (ErrorInfo/BadRequest), but only `code` + `message` are forwarded on this one path. **Fix:** accept the details slice and reuse `details_for_adbc`.

- [ ] **COR-9 (Low)** — Bound query with zero total bound rows advertises an empty schema — `src/statement.rs:1031-1036`
  A zero-row bound batch takes the bound-query path, `groups` is empty, and `empty_reader()` returns `Schema::empty()` — not the query's real schema. A DBAPI `executemany` with an empty parameter set gets a schema that disagrees with every non-empty execution. **Fix:** run the PLAN probe (as `execute_schema` does) and return a zero-row reader with the real schema.

- [ ] **COR-10 (Low)** — `commit()` / autocommit-toggle apply buffered writes even when the connection is read-only — `src/connection.rs:1083-1108,754-780`
  `adbc.connection.readonly` is only enforced in the statement write paths; buffer DML → set `readonly=true` → `commit()` (or `autocommit=true`) still writes. **Fix:** check the flag on the commit paths, or document the exception to the "reject all writes" contract.

- [ ] **COR-11 (Low)** — `execute_partitions` rejects DDL cleanly but lets DML through to a raw Spanner error — `src/statement.rs:1576-1581`
  Only `is_ddl` gets the clean `InvalidState`; `INSERT …` reaches `partition_query` and surfaces Spanner's read-only-transaction error. **Fix:** mirror the `check_schema_query` guard (which handles both with clear messages).

- [ ] **COR-12 (Low)** — Inconsistent bound-data consumption on error between execute paths — `src/statement.rs:1403-1404,1500-1501` vs `:1427-1430`
  DML paths clear `self.bound` even on error (deliberate); the bound *query* path clears only on success, so a failed bound query leaves stale rows a later, unrelated `execute` silently reuses. **Fix:** clear on the bound-query error path too. (Related: SPEC-3 — `execute_partitions` never clears bound data at all.)

## 2. Concurrency & the sync-over-async bridge

- [ ] **CON-1 (Medium)** — `block_on` panics (call and drop) when the driver is entered from an async context — `src/runtime.rs:123`, `src/conversion.rs:364`, `src/driver.rs:369`
  Any ADBC call or `RecordBatchReader::next` from a tokio worker thread panics ("Cannot block the current thread from within a runtime"); additionally, if a reader is the *last* `Arc<Runtime>` holder and is dropped on an async thread, `Runtime::drop` panics ("Cannot drop a runtime…"). **Fix:** detect `tokio::runtime::Handle::try_current()` in `block_on_cancellable` (and `connect`'s plain `block_on`) and return a clean error advising `spawn_blocking`; wrap the runtime in a newtype whose `Drop` uses `shutdown_background()` when a runtime context is detected. (Root cause is adbc_core's sync trait design — see UP-13.)

- [x] **CON-2 (Medium)** — Entry-point `cancel.reset()` can silently un-cancel a live streamed reader, and a cancelled stream can present as *cleanly complete* — `src/statement.rs:1369,1434`, `src/connection.rs:932,1151`, `src/runtime.rs:180-185`, `src/conversion.rs:364-369`
  Readers hold a **clone** of the owner's sticky `CancelSignal`; every new operation on the owner resets the shared latch. A `cancel()` that lands between chunk fetches evaporates if any new operation starts before the consumer's next `next()`. Worse: if the cancel arrives while the prefetch channel is full, the task's `try_send(Err(cancelled))` fails and it exits, leaving one buffered `Ok` chunk; after a reset, the old reader yields that chunk then `Ok(None)` — a truncated result presented as a clean end of stream. **Fix:** mint a fresh `CancelSignal` per operation/reader; the owner keeps a handle to the *current* one and `cancel()` forwards to it, so no later operation can clear a reader's latch.

- [x] **CON-3 (Low)** — Read-your-writes guard is check-then-act on the shared `TxnState` — `src/statement.rs:1187,1425`
  The guard unlocks before the read-only snapshot begins; a second statement buffering DML in that window reproduces exactly the silent stale read the guard exists to reject. Only affects concurrent multi-statement use of one manual transaction. **Fix:** re-lock and re-check `query_would_miss_buffered_writes()` once the snapshot/first chunk is established; fail the query if writes appeared in the window. *Resolved by the kind-exclusive manual-transaction rework: a manual-mode query now adopts the transaction's shared read-only snapshot via `TxnState::start_read_txn`, which re-checks the transaction kind under the same lock the write paths buffer under (`buffer_dml`/`buffer_mutation` check-and-buffer in one acquisition), so DML buffered in the window rejects the query instead of it silently reading a stale snapshot.*

- [ ] **CON-4 (Low)** — O(rows) mutation building inside the `TxnState` critical section; blanket `.lock().unwrap()` poisoning policy — `src/statement.rs:567-575` (+ 13 `lock().unwrap()` sites)
  A large manual-mode ingest holds the connection-wide txn mutex for the whole CPU-bound mutation build, stalling every concurrent txn-state user; a panic inside `bind::insert_mutation` poisons the mutex and permanently bricks the connection (every later op panics into the FFI poison latch). **Fix:** build mutations outside the lock, re-check the mode, then append (the COR-2 fix naturally does this); and/or adopt `lock().unwrap_or_else(PoisonError::into_inner)` — `TxnState` has no invariant a poisoned-but-consistent state violates.

- [ ] **CON-5 (Low)** — A cancelled/timed-out ingest-chunk commit is ambiguous, but the error claims exact accounting — `src/statement.rs:1824`, `src/runtime.rs:123`
  Cancel/timeout *drops* the in-flight `Commit` future, which may still land server-side; the "exact rows already committed" annotation counts only earlier chunks, so a caller-driven retry can duplicate rows. **Fix:** when the failing chunk's status is `Timeout`/`Cancelled`, state that the failing chunk's own commit outcome is unknown.

## 3. ADBC spec compliance

(Verified against the pinned `arrow-adbc` checkout, rev `198f39a9…`. The big-ticket items — get_info/get_objects/statistics schemas, depth semantics, option contracts, ingest statuses, FFI export — all check out; see also UP-9..11.)

- [x] **SPEC-1 (Medium)** — covered by **COR-3** (execute_update vs `ExecuteQuery(out=NULL)` for result-producing statements). Tick there.

- [ ] **SPEC-2 (Low)** — `adbc.statement.exec.incremental` rejected even at its spec default — `src/statement.rs:1271-1276`
  Spec default is DISABLED; setting `"false"` should be an accept-default no-op (the `check_ingest_temporary` pattern at `statement.rs:1221-1226`), `"true"` → `NotImplemented`, and the getter should report `"false"` instead of `NotFound`. Generic clients that always write defaults currently break.

- [ ] **SPEC-3 (Low)** — `execute_partitions` neither clears nor fully uses bound data — `src/statement.rs:1145-1157`
  Multi-row bound data is silently truncated to row 0 ("only the first bound row is used"), and bound rows survive the call on a reused handle — inconsistent with every other execute path. **Fix:** `InvalidArguments` on >1 bound rows (or document), and clear `bound` after partitioning.

- [ ] **SPEC-4 (Low, documented deviation — decide & record)** — Isolation-level promotion instead of the spec-recommended error — `src/connection.rs:397-419`
  The spec says a driver *should* error on unsupported levels; the driver promotes upward (documented, JDBC-sanctioned, get_option reports the effective level). Recording as a knowing deviation; literal-minded conformance tooling could flag it. Recommended action: none (tick once acknowledged).

- [ ] **SPEC-5 (Low)** — `get_info(None)` returns a curated subset; explicit requests for the omitted codes return null rows — `src/info.rs:38-46`
  Asymmetric: "all" omits `VendorVersion`/`VendorArrowVersion`/Substrait min-max entirely, while an explicit request yields a null-valued row. Pick one behavior (include the null rows in the default set, or omit them from explicit requests).

- [ ] **SPEC-6 (Low)** — Status-code consistency nits — `src/connection.rs:809-814,1249-1260`, `src/statement.rs:1731-1742`
  `current_catalog`/`current_db_schema` set to non-empty → `InvalidArguments` (C++ PostgreSQL driver uses `NotImplemented` for the same class); `execute_schema` guard returns `InvalidState` for DDL but `InvalidArguments` for DML on the same "not a query" class. Cosmetic alignment only.

## 4. Type conversion (correctness)

- [x] **CONV-1 (Medium)** — Null-typed bind columns rejected, contradicting the driver's own `get_parameter_schema` — `src/bind.rs:263-366`, `src/statement.rs:1678`
  `get_parameter_schema` advertises every `@param` as `DataType::Null`, but `scalar_binder`/`cell_value` have no `Null` arm — a batch built from that very schema (or pyarrow's inferred `null` column for all-`None` params) fails `InvalidArguments`. **Fix:** add a `DataType::Null` arm returning `null_value()`.

- [x] **CONV-2 (Medium)** — No `Dictionary` (or `RunEndEncoded`) support on the Arrow→Spanner path — `src/bind.rs:263-366`
  Pandas categoricals / dictionary-encoded string columns are rejected wholesale even though the value type is supported. **Fix:** unwrap `Dictionary(_, value_ty)` in `cell_value` (resolve `keys[row] → values[key]`, or `arrow_cast` once per batch). *Resolved:* `cell_value` resolves `keys[row] → values[key]` and recurses on the value type (bind + mutation ingest + create-mode DDL). `RunEndEncoded` stays rejected — no known producer emits it over the C data interface today; revisit on demand.

- [x] **CONV-3 (Medium)** — `INTERVAL` missing from `is_groupable`'s non-groupable list can break `get_statistics` for the whole database — `src/statistics.rs:328-335`
  `COUNT(DISTINCT interval_col)` fails the per-table aggregate and the error propagates out of `collect_statistics`, so one INTERVAL column anywhere fails the entire call. The function's own doc says the list must stay complete. **Fix:** add `INTERVAL` (excluding a groupable type is always safe; the reverse fails the whole scan). Check `UUID` on the emulator while at it. *Resolved:* `INTERVAL` added to the non-groupable list (defensive: the docs currently disallow INTERVAL as a column type, but `SPANNER_TYPE = 'INTERVAL'` already surfaces via views and excluding a groupable type is always safe). `UUID` verified groupable on the emulator (`COUNT(DISTINCT)`/`GROUP BY`/PK all work), so it stays distinct-countable — locked in by a UUID column in the `get_statistics_reports_real_counts` integration test; INTERVAL keeps unit-test coverage only (the emulator rejects INTERVAL table columns outright).

- [ ] **CONV-4 (Low)** — Unchecked `children.len() as i32` offset cast in `build_list`, inconsistent with `nested.rs` — `src/conversion.rs:948`
  `nested.rs:76-89` does checked `i32::try_from` + `checked_add`; `build_list` wraps silently. Practically unreachable, but it's the silent-corruption class the file otherwise eliminates. **Fix:** mirror the checked pattern.

- [ ] **CONV-5 (Low)** — Write-back asymmetry for ENUM / PROTO / INTERVAL / UUID is undocumented (unlike JSON's) — `src/conversion.rs:691-694`, `src/bind.rs`
  Values read from these columns can't be bound back via DML params (untyped params infer INT64/BYTES/STRING; Spanner won't coerce), though mutation-based ingest works. JSON got the `arrow.json` mechanism + docs; these got neither. **Fix:** at minimum document the `CAST(@p AS …)` workaround in the type-mapping docs.

- [ ] **CONV-6 (Low)** — Duplicate struct field names collapse to the first value in the keyed `build_struct` path — `src/conversion.rs:985-991`
  Only reachable through the defensive `Kind::Struct` branch (the wire encodes STRUCT positionally, handled correctly, dups included) — latent inconsistency, not a live bug.

- [ ] **CONV-7 (Low)** — `parse_numeric_i128` accepts non-canonical `".5"` / `"5."` against its own comment — `src/conversion.rs:1158-1163`
  Spanner never emits these and the parse is correct; strictness/doc nit only.

## 5. Performance & efficiency

- [ ] **PERF-1 (Medium)** — Bound-query stream has no prefetch; fetch and conversion fully serialized — `src/conversion.rs:514-553`
  Unlike `SpannerBatchReader`, `BoundQueryBatchReader::next` fetches inline, so chunk N+1's fetch waits for chunk N's conversion *and* consumption — the `executemany`-style SELECT case degrades to strictly alternating I/O and CPU. **Fix:** implement `ChunkSource` over `(transaction, statements, result_set)` and route through `spawn_prefetch` like `stream_query`.

- [x] **PERF-2 (Low)** — One heap `Vec` allocated and freed per BYTES cell — `src/conversion.rs:892-904`
  `STANDARD.decode(s)` allocates per cell, then `append_value` copies again. **Fix:** a reused scratch `Vec` + `decode_vec` (buffer *reuse*, not the pre-sizing that previously regressed). *Resolved:* the `DataType::Binary` arm of `build_array` hoists one scratch `Vec<u8>` above the row loop and decodes each cell via `STANDARD.decode_vec` (cleared per cell — `decode_vec` appends), keeping the strict decode-error semantics; nested ARRAY/STRUCT recurse through the same arm. Measured ~11% faster on the new `bytes_binary` bench in `benches/conversion.rs` (8192 rows × 48 decoded bytes: 242 µs → 214 µs/chunk).

- [ ] **PERF-3 (Low)** — Residual chrono cost per DATE/TIMESTAMP cell — `src/conversion.rs:1052-1054` + timestamp arms
  Each DATE cell builds two `NaiveDate`s (value + re-derived epoch); each TIMESTAMP runs the full ymd/hms/nanos chain. **Ideas:** days-from-civil formula (or `num_days_from_ce() - 719_162`) for dates; memoize the last 10-byte date prefix → epoch-days for timestamps (result sets cluster on one day). Bench with `bench_support` before/after.

- [ ] **PERF-4 (Low)** — `pull_chunk` allocates full 8192-row capacity regardless of result size — `src/conversion.rs:200`
  A `SELECT 1` pays the full chunk allocation; every tail chunk over-allocates. **Fix:** cap the initial reserve (e.g. `max.min(1024)`) or grow naturally — measure first.

- [ ] **PERF-5 (Low)** — Per-cell `String` clones building metadata arrays — `src/objects.rs:715,728-733,795-807,862-867,889-897`, `src/statistics.rs:190,355-357,404-409`, `src/objects.rs:315`
  `StringArray::from_iter(iter.map(|x| Some(x.name.clone())))` → use `Option<&str>` / `from_iter_values`; `(0..n).map(|_| None).collect()` → `vec![None; n]`; don't `to_string()` before the table-type filter can drop the row.

- [x] **PERF-6 (Low)** — `LikeMatcher::matches` allocates a `Vec<char>` per candidate — `src/connection.rs:684`
  The type amortizes pattern compilation but re-allocates every value. Walk by byte offset instead. Metadata-path only. *Resolved:* `matches` walks the value by byte offset, decoding one `char` at a time and advancing by `len_utf8()` (backtracking included), so matching a candidate allocates nothing while `_` still consumes exactly one character of any UTF-8 width; multi-byte semantics pinned by `like_matching_multibyte_utf8` in `src/connection.rs`.

- [x] **PERF-7 (Low)** — `is_raw_prefix` allocates per word lexeme — `src/sql.rs:90`
  `to_ascii_lowercase()` per `Word` in every lexed SQL string; `eq_ignore_ascii_case` is allocation-free. *Resolved:* replaced the `to_ascii_lowercase()` + `matches!` with three `eq_ignore_ascii_case` comparisons (allocation-free); case-variant coverage added as `raw_prefix_detection` in `src/sql.rs`.

- [ ] **PERF-8 (Low)** — `InfoValue::Str(String)` for static-only strings; duplicated `arrow_err` helper — `src/info.rs:49-77,159`
  All `Str` values are `&'static str`; switch the variant and drop four allocations. Reuse `nested.rs:19`'s `arrow_err`.

## 6. Utilizing Spanner well

- [x] **SPAN-1 (Medium)** — Every ADBC connection rebuilds the whole Spanner client stack (4 TLS channels + CreateSession + credential resolution + a lingering session-maintenance task) — `src/driver.rs:567,369-448`
  The client's docs say `DatabaseClient` is long-lived, one per database, and *cloning* is cheap (shares session + channels); the ADBC `Database` object is exactly the right owner. DBAPI-style pools currently multiply handshakes/sessions/maintenance tasks by pool size. **Fix:** cache the built `Spanner` + `DatabaseClient` in `SpannerDatabase` (invalidated when a database option changes) and clone into each connection; make per-connection isolation opt-in if ever wanted.

- [ ] **SPAN-2 (Medium)** — Partitioned DML not exposed — `src/statement.rs:1472`; client `partitioned_dml_transaction.rs`
  Large backfills/`DELETE WHERE` are forced through a single read/write transaction into the mutation-cap cliff the ingest bisect exists to dodge. **Fix:** a `spanner.dml.partitioned` boolean statement option routing single, non-`THEN RETURN` DML through `partitioned_dml_transaction().execute_update(...)` (reject in manual mode and for `;`-batches; return PDML's lower-bound count).

- [ ] **SPAN-3 (Medium)** — `get_statistics` aggregate scans ignore priority/tag/directed-read — `src/statistics.rs:197-201`
  These full-table `COUNT(*)`/`COUNTIF`/`COUNT(DISTINCT)` scans are the heaviest queries the driver issues on its own, yet run untagged at default priority on default replicas even when the connection configured otherwise. Staleness *is* honored here, so the precedent exists. **Fix:** apply the connection's `RequestConfig`, `DirectedRead`, and `RetryConfig` to the scan statements.

- [ ] **SPAN-4 (Low)** — `get_objects` snapshot (and the `execute_schema` PLAN probe) ignore `spanner.read.staleness` — `src/objects.rs:246-250`, `src/statement.rs:1538`
  `get_table_schema`, the statistics scans, and the `execute_partitions` PLAN probe honor the bound; these two don't. `ReadStaleness::multi_use_timestamp_bound` already exists — one line each.

- [ ] **SPAN-5 (Low)** — `get_statistics` has no snapshot consistency across discovery and per-table scans — `src/statistics.rs:97-119,197`
  Every table's counts are taken at a different timestamp; a table created between discovery and scan can fail the call. **Fix:** run discovery + scans in one multi-use read-only transaction (as `collect_objects` does), or pin one timestamp.

- [x] **SPAN-6 (Low)** — Mutations-only manual commit uses the read/write runner instead of the write-only transaction — `src/connection.rs:320-343,498`
  `WriteOnlyTransaction::write` documents exactly-once replay protection — precisely the ambiguous-transport double-apply caveat the module doc warns about. It already gets the same config via `apply_to_write_only`. **Fix:** in `apply_transaction`, when `statements.is_empty()`, route through the write-only machinery. *Resolved:* `apply_transaction` routes a mutations-only commit through the shared `write_mutations_txn` (`src/connection.rs`; `write_mutation_chunk` now delegates to it too), preserving the `apply_to_write_only` config — commit priority/transaction tag/`max_commit_delay`/commit-stats capture/retry+backoff — while the isolation level is inapplicable (no reads, and the write-only builder has no isolation setter). Wire-asserted by `mutations_only_manual_commit_uses_the_write_only_path` in `tests/mock_spanner.rs` (mutations-only: one begin *with* `mutation_key`, commit by id, no `ExecuteBatchDml`; with DML: the read/write runner as before).

- [x] **SPAN-7 (Low)** — `last_statements` left off for multi-statement autocommit `;`-batches — `src/connection.rs:451-477`
  `set_last_statements(true)` only when `len() == 1`, yet every mutation-free autocommit batch is by construction the transaction's entire content. Extending to `>= 1` is equally safe and covers dbt-style `DELETE; INSERT`. *Resolved:* `run_batch_dml` now flags every autocommit batch (`last_statements = true` unconditionally — the batch is always the transaction's whole content there); the manual-commit path still goes through `run_batch_txn` with the flag off (its commit may apply buffered mutations after the batch). Wire-asserted both ways by `autocommit_batch_dml_is_flagged_last_statements_but_manual_commit_is_not` in `tests/mock_spanner.rs`.

- [ ] **SPAN-8 (Low, tracked limitation)** — `spanner.request.priority` never reaches the `ExecuteBatchDml` RPC (all plain DML) — `src/request.rs:224-232`
  `BatchDmlBuilder` exposes only `set_request_tag`, though the proto carries full `RequestOptions`. Priority applies to the commit only. Documented driver-side; blocked on UP-4. Tick when tracked/linked to the upstream issue.

- [ ] **SPAN-9 (Low)** — Client features worth exposing as driver options — client `database_client.rs`, `transaction_runner.rs`
  `with_database_role` (FGAC → `spanner.database_role`), `with_leader_aware_routing(bool)`, `set_exclude_txn_from_change_streams` (ingest/ETL commits → `spanner.transaction.exclude_from_change_streams`; BatchWrite blocked on UP-5), `set_read_lock_mode` (pairs with `repeatable_read`), and documenting `SPANNER_NUM_CHANNELS` in a README tuning note.

- [x] **SPAN-10 (Low)** — A new Database Admin client is built per DDL statement (incl. create-mode ingests) — `src/statement.rs:1115-1119`
  Cache it lazily on the connection and clone thereafter. *Resolved:* the admin client now lives in a shared `Arc<tokio::sync::OnceCell<DatabaseAdmin>>` on the database's cached `Connected` stack (`SharedDatabaseAdmin` in `src/driver.rs` — the SPAN-1 owner, so a `set_option` that invalidates the stack drops the admin client with it), threaded into every connection/statement; `run_ddl` builds it once via `get_or_try_init` (still inside the update timeout; a failed build stays uncached and retries) and reuses it thereafter.

## 7. Security

(All Low. Injection surfaces, credential redaction/scrubbing, the emulator credential guard, FFI panic containment, and the supply chain/CI posture were each examined and found solid — see `quote_ident`'s correct GoogleSQL escaping, bound-parameter metadata filters, `set_sensitive(true)` on the bearer header, `scrub_credential_error`, digest-pinned workflows and OIDC publishing.)

- [x] **SEC-1 (Low)** — `get_option` returns live secrets verbatim (`spanner.auth.keyfile_json`, `spanner.auth.access_token`) — `src/driver.rs:519,535`
  Any tooling that dumps connection options prints a usable private key / bearer token. `NotFound` or a `"<redacted>"` sentinel for these two keys is spec-conformant. Keep `spanner.auth.keyfile` (a path) readable. *Resolved:* the two secret-holding keys are now **write-only** — `get_option_string` (and the bytes/int/double getters that funnel through it) always fails with `NotFound` and a "write-only (it holds a secret)" message, whether the option is set or not, mirroring the `Debug` redaction of the same fields; `spanner.auth.keyfile` (a path) still round-trips. `NotFound` over a `"<redacted>"` sentinel follows the surveyed drivers: the C++ PostgreSQL driver returns `NOT_FOUND` for every database option (its password-bearing `uri` included), the Go Snowflake driver's JWT private-key material is likewise never gettable, and the Go driverbase reports unknown options as `StatusNotFound` — no surveyed driver uses a sentinel. Covered by `keyfile_path_round_trips_but_inline_json_is_write_only` / `access_token_is_write_only` in `src/driver.rs`; documented in `docs/options.md`, README § Authentication, `python/README.md` + `_options.py`, and the two `OPTION_*` rustdocs.

- [ ] **SEC-2 (Low)** — Connection URI accepts secret-bearing query parameters — `src/driver.rs:601-612`
  `URI_QUERY_OPTIONS` includes `keyfile_json` and `access_token`; URIs are the most-logged config artifact there is (shell history, process listings, tracing spans). **Fix:** drop the two secret options from `URI_QUERY_OPTIONS`, or at minimum document the hazard.

- [ ] **SEC-3 (Low)** — Partition-descriptor "executable, unauthenticated" caveat is rustdoc-only — `README.md` partitioned-execution bullet, `python/README.md`
  The primary consumers of descriptors-as-bytes (Python/driver-manager users) never see the `read_partition` `# Security` rustdoc. One sentence in each README.

- [ ] **SEC-4 (Low)** — `workflow_dispatch` input interpolated directly into a shell script — `.github/workflows/fuzz.yml:79`
  `-max_total_time=${{ github.event.inputs.max_total_time || '1200' }}` in `run:`. Requires dispatch access and the job is `contents: read`, but it's the one deviation from otherwise clean expression hygiene. **Fix:** pass via `env:`.

- [ ] **SEC-5 (Low)** — No driver-side depth cap on nested STRUCT/ARRAY type recursion from the server — `src/conversion.rs:658-696`
  A hostile endpoint (attacker-controlled `SPANNER_EMULATOR_HOST`) returning pathological `STRUCT<STRUCT<…>>` metadata could drive stack exhaustion (abort, not corruption — but it kills the host app embedding the cdylib). Likely bounded in practice by prost's decode recursion limit (default 100) — **verify that limit holds on the pinned transport, or add a cheap depth check in `arrow_type`**. Re-verify on every `google-cloud-rust` rev bump.

## 8. Testing

- [x] **TEST-1 (High)** — `spanner.read.staleness` has zero behavioral coverage — nothing verifies a non-strong `TransactionSelector` ever reaches the wire
  Parse/round-trip unit tests only; a regression dropping the `staleness::single_use` call at the query sites would pass all gating CI. **Fix:** mock-server tests capturing `ExecuteSqlRequest.transaction` for each of the four prefixes, plus a bound-query case asserting the multi-use begin pins `max:`/`min:` per `pinned_for_multi_use`. Optionally an emulator test with `exact:1ms`.

- [x] **TEST-2 (High)** — `spanner.directed_read` never asserted on the wire; the "read-only paths only" contract untested
  A regression applying it to DML would break *writes* whenever the option is set (Spanner rejects directed reads on r/w transactions). **Fix:** mock test asserting `ExecuteSqlRequest.directed_read_options` populated on a query and absent on DML.

- [ ] **TEST-3 (Medium)** — Isolation-level promotion never verified to reach `TransactionOptions` — mock test asserting `isolation_level` on the begin of an autocommit DML.

- [ ] **TEST-4 (Medium)** — Priority/tags integration test is wire-vacuous (`tests/integration.rs:6977` — the emulator ignores `RequestOptions`) — mock test capturing `request_options` on query + commit, plus a metadata path asserting *empty* options.

- [ ] **TEST-5 (Medium)** — `spanner.commit.max_delay` never observed on a `CommitRequest` — extend the existing `commit_stats_mutation_count_is_captured_from_the_commit_response` mock test (which already asserts `return_commit_stats`) with `max_commit_delay`.

- [ ] **TEST-6 (Medium)** — Retry knobs never shown to bound attempts (`tests/integration.rs:4685` is happy-path) — mock: always-`UNAVAILABLE` `ExecuteStreamingSql`, `max_attempts=2`, assert exactly 2 attempts via the existing `AtomicUsize` pattern.

- [ ] **TEST-7 (Medium)** — Fetch timeout never observed firing — reuse the silent-stream script from `cancel_unblocks_a_reader_hung_on_a_silent_stream` (its own doc comment calls it "the foundation for future timeout tests"), `fetch=0.5`, assert the second `next()` yields `Status::Timeout`. Also gives resilience's timeout assertion a gating twin (its cancel twin already exists).

- [ ] **TEST-8 (Medium)** — Mock error-path gaps: (a) `ExecuteBatchDml` mid-batch non-OK status (`;`-batch error semantics undetermined); (b) `BatchWrite` per-group failure — the documented `from_status_parts` remap never driven through the wire (cheap: one group with `ALREADY_EXISTS`); (c) commit-ABORT-and-replay — documented future work in `RESILIENCE.md`, but buffer-and-replay under abort is the driver's core transaction claim; keep on the list.

- [ ] **TEST-9 (Medium)** — Fuzzing misses `staleness::parse_read_bound`/`parse_duration` (COR-1 would have been found by this), `directed_read::parse` (the most complex hand parser), and the `spanner:` URI expansion (unreachable from the `options` target, which feeds only `Other(key)`); lower-value: an `arbitrary`-driven hostile proto-value target for `rows_to_arrow` (pairs with SEC-5). Each ~10 lines on the existing wrapper pattern.

- [ ] **TEST-10 (Low)** — `python/tests/test_options.py` asserts only enum *names* — one pytest driving a vendor option through `db_kwargs` end-to-end.

- [ ] **TEST-11 (Low)** — Benchmarks (`benches/`) have no CI consumer — no perf-regression gate; acceptable now, worth a nightly smoke eventually.

## 9. Idiomatic code & clarity

- [ ] **IDIO-1 (Medium)** — Broken quickstart URI example the driver rejects — `src/ffi.rs:19`, `docs/adbc.md:135`
  Both show `uri="projects/p/…"`; `driver.rs:193` rejects bare paths. **Fix:** `spanner:///projects/p/instances/i/databases/d`.

- [ ] **IDIO-2 (Medium)** — Commit/read-config plumbing threads 9–14 positional args — `src/connection.rs:451,498`, `src/statement.rs:211`
  The same bundle the `impl_shared_option_dispatch!` macro already treats as one unit travels as loose params (two `#[allow(too_many_arguments)]`s; transposition risk; every new knob touches every call site). **Fix:** a `SharedConfig` struct owned by connection and statement, passed as one argument.

- [ ] **IDIO-3 (Medium)** — Byte-identical `apply_to_runner`/`apply_to_write_only` bodies — `src/request.rs:236-274`, `src/retry.rs:273-307`
  A fifth commit-site option means editing two bodies per file; forgetting one is silent. A small per-file `macro_rules!` emits both from one body (no shared trait exists on the client builders).

- [ ] **IDIO-4 (Low)** — Four near-identical f64 "seconds" option parsers — `src/retry.rs:344,378,410`, `src/timeout.rs:120`
  One `seconds_option(value, what, allow_zero)` in `options.rs` next to `bool_option` absorbs them. (Fixing COR-1 in the shared helper covers the whole family.)

- [ ] **IDIO-5 (Low)** — Four private `as_string` copies shadow `options::string_option` — `src/request.rs:320`, `src/staleness.rs:154`, `src/directed_read.rs:163`, `src/query_options.rs:83` (+ duplicated `non_empty` helpers)
  Exactly the coercion `options.rs` exists to centralize, per its own module doc.

- [ ] **IDIO-6 (Low)** — `first_keyword` hand-rolls a second comment/quote scanner beside the shared lexer — `src/sql.rs:318,357`
  Contradicts the `Lexeme` rustdoc's one-lexer claim (sql.rs:146). Rewriting over `lex()` deletes both helpers and drops a per-classification `String` allocation.

- [ ] **IDIO-7 (Low)** — Option-error labels inconsistently name the option — `src/statement.rs:1722,1790,1806,1811`, `src/connection.rs:1238`
  Some errors say which key failed; others say `"option expects a boolean"` or use a short name that isn't the key. Pass the full key as `what` everywhere.

- [ ] **IDIO-8 (Low)** — Stale rustdoc on `run_batch_dml` — `src/connection.rs:436`
  Claims three callers; verified only one (`run_or_buffer`). Trim to reality.

- [ ] **IDIO-9 (Low)** — Three probe-remap sites, three behaviors on probe failure — `src/connection.rs:1001` vs `remap_ingest_create_error` / `remap_ingest_append_error`
  `get_table_schema`'s fallback drops the original error when the probe itself fails. Align on the `Ok(false) => NotFound, _ => original` shape.

- [ ] **IDIO-10 (Low)** — Missing `#[must_use]` on the builder-threading `apply_to_*` family — `src/request.rs`, `src/retry.rs`, `src/directed_read.rs:154`, `src/query_options.rs:67`
  A discarded return silently loses builder *and* config.

- [ ] **IDIO-11 (Low)** — `explicit_credential_option` duplicates `conflicting_credential_with_access_token` — `src/driver.rs:255-282`
  Express one in terms of the other so a new credential option can't be added to only one.

- [ ] **IDIO-12 (Low)** — Five copy-pasted `match`-on-`Option` grouping blocks — `src/objects.rs:269-288`
  `x_batch.as_ref().map(group_x).transpose()?.unwrap_or_default()`.

- [ ] **IDIO-13 (Low)** — Typed-getter boilerplate repeated at all three levels — `src/driver.rs:549-561`, `src/connection.rs:858-870`, `src/statement.rs:1332-1344`
  A sibling `impl_typed_option_getters!` macro in options.rs absorbs it. (Note COR-4 changes this code first.)

- [ ] **IDIO-14 (Low)** — `pub(crate)` on module-internal items — `src/sql.rs:89-176`, `src/retry.rs:204`, `src/request.rs:89,309`, `src/staleness.rs:172`
  Verified no external users; making them private re-arms dead-code lints.

- [ ] **IDIO-15 (Low)** — Six near-identical primitive bind arms — `src/bind.rs:265-296`
  A generic `fn primitive<T: ArrowPrimitiveType, …>` coerced to the fn-pointer table removes the drift-prone bodies.

- [ ] **IDIO-16 (Low)** — `b.unwrap()` in a cdylib-facing metadata path — `src/statistics.rs:218`
  Holds today; a future early-`break` turns it into a panic across the C ABI, against `nested.rs`'s stated house rule. Use `ok_or_else` + `collect::<Result<_>>()`.

- [ ] **IDIO-17 (Low)** — Stale "the three driver options" count in module doc — `src/request.rs:8` (five are documented below it). Reword count-free.

- [ ] **IDIO-18 (Low)** — Oversized functions worth a seam — `src/driver.rs:289` (`connect` ~160 lines: extract the testable five-way credential ladder), `src/objects.rs:131` (`collect_objects` ~220 lines: the pure assemble loop at `:291-349` extracts cleanly and becomes offline-testable; also `&Option<T>` → `Option<&T>` params). The other >100-line functions are exhaustive dispatch tables and fine.

- [ ] **IDIO-19 (Low)** — Micro-fix while nearby — `src/conversion.rs:975`: `children.iter_mut().for_each(|c| c.push(None))` → a plain `for` loop.

## 10. Upstream candidates

Things to file or PR against `apache/arrow-adbc` or `googleapis/google-cloud-rust`.

### google-cloud-rust

- [ ] **UP-1** — `StatementBuilder::add_param` / `ValueBinder::to` take `&T: ToValue`, deep-cloning every cell (`impl ToValue for Value` = `self.clone()`, `to_value.rs:54`) — a second full copy of every string/bytes/array payload on every bound-DML row and ingest cell. Ask for by-value overloads (`impl Into<Value>`).
- [ ] **UP-2** — No `impl ToValue for &[u8]` (only `Vec<u8>`), forcing `.to_vec()` per binary cell in `src/bind.rs:314-328` (slice → Vec → base64 String → UP-1's clone = 3 copies).
- [ ] **UP-3** — `SpannerRetryPolicy` is private, forcing the driver's byte-for-byte behavioral copy (`src/retry.rs:85`) that can silently drift on a rev bump. Ask to export it, or for a hook to bound the default policy.
- [ ] **UP-4** — `BatchDmlBuilder` has no priority setter though `ExecuteBatchDmlRequest.request_options` carries one (it already stores `Option<RequestOptions>` internally). Blocks SPAN-8.
- [ ] **UP-5** — `BatchWriteTransactionBuilder` exposes no setters though the proto has `request_options` *and* `exclude_txn_from_change_streams`. Blocks tags/priority/change-stream-exclusion on the firehose ingest path.
- [ ] **UP-6** — No public begin/commit read-write transaction handle — the root cause of buffer-and-replay manual transactions and the no-read-your-writes guard. File the feature request.
- [ ] **UP-7** — Channel-pool size is env-var-only (`SPANNER_NUM_CHANNELS`); a `ClientBuilder` setter would let drivers expose it as a real option.
- [ ] **UP-8** — Confirm the transport's proto-decode recursion limit bounds hostile nested STRUCT metadata (SEC-5); if not, that's an upstream hardening ask.

### apache/arrow-adbc

- [ ] **UP-9** — Rust FFI exporter fails the whole `GetInfo` call on any unrecognized info code (`driver_exporter.rs:1162-1169`; `InfoCode::try_from` errors on anything outside the 11 known codes, including the spec-reserved XDBC range and driver-specific codes ≥ 10_000), where adbc.h requires the row to be omitted. `InfoCode` needs an `Other(u32)` variant or the exporter should filter.
- [ ] **UP-10** — `vendor_code` is unavoidably overwritten with the `INT32_MIN` sentinel for 1.1.0-layout C callers (`types.rs:644-651`, spec-mandated discriminant). This driver's "key off `vendor_code == 10` (ABORTED)" contract (`src/error.rs:36-39`) holds only for Rust-native and 1.0.0-layout consumers — add a doc note in `error.rs`, and suggest upstream forwarding the numeric code as an error *detail*.
- [x] **UP-11** — Spec ambiguity: `ExecuteQuery(out=NULL)` on a result-producing statement (execute-and-discard vs error) is undefined; COR-3 is this driver's sharp edge of it. Ask for a clarifying sentence in adbc.h. *Resolved:* no upstream ask needed — the ambiguity is only in the spec *text*; the de-facto consensus is unambiguous, and since the COR-3 fix (#270) this driver already implements it. What we do: `execute_update` classifies non-DML/non-DDL SQL and runs it through the read-only query machinery, draining and discarding the rows, returning `None` (`rows_affected = -1` at the FFI layer, which the `adbc_validation` suite always accepts — its assertions are `AnyOf(Eq(n), Eq(-1))`); bound params on a SELECT ride the bound-query path the same way, and in manual mode the query arm buffers nothing. This aligns with the ADBC BigQuery Go driver (github.com/adbc-drivers/bigquery), which does no classification at all: `ExecuteUpdate` runs any SQL as a normal query job, never reads the result rows, and returns `NumDmlAffectedRows` verbatim (`0` for a SELECT, no error). It also matches the C++ SQLite and PostgreSQL reference drivers (both execute-and-discard on `out == NULL`) and the maintainer's stated intent — lidavidm in apache/arrow-adbc#61: "I think we can just ignore the result set in that case"; in #540: "there isn't an actual ExecuteUpdate, it's just ExecuteQuery without an output ArrowArrayStream". No issue or PR has ever proposed erroring instead.
- [ ] **UP-12** — FFI exporter soundness: concurrent `statement_cancel`/`connection_cancel` materializes `&mut ExportedStatement` in two threads simultaneously (`driver_exporter.rs:1678`) — UB by aliasing rules even though it happens to work (cancel only touches `Arc`-backed atomics). Needs interior mutability / raw-pointer access for the cancel path upstream; note driver-side that cancel-during-execute relies on this.
- [ ] **UP-13** — (Informational) adbc_core's synchronous trait design is the root cause of CON-1; the driver can only mitigate (error instead of panic), not fix. An async or executor-aware trait surface would be the real solution.

---

## Cross-cutting fix ordering (suggested)

1. **COR-1** (one-line panic fix; add the TEST-9 staleness/duration fuzz target alongside).
2. **COR-2 + CON-4** (one refactor: build mutations outside the lock, buffer atomically).
3. **COR-3 / SPEC-1** (decide execute-and-discard vs reject; both close the manual-mode poisoning).
4. **CON-2** (per-operation cancel signals — the only silent-wrong-data race).
5. **TEST-1..5** (one mock-server pattern — request capture — closes the whole "option never reaches the wire" class).
6. Everything else opportunistically; SPAN-1 (client caching) is the biggest single win for real-world deployments.

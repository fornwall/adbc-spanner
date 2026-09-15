# Project review — adbc-spanner

The **open** findings from the full-project review originally run on 2026-07-13 by eight parallel
passes (correctness & error handling, concurrency & the sync-over-async bridge, ADBC spec
compliance, type conversion & the data path, performance, Spanner utilization, security, testing,
and idiomatic code/clarity). Resolved findings have been removed; everything below was
re-verified against the source on 2026-09-15 (line references refreshed, wording corrected where
the codebase moved under it).

Each finding is a checkbox — tick it when fixed (or explicitly decided against, noting why next to
the item), and delete it once it is no longer relevant. IDs are stable for cross-referencing, so a
new finding never reuses a retired ID.

**Severity counts:** Medium 4 · Low 2 · Upstream 4.

---

## Concurrency & the sync-over-async bridge

- [ ] **CON-1 (Medium)** — `block_on` panics (call and drop) when the driver is entered from an async context — `src/runtime.rs:169,181`, `src/conversion.rs:369,385`, `src/driver.rs:524`
  Any ADBC call or `RecordBatchReader::next` from a tokio worker thread panics ("Cannot block the current thread from within a runtime"); additionally, if a reader is the *last* `Arc<Runtime>` holder and is dropped on an async thread, `Runtime::drop` panics ("Cannot drop a runtime…"). There is no `Handle::try_current()` anywhere in the crate, no mitigation and no user-facing doc warning. The drop hazard now also has a **C-ABI surface**: `src/ffi/stream.rs:52`'s `PrivateData::drop` owns the boxed reader, so a driver manager calling the stream's release callback from a tokio worker thread drops the `SpannerBatchReader` — and possibly the last `Arc<Runtime>` — on an async thread. **Fix:** detect `tokio::runtime::Handle::try_current()` in `block_on_cancellable` (and `connect`'s plain `block_on`) and return a clean error advising `spawn_blocking`; replace the bare `SharedRuntime = Arc<Runtime>` alias (`src/runtime.rs:22`) with a newtype whose `Drop` uses `shutdown_background()` when a runtime context is detected. (Root cause is adbc_core's sync trait design — see UP-13.)

## Performance & efficiency

- [ ] **PERF-9 (Low)** — Bound cells are still passed by reference, taking the deep clone the upstream by-value API now avoids — `src/bind.rs:172,173,190`
  `add_param` / `add_typed_param` / `ValueBinder::to` take `T: Into<Value>` as of googleapis/google-cloud-rust#6184 (`c317aab96`), but the driver still passes `&value`, which routes through `impl<T: ToValue> From<&T> for Value` → `impl ToValue for Value { self.clone() }` — a second full copy of every string/bytes/array payload on every bound-DML row and ingest cell. `cell_value` already returns an owned `Value`, so the fix is dropping the three `&`. (Was **UP-1**, an upstream ask; the upstream half landed.)

- [ ] **PERF-10 (Low)** — `.to_vec()` per binary cell, no longer needed — `src/bind.rs:321-343`
  Four arms (`Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary`) copy each slice into a `Vec<u8>` because only `Vec<u8>` used to implement `ToValue`. The same upstream commit added `impl ToValue for [u8]` and `for &[u8]`, and `scalar_value`'s bound is already `T: ToValue`, so the four `.to_vec()`s can simply be deleted — copies drop 3 → 1 (the base64 encode is unavoidable; it is the wire form). (Was **UP-2**.)

## Utilizing Spanner well

- [ ] **SPAN-2 (Medium)** — Partitioned DML not exposed — `src/statement.rs:940-957,1055,1894-1906`; client `partitioned_dml_transaction.rs:169`
  Large backfills/`DELETE WHERE` are forced through a single read/write transaction into the mutation-cap cliff the ingest bisect exists to dodge. Every DML statement funnels through `run_or_buffer`, whose autocommit arm calls `connection::run_batch_dml`; nothing in `src/` references partitioned DML (`docs/transactions.md` records the non-use explicitly). **Fix:** a `spanner.dml.partitioned` boolean statement option routing single, non-`THEN RETURN` DML through `partitioned_dml_transaction().execute_update(...)` (reject in manual mode and for `;`-batches; return PDML's lower-bound count).

- [ ] **SPAN-3 (Medium)** — `get_statistics` (and `get_objects`) scans ignore priority/tag/directed-read/retry config — `src/statistics.rs:206-219,248-256`
  The full-table `COUNT(*)`/`COUNTIF`/`COUNT(DISTINCT)` scans are the heaviest queries the driver issues on its own, yet `query_txn` builds a bare `SpannerSql::builder(sql).build()` — default priority, no tags, default replicas, the client's unbounded retry policy — even when the connection configured otherwise. Staleness *is* honored (the SPAN-5 shared multi-use transaction, `src/statistics.rs:101-111`) and so are the RPC timeouts, so the precedent exists; `collect_statistics`' signature (`src/statistics.rs:86-94`, called from `src/connection.rs:1555-1563`) is the structural proof that the rest is never even passed in. **Fix:** apply the connection's `RequestConfig`, `DirectedRead`, and `RetryConfig` to the scan statements — noting that the `RequestConfig` half needs an explicit ruling first, since the driver deliberately leaves driver-internal metadata queries untagged. `get_objects` (`src/objects.rs`, called at `src/connection.rs:1422-1435`) has the identical shape and should be fixed in the same pass.

## Testing

- [ ] **TEST-7 (Medium)** — Fetch timeout never observed firing end-to-end through the option on a real stream
  `src/timeout.rs:351` (`fetch_timeout_fires_inside_the_prefetch_task`) fires one, but synthetically — a hand-rolled stalling source through `with_timeout` + `spawn_prefetch`, no option plumbing, no `SpannerBatchReader`, no gRPC stream. `rpc_timeouts` (`tests/integration.rs:8354`) exercises `fetch` only on the happy path; the deadlines it makes fire are `query` and `update`. **Fix:** reuse the silent-stream script from `cancel_unblocks_a_reader_hung_on_a_silent_stream` (`tests/mock_spanner.rs:1356`, whose doc comment calls it "the foundation for future timeout tests"), set `fetch=0.5`, and assert the second `next()` yields `Status::Timeout`. (The update-path gating twin already exists — `ddl_update_timeout_fires_on_a_silent_admin_endpoint`, `tests/mock_spanner.rs:1449`; the fetch path has none.)

## Upstream candidates

Things to file or PR against `googleapis/google-cloud-rust` or `apache/arrow-adbc`. Verified
against the pinned checkouts (`google-cloud-rust` `ec54ef0a`, `apache/arrow-adbc` `32c67b09`).

### google-cloud-rust

- [ ] **UP-6** — No public begin/commit read-write transaction handle — the root cause of buffer-and-replay manual transactions and the no-read-your-writes guard. The only read/write entry point is the callback-driven `DatabaseClient::read_write_transaction()` → `TransactionRunner::run(work)` (`transaction_runner.rs:495`); `ReadWriteTransaction` (`read_write_transaction.rs:373`) is `pub` but has no public constructor and exposes no `commit`/`rollback` (`begin` is private, `read_write_transaction.rs:167`). File the feature request.
- [ ] **UP-7** — Channel-pool size is env-var-only (`SPANNER_NUM_CHANNELS`, read at `client.rs:117`, default 4); a `ClientBuilder` setter would let drivers expose it as a real option. `StaticChannelPoolConfig.num_channels` is `pub(crate)`, and the client carries a `// TODO(channel-pool): … once ChannelPool is integrated into the Spanner client` right above that read — the pool module exists but is not wired in yet, so the ask is well-timed.
- [ ] **UP-14** (half fixed upstream) — `ResultSet::check_retry` seeds gax's `RetryState` per resume decision rather than being driven by gax's own `retry_loop` (server-streaming RPCs bypass it), so on the streaming query path both retry limits were wrong. **The attempt half is fixed**: as of the `ec54ef0a` pin the seed is `1 + self.retry_count` (`result_set.rs:792`), so `spanner.retry.max_attempts = N` permits exactly `N` attempts there, matching the unary paths. **The elapsed half is still open**: `start` is re-taken on every decision (`result_set.rs:793`, via `RetryState::default()` at `gax/src/retry_state.rs:70`), so an elapsed-time limit never fires on a query. Ask upstream to thread the real loop start through — cheap to land, since `RetryState::set_start` already exists (`gax/src/retry_state.rs:55-58`): store the loop start on `ResultSet` and chain it at `result_set.rs:793`. Driver-side this is documented, not compensated (see `src/retry.rs`'s module doc); `retry_max_elapsed_seconds_bounds_unary_rpcs_but_is_inert_on_the_streaming_path` in `tests/mock_spanner.rs` pins the remaining gap.

### apache/arrow-adbc

- [ ] **UP-13** — (Informational) adbc_core's synchronous trait design is the root cause of CON-1 — `rust/core/src/sync.rs` declares every trait with no `async fn` at all. The driver can only mitigate (error instead of panic), not fix. An async or executor-aware trait surface would be the real solution.

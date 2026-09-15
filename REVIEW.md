# Project review — adbc-spanner

The **open** findings from the full-project review originally run on 2026-07-13 by eight parallel
passes (correctness & error handling, concurrency & the sync-over-async bridge, ADBC spec
compliance, type conversion & the data path, performance, Spanner utilization, security, testing,
and idiomatic code/clarity). Resolved findings have been removed; everything below was
re-verified against the source on 2026-09-16 (line references refreshed, wording corrected where
the codebase moved under it).

Each finding is a checkbox — tick it when fixed (or explicitly decided against, noting why next to
the item), and delete it once it is no longer relevant. IDs are stable for cross-referencing, so a
new finding never reuses a retired ID.

**Severity counts:** Upstream 4 (nothing actionable in-repo).

**Declined:** CON-1 (async-context `block_on` panics) — the user ruled it out of scope on
2026-09-15; the driver keeps panicking when entered from a Tokio worker thread. UP-13 records the
root cause upstream.

---

## Upstream candidates

Things to file or PR against `googleapis/google-cloud-rust` or `apache/arrow-adbc`. Verified
against the pinned checkouts (`google-cloud-rust` `ec54ef0a`, `apache/arrow-adbc` `32c67b09`).

### google-cloud-rust

- [ ] **UP-6** — No public begin/commit read-write transaction handle — the root cause of buffer-and-replay manual transactions and the no-read-your-writes guard. The only read/write entry point is the callback-driven `DatabaseClient::read_write_transaction()` → `TransactionRunner::run(work)` (`transaction_runner.rs:495`); `ReadWriteTransaction` (`read_write_transaction.rs:373`) is `pub` but has no public constructor and exposes no `commit`/`rollback` (`begin` is private, `read_write_transaction.rs:167`). File the feature request.
- [ ] **UP-7** — Channel-pool size is env-var-only (`SPANNER_NUM_CHANNELS`, read at `client.rs:117`, default 4); a `ClientBuilder` setter would let drivers expose it as a real option. `StaticChannelPoolConfig.num_channels` is `pub(crate)`, and the client carries a `// TODO(channel-pool): … once ChannelPool is integrated into the Spanner client` right above that read — the pool module exists but is not wired in yet, so the ask is well-timed.
- [ ] **UP-14** (half fixed upstream) — `ResultSet::check_retry` seeds gax's `RetryState` per resume decision rather than being driven by gax's own `retry_loop` (server-streaming RPCs bypass it), so on the streaming query path both retry limits were wrong. **The attempt half is fixed**: as of the `ec54ef0a` pin the seed is `1 + self.retry_count` (`result_set.rs:792`), so `spanner.retry.max_attempts = N` permits exactly `N` attempts there, matching the unary paths. **The elapsed half is still open**: `start` is re-taken on every decision (`result_set.rs:793`, via `RetryState::default()` at `gax/src/retry_state.rs:70`), so an elapsed-time limit never fires on a query. Ask upstream to thread the real loop start through — cheap to land, since `RetryState::set_start` already exists (`gax/src/retry_state.rs:55-58`): store the loop start on `ResultSet` and chain it at `result_set.rs:793`. Driver-side this is documented, not compensated (see `src/retry.rs`'s module doc); `retry_max_elapsed_seconds_bounds_unary_rpcs_but_is_inert_on_the_streaming_path` in `tests/mock_spanner.rs` pins the remaining gap.

### apache/arrow-adbc

- [ ] **UP-13** — (Informational) adbc_core's synchronous trait design is the root cause of CON-1 — `rust/core/src/sync.rs` declares every trait with no `async fn` at all. The driver can only mitigate (error instead of panic), not fix. An async or executor-aware trait surface would be the real solution.

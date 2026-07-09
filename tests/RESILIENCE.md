# Resilience / fault-injection harness

> Part of the testing overview in [docs/testing.md](../docs/testing.md).

`tests/resilience.rs` drives the ADBC driver against the Spanner **emulator reached through a
[Toxiproxy](https://github.com/Shopify/toxiproxy) TCP proxy**, injects transport-level faults, and
asserts the driver behaves sanely under them.

There are two fault-injection harnesses, split by layer:

- **This one (Toxiproxy) — transport faults**: latency, bandwidth throttles, TCP resets. Needs
  Docker; non-gating CI.
- **`tests/mock_spanner.rs` — logical gRPC faults**: an in-process mock
  `google.spanner.v1.Spanner` server (the pinned client's own `spanner-grpc-mock` crate) scripted
  per RPC to return exact gRPC statuses (`ABORTED` + `RetryInfo`, mid-stream `UNAVAILABLE`, a
  stream that goes silent). Runs fully offline in plain `cargo test`; gating CI. This is the
  complement the "Honest limitations" section below asks for — it *can* produce `ABORTED` and
  other logical statuses that an L4 proxy cannot.

```
 driver ─(data-plane gRPC)─▶ Toxiproxy listener ─(upstream)─▶ emulator :9010
                               127.0.0.1:8666                   (container IP)
                                    ▲
                                    │ HTTP admin API :8475  (add/remove toxics)
                               tests/resilience.rs

 schema setup (create instance/DB/table) ───────────────────▶ emulator :9010 direct
```

## Running it

Docker is required. `scripts/with-toxiproxy.sh` starts the emulator and Toxiproxy, wires up the
proxy, exports the environment, runs the command, and tears everything down:

```sh
cargo build
scripts/with-toxiproxy.sh cargo test --test resilience -- --nocapture --test-threads=1
```

Run the tests **serially** (`--test-threads=1`): they share one global proxy, so their toxics
must not overlap.

Without `TOXIPROXY_URL` + `SPANNER_EMULATOR_HOST` set, the tests **self-skip**, so a plain
`cargo test` stays green everywhere — exactly like `tests/integration.rs`.

CI runs it non-gating via `.github/workflows/resilience.yml` (manual dispatch + nightly).

## What the harness injects, and what each test proves

| Toxic | Test | Assertion |
| --- | --- | --- |
| `bandwidth` (downstream throttle) | `cancel_interrupts_in_flight_query` | Throttling a ~48 MB result to 2000 KB/s keeps the streaming reader blocked in a real network read; `Statement::cancel` from another thread interrupts it in **milliseconds** with `Status::Cancelled`, instead of blocking ~18s for the rest of the stream. Drives the real `CancelSignal` → `block_on_cancellable` path. |
| `reset_peer` (immediate TCP RST) | `reset_peer_surfaces_error_then_recovers` | A query under the reset surfaces a **clean ADBC error** (no panic; no unbounded hang — a watchdog thread bounds it), and once the toxic is removed a subsequent query **recovers** as the gRPC channel re-establishes through the proxy. |
| `reset_peer` (immediate TCP RST) | `commit_under_transport_fault_never_loses_the_write` | A manual-transaction `commit()` started under the reset must **never lose the buffered DML**: the write has to land once the toxic is removed. With the pinned client the commit *blocks and heals* — it retries internally and succeeds on its own once the transport recovers (see the limitation below). If a future client surfaces the error instead, the test's other branch asserts the driver kept the buffer so a retried `commit()` replays it (guarding the take-before-apply regression, where a retried commit vacuously succeeded on an emptied buffer — a silent lost write). The buffered DML is an idempotent UPDATE, so the assertion holds regardless of whether the faulted commit reached the emulator. |
| `reset_peer` (immediate TCP RST) | `update_timeout_bounds_a_faulted_write_then_recovers_when_unset` | With `spanner.rpc.timeout_seconds.update` set to a short deadline, an autocommit write launched under the reset — which the client would otherwise retry unboundedly (*block-and-heal*, per the row above) — fails with ADBC **`Status::Timeout`** within the deadline, naming the option, instead of hanging (a watchdog thread bounds it, so a real hang fails the test loudly). Then the **contrast**: with the deadline unset (`""`) and the toxic removed, the *same* write succeeds once the channel re-establishes — proving the expired deadline poisoned nothing (no lingering cancellation, session damage, or lost write). The idempotent `SET Val = 1` makes the recovery assertion immune to whether any faulted attempt reached the emulator. |
| `reset_peer` (immediate TCP RST) | `mid_stream_disconnect_after_batches_surfaces_error_then_recovers` | The consumer first pulls **several real batches** off a large streamed result (with no fault), proving rows were delivered; then a `reset_peer` toxic is injected and the rest is drained on a watchdog-bounded worker thread. The next post-buffer chunk fetch must surface a **clean ADBC error** — not a hang, not a panic, and not a silently-short result — and once the toxic is removed a fresh query **recovers**. This drives the streaming **resumption** path (a later chunk fetched over a channel that was just reset), complementing `reset_peer_surfaces_error_then_recovers`, which resets a query *before* it starts. |
| `limit_data` (orderly close after N bytes) | `truncated_stream_surfaces_error_then_recovers` | A `limit_data` toxic closes the connection cleanly after `TRUNCATE_AFTER_BYTES` downstream bytes — a cap above the ~12 MB receive buffer (so `execute()` and the first batches succeed) but well below the ~48 MB result (so the stream is cut mid-flight). The reader must surface an **error** on the fetch that runs past the cap, never treating a **truncated** result as complete; then a fresh query **recovers** once the cap is removed. Unlike `reset_peer`'s abrupt RST this is an orderly mid-message close, specifically guarding against silent truncation. |

## Honest limitations (read this)

- **Transport faults only.** Toxiproxy is an L4 TCP proxy. It injects latency, bandwidth limits and
  connection resets/timeouts — it does **not** speak gRPC and cannot make the emulator return a
  logical gRPC status. In particular it **cannot produce `ABORTED`**, so this harness does **not**
  test Spanner's ABORTED-driven read/write transaction replay (the driver's buffer-and-retry commit
  path). That would need either a real Spanner instance under contention or a gRPC-aware fault
  source — the in-process mock server in `tests/mock_spanner.rs` is that source for logical
  statuses on the *query* path (it asserts, e.g., that a surfaced `ABORTED` keeps `vendor_code`
  10); scripting the full commit-abort-replay protocol through it is future work.

- **A transport fault cannot make `commit()` fail — it makes it block.** Verified empirically while
  building the commit-fault test: the pinned client's `apply_request_defaults` marks *every* Spanner
  data-plane RPC idempotent (safe for DML because `seqno` provides replay protection), and its
  `SpannerRetryPolicy` has no attempt cap, so under a persistent transport fault a commit retries
  inside the client indefinitely — the driver call neither succeeds nor errors until the network
  heals (then it succeeds) or something cancels it. Two consequences: a *failed* commit can only be
  produced at the SQL level (covered in `tests/integration.rs`), and an unreachable network can
  block a commit unboundedly unless a deadline is configured — the
  `spanner.rpc.timeout_seconds.update` option (see `docs/options.md`) bounds the whole commit,
  including the client's internal retries, and fails it with ADBC `Timeout`. Unset (the default)
  preserves the block-and-heal behaviour described above. This is now driven under a toxic by
  `update_timeout_bounds_a_faulted_write_then_recovers_when_unset` (see the table above), which puts
  a `reset_peer` toxic in front of the emulator and asserts a deadline-bounded write fails with
  `Timeout` in-deadline rather than hanging, then recovers once the deadline is unset.

- **Cancel is tested on the streaming read path, and needs the server to chunk.** The only in-flight
  network operation the safe (`&mut self`) Rust API lets a second thread cancel is a **streamed chunk
  fetch** on the reader returned by `execute()` (the reader carries a clone of the statement's cancel
  signal). Against the emulator a *small* result arrives in a single message that `execute()` drains
  up front, so the reader never blocks and there is nothing to interrupt — which is why the test uses
  a deliberately **large** (~48 MB) result: it exceeds the transport receive buffer (~12 MB observed),
  forcing the remainder to stream message-by-message where the throttle makes each pull slow and the
  cancel can land. Cancelling the *initial* `execute()` call itself from another thread is not
  expressible through the synchronous `&mut self` trait API (that path is what the C-ABI
  `AdbcStatementCancel` from a separate thread would hit). The `CancelSignal` is sticky (latched
  until the statement's next operation resets it), so the *between-chunks* case — a cancel landing
  while the reader is idle between fetches — is covered natively, without Toxiproxy, by
  `cancel_between_stream_chunks_cancels_the_next_fetch` in `tests/integration.rs`; the harness here
  adds the orthogonal case of interrupting a fetch that is genuinely *blocked* on a slow network.

- **The `reset_peer` recovery assertion is transport-level.** It shows the driver does not wedge on a
  reset and that a fresh RPC re-establishes the channel. It is not a claim about Spanner session
  invalidation semantics.

## Emulator networking note

Setup (create instance/DB/table) goes **directly** to the emulator's real `:9010` endpoint, not
through the proxy. The `google-cloud-rust` admin client only remaps the emulator admin port for an
endpoint ending exactly in `:9010` (→ `:9020`); pointed at the proxy's port it would send the admin
HTTP request to the wrong place and DB/table creation would fail. So the emulator keeps its internal
`:9010/:9020` (no host-port publishing, to avoid clashes with other local emulators) and is reached
by its docker-bridge container IP; only the driver-under-test's data-plane query/DML traffic is
proxied. `scripts/with-toxiproxy.sh` sets this up and exports both `SPANNER_EMULATOR_HOST` (→ proxy)
and `SPANNER_EMULATOR_DIRECT` (→ `<ip>:9010`, used by setup).

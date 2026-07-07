# Resilience / fault-injection harness

`tests/resilience.rs` drives the ADBC driver against the Spanner **emulator reached through a
[Toxiproxy](https://github.com/Shopify/toxiproxy) TCP proxy**, injects transport-level faults, and
asserts the driver behaves sanely under them. Nothing else in the suite exercises the failure paths.

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

Run the two tests **serially** (`--test-threads=1`): they share one global proxy, so their toxics
must not overlap.

Without `TOXIPROXY_URL` + `SPANNER_EMULATOR_HOST` set, the tests **self-skip**, so a plain
`cargo test` stays green everywhere — exactly like `tests/integration.rs`.

CI runs it non-gating via `.github/workflows/resilience.yml` (manual dispatch + nightly).

## What the harness injects, and what each test proves

| Toxic | Test | Assertion |
| --- | --- | --- |
| `bandwidth` (downstream throttle) | `cancel_interrupts_in_flight_query` | Throttling a ~48 MB result to 2000 KB/s keeps the streaming reader blocked in a real network read; `Statement::cancel` from another thread interrupts it in **milliseconds** with `Status::Cancelled`, instead of blocking ~18s for the rest of the stream. Drives the real `CancelSignal` → `block_on_cancellable` path. |
| `reset_peer` (immediate TCP RST) | `reset_peer_surfaces_error_then_recovers` | A query under the reset surfaces a **clean ADBC error** (no panic; no unbounded hang — a watchdog thread bounds it), and once the toxic is removed a subsequent query **recovers** as the gRPC channel re-establishes through the proxy. |

## Honest limitations (read this)

- **Transport faults only.** Toxiproxy is an L4 TCP proxy. It injects latency, bandwidth limits and
  connection resets/timeouts — it does **not** speak gRPC and cannot make the emulator return a
  logical gRPC status. In particular it **cannot produce `ABORTED`**, so this harness does **not**
  test Spanner's ABORTED-driven read/write transaction replay (the driver's buffer-and-retry commit
  path). That would need either a real Spanner instance under contention or a gRPC-aware fault proxy.

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

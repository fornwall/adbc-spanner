# Resilience and fault injection

[`resilience.rs`](resilience.rs) runs the driver through a
[Toxiproxy TCP proxy](https://github.com/Shopify/toxiproxy) to test transport failures against the
Spanner emulator. See [the testing overview](../docs/testing.md) for the other suites.

## Run

```sh
scripts/with-toxiproxy.sh cargo test --test resilience -- --nocapture --test-threads=1
```

The helper starts and removes both containers. It needs Docker with host networking and a
host-reachable container bridge IP, as used by Linux CI. Run serially: all tests share one proxy.
Without `TOXIPROXY_URL` and `SPANNER_EMULATOR_HOST`, tests skip; `ADBC_TEST_REQUIRE_TARGET=1` makes
missing configuration fail instead.

The [workflow](../.github/workflows/resilience.yml) runs on pushes to `main`, pull requests,
manual dispatch and nightly. Scheduled failures create or update a tracking issue. The check
name is `Resilience harness (emulator + Toxiproxy)`; merge enforcement depends on repository rules.

## Coverage

| Fault | Test | Required behavior |
| --- | --- | --- |
| Downstream `bandwidth` throttle | `cancel_interrupts_in_flight_query` | Cancelling a blocked stream fetch returns `Cancelled` within the test's bound. |
| TCP `reset_peer` | `reset_peer_surfaces_error_then_recovers` | A query reports a transport error; a fresh query succeeds after the reset is removed. |
| TCP `reset_peer` | `commit_under_transport_fault_never_loses_the_write` | A buffered idempotent update survives the fault, either through client retries or a retried commit. |
| TCP `reset_peer` | `update_timeout_bounds_a_faulted_write_then_recovers_when_unset` | An update deadline returns `Timeout`; clearing it and removing the reset allows the next write. |
| TCP `reset_peer` after initial batches | `mid_stream_disconnect_after_batches_surfaces_error_then_recovers` | A partially consumed stream reports an error before all rows arrive, then a fresh query recovers. |
| `limit_data` close after a byte cap | `truncated_stream_surfaces_error_then_recovers` | Some rows arrive before an error; truncation never appears as successful end-of-stream. Recovery succeeds. |

Large results force network reads after `execute()` returns. Watchdog timeouts bound worker
waits. `assert_transport_error` requires ADBC `Internal` or `IO` plus the `Spanner error:` prefix;
cancellation and deadline cases check `Cancelled` and `Timeout` directly. Stream errors are
unwrapped from `ArrowError::ExternalError` to inspect the underlying ADBC status.

## Scope and limitations

- Toxiproxy injects TCP faults, not logical gRPC statuses such as `ABORTED`. The in-process
  [`mock_spanner.rs`](mock_spanner.rs) suite covers logical statuses, error details, silent
  streams, retry bounds and request options. The transport suite does not establish correctness
  of the full commit-abort-replay protocol.
- Under the tested reset, the pinned client normally keeps retrying a commit until transport
  recovers. The commit test therefore also handles a future client returning an error, but that
  branch is not guaranteed to execute. Configured retry limits and the update RPC timeout can
  bound retries; see [options](../docs/options.md).
- Cancellation here interrupts a streamed fetch; this suite does not test cancellation during
  the initial `execute()`. The integration suite separately tests cancellation between chunks.
- Recovery demonstrates a working fresh RPC after removing a fault; it does not test real
  Spanner session invalidation.

## Networking

```text
driver -> proxy 127.0.0.1:8666 -> emulator <container-ip>:9010
             ^
             | HTTP control 127.0.0.1:8475
        resilience tests

schema setup ----------------> emulator directly (:9010 / :9020)
```

The helper exports `SPANNER_EMULATOR_HOST` for proxied data traffic and
`SPANNER_EMULATOR_DIRECT` for setup. Direct setup preserves the pinned client's `9010` to `9020`
admin-port mapping. The emulator publishes no host ports; proxy ports and container names are
configurable in [`with-toxiproxy.sh`](../scripts/with-toxiproxy.sh).

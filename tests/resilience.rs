//! Transport-level fault-injection / resilience tests for the ADBC driver.
//!
//! These drive the driver against the Spanner **emulator reached through a Toxiproxy TCP proxy**,
//! toggling transport faults (latency, connection reset) via Toxiproxy's HTTP admin API and
//! asserting the driver behaves sanely. They are **skipped automatically** unless both
//! `TOXIPROXY_URL` (the admin API base) and `SPANNER_EMULATOR_HOST` (pointed at the proxy) are set,
//! so a plain `cargo test` stays green everywhere — exactly like `tests/integration.rs`.
//!
//! Bring the topology up and run them with:
//!
//! ```sh
//! scripts/with-toxiproxy.sh cargo test --test resilience -- --nocapture
//! ```
//!
//! ## Honest scope (read `tests/RESILIENCE.md`)
//!
//! Toxiproxy injects **transport** faults only — latency, bandwidth, connection resets/timeouts. It
//! cannot make the emulator emit a gRPC status such as `ABORTED`, so these tests do **not** exercise
//! Spanner's ABORTED-driven transaction replay. What they *do* exercise:
//!
//! - **`cancel_interrupts_in_flight_query`** — with a bandwidth toxic throttling a multi-MB result to
//!   a trickle so each streamed chunk fetch blocks for seconds, `Statement::cancel` (the
//!   `CancelSignal` → `block_on_cancellable` path) interrupts a query whose result is still
//!   streaming, and it returns promptly with an error rather than blocking for the full (slow) read.
//!   This is the strongest assertion: it drives real driver code.
//! - **`reset_peer_surfaces_error_then_recovers`** — a `reset_peer` toxic makes RPCs fail; the driver
//!   surfaces a clean ADBC error (no panic/hang, bounded by a watchdog), and once the toxic is
//!   removed a subsequent query **recovers** (the gRPC channel re-establishes through the proxy).
//! - **`commit_under_transport_fault_never_loses_the_write`** — a manual-transaction `commit()`
//!   started under a `reset_peer` toxic must not lose the buffered DML: the write has to land
//!   once the fault clears. (Verified along the way: the pinned client marks all data-plane RPCs
//!   idempotent and retries transport faults without an attempt cap, so such a commit *blocks and
//!   heals* rather than erroring — a driver-level commit failure cannot be transport-injected;
//!   the failed-commit/retry contract is covered at the SQL level in `tests/integration.rs`.)
//! - **`update_timeout_bounds_a_faulted_write_then_recovers_when_unset`** — with
//!   `spanner.rpc.timeout_seconds.update` set, a write launched under a persistent `reset_peer`
//!   toxic (which otherwise block-and-heals, per the test above) fails with `Status::Timeout`
//!   within the deadline — naming the option — instead of hanging; and with the deadline unset and
//!   the fault cleared the same write succeeds, showing the expired deadline poisoned nothing.
//! - **`mid_stream_disconnect_after_batches_surfaces_error_then_recovers`** — a `reset_peer` toxic
//!   injected *after* the consumer has already pulled several real batches off a streaming reader
//!   makes the next (post-buffer) chunk fetch surface a clean ADBC error rather than hang or panic
//!   (bounded by a watchdog), and once the toxic is removed a fresh query recovers. Exercises the
//!   resumption path, complementing `reset_peer_surfaces_error_then_recovers` (which resets a query
//!   *before* it starts).
//! - **`truncated_stream_surfaces_error_then_recovers`** — a `limit_data` toxic closes the
//!   connection cleanly after a byte cap that sits above the receive buffer but well below the full
//!   result, truncating the stream mid-flight; the reader must surface an error on the fetch past
//!   the cap (never a silently-short "successful" result), and a fresh query recovers once the cap
//!   is removed.

use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_spanner::{SpannerConnection, SpannerDatabase, SpannerDriver};
use arrow_array::Int64Array;
use google_cloud_lro::Poller;
use google_cloud_spanner::client::Spanner;
use google_cloud_spanner_admin_instance_v1::model::Instance;

// Fixed identifiers, matching tests/integration.rs so the emulator setup is interchangeable.
const PROJECT: &str = "test-project";
const INSTANCE: &str = "test-instance";
const DATABASE: &str = "adbc-test";
const STREAM_TABLE: &str = "ResilStream";
// A tiny table for the manual-transaction commit-fault test. The buffered DML is an idempotent
// UPDATE so the test's assertions hold regardless of whether the faulted commit RPC reached the
// emulator before the reset (a failed commit's outcome is inherently ambiguous at the transport
// layer).
const TXN_TABLE: &str = "ResilTxn";
// The streaming table carries a wide payload so the result set is far larger than the gRPC/HTTP-2
// receive buffer (empirically ~12 MB against the emulator). `execute()` fills that buffer and
// returns; the remaining rows must then be pulled message-by-message over the network as the reader
// is iterated — which is where a bandwidth toxic makes each pull slow and a cancel can land. Sized
// generously (~48 MB) so plenty of data always remains to stream after the buffer fills, regardless
// of the exact buffer size. The rows are generated server-side (REPEAT + GENERATE_ARRAY), so seeding
// is cheap despite the volume.
const STREAM_ROWS: i64 = 24000;
const PAYLOAD_BYTES: usize = 2000;
// Downstream byte cap for the truncated-stream test's `limit_data` toxic: comfortably above the
// ~12 MB transport receive buffer (so `execute()` and the first batches succeed) but far below the
// ~48 MB result (so the stream is cut mid-way, on a later chunk fetch, not up front).
const TRUNCATE_AFTER_BYTES: u64 = 20 * 1024 * 1024;

fn database_path() -> String {
    format!("projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}")
}

/// Whether `ADBC_TEST_REQUIRE_TARGET` demands a configured target (CI sets it).
///
/// When truthy, [`toxi`] panics instead of returning `None` when the Toxiproxy / emulator env
/// wiring is missing, so a broken workflow refactor fails loudly rather than passing vacuously.
fn require_target() -> bool {
    matches!(
        std::env::var("ADBC_TEST_REQUIRE_TARGET").ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

/// The Toxiproxy admin API base and target proxy name, or `None` to skip the whole file.
///
/// Requires the proxy (created by `scripts/with-toxiproxy.sh`) *and* `SPANNER_EMULATOR_HOST` to be
/// set — the latter must point at the proxy listener, which the script guarantees.
///
/// When `ADBC_TEST_REQUIRE_TARGET` is truthy (CI) and either variable is missing, this panics
/// instead of returning `None`, so the resilience suite cannot silently skip in CI.
fn toxi() -> Option<Toxiproxy> {
    let resolved = resolve_toxi();
    if resolved.is_none() && require_target() {
        panic!(
            "ADBC_TEST_REQUIRE_TARGET is set but TOXIPROXY_URL / SPANNER_EMULATOR_HOST are not both \
             configured — the resilience harness env wiring is missing, so this suite would skip \
             all fault-injection coverage. Refusing to pass vacuously."
        );
    }
    resolved
}

/// Inner resolver for [`toxi`]; returns `None` when the required env vars are unset.
fn resolve_toxi() -> Option<Toxiproxy> {
    let url = std::env::var("TOXIPROXY_URL")
        .ok()
        .filter(|s| !s.is_empty())?;
    // Without an emulator host the driver has nothing to talk to; the script always sets it.
    std::env::var("SPANNER_EMULATOR_HOST")
        .ok()
        .filter(|s| !s.is_empty())?;
    let proxy = std::env::var("TOXIPROXY_PROXY").unwrap_or_else(|_| "spanner".to_string());
    Some(Toxiproxy { url, proxy })
}

/// A thin client for the Toxiproxy HTTP admin API, driven by shelling out to `curl` (already a
/// dependency of the harness scripts) so the test needs no HTTP crate.
struct Toxiproxy {
    url: String,
    proxy: String,
}

impl Toxiproxy {
    /// POST a toxic to the proxy. `body` is the toxic's JSON. Panics on a non-2xx response.
    fn add_toxic(&self, body: &str) {
        let (code, out) = curl(&[
            "-s",
            "-X",
            "POST",
            &format!("{}/proxies/{}/toxics", self.url, self.proxy),
            "-H",
            "Content-Type: application/json",
            "-d",
            body,
        ]);
        assert!(
            (200..300).contains(&code),
            "adding toxic failed (HTTP {code}): {out}\nbody was: {body}"
        );
    }

    /// Delete a toxic by name, ignoring a 404 (not present).
    fn remove_toxic(&self, name: &str) {
        let _ = curl(&[
            "-s",
            "-X",
            "DELETE",
            &format!("{}/proxies/{}/toxics/{}", self.url, self.proxy, name),
        ]);
    }

    /// A downstream bandwidth cap of `rate` KB/s (server→client), named so it can be removed again.
    /// Throttling a large result set to a trickle keeps the reader blocked in a genuine network read
    /// for seconds, giving a deterministic in-flight operation for the cancel to interrupt.
    fn add_bandwidth(&self, name: &str, rate_kbps: u64) {
        self.add_toxic(&format!(
            "{{\"name\":\"{name}\",\"type\":\"bandwidth\",\"stream\":\"downstream\",\
              \"toxicity\":1.0,\"attributes\":{{\"rate\":{rate_kbps}}}}}"
        ));
    }

    /// A `reset_peer` toxic: after `timeout` ms of a stalled connection, close it with a TCP RST.
    /// `timeout=0` resets immediately on the next data.
    fn add_reset_peer(&self, name: &str, timeout_ms: u64) {
        self.add_toxic(&format!(
            "{{\"name\":\"{name}\",\"type\":\"reset_peer\",\"stream\":\"downstream\",\
              \"toxicity\":1.0,\"attributes\":{{\"timeout\":{timeout_ms}}}}}"
        ));
    }

    /// A `limit_data` toxic: close the connection cleanly once `bytes` bytes have been transmitted
    /// downstream (server→client). This truncates a large streamed result mid-flight — an orderly
    /// close mid-message, as opposed to `reset_peer`'s abrupt TCP RST.
    fn add_limit_data(&self, name: &str, bytes: u64) {
        self.add_toxic(&format!(
            "{{\"name\":\"{name}\",\"type\":\"limit_data\",\"stream\":\"downstream\",\
              \"toxicity\":1.0,\"attributes\":{{\"bytes\":{bytes}}}}}"
        ));
    }
}

/// Run `curl` with the given args, returning `(http_status, body)`. The `-w` trailer appends the
/// status code on its own line so we can separate it from the body.
fn curl(args: &[&str]) -> (u16, String) {
    let mut full: Vec<&str> = vec!["-w", "\n%{http_code}"];
    full.extend_from_slice(args);
    let output = Command::new("curl")
        .args(&full)
        .output()
        .expect("failed to run curl");
    let text = String::from_utf8_lossy(&output.stdout).to_string();
    let (body, code) = match text.rsplit_once('\n') {
        Some((b, c)) => (b.to_string(), c.trim().parse().unwrap_or(0)),
        None => (text, 0),
    };
    (code, body)
}

/// Count of resilience tests currently executing their body. The suite must run single-threaded
/// (see [`SerialGuard`]); this backs the runtime assertion that enforces it.
static ACTIVE_TESTS: AtomicUsize = AtomicUsize::new(0);

/// RAII guard asserting the resilience suite runs **serially** (`--test-threads=1`).
///
/// [`ensure_setup`] temporarily repoints the process-global `SPANNER_EMULATOR_HOST` env var (via
/// `std::env::set_var`) while creating the database/schema against the emulator's direct admin
/// endpoint, then restores it. Process env is process-global, so that swap — and `set_var` itself,
/// which is not thread-safe against concurrent `getenv` — is only sound if no other test thread is
/// running at the same time (opening a driver connection reads the env; entering setup mutates it).
/// The whole harness is therefore designed to run single-threaded: both `scripts/with-toxiproxy.sh`
/// and `.github/workflows/resilience.yml` pass `--test-threads=1`.
///
/// Constructing this guard at the start of each test's *body* (after the self-skip check, so a
/// plain multi-threaded `cargo test` with the env unset still skips cleanly) detects a violation —
/// two test bodies overlapping — and fails loudly with an actionable message instead of racing the
/// env swap silently. Dropped at the end of the test, it releases the slot for the next one.
struct SerialGuard;

impl SerialGuard {
    fn new() -> Self {
        let previous = ACTIVE_TESTS.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            previous, 0,
            "resilience tests must run serially, but another test body is already running. \
             tests/resilience.rs mutates the process-global SPANNER_EMULATOR_HOST env var in \
             ensure_setup(), which races with concurrent test threads. Re-run with \
             `--test-threads=1` (scripts/with-toxiproxy.sh and resilience.yml already do)."
        );
        SerialGuard
    }
}

impl Drop for SerialGuard {
    fn drop(&mut self) {
        ACTIVE_TESTS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Create the instance, database and the streaming test table once per binary. Best-effort: the
/// create calls fail with `AlreadyExists` on a re-run, which is fine.
///
/// Schema/DDL setup goes to the emulator's **real** `<ip>:9010` endpoint (`SPANNER_EMULATOR_DIRECT`),
/// not the proxy: the google-cloud-rust admin client only remaps the emulator admin port for an
/// endpoint ending in `:9010`, so DB/table creation through the proxy's port would send the admin
/// request to the wrong place and fail. Only the driver-under-test's data-plane traffic uses the
/// proxy. We temporarily repoint `SPANNER_EMULATOR_HOST` for the duration of setup, then restore it
/// so the tests' driver connections go through the proxy again. (Setup runs once under the mutex,
/// before any driver connection; and every test holds a [`SerialGuard`] for its whole body, which
/// asserts no other test runs concurrently — so the transient env swap has no concurrent reader.)
fn ensure_setup() {
    static DONE: Mutex<bool> = Mutex::new(false);
    let mut done = DONE.lock().expect("setup lock poisoned");
    if *done {
        return;
    }

    let proxy_host = std::env::var("SPANNER_EMULATOR_HOST").expect("SPANNER_EMULATOR_HOST");
    // Fall back to the proxy host if no direct endpoint is provided (e.g. a manual run that already
    // points HOST at a real `:9010`).
    let direct = std::env::var("SPANNER_EMULATOR_DIRECT").unwrap_or_else(|_| proxy_host.clone());
    // SAFETY: setup runs once under the `DONE` mutex, before any driver connection or other thread
    // touches the environment, so this transient swap has no concurrent readers.
    unsafe { std::env::set_var("SPANNER_EMULATOR_HOST", &direct) };

    tokio::runtime::Runtime::new()
        .expect("setup runtime")
        .block_on(async {
            let spanner = Spanner::builder().build().await.expect("build client");
            let instance_admin = spanner
                .instance_admin_builder()
                .build()
                .await
                .expect("instance admin");
            let instance = instance_admin
                .create_instance()
                .set_parent(format!("projects/{PROJECT}"))
                .set_instance_id(INSTANCE)
                .set_instance(
                    Instance::new()
                        .set_config(format!(
                            "projects/{PROJECT}/instanceConfigs/emulator-config"
                        ))
                        .set_display_name("ADBC resilience instance")
                        .set_node_count(1),
                )
                .poller()
                .until_done()
                .await;
            if let Err(e) = &instance {
                eprintln!("create_instance: {e}");
            }
            let database_admin = spanner
                .database_admin_builder()
                .build()
                .await
                .expect("database admin");
            let database = database_admin
                .create_database()
                .set_parent(format!("projects/{PROJECT}/instances/{INSTANCE}"))
                .set_create_statement(format!("CREATE DATABASE `{DATABASE}`"))
                .set_extra_statements(vec![
                    format!(
                        "CREATE TABLE {STREAM_TABLE} (Id INT64, Payload STRING(MAX)) \
                         PRIMARY KEY (Id)"
                    ),
                    format!("CREATE TABLE {TXN_TABLE} (Id INT64, Val INT64) PRIMARY KEY (Id)"),
                ])
                .poller()
                .until_done()
                .await;
            if let Err(e) = &database {
                eprintln!("create_database: {e}");
            }
        });

    // Restore the proxy endpoint so the driver-under-test's data plane goes through Toxiproxy.
    // SAFETY: still under the `DONE` mutex, before any driver connection — see the note above.
    unsafe { std::env::set_var("SPANNER_EMULATOR_HOST", &proxy_host) };
    *done = true;
}

/// Open a driver connection through the proxy, retrying briefly (a freshly-created emulator database
/// can momentarily lag).
fn connect() -> SpannerConnection {
    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database: SpannerDatabase = driver
        .new_database_with_opts([(OptionDatabase::Uri, OptionValue::String(database_path()))])
        .expect("create database");
    let mut last = None;
    for _ in 0..40 {
        match database.new_connection() {
            Ok(c) => return c,
            Err(e) => {
                last = Some(e);
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }
    panic!("connect failed after retries: {last:?}");
}

/// Run a statement for its side effect (DDL/DML), panicking on error.
fn run(connection: &mut SpannerConnection, sql: &str) {
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query(sql).unwrap();
    s.execute_update()
        .unwrap_or_else(|e| panic!("run {sql:?}: {e:?}"));
}

/// Seed the streaming table with `STREAM_ROWS` wide rows (idempotent: clears first). Runs with no
/// toxics active. Inserted in batches so a single DML stays well under Spanner's commit-size limit.
fn seed_stream_table(connection: &mut SpannerConnection) {
    run(
        connection,
        &format!("DELETE FROM {STREAM_TABLE} WHERE true"),
    );
    let batch = 500i64;
    let mut start = 1i64;
    while start <= STREAM_ROWS {
        let end = (start + batch - 1).min(STREAM_ROWS);
        run(
            connection,
            &format!(
                "INSERT INTO {STREAM_TABLE} (Id, Payload) \
                 SELECT n, REPEAT('x', {PAYLOAD_BYTES}) \
                 FROM UNNEST(GENERATE_ARRAY({start}, {end})) AS n"
            ),
        );
        start = end + 1;
    }
}

/// **The strong assertion.** With a downstream bandwidth toxic throttling a large result so every
/// streamed chunk fetch blocks for real in a slow network read, cancelling the statement mid-stream
/// (from another thread, while the result reader is blocked pulling the next chunk on a worker
/// thread) must interrupt that blocked read promptly with an error — instead of waiting for the full
/// slow fetch. This drives the real `CancelSignal` / `block_on_cancellable` path the driver uses for
/// every network operation.
///
/// This complements `cancel_between_stream_chunks_cancels_the_next_fetch` in `tests/integration.rs`,
/// which (against the un-throttled emulator) covers the *sticky-latch* behaviour — a cancel landing
/// while the stream is idle between chunks still cancels the next fetch. Here the fetch is genuinely
/// in flight and blocked on the (throttled) network, so this asserts the cancel *interrupts* a live
/// `block_on`, not just that the latch is observed before a fast fetch.
#[test]
fn cancel_interrupts_in_flight_query() {
    let Some(toxi) = toxi() else {
        eprintln!("TOXIPROXY_URL / SPANNER_EMULATOR_HOST not set — skipping resilience tests");
        return;
    };
    // Enforce the single-threaded contract that makes the env swap in `ensure_setup` sound. Placed
    // after the skip check so a plain multi-threaded `cargo test` (env unset) still self-skips.
    let _serial = SerialGuard::new();
    ensure_setup();

    let mut connection = connect();
    seed_stream_table(&mut connection);

    // Throttle the server→client direction so each streamed message takes a noticeable fraction of a
    // second and the ~48 MB result would take tens of seconds to drain — leaving the reader blocked
    // in a genuine network read for the whole test. Small chunks (300 rows) so the reader issues many
    // network pulls after the first.
    let rate_kbps = 2000u64; // KB/s; the ~36 MB left after the receive buffer fills → ~18s to drain.
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(300),
        )
        .expect("set rows_per_batch");
    statement
        .set_sql_query(format!(
            "SELECT Id, Payload FROM {STREAM_TABLE} ORDER BY Id"
        ))
        .unwrap();

    // Throttle, then execute. `execute()` fills the transport receive buffer and returns a lazy
    // reader; the rest of the result is pulled over the (now slow) network as the reader is iterated.
    toxi.add_bandwidth("resil_bandwidth", rate_kbps);
    let reader = statement
        .execute()
        .expect("execute (first chunk) should succeed");

    // Iterate the reader on a worker thread so it blocks pulling the *next* (slow) chunk while we
    // cancel from here. The reader carries a clone of the statement's cancel signal, so
    // `statement.cancel()` wakes the in-flight `block_on`.
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let start = Instant::now();
        // Consume the reader; early chunks (already buffered by execute) come instantly, later ones
        // block on slow throttled network pulls — where the cancel lands and turns into an error.
        let result: Result<Vec<_>, _> = reader.collect();
        let elapsed = start.elapsed();
        let _ = tx.send((result.is_err(), format!("{:?}", result.err()), elapsed));
    });

    // Give the worker time to drain the buffered rows and settle into a slow throttled network pull,
    // then cancel while that pull is genuinely in flight — so we are measuring interruption of a
    // live, blocked `block_on`, not just the sticky latch being observed before the next fetch. (The
    // `CancelSignal` latches, so even a cancel landing between chunks would not be lost; the timing
    // here is about exercising the in-flight-interrupt path, not about avoiding a missed signal.)
    std::thread::sleep(Duration::from_millis(2000));
    let cancel_at = Instant::now();
    statement.cancel().expect("cancel");

    let (is_err, err_debug, worker_elapsed) = rx.recv_timeout(Duration::from_secs(20)).expect(
        "worker did not finish within 20s — cancel failed to interrupt the in-flight fetch",
    );
    worker.join().ok();
    let cancel_latency = cancel_at.elapsed();

    // Clean up the toxic regardless.
    toxi.remove_toxic("resil_bandwidth");

    assert!(
        is_err,
        "cancelled streaming query should yield an error, got Ok. worker elapsed {worker_elapsed:?}"
    );
    assert!(
        err_debug.to_lowercase().contains("cancel"),
        "error should be a cancellation, got: {err_debug}"
    );
    // The cancel must land quickly — far under the ~18s a full throttled read of the remaining result
    // would take. If cancellation did nothing, the worker would still be streaming at 2000 KB/s.
    assert!(
        cancel_latency < Duration::from_secs(4),
        "cancel took {cancel_latency:?}; a full throttled read of the rest would take ~18s, so this \
         shows cancellation did not interrupt the in-flight operation"
    );
    eprintln!(
        "cancel interrupted the in-flight streamed fetch in {cancel_latency:?} \
         (worker total {worker_elapsed:?}); error = {err_debug}"
    );
}

/// A `reset_peer` toxic makes RPCs fail at the transport layer. Assert the driver surfaces a clean
/// ADBC error (no panic, and — enforced by a watchdog — no unbounded hang), and that once the toxic
/// is removed a subsequent query **recovers** (the gRPC channel re-establishes through the proxy).
#[test]
fn reset_peer_surfaces_error_then_recovers() {
    let Some(toxi) = toxi() else {
        eprintln!("TOXIPROXY_URL / SPANNER_EMULATOR_HOST not set — skipping resilience tests");
        return;
    };
    // Enforce the single-threaded contract that makes the env swap in `ensure_setup` sound. Placed
    // after the skip check so a plain multi-threaded `cargo test` (env unset) still self-skips.
    let _serial = SerialGuard::new();
    ensure_setup();

    // Wrap the connection so a watchdog thread can drive a query and we can bound its wall-clock.
    let connection = Arc::new(Mutex::new(connect()));

    // Sanity: with no faults, a trivial query succeeds.
    assert!(
        query_one(&connection).is_ok(),
        "baseline query (no toxics) should succeed"
    );

    // Inject an immediate connection reset, then run a query on a watchdog thread. It must return an
    // error (not hang, not panic) within a generous bound.
    toxi.add_reset_peer("resil_reset", 0);
    let faulted = run_query_bounded(&connection, Duration::from_secs(45));
    // Always clear the toxic before asserting, so a failure doesn't leave the proxy poisoned.
    toxi.remove_toxic("resil_reset");

    match faulted {
        Some(result) => assert!(
            result.is_err(),
            "query under reset_peer should surface an error, got Ok"
        ),
        None => panic!("query under reset_peer hung past the watchdog — driver did not fail fast"),
    }

    // Recovery: with the toxic gone, a query should succeed again once the channel re-establishes.
    // Allow a few attempts for the reconnect.
    let mut recovered = false;
    for _ in 0..20 {
        if query_one(&connection).is_ok() {
            recovered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        recovered,
        "driver did not recover after the reset_peer toxic was removed"
    );
    eprintln!("reset_peer surfaced a clean error and the driver recovered afterwards");
}

/// A manual-transaction `commit()` under a transient transport fault must never lose the buffered
/// write: once the fault clears, the update has to land — through whichever path the client
/// surfaces the fault.
///
/// Empirically (verified against the pinned client) a *persistent* transport fault cannot make
/// `commit()` return an error at all: `apply_request_defaults` marks every Spanner data-plane RPC
/// idempotent (safe for DML because `seqno` gives replay protection) and `SpannerRetryPolicy` has
/// no attempt cap, so the client retries the faulted RPC internally until the network heals and
/// the same `commit()` call then succeeds. That is the branch this test pins down today. If a
/// future client version starts surfacing transport errors instead, the `Err` branch takes over
/// and exercises the driver's failed-commit contract directly: the buffer must survive the error
/// (the take-before-apply regression consumed it, so a retried commit "succeeded" on an empty
/// batch — a silent lost write) and a retried `commit()` must replay it. The SQL-level version of
/// that regression is covered deterministically in `tests/integration.rs`.
///
/// The buffered DML is an idempotent UPDATE (`SET Val = 1`), so the final assertion is immune to
/// the inherent ambiguity of a faulted commit (the RPC may or may not have reached the emulator
/// before the reset): however many times the update is applied, `Val` must end up 1. Under the
/// regression a retried commit succeeds instantly on an empty buffer and `Val` stays 0.
#[test]
fn commit_under_transport_fault_never_loses_the_write() {
    let Some(toxi) = toxi() else {
        eprintln!("TOXIPROXY_URL / SPANNER_EMULATOR_HOST not set — skipping resilience tests");
        return;
    };
    // Enforce the single-threaded contract that makes the env swap in `ensure_setup` sound. Placed
    // after the skip check so a plain multi-threaded `cargo test` (env unset) still self-skips.
    let _serial = SerialGuard::new();
    ensure_setup();

    let connection = Arc::new(Mutex::new(connect()));

    // Seed one row (Id=1, Val=0) in autocommit mode, idempotently across re-runs.
    {
        let mut conn = connection.lock().expect("connection lock");
        run(&mut conn, &format!("DELETE FROM {TXN_TABLE} WHERE true"));
        run(
            &mut conn,
            &format!("INSERT INTO {TXN_TABLE} (Id, Val) VALUES (1, 0)"),
        );
    }

    // Enter manual mode and buffer the update — buffering is local, no RPC happens yet.
    connection
        .lock()
        .expect("connection lock")
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");
    {
        let mut conn = connection.lock().expect("connection lock");
        let mut s = conn.new_statement().expect("new statement");
        s.set_sql_query(format!("UPDATE {TXN_TABLE} SET Val = 1 WHERE Id = 1"))
            .unwrap();
        assert_eq!(
            s.execute_update().expect("buffer update"),
            None,
            "manual mode must buffer the DML (count unknown until commit)"
        );
    }

    // Break the transport before the commit starts, run the commit on a worker thread, and give
    // it a few seconds under the fault. Expected today: the commit neither succeeds nor fails —
    // the client is retrying inside the call (see the doc comment). Clear the toxic afterwards
    // regardless, so a test failure doesn't leave the proxy poisoned for the other tests.
    toxi.add_reset_peer("resil_commit_reset", 0);
    let (tx, rx) = mpsc::channel();
    let commit_conn = connection.clone();
    let worker = std::thread::spawn(move || {
        let result = commit_conn.lock().expect("connection lock").commit();
        let _ = tx.send(result);
    });
    let during_fault = rx.recv_timeout(Duration::from_secs(5));
    toxi.remove_toxic("resil_commit_reset");

    match during_fault {
        // Today's client: the commit is blocked in the client's internal retry loop. With the
        // fault gone it must complete successfully on its own (allow generously for the retry
        // backoff plus channel re-establishment).
        Err(_) => {
            eprintln!("commit is blocked retrying under the fault (expected); removing the fault");
            let healed = rx
                .recv_timeout(Duration::from_secs(60))
                .expect("commit did not complete within 60s of the fault being removed");
            worker.join().expect("commit worker panicked");
            healed.expect("commit should succeed once the transport fault is gone");
        }
        // A client that surfaces the transport error instead: the driver must have kept the
        // buffer, so a retried commit() replays it once the channel re-establishes.
        Ok(Err(error)) => {
            eprintln!("commit surfaced the fault as an error ({error}); retrying the commit");
            worker.join().expect("commit worker panicked");
            let mut last = None;
            let retried =
                (0..20).any(
                    |_| match connection.lock().expect("connection lock").commit() {
                        Ok(()) => true,
                        Err(e) => {
                            last = Some(e);
                            std::thread::sleep(Duration::from_millis(500));
                            false
                        }
                    },
                );
            assert!(
                retried,
                "retried commit did not succeed after the fault was removed: {last:?}"
            );
        }
        Ok(Ok(())) => panic!(
            "commit reported success while the transport to the emulator was reset — \
             the fault did not take effect (toxiproxy problem?)"
        ),
    }

    // THE assertion: the buffered update landed, whichever path delivered it. Under the
    // take-before-apply regression a failed attempt emptied the buffer, so the follow-up commit
    // reported success with nothing written and Val stayed 0.
    assert_eq!(
        query_int(
            &connection,
            &format!("SELECT Val FROM {TXN_TABLE} WHERE Id = 1"),
        ),
        1,
        "the buffered update must not be lost by a commit under a transport fault"
    );

    // Leave the connection in autocommit mode (the buffer is empty now, so this commits nothing).
    connection
        .lock()
        .expect("connection lock")
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("re-enable autocommit");
    eprintln!("the write survived a transport fault during commit");
}

/// **The RPC-timeout assertion.** With `spanner.rpc.timeout_seconds.update` set, a write launched
/// under a *persistent* transport fault must fail with ADBC [`Status::Timeout`] within the deadline
/// — instead of blocking indefinitely — and the error must name the responsible option. Then the
/// contrast: with the deadline unset (and the fault cleared) the same write succeeds, proving the
/// expired deadline poisoned nothing (no lingering cancellation, session damage, or lost write).
///
/// This is the toxic-driven counterpart to `commit_under_transport_fault_never_loses_the_write`,
/// which established the behaviour this test bounds: under a `reset_peer` toxic the pinned client
/// marks every data-plane RPC idempotent and retries with no attempt cap, so an autocommit write
/// (a read/write transaction: begin + `ExecuteBatchDml` + commit) *blocks and heals* — it never
/// surfaces the transport error, it just retries until the network recovers. Without a deadline
/// that is an unbounded hang; `spanner.rpc.timeout_seconds.update` wraps the whole driver-side
/// operation (including the client's internal retries) in a `tokio::time::timeout` and turns the
/// hang into a prompt `Timeout`. This closes the gap `tests/RESILIENCE.md` previously flagged: the
/// option was documented to bound a commit under a fault, but nothing drove it under a toxic.
///
/// The write runs on a worker thread bounded by a generous watchdog: if the deadline fails to fire
/// the test fails loudly rather than hanging the suite. The seeded row plus the idempotent
/// `SET Val = 1` make the recovery assertion immune to whether any faulted attempt happened to
/// reach the emulator before the reset.
#[test]
fn update_timeout_bounds_a_faulted_write_then_recovers_when_unset() {
    let Some(toxi) = toxi() else {
        eprintln!("TOXIPROXY_URL / SPANNER_EMULATOR_HOST not set — skipping resilience tests");
        return;
    };
    ensure_setup();

    let connection = Arc::new(Mutex::new(connect()));

    // Seed one row (Id=1, Val=0) in autocommit mode, idempotently across re-runs. This also warms
    // the session/channel through the proxy while there is no fault, so the faulted write below is
    // a genuine block-and-heal (retrying data-plane RPC), not a cold-start failure.
    {
        let mut conn = connection.lock().expect("connection lock");
        run(&mut conn, &format!("DELETE FROM {TXN_TABLE} WHERE true"));
        run(
            &mut conn,
            &format!("INSERT INTO {TXN_TABLE} (Id, Val) VALUES (1, 0)"),
        );
    }

    // A short update deadline on the connection; the statement created below inherits it.
    let deadline_secs = 2.0_f64;
    connection
        .lock()
        .expect("connection lock")
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_RPC_TIMEOUT_UPDATE.into()),
            OptionValue::Double(deadline_secs),
        )
        .expect("set update timeout");

    // Break the transport, then run an autocommit UPDATE on a worker thread. Under `reset_peer` the
    // client retries internally with no cap (see the commit-fault test), so without the deadline
    // this call would block forever; with it, it must return a `Timeout` in ~`deadline_secs`.
    toxi.add_reset_peer("resil_update_timeout_reset", 0);
    let (tx, rx) = mpsc::channel();
    let write_conn = connection.clone();
    let start = Instant::now();
    let worker = std::thread::spawn(move || {
        let mut conn = write_conn.lock().expect("connection lock");
        let mut s = conn.new_statement().expect("new statement");
        s.set_sql_query(format!("UPDATE {TXN_TABLE} SET Val = 1 WHERE Id = 1"))
            .unwrap();
        let result = s.execute_update();
        let _ = tx.send(result);
    });

    // Watchdog: the 2s deadline must fire far under this bound. If it hangs past here the timeout
    // did not bound the operation — fail loudly (leaking the worker is fine; the process reaps it).
    let outcome = rx.recv_timeout(Duration::from_secs(30));
    let elapsed = start.elapsed();
    // Clear the toxic regardless, so a failure doesn't leave the proxy poisoned for other tests.
    toxi.remove_toxic("resil_update_timeout_reset");

    let Ok(result) = outcome else {
        panic!(
            "execute_update under a transport fault hung past the 30s watchdog — the \
             spanner.rpc.timeout_seconds.update deadline ({deadline_secs}s) did not bound it"
        )
    };
    worker.join().ok();

    let error = result.expect_err(
        "a write under a persistent transport fault should fail with Timeout, not report success",
    );
    assert_eq!(
        error.status,
        adbc_core::error::Status::Timeout,
        "the expired deadline must surface as ADBC Timeout; got: {error:?}"
    );
    assert!(
        error
            .message
            .contains(adbc_spanner::OPTION_RPC_TIMEOUT_UPDATE),
        "the timeout error must name the responsible option; got: {}",
        error.message
    );
    // Bounded time: it fired near the 2s deadline, nowhere near a hang. The upper bound is generous
    // (retry backoff + scheduling) but far below the 30s watchdog, so an actual hang still fails.
    assert!(
        elapsed < Duration::from_secs(15),
        "the timeout fired after {elapsed:?}; expected ~{deadline_secs}s, well under the watchdog — \
         a value near the watchdog would mean the deadline did not bound the retry loop"
    );

    // Contrast / recovery: unset the deadline (`""` unsets) and, with the fault gone, run the SAME
    // write — it must succeed once the channel re-establishes through the proxy, proving the expired
    // deadline left no lingering damage. Unbounded again, so it block-and-heals if still reconnecting.
    connection
        .lock()
        .expect("connection lock")
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_RPC_TIMEOUT_UPDATE.into()),
            OptionValue::String(String::new()),
        )
        .expect("unset update timeout");
    let mut recovered = false;
    let mut last = None;
    for _ in 0..20 {
        let mut conn = connection.lock().expect("connection lock");
        let mut s = conn.new_statement().expect("new statement");
        s.set_sql_query(format!("UPDATE {TXN_TABLE} SET Val = 1 WHERE Id = 1"))
            .unwrap();
        match s.execute_update() {
            Ok(_) => {
                recovered = true;
                break;
            }
            Err(e) => {
                last = Some(e);
                drop(conn);
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
    assert!(
        recovered,
        "the write did not succeed after the deadline was unset and the fault removed: {last:?}"
    );

    // The recovered write landed.
    assert_eq!(
        query_int(
            &connection,
            &format!("SELECT Val FROM {TXN_TABLE} WHERE Id = 1"),
        ),
        1,
        "the write must have applied once the deadline was unset and the transport healed"
    );
    eprintln!(
        "update timeout bounded a faulted write in {elapsed:?} with Status::Timeout naming \
         spanner.rpc.timeout_seconds.update; the same write succeeded once the deadline was unset"
    );
}

/// A mid-stream transport disconnect **after** some batches have already been consumed must surface
/// a clean ADBC error on the next chunk fetch — not a panic, not an unbounded hang, and not a
/// silently-short result that looks complete. Then, once the fault clears, a fresh query
/// **recovers**.
///
/// This is the counterpart to `reset_peer_surfaces_error_then_recovers`, which resets a query
/// *before* it starts: here the query is already streaming and the consumer has pulled real rows
/// off the reader before the connection dies, so it drives the resumption path (`SpannerBatchReader`
/// fetching a later chunk over a channel that has just been reset). The result is sized well past
/// the transport receive buffer (see `STREAM_ROWS`/`PAYLOAD_BYTES`) and read in small chunks, so
/// plenty of rows still have to come over the network after the first few batches are consumed —
/// guaranteeing the reset lands on a genuine mid-stream fetch rather than on already-buffered data.
#[test]
fn mid_stream_disconnect_after_batches_surfaces_error_then_recovers() {
    let Some(toxi) = toxi() else {
        eprintln!("TOXIPROXY_URL / SPANNER_EMULATOR_HOST not set — skipping resilience tests");
        return;
    };
    // Enforce the single-threaded contract that makes the env swap in `ensure_setup` sound. Placed
    // after the skip check so a plain multi-threaded `cargo test` (env unset) still self-skips.
    let _serial = SerialGuard::new();
    ensure_setup();

    let mut connection = connect();
    seed_stream_table(&mut connection);

    // Small chunks so the reader issues many network pulls after the first, and the buffered prefix
    // is only a fraction of the ~48 MB result.
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(300),
        )
        .expect("set rows_per_batch");
    statement
        .set_sql_query(format!(
            "SELECT Id, Payload FROM {STREAM_TABLE} ORDER BY Id"
        ))
        .unwrap();

    let mut reader = statement
        .execute()
        .expect("execute (first chunk) should succeed");

    // Consume a few batches with no fault active — these come from the prefix `execute()` already
    // buffered, so they must all succeed. This proves the disconnect below lands *after* real rows
    // were delivered to the consumer, i.e. it is a genuine mid-stream disconnect.
    let mut consumed_rows = 0usize;
    for _ in 0..3 {
        match reader.next() {
            Some(Ok(batch)) => consumed_rows += batch.num_rows(),
            Some(Err(e)) => panic!("a batch failed before any fault was injected: {e}"),
            None => break,
        }
    }
    assert!(
        consumed_rows > 0,
        "expected to consume at least one batch before injecting the fault"
    );

    // Now break the transport and drain the rest on a worker thread (bounded by a watchdog). Once
    // the buffered prefix is exhausted the reader must pull a later chunk over the reset channel,
    // which has to surface an error rather than hang or panic.
    toxi.add_reset_peer("resil_midstream_reset", 0);
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result: Result<Vec<_>, _> = reader.collect();
        let _ = tx.send((result.is_err(), format!("{:?}", result.err())));
    });
    let outcome = rx.recv_timeout(Duration::from_secs(45));
    // Clear the toxic regardless, so a failure doesn't leave the proxy poisoned for other tests.
    toxi.remove_toxic("resil_midstream_reset");

    let Ok((is_err, err_debug)) = outcome else {
        panic!(
            "draining the reader after a mid-stream reset hung past the 45s watchdog — the driver \
             did not surface the disconnect"
        )
    };
    worker.join().ok();
    assert!(
        is_err,
        "a mid-stream disconnect after {consumed_rows} rows were consumed must surface an error, \
         got Ok — the driver treated a truncated stream as complete"
    );
    eprintln!(
        "mid-stream disconnect surfaced a clean error after {consumed_rows} rows: {err_debug}"
    );

    // Recovery: with the toxic gone, a fresh query succeeds again once the channel re-establishes.
    let connection = Arc::new(Mutex::new(connection));
    let mut recovered = false;
    for _ in 0..20 {
        if query_one(&connection).is_ok() {
            recovered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        recovered,
        "driver did not recover after the mid-stream reset toxic was removed"
    );
    eprintln!("driver recovered after the mid-stream disconnect");
}

/// A **truncated** stream — the server-side connection closed part-way through a large result,
/// after the driver has already received and returned some rows — must surface a clean ADBC error
/// on the fetch that runs past the truncation point, never a silently-short (but "successful")
/// result. Then a fresh query **recovers** once the fault clears.
///
/// Toxiproxy's `limit_data` toxic closes the connection cleanly after a fixed number of downstream
/// bytes, which is exactly a truncated stream: the byte cap ([`TRUNCATE_AFTER_BYTES`]) sits above
/// the transport receive buffer (so `execute()` and the first batches succeed) but well below the
/// ~48 MB result (so the reader runs into the truncation while fetching a later chunk). Unlike
/// `reset_peer` (an abrupt TCP RST), this is an orderly close mid-message, so it specifically guards
/// against the driver mistaking a truncated result for a complete one.
#[test]
fn truncated_stream_surfaces_error_then_recovers() {
    let Some(toxi) = toxi() else {
        eprintln!("TOXIPROXY_URL / SPANNER_EMULATOR_HOST not set — skipping resilience tests");
        return;
    };
    // Enforce the single-threaded contract that makes the env swap in `ensure_setup` sound. Placed
    // after the skip check so a plain multi-threaded `cargo test` (env unset) still self-skips.
    let _serial = SerialGuard::new();
    ensure_setup();

    let mut connection = connect();
    seed_stream_table(&mut connection);

    // Small chunks so the reader issues many network pulls after the first — plenty of fetches to
    // run past the byte cap.
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(300),
        )
        .expect("set rows_per_batch");
    statement
        .set_sql_query(format!(
            "SELECT Id, Payload FROM {STREAM_TABLE} ORDER BY Id"
        ))
        .unwrap();

    // Install the byte cap *before* execute so it counts from the very start of the result stream,
    // then drain the reader on a worker thread bounded by a watchdog. `execute()` succeeds (the cap
    // is above the receive buffer) and the first batches come through, but a later chunk fetch runs
    // past the cap onto a closed connection and must fail rather than hang or return short.
    toxi.add_limit_data("resil_truncate", TRUNCATE_AFTER_BYTES);
    let reader = statement
        .execute()
        .expect("execute (first chunk) should succeed under the byte cap");

    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result: Result<Vec<_>, _> = reader.collect();
        let rows: usize = result
            .as_ref()
            .map_or(0, |batches| batches.iter().map(|b| b.num_rows()).sum());
        let _ = tx.send((result.is_err(), rows, format!("{:?}", result.err())));
    });
    let outcome = rx.recv_timeout(Duration::from_secs(45));
    // Clear the toxic regardless, so a failure doesn't leave the proxy poisoned for other tests.
    toxi.remove_toxic("resil_truncate");

    let Ok((is_err, rows, err_debug)) = outcome else {
        panic!(
            "draining a truncated stream hung past the 45s watchdog — the driver did not surface \
             the truncation"
        )
    };
    worker.join().ok();
    assert!(
        is_err,
        "a truncated stream must surface an error, got Ok with {rows} rows — the driver treated a \
         truncated result as complete (the query selects all {STREAM_ROWS} rows)"
    );
    eprintln!("truncated stream surfaced a clean error: {err_debug}");

    // Recovery: with the cap gone, a fresh query succeeds again once the channel re-establishes.
    let connection = Arc::new(Mutex::new(connection));
    let mut recovered = false;
    for _ in 0..20 {
        if query_one(&connection).is_ok() {
            recovered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        recovered,
        "driver did not recover after the limit_data truncation toxic was removed"
    );
    eprintln!("driver recovered after the truncated stream");
}

/// Run a query expected to return a single INT64 cell and return its value.
fn query_int(connection: &Arc<Mutex<SpannerConnection>>, sql: &str) -> i64 {
    let mut conn = connection.lock().expect("connection lock");
    let mut s = conn.new_statement().expect("new statement");
    s.set_sql_query(sql).unwrap();
    let batches: Vec<_> = s
        .execute()
        .expect("int query")
        .collect::<Result<Vec<_>, _>>()
        .expect("int query stream");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("INT64 column")
        .value(0)
}

/// Run `SELECT 1` on the shared connection, returning the ADBC result.
fn query_one(connection: &Arc<Mutex<SpannerConnection>>) -> adbc_core::error::Result<()> {
    let mut conn = connection.lock().expect("connection lock");
    let mut s = conn.new_statement()?;
    s.set_sql_query("SELECT 1 AS one")?;
    let reader = s.execute()?;
    for batch in reader {
        batch.map_err(|e| {
            adbc_core::error::Error::with_message_and_status(
                format!("stream error: {e}"),
                adbc_core::error::Status::Internal,
            )
        })?;
    }
    Ok(())
}

/// Drive `query_one` on a worker thread and wait up to `timeout`. Returns `Some(result)` if the
/// query completed in time, or `None` if it hung past the watchdog (a leaked worker thread is
/// acceptable — the process reaps it on exit).
fn run_query_bounded(
    connection: &Arc<Mutex<SpannerConnection>>,
    timeout: Duration,
) -> Option<adbc_core::error::Result<()>> {
    let conn = connection.clone();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = query_one(&conn);
        let _ = tx.send(result);
    });
    rx.recv_timeout(timeout).ok()
}

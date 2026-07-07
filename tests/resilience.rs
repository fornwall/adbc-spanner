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

use std::process::Command;
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

fn database_path() -> String {
    format!("projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}")
}

/// The Toxiproxy admin API base and target proxy name, or `None` to skip the whole file.
///
/// Requires the proxy (created by `scripts/with-toxiproxy.sh`) *and* `SPANNER_EMULATOR_HOST` to be
/// set — the latter must point at the proxy listener, which the script guarantees.
fn toxi() -> Option<Toxiproxy> {
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

/// Create the instance, database and the streaming test table once per binary. Best-effort: the
/// create calls fail with `AlreadyExists` on a re-run, which is fine.
///
/// Schema/DDL setup goes to the emulator's **real** `<ip>:9010` endpoint (`SPANNER_EMULATOR_DIRECT`),
/// not the proxy: the google-cloud-rust admin client only remaps the emulator admin port for an
/// endpoint ending in `:9010`, so DB/table creation through the proxy's port would send the admin
/// request to the wrong place and fail. Only the driver-under-test's data-plane traffic uses the
/// proxy. We temporarily repoint `SPANNER_EMULATOR_HOST` for the duration of setup, then restore it
/// so the tests' driver connections go through the proxy again. (Setup runs once under the mutex,
/// before any driver connection, so the transient env swap is safe here.)
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
    std::env::set_var("SPANNER_EMULATOR_HOST", &direct);

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
    std::env::set_var("SPANNER_EMULATOR_HOST", &proxy_host);
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

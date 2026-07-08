//! Deterministic fault injection against an **in-process mock Spanner gRPC server**.
//!
//! These tests run fully **offline** in a plain `cargo test` — no Docker, no emulator, no
//! network beyond a loopback TCP port. They complement the two other suites:
//!
//! - `tests/integration.rs` — correctness against the real emulator / a real database
//!   (self-skips when no target is configured).
//! - `tests/resilience.rs` + `tests/RESILIENCE.md` — **transport**-level faults through
//!   Toxiproxy (an L4 proxy). Its "Honest limitations" section notes it cannot produce
//!   *logical* gRPC statuses (no `ABORTED`, no mid-stream status, no scripted hang).
//!
//! This file is that missing complement: a scriptable `google.spanner.v1.Spanner` server that
//! returns exactly the gRPC status/stream shape a test asks for, so the driver's error mapping,
//! stream-failure and cancellation paths get deterministic regression coverage. The division of
//! labor is: **Toxiproxy = transport faults, this harness = logical gRPC faults.** The same
//! approach carries most of the historical-regression coverage in the ADBC Flight SQL driver
//! (its in-process mock-server suite).
//!
//! The server reuses `spanner-grpc-mock`, the mockall/tonic-based mock the pinned
//! `google-cloud-spanner` client uses for its own end-to-end tests (see `src/spanner/grpc-mock`
//! in the `google-cloud-rust` checkout), pinned to the same git revision as the client so the
//! wire protos match. The harness here ([`MockServer`]) binds it to an ephemeral localhost port
//! and points the driver at it via `spanner.endpoint` + `spanner.emulator=true` (anonymous
//! credentials over plaintext HTTP/2 — the same path a real emulator uses). Only **data-plane**
//! RPCs exist here: the driver's admin clients (DDL) are never built, so the client's
//! emulator-only `9010`→`9020` admin-endpoint remap never applies to these tests.
//!
//! Scripting rules (mockall matches expectations in FIFO order):
//! - the harness always serves `CreateSession` (the client creates one multiplexed session per
//!   `DatabaseClient`) before the test's script runs;
//! - after the script, every RPC gets a trailing catch-all returning `UNIMPLEMENTED`, so an
//!   unexpected RPC fails the test loudly instead of hanging it (a mockall "no expectation"
//!   panic inside the server task would surface as an endlessly-retried transport error).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use adbc_core::error::Status as AdbcStatus;
use adbc_core::options::{OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_spanner::{SpannerConnection, SpannerDriver};
use arrow_array::cast::AsArray;
use arrow_schema::{ArrowError, DataType};
use prost::Message;
use spanner_grpc_mock::MockSpanner;
use spanner_grpc_mock::google::spanner::v1;

/// The database the driver is pointed at; the mock does not validate it, but the client sends it
/// in `CreateSessionRequest.database` and session names are derived from it.
const DATABASE: &str = "projects/mock-project/instances/mock-instance/databases/mock-db";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// An in-process mock `google.spanner.v1.Spanner` gRPC server on an ephemeral localhost port.
///
/// Dropping it aborts the server task and shuts the private Tokio runtime down.
struct MockServer {
    /// Plaintext endpoint (`http://127.0.0.1:<port>`) to hand to `spanner.endpoint`.
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
    /// Keeps the server task's runtime alive for the duration of the test.
    _runtime: tokio::runtime::Runtime,
}

impl MockServer {
    /// Start a mock server: serve sessions, apply the test's `script`, then reject everything
    /// unscripted with `UNIMPLEMENTED` (see the module doc for why the order matters).
    fn start(script: impl FnOnce(&mut MockSpanner)) -> Self {
        let mut mock = MockSpanner::new();
        serve_sessions(&mut mock);
        script(&mut mock);
        reject_unscripted_rpcs(&mut mock);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build mock-server runtime");
        let (endpoint, server) = runtime
            .block_on(spanner_grpc_mock::start("127.0.0.1:0", mock))
            .expect("start mock Spanner server");
        Self {
            endpoint,
            server,
            _runtime: runtime,
        }
    }

    /// Connect the driver to this mock server the same way it reaches an emulator: explicit
    /// plaintext endpoint + `spanner.emulator=true` (anonymous credentials). The explicit
    /// endpoint wins over any ambient `SPANNER_EMULATOR_HOST`.
    fn connect(&self) -> SpannerConnection {
        let mut driver = SpannerDriver::try_new().expect("create driver");
        let database = driver
            .new_database_with_opts([
                (OptionDatabase::Uri, OptionValue::String(DATABASE.into())),
                (
                    OptionDatabase::Other(adbc_spanner::OPTION_ENDPOINT.into()),
                    OptionValue::String(self.endpoint.clone()),
                ),
                (
                    OptionDatabase::Other(adbc_spanner::OPTION_EMULATOR.into()),
                    OptionValue::String("true".into()),
                ),
            ])
            .expect("create database");
        database.new_connection().expect("connect to mock server")
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Serve `CreateSession` (the client creates a single multiplexed session per `DatabaseClient`,
/// and a background maintainer may re-create it), unbounded.
fn serve_sessions(mock: &mut MockSpanner) {
    mock.expect_create_session().returning(|request| {
        let database = request.into_inner().database;
        Ok(tonic::Response::new(v1::Session {
            name: format!("{database}/sessions/mock-session"),
            multiplexed: true,
            ..Default::default()
        }))
    });
}

/// Trailing catch-alls: any RPC the test did not script fails fast with `UNIMPLEMENTED` (which
/// nothing retries) instead of a mockall panic inside the server task (which the client would
/// see as a retryable transport error — i.e. a hang).
fn reject_unscripted_rpcs(mock: &mut MockSpanner) {
    macro_rules! reject {
        ($($method:ident),+ $(,)?) => {
            $(
                mock.$method().returning(|_| {
                    Err(tonic::Status::unimplemented(concat!(
                        "mock spanner server: unscripted RPC ",
                        stringify!($method),
                    )))
                });
            )+
        };
    }
    reject!(
        expect_create_session,
        expect_batch_create_sessions,
        expect_get_session,
        expect_list_sessions,
        expect_delete_session,
        expect_execute_sql,
        expect_execute_streaming_sql,
        expect_execute_batch_dml,
        expect_read,
        expect_streaming_read,
        expect_begin_transaction,
        expect_commit,
        expect_rollback,
        expect_partition_query,
        expect_partition_read,
        expect_batch_write,
        expect_fetch_cache_update,
    );
}

/// Abort the whole test process if `what` outlives `limit` — these tests exist to prove the
/// driver cannot hang, so a hang must fail CI instead of blocking it forever.
struct Watchdog {
    disarmed: Arc<AtomicBool>,
}

impl Watchdog {
    fn arm(limit: Duration, what: &'static str) -> Self {
        let disarmed = Arc::new(AtomicBool::new(false));
        let flag = disarmed.clone();
        std::thread::spawn(move || {
            std::thread::sleep(limit);
            if !flag.load(Ordering::SeqCst) {
                eprintln!("watchdog: {what} still running after {limit:?} — aborting");
                std::process::abort();
            }
        });
        Self { disarmed }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.disarmed.store(true, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// PartialResultSet scripting helpers
// ---------------------------------------------------------------------------

/// Result metadata for a single STRING column `c`.
fn string_column_metadata() -> v1::ResultSetMetadata {
    v1::ResultSetMetadata {
        row_type: Some(v1::StructType {
            fields: vec![v1::struct_type::Field {
                name: "c".to_string(),
                r#type: Some(v1::Type {
                    code: v1::TypeCode::String as i32,
                    ..Default::default()
                }),
            }],
        }),
        ..Default::default()
    }
}

fn string_value(s: &str) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StringValue(s.to_string())),
    }
}

/// A `PartialResultSet` of STRING values; `with_metadata` must be true on the first message of a
/// stream (Spanner sends the row type exactly once, up front).
fn partial_result_set(
    with_metadata: bool,
    values: &[&str],
    resume_token: &[u8],
    last: bool,
) -> v1::PartialResultSet {
    v1::PartialResultSet {
        metadata: with_metadata.then(string_column_metadata),
        values: values.iter().map(|s| string_value(s)).collect(),
        resume_token: resume_token.to_vec(),
        last,
        ..Default::default()
    }
}

/// The stream type the mock's `execute_streaming_sql` expectation returns.
type PartialResultSetSender = tokio::sync::mpsc::Sender<tonic::Result<v1::PartialResultSet>>;

/// A finished response stream carrying the given items.
fn stream_of(
    items: Vec<tonic::Result<v1::PartialResultSet>>,
) -> tonic::Response<tokio::sync::mpsc::Receiver<tonic::Result<v1::PartialResultSet>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(items.len().max(1));
    for item in items {
        tx.try_send(item)
            .expect("scripted stream channel sized to fit");
    }
    tonic::Response::new(rx)
}

// ---------------------------------------------------------------------------
// google.rpc.Status / RetryInfo details
// ---------------------------------------------------------------------------

/// `google.rpc.RetryInfo` — not in the mock crate's generated protos, so declared locally (one
/// field; the wire format is stable).
#[derive(Clone, PartialEq, prost::Message)]
struct RetryInfo {
    #[prost(message, optional, tag = "1")]
    retry_delay: Option<prost_types::Duration>,
}

/// An `ABORTED` status carrying a `google.rpc.RetryInfo` detail in `grpc-status-details-bin`,
/// exactly as Cloud Spanner sends on transaction aborts.
fn aborted_with_retry_info(message: &str) -> tonic::Status {
    let retry_info = RetryInfo {
        retry_delay: Some(prost_types::Duration {
            seconds: 0,
            nanos: 50_000_000,
        }),
    };
    let status_proto = spanner_grpc_mock::google::rpc::Status {
        code: tonic::Code::Aborted as i32,
        message: message.to_string(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.RetryInfo".to_string(),
            value: retry_info.encode_to_vec(),
        }],
    };
    tonic::Status::with_details(
        tonic::Code::Aborted,
        message,
        bytes::Bytes::from(status_proto.encode_to_vec()),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Harness proof: a scripted query round-trips through the real driver stack (session creation,
/// `ExecuteStreamingSql`, PartialResultSet → Arrow conversion) against the mock server.
#[test]
fn mock_server_round_trips_a_query() {
    let _watchdog = Watchdog::arm(Duration::from_secs(120), "mock_server_round_trips_a_query");

    let seen_sql: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let record_sql = seen_sql.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                *record_sql.lock().unwrap() = Some(request.into_inner().sql);
                Ok(stream_of(vec![Ok(partial_result_set(
                    true,
                    &["v1", "v2"],
                    b"rt-1",
                    true,
                ))]))
            });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_sql_query("SELECT c FROM MockTable")
        .expect("set query");
    let batches: Vec<_> = statement
        .execute()
        .expect("query against mock server")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");

    assert_eq!(
        seen_sql.lock().unwrap().as_deref(),
        Some("SELECT c FROM MockTable"),
        "the mock server should have received the statement's SQL verbatim"
    );
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema().field(0).name(), "c");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    let column = batch.column(0).as_string::<i32>();
    assert_eq!(column.value(0), "v1");
    assert_eq!(column.value(1), "v2");
}

/// (a) `ABORTED` (with a `google.rpc.RetryInfo` detail) on `ExecuteStreamingSql` surfaces as a
/// clean ADBC error with the numeric gRPC code preserved in `vendor_code` (ABORTED = 10).
///
/// `ExecuteStreamingSql` is the right RPC to fault: an `ABORTED` *commit* is retried by the
/// client's transaction runner by design (Spanner's abort-and-replay protocol), so it would
/// never surface, while a single-use read-only query has no replay protocol — the driver must
/// hand the caller the error, and the caller's own retry logic needs `vendor_code` 10 to
/// recognise it (see `from_spanner` in `src/error.rs`).
///
/// The mock attaches the `RetryInfo` detail Spanner really sends; once the driver forwards
/// error details into ADBC `Error::details` (the `feat/error-details` branch, unmerged at the
/// time of writing), this test is the place to assert the detail round-trips.
#[test]
fn aborted_surfaces_vendor_code_10() {
    let _watchdog = Watchdog::arm(Duration::from_secs(120), "aborted_surfaces_vendor_code_10");

    let server = MockServer::start(|mock| {
        mock.expect_execute_streaming_sql().returning(|_| {
            Err(aborted_with_retry_info(
                "Transaction was aborted by the mock server",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let error = statement
        .execute()
        .err()
        .expect("an ABORTED query must fail, not hang or succeed");

    assert_eq!(
        error.vendor_code, 10,
        "vendor_code must carry the numeric gRPC code (ABORTED = 10); got error: {error}"
    );
    // ABORTED maps to Status::IO (retryable-by-caller), per src/error.rs.
    assert_eq!(error.status, AdbcStatus::IO, "got error: {error}");
    assert!(
        error.message.contains("aborted by the mock server"),
        "the server's status message must survive into the ADBC error, got: {}",
        error.message
    );
}

/// (a′) The companion to [`aborted_surfaces_vendor_code_10`]: the `google.rpc.RetryInfo` detail the
/// mock attaches to its `ABORTED` status must survive **the whole real driver stack** — the gRPC
/// `grpc-status-details-bin` trailer, the client's `google.rpc.Status` decode, and `from_spanner`'s
/// `details_for_adbc` mapping — and land on the surfaced [`adbc_core::error::Error::details`]. This
/// is the end-to-end complement to the `from_spanner` unit tests in `src/error.rs`, which construct
/// gax errors directly and so never exercise the wire decode.
///
/// The assertion pins the exact contract `from_spanner` documents: key = the lowercased
/// fully-qualified proto type name (`google.rpc.retryinfo`), value = the detail's ProtoJSON, whose
/// `retryDelay` round-trips the 50 ms the mock sent (`0.05s`).
///
/// **Fidelity note.** This drives the driver's public `adbc_core` traits (`Connection` /
/// `Statement`), *not* the C-ABI FFI. Empirically the detail does **not** survive the FFI boundary:
/// the driver stores the numeric gRPC code (`ABORTED` = 10) in `vendor_code`, but the ADBC C detail
/// transport only re-reads `ErrorGetDetail`/`ErrorGetDetailCount` when `vendor_code ==
/// ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA` (`i32::MIN`); with any other `vendor_code` the forwarded
/// details are dropped in the driver-manager round-trip. So the trait boundary is the highest
/// fidelity at which the detail is actually retrievable today.
#[test]
fn aborted_retry_info_detail_reaches_adbc_error_details() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "aborted_retry_info_detail_reaches_adbc_error_details",
    );

    let server = MockServer::start(|mock| {
        mock.expect_execute_streaming_sql().returning(|_| {
            Err(aborted_with_retry_info(
                "Transaction was aborted by the mock server",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let error = statement
        .execute()
        .err()
        .expect("an ABORTED query must fail, not hang or succeed");

    // The RetryInfo the mock attached must be forwarded into ADBC's structured error details.
    let details = error.details.as_ref().expect(
        "the ABORTED status carried a google.rpc.RetryInfo detail; Error.details must be Some",
    );
    let (_, value) = details
        .iter()
        .find(|(key, _)| key == "google.rpc.retryinfo")
        .unwrap_or_else(|| {
            panic!(
                "expected a `google.rpc.retryinfo` detail, got keys: {:?}",
                details.iter().map(|(k, _)| k).collect::<Vec<_>>()
            )
        });

    // The value is the detail's self-describing ProtoJSON; parse it rather than byte-compare so the
    // assertion states the contract (not an incidental field order).
    let json: serde_json::Value =
        serde_json::from_slice(value).expect("the detail value is UTF-8 ProtoJSON");
    assert_eq!(
        json["@type"], "type.googleapis.com/google.rpc.RetryInfo",
        "the detail must self-describe as a RetryInfo, got: {json}"
    );
    assert_eq!(
        json["retryDelay"], "0.05s",
        "the RetryInfo's retryDelay must round-trip the 50ms the mock sent, got: {json}"
    );
}

/// (b) `UNAVAILABLE` mid-stream: the server sends one `PartialResultSet` (with a resume token),
/// then fails the stream. The client resumes once — from exactly the token it saw — and when the
/// resume attempt is refused permanently, the driver surfaces a clean ADBC error: no panic, no
/// hang (watchdog-enforced), message and code intact.
#[test]
fn unavailable_mid_stream_surfaces_a_clean_error() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "unavailable_mid_stream_surfaces_a_clean_error",
    );

    let calls = Arc::new(AtomicUsize::new(0));
    let resume_tokens: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_in_mock = calls.clone();
    let tokens_in_mock = resume_tokens.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                if calls_in_mock.fetch_add(1, Ordering::SeqCst) == 0 {
                    // First attempt: one good message, then the stream dies.
                    Ok(stream_of(vec![
                        Ok(partial_result_set(true, &["v1"], b"resume-1", false)),
                        Err(tonic::Status::unavailable(
                            "mock server: connection lost mid-stream",
                        )),
                    ]))
                } else {
                    // The resume attempt. Record the token it resumes from, and refuse with a
                    // permanent (non-retryable) status so the error must surface to the caller.
                    tokens_in_mock
                        .lock()
                        .unwrap()
                        .push(request.into_inner().resume_token);
                    Err(tonic::Status::internal(
                        "mock server: resume attempt refused",
                    ))
                }
            });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();

    // `execute` pulls the first row chunk (default 8192 rows), so the mid-stream failure — and
    // the failed resume — surface right here, as a clean error rather than a panic or hang.
    let started = Instant::now();
    let error = statement
        .execute()
        .err()
        .expect("a query whose stream dies mid-flight must fail, not hang or succeed");
    let elapsed = started.elapsed();

    assert_eq!(
        error.vendor_code, 13,
        "the *resume* refusal (INTERNAL = 13) is what surfaces; got error: {error}"
    );
    assert!(
        error.message.contains("resume attempt refused"),
        "expected the resume refusal to surface, got: {}",
        error.message
    );
    // The client resumed exactly once, from exactly the token the first stream delivered.
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        resume_tokens.lock().unwrap().as_slice(),
        &[b"resume-1".to_vec()],
        "the resume attempt must carry the last resume token the stream delivered"
    );
    // One backoff sleep (~1s default) is expected between the failure and the resume; anything
    // approaching the watchdog means retries ran away.
    assert!(
        elapsed < Duration::from_secs(60),
        "error took {elapsed:?} to surface — runaway retries?"
    );
}

/// (c) A server that accepts the RPC, sends one row, then goes silent (stream held open,
/// nothing more ever arrives): `Statement::cancel` from another thread unblocks the reader
/// promptly with `Status::Cancelled`. This is the foundation for future timeout tests — the
/// blocked position is a genuine in-flight `block_on` on a live gRPC stream, produced without
/// Toxiproxy's bandwidth throttling (compare `cancel_interrupts_in_flight_query` in
/// tests/resilience.rs).
#[test]
fn cancel_unblocks_a_reader_hung_on_a_silent_stream() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "cancel_unblocks_a_reader_hung_on_a_silent_stream",
    );

    // Keep every scripted stream's sender alive so the streams never end and never error: from
    // the client's side the server has simply gone silent mid-result.
    let open_streams: Arc<Mutex<Vec<PartialResultSetSender>>> = Arc::new(Mutex::new(Vec::new()));
    let streams_in_mock = open_streams.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql().returning(move |_| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.try_send(Ok(partial_result_set(true, &["v1"], b"rt-1", false)))
                .expect("first message fits the channel");
            streams_in_mock.lock().unwrap().push(tx);
            Ok(tonic::Response::new(rx))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    // One row per batch, so `execute` completes with the one delivered row and the *next* fetch
    // is what blocks on the silent stream.
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(1),
        )
        .expect("set rows_per_batch");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let mut reader = statement.execute().expect("execute settles the schema");

    // Drive the reader on a worker thread: the first batch is already buffered, the second fetch
    // blocks forever on the silent stream — until the cancel lands.
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let first = reader.next();
        let second = reader.next();
        let _ = tx.send((first, second));
    });

    // Let the worker settle into the blocked fetch, then cancel from this thread.
    std::thread::sleep(Duration::from_millis(300));
    let cancel_at = Instant::now();
    statement.cancel().expect("cancel");

    let (first, second) = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("cancel did not unblock the reader stuck on a silent stream");
    let cancel_latency = cancel_at.elapsed();
    worker.join().expect("worker thread");

    let first = first
        .expect("first batch exists")
        .expect("first batch is the row delivered before the server went silent");
    assert_eq!(first.num_rows(), 1);

    let error = second
        .expect("the cancelled fetch yields an item")
        .expect_err("the fetch blocked on the silent stream must fail with the cancel");
    let ArrowError::ExternalError(source) = &error else {
        panic!("expected the reader to surface the driver error, got: {error}");
    };
    let adbc_error = source
        .downcast_ref::<adbc_core::error::Error>()
        .expect("the reader error wraps the ADBC error");
    assert_eq!(
        adbc_error.status,
        AdbcStatus::Cancelled,
        "got error: {adbc_error}"
    );
    assert!(
        cancel_latency < Duration::from_secs(10),
        "cancel took {cancel_latency:?} to unblock the reader"
    );
}

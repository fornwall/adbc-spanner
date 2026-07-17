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
//! RPCs are scripted here: the mock speaks `google.spanner.v1.Spanner`, not the REST admin API the
//! driver's DDL path uses. Because the harness binds an ephemeral port, the client's emulator-only
//! `:9010`→`:9020` admin-endpoint remap never applies, so an admin client built in these tests
//! targets the harness's own socket — which [`GatedEndpoint`] exploits to bound the DDL path
//! deterministically without serving admin RPCs at all.
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
use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_spanner::{SpannerConnection, SpannerDriver};
use arrow_array::cast::AsArray;
use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema};
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
                (
                    OptionDatabase::Uri,
                    OptionValue::String(format!("spanner:///{DATABASE}")),
                ),
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

/// A TCP gate in front of a [`MockServer`], used to make the driver's **admin** (DDL) endpoint
/// unanswerable *by construction* — the one thing this data-plane mock cannot script itself.
///
/// The client derives the admin endpoint from the data-plane one, remapping the port only for an
/// endpoint ending in `:9010` (`map_emulator_admin_endpoint` in the pinned client); the harness
/// binds an *ephemeral* port, so the remap never applies and **both clients target this one
/// socket**. That is what lets a DDL test run offline: the driver reaches the mock through the
/// gate, and once it is [switched silent](Self::go_silent) every *new* connection is accepted and
/// then never answered — a black hole. Accepted sockets are deliberately **held open** rather than
/// dropped: a closed socket would surface as a transport *error*, whereas the point here is a
/// request that hangs.
///
/// Ordering is explicit at the call site: connect first (letting `CreateSession` through), then go
/// silent, then issue the DDL — whose admin client dials a *fresh* HTTP/1.1 connection (a separate
/// pool from the already-established h2c data-plane channel) and waits forever.
struct GatedEndpoint {
    /// Plaintext endpoint (`http://127.0.0.1:<port>`) to hand to `spanner.endpoint`.
    endpoint: String,
    silent: Arc<AtomicBool>,
}

impl GatedEndpoint {
    /// Start a gate forwarding to `upstream` (a `MockServer` endpoint).
    fn start(upstream: &str) -> Self {
        let upstream = upstream.trim_start_matches("http://").to_string();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind gate listener");
        let address = listener.local_addr().expect("gate address");
        let silent = Arc::new(AtomicBool::new(false));

        let gate_silent = silent.clone();
        std::thread::spawn(move || {
            // Sockets accepted while silent are parked here: held open (so the peer sees a live
            // connection that simply never replies) until the process exits.
            let mut parked = Vec::new();
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                if gate_silent.load(Ordering::SeqCst) {
                    parked.push(client);
                    continue;
                }
                let Ok(server) = std::net::TcpStream::connect(&upstream) else {
                    continue;
                };
                // Pump both directions; each half ends when its side closes.
                for (mut from, mut to) in [
                    (
                        client.try_clone().expect("clone client socket"),
                        server.try_clone().expect("clone server socket"),
                    ),
                    (server, client),
                ] {
                    std::thread::spawn(move || {
                        let _ = std::io::copy(&mut from, &mut to);
                    });
                }
            }
        });

        Self {
            endpoint: format!("http://{address}"),
            silent,
        }
    }

    /// Black-hole every subsequent connection. Already-established ones keep flowing.
    fn go_silent(&self) {
        self.silent.store(true, Ordering::SeqCst);
    }

    /// Connect the driver through the gate, exactly as [`MockServer::connect`] does directly.
    fn connect(&self) -> SpannerConnection {
        let mut driver = SpannerDriver::try_new().expect("create driver");
        let database = driver
            .new_database_with_opts([
                (
                    OptionDatabase::Uri,
                    OptionValue::String(format!("spanner:///{DATABASE}")),
                ),
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
        database.new_connection().expect("connect through the gate")
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

/// Result metadata for an arbitrary list of `(name, type)` columns — used by the `get_statistics`
/// mock, whose discovery queries return STRING columns and whose aggregate scans return INT64 ones.
fn result_metadata(fields: &[(&str, v1::TypeCode)]) -> v1::ResultSetMetadata {
    v1::ResultSetMetadata {
        row_type: Some(v1::StructType {
            fields: fields
                .iter()
                .map(|(name, code)| v1::struct_type::Field {
                    name: name.to_string(),
                    r#type: Some(v1::Type {
                        code: *code as i32,
                        ..Default::default()
                    }),
                })
                .collect(),
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

/// Serve `ExecuteStreamingSql` (unbounded) with a fixed single-message STRING result, echoing a
/// transaction id (`manual-ro-txn`) back in the result metadata whenever the request carries an
/// inline `transaction.begin` selector — which is what a query in a manual transaction sends: it
/// begins the transaction's shared multi-use read-only transaction inline with its first
/// statement, and the client requires the created transaction's id in the first response.
/// Optionally records each request's transaction selector for wire assertions.
fn serve_streaming_sql_begin_aware(
    mock: &mut MockSpanner,
    values: &'static [&'static str],
    record: Option<Arc<Mutex<Vec<Option<v1::TransactionSelector>>>>>,
) {
    mock.expect_execute_streaming_sql()
        .returning(move |request| {
            let request = request.into_inner();
            let inline_begin = matches!(
                request
                    .transaction
                    .as_ref()
                    .and_then(|t| t.selector.as_ref()),
                Some(v1::transaction_selector::Selector::Begin(_))
            );
            if let Some(record) = &record {
                record.lock().unwrap().push(request.transaction);
            }
            let mut first = partial_result_set(true, values, b"ro-1", true);
            if inline_begin {
                first
                    .metadata
                    .as_mut()
                    .expect("first message carries metadata")
                    .transaction = Some(v1::Transaction {
                    id: b"manual-ro-txn".to_vec(),
                    ..Default::default()
                });
            }
            Ok(stream_of(vec![Ok(first)]))
        });
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

/// `google.rpc.ErrorInfo` — like [`RetryInfo`], not in the mock crate's generated protos, so
/// declared locally against the (stable) wire format. `metadata` (field 3) is unused here and
/// omitted.
#[derive(Clone, PartialEq, prost::Message)]
struct ErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
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
// Bulk-ingest scripting helpers
// ---------------------------------------------------------------------------

/// A one-STRING-column (`c`) record batch of `n` rows — each row becomes one insert mutation, so
/// the mock sees exactly `n` mutations in the resulting `Commit`.
fn ingest_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)]));
    let column = StringArray::from((0..n).map(|i| format!("v{i}")).collect::<Vec<_>>());
    RecordBatch::try_new(schema, vec![Arc::new(column)]).expect("build ingest batch")
}

/// Spanner's per-commit mutation-limit rejection: an `INVALID_ARGUMENT` carrying the stable "too
/// many mutations" phrasing the driver's `is_mutation_limit_exceeded` keys off.
fn too_many_mutations_status() -> tonic::Status {
    tonic::Status::invalid_argument(
        "The transaction contains too many mutations. Insert and update operations count with the \
         multiplicity of the number of columns they affect. The total mutation count includes any \
         changes to indexes that the transaction generates. Please reduce the number of writes, or \
         use fewer indexes. (Maximum number: 80000)",
    )
}

/// A default successful `Commit` response (no commit stats requested).
fn commit_ok() -> tonic::Result<tonic::Response<v1::CommitResponse>> {
    Ok(tonic::Response::new(v1::CommitResponse::default()))
}

/// One streamed `BatchWriteResponse` reporting that mutation group `0` failed with `ALREADY_EXISTS`
/// and a `google.rpc.ErrorInfo` detail — the in-band, per-group failure shape BatchWrite uses (the
/// RPC itself succeeds; the status rides *inside* the response, not the gRPC trailer).
fn batch_write_group_already_exists() -> v1::BatchWriteResponse {
    let error_info = ErrorInfo {
        reason: "DUPLICATE_KEY".to_string(),
        domain: "spanner.googleapis.com".to_string(),
    };
    v1::BatchWriteResponse {
        indexes: vec![0],
        status: Some(spanner_grpc_mock::google::rpc::Status {
            code: tonic::Code::AlreadyExists as i32,
            message: "Row [v0] in table MockTable already exists".to_string(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/google.rpc.ErrorInfo".to_string(),
                value: error_info.encode_to_vec(),
            }],
        }),
        commit_timestamp: None,
    }
}

/// One streamed `BatchWriteResponse` reporting that the mutation groups `indexes` applied (an
/// absent/`OK` status is BatchWrite's "these groups committed" signal).
fn batch_write_groups_ok(indexes: Vec<i32>) -> v1::BatchWriteResponse {
    v1::BatchWriteResponse {
        indexes,
        status: None,
        commit_timestamp: Some(prost_types::Timestamp::default()),
    }
}

/// Serve `BeginTransaction` (the write-only ingest path begins a read/write transaction before each
/// `Commit`), returning a fixed transaction id.
fn serve_begin_transaction(mock: &mut MockSpanner) {
    mock.expect_begin_transaction().returning(|_| {
        Ok(tonic::Response::new(v1::Transaction {
            id: b"mock-txn".to_vec(),
            ..Default::default()
        }))
    });
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

/// Manual-transaction read-your-writes guard: a data-returning query issued while writes are
/// buffered in a manual transaction is rejected up front with `InvalidState` (the kind-mixing
/// rule: the transaction began with DML), instead of silently running against a snapshot that
/// misses the buffered writes. The negatives — a query in autocommit mode and a query in a
/// *fresh* manual transaction (which then fixes the transaction's kind to queries) — must still
/// run.
///
/// The guard fires before any RPC, so the mock only needs to serve `ExecuteStreamingSql` for the
/// queries that are *allowed* through; the buffered `INSERT` just adds to the in-memory buffer
/// (no RPC) and the guarded query never reaches the wire.
#[test]
fn query_while_dml_buffered_in_manual_txn_is_rejected() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "query_while_dml_buffered_in_manual_txn_is_rejected",
    );

    let server = MockServer::start(move |mock| {
        // Served for every *allowed* query — including the manual-mode ones, which begin the
        // transaction's shared read-only transaction inline; unbounded times (the guarded query
        // never gets here).
        serve_streaming_sql_begin_aware(mock, &["v1", "v2"], None);
    });

    let mut connection = server.connect();

    // Negative 1: autocommit mode (the default) is unaffected — the query runs.
    let mut auto_q = connection.new_statement().expect("new statement");
    auto_q.set_sql_query("SELECT c FROM MockTable").unwrap();
    let batches: Vec<_> = auto_q
        .execute()
        .expect("autocommit query must run")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect autocommit batches");
    assert_eq!(batches[0].num_rows(), 2);

    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");

    // Negative 2: a fresh manual transaction allows a query (which fixes its kind to queries —
    // roll back afterwards so the DML below starts a fresh transaction).
    let mut empty_q = connection.new_statement().expect("new statement");
    empty_q.set_sql_query("SELECT c FROM MockTable").unwrap();
    let batches: Vec<_> = empty_q
        .execute()
        .expect("query in a fresh manual transaction must run")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect empty-buffer batches");
    assert_eq!(batches[0].num_rows(), 2);
    connection.rollback().expect("end the query transaction");

    // Buffer a DML statement (returns None in manual mode, no RPC).
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO MockTable (c) VALUES ('x')")
        .unwrap();
    assert_eq!(
        insert.execute_update().expect("buffered insert"),
        None,
        "DML in manual mode buffers (returns None), not commits"
    );

    // Positive: a query while a write is buffered is rejected up front with InvalidState.
    let mut guarded = connection.new_statement().expect("new statement");
    guarded.set_sql_query("SELECT c FROM MockTable").unwrap();
    let Err(error) = guarded.execute() else {
        panic!("a query while DML is buffered must be rejected, not silently run");
    };
    assert_eq!(
        error.status,
        AdbcStatus::InvalidState,
        "querying with buffered writes must fail with InvalidState (no read-your-writes)"
    );
    assert!(
        error.message.contains("read-your-writes"),
        "the error should explain the read-your-writes hazard: {:?}",
        error.message
    );

    // execute_partitions is guarded the same way.
    let mut guarded_partitions = connection.new_statement().expect("new statement");
    guarded_partitions
        .set_sql_query("SELECT c FROM MockTable")
        .unwrap();
    let Err(error) = guarded_partitions.execute_partitions() else {
        panic!("execute_partitions while DML is buffered must be rejected");
    };
    assert_eq!(error.status, AdbcStatus::InvalidState);

    // Rolling back empties the buffer, so a query is allowed again.
    connection.rollback().expect("rollback buffered insert");
    let mut after = connection.new_statement().expect("new statement");
    after.set_sql_query("SELECT c FROM MockTable").unwrap();
    let batches: Vec<_> = after
        .execute()
        .expect("query after rollback must run")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect post-rollback batches");
    assert_eq!(batches[0].num_rows(), 2);
}

/// TEST (wire): a manual transaction that begins with a query runs **every** query of the
/// transaction on ONE shared multi-use read-only transaction — the first statement begins it
/// inline (a read-only `transaction.begin` selector on the wire), and later statements, from a
/// *different* statement handle on the same connection, reference the begun transaction by id.
/// While it is active, DML is rejected with `InvalidState` (kind-mixing); `commit` ends it
/// **without any RPC** (a Spanner read-only transaction needs no commit — anything else would
/// hit the unscripted `Commit`/`Rollback` catch-alls and fail loudly); and the next transaction
/// is fresh, so DML buffers again. (DDL is not covered here: it is not transaction-aware — it
/// always runs immediately through the admin API, which this data-plane mock does not serve.)
#[test]
fn manual_transaction_queries_share_one_read_only_transaction() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "manual_transaction_queries_share_one_read_only_transaction",
    );

    let selectors: Arc<Mutex<Vec<Option<v1::TransactionSelector>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let server = {
        let record = selectors.clone();
        MockServer::start(move |mock| {
            serve_streaming_sql_begin_aware(mock, &["v1"], Some(record));
        })
    };

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");

    // Two queries on two different statement handles of the same connection.
    for _ in 0..2 {
        let mut query = connection.new_statement().expect("new statement");
        query.set_sql_query("SELECT c FROM MockTable").unwrap();
        let batches: Vec<_> = query
            .execute()
            .expect("manual-transaction query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect batches");
        assert_eq!(batches[0].num_rows(), 1);
    }

    {
        let seen = selectors.lock().unwrap();
        assert_eq!(seen.len(), 2, "both queries must reach the wire");
        // The first query begins the transaction inline, and it must be read-only.
        let first = seen[0]
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .expect("the first query must carry a transaction selector");
        let v1::transaction_selector::Selector::Begin(options) = first else {
            panic!("the first manual-mode query must begin the transaction, got: {first:?}");
        };
        assert!(
            matches!(
                options.mode,
                Some(v1::transaction_options::Mode::ReadOnly(_))
            ),
            "the begun transaction must be read-only: {options:?}"
        );
        // The second query — a different ADBC statement — reuses it by id: one shared snapshot.
        let second = seen[1]
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .expect("the second query must carry a transaction selector");
        assert_eq!(
            second,
            &v1::transaction_selector::Selector::Id(b"manual-ro-txn".to_vec()),
            "later queries must reuse the begun read-only transaction"
        );
    }

    // DML in a query transaction is rejected — the write would commit in a separate read/write
    // transaction, invisible to the snapshot the reads observed.
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO MockTable (c) VALUES ('x')")
        .unwrap();
    let Err(error) = insert.execute_update() else {
        panic!("DML in a manual transaction that began with a query must be rejected");
    };
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert!(
        error.message.contains("began with a query"),
        "the rejection should name the active kind: {:?}",
        error.message
    );

    // Commit ends the read-only transaction locally: no RPC (unscripted Commit would fail).
    connection
        .commit()
        .expect("committing a query transaction needs no RPC");

    // The next transaction is fresh: DML buffers again (returns None, no RPC).
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO MockTable (c) VALUES ('x')")
        .unwrap();
    assert_eq!(
        insert
            .execute_update()
            .expect("fresh transaction buffers DML"),
        None,
        "after commit the connection must accept a DML-kind transaction again"
    );
    connection.rollback().expect("discard the buffered insert");
}

/// COR-3 regression, autocommit half: `execute_update` on SQL that is neither DDL nor DML (a
/// SELECT) must execute it through the **read-only query** machinery — `ExecuteStreamingSql`, not
/// `ExecuteBatchDml` — drain and discard the rows, and report no count (`None`). `adbc.h`
/// sanctions running any statement without expecting a result set (`ExecuteQuery` with a NULL
/// out-stream), which is exactly the call that lands here.
///
/// The mock scripts only `ExecuteStreamingSql`; the old mis-routing to the DML pipeline would hit
/// the unscripted `ExecuteBatchDml` catch-all (`UNIMPLEMENTED`) and fail loudly. Bound parameters
/// must ride the same path: a bound SELECT runs the bound-query machinery (with the parameter
/// attached), drained and discarded the same way.
#[test]
fn execute_update_routes_a_query_to_the_read_only_path() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "execute_update_routes_a_query_to_the_read_only_path",
    );

    // Record every ExecuteStreamingSql request (SQL + params) the mock receives.
    type SeenQuery = (String, Vec<String>);
    let seen: Arc<Mutex<Vec<SeenQuery>>> = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                let request = request.into_inner();
                let params = request
                    .params
                    .map(|p| p.fields.into_keys().collect::<Vec<_>>())
                    .unwrap_or_default();
                record.lock().unwrap().push((request.sql, params));
                Ok(stream_of(vec![Ok(partial_result_set(
                    true,
                    &["v1", "v2"],
                    b"cor3-1",
                    true,
                ))]))
            });
    });

    let mut connection = server.connect();

    // Plain SELECT: executes as a read-only query, rows drained and discarded, count unknown.
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    assert_eq!(
        statement
            .execute_update()
            .expect("execute_update on a SELECT must run it as a read-only query"),
        None,
        "a read query has no affected-row count"
    );

    // Bound parameters on a SELECT ride the same (bound-query) read path, drained and discarded.
    let mut bound = connection.new_statement().expect("new statement");
    bound
        .set_sql_query("SELECT c FROM MockTable WHERE c = @p")
        .unwrap();
    let param_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)]));
    let param_batch =
        RecordBatch::try_new(param_schema, vec![Arc::new(StringArray::from(vec!["v1"]))])
            .expect("build bound batch");
    bound.bind(param_batch).expect("bind parameter row");
    assert_eq!(
        bound
            .execute_update()
            .expect("execute_update on a bound SELECT must run the bound-query path"),
        None
    );

    let seen = seen.lock().unwrap();
    assert_eq!(
        seen.iter().map(|(sql, _)| sql.as_str()).collect::<Vec<_>>(),
        vec![
            "SELECT c FROM MockTable",
            "SELECT c FROM MockTable WHERE c = @p",
        ],
        "both statements must arrive via ExecuteStreamingSql (never ExecuteBatchDml)"
    );
    assert_eq!(
        seen[1].1,
        vec!["p".to_string()],
        "the bound row must be attached as the @p parameter"
    );
}

/// SPEC-3 regression: `execute_partitions` must reject more than one bound parameter row with
/// `InvalidArguments` **before any RPC** — partitioned execution has no per-row fan-out, and the
/// old behaviour silently truncated the bound data to row 0 — and it must consume the bound rows
/// however the call ends (the DML-path convention), so a reused statement handle never silently
/// re-applies stale rows to a later, unrelated execution.
#[test]
fn execute_partitions_rejects_multiple_bound_rows_and_consumes_them() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "execute_partitions_rejects_multiple_bound_rows_and_consumes_them",
    );

    // Nothing is scripted: any RPC would hit the UNIMPLEMENTED catch-alls and fail with a
    // different status/message, so the InvalidArguments asserted below proves the rejection
    // happens driver-side, before anything reaches the server.
    let server = MockServer::start(|_| {});
    let mut connection = server.connect();

    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT 1").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)]));
    let two_rows = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
        .expect("build bound batch");
    statement.bind(two_rows).expect("bind two parameter rows");

    let Err(error) = statement.execute_partitions() else {
        panic!("execute_partitions with two bound rows must be rejected, not truncated to row 0");
    };
    assert_eq!(error.status, AdbcStatus::InvalidArguments);
    assert!(
        error
            .message
            .contains("at most one bound parameter row, but 2 rows"),
        "the error must name the limitation and the row count: {}",
        error.message
    );

    // The failed attempt consumed the bound rows (the DML-path convention). With no bound data,
    // `get_parameter_schema` falls back to the SQL's `@name` references — none in `SELECT 1`, so
    // an empty schema (and no RPC) — instead of reflecting the stale bound batch's schema.
    let params = statement.get_parameter_schema().expect("parameter schema");
    assert_eq!(
        params.fields().len(),
        0,
        "bound rows must not survive execute_partitions on a reused statement handle"
    );
}

/// COR-3 regression, manual-mode half: in a manual transaction `execute_update` on a SELECT must
/// run it immediately as a read-only query (on the transaction's shared multi-use read-only
/// transaction, which it begins) and buffer **nothing** to commit — the old mis-routing buffered
/// the SELECT as pending "DML", which poisoned the eventual `ExecuteBatchDml` commit (only
/// `rollback` recovered). Also covered here:
///
/// - a mixed `;`-batch (`DELETE …; SELECT 1`) is rejected up front with `InvalidArguments`,
///   before anything is buffered;
/// - with real DML buffered, `execute_update` on a SELECT hits the kind guard (`InvalidState`,
///   read-your-writes rationale) exactly like `execute`.
///
/// The proof nothing was buffered: `commit()` succeeds without any transaction RPC — the mock
/// scripts only `ExecuteStreamingSql`, so a commit that tried to apply a buffered statement would
/// hit the unscripted `BeginTransaction`/`ExecuteBatchDml`/`Commit` catch-alls and fail. (A
/// query-kind transaction's commit is a pure no-op on the wire: a Spanner read-only transaction
/// needs no commit RPC.)
#[test]
fn execute_update_query_in_manual_mode_buffers_nothing() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "execute_update_query_in_manual_mode_buffers_nothing",
    );

    let server = MockServer::start(move |mock| {
        // Served for the immediate read-only queries (the manual-mode one begins the shared
        // read-only transaction inline); nothing else is scripted.
        serve_streaming_sql_begin_aware(mock, &["v1"], None);
    });

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");

    // A SELECT through execute_update runs immediately (read-only) and buffers nothing.
    let mut query = connection.new_statement().expect("new statement");
    query.set_sql_query("SELECT c FROM MockTable").unwrap();
    assert_eq!(
        query.execute_update().expect("query must run immediately"),
        None
    );

    // A mixed batch is rejected up front with InvalidArguments — and buffers nothing either.
    let mut mixed = connection.new_statement().expect("new statement");
    mixed
        .set_sql_query("DELETE FROM MockTable WHERE true; SELECT 1")
        .unwrap();
    let Err(error) = mixed.execute_update() else {
        panic!("a mixed DML/query batch must be rejected");
    };
    assert_eq!(
        error.status,
        AdbcStatus::InvalidArguments,
        "mixed batch must fail with InvalidArguments: {:?}",
        error.message
    );
    assert!(
        error.message.contains("all-DML"),
        "the error should explain the all-DML batch requirement: {:?}",
        error.message
    );

    // Nothing was buffered by either statement: the commit applies an empty batch, which needs no
    // RPC at all — any buffered statement would hit an unscripted RPC and fail this commit.
    connection
        .commit()
        .expect("commit must succeed with an empty buffer (nothing may have been buffered)");

    // With real DML buffered, a SELECT through execute_update hits the read-your-writes guard.
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO MockTable (c) VALUES ('x')")
        .unwrap();
    assert_eq!(insert.execute_update().expect("buffered insert"), None);
    let mut guarded = connection.new_statement().expect("new statement");
    guarded.set_sql_query("SELECT c FROM MockTable").unwrap();
    let Err(error) = guarded.execute_update() else {
        panic!("execute_update on a query while DML is buffered must be rejected");
    };
    assert_eq!(
        error.status,
        AdbcStatus::InvalidState,
        "the read-your-writes guard must fire on execute_update's query arm too"
    );
    assert!(
        error.message.contains("read-your-writes"),
        "the error should explain the read-your-writes hazard: {:?}",
        error.message
    );
    connection.rollback().expect("discard the buffered insert");
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
/// The mock attaches the `RetryInfo` detail Spanner really sends; that the detail itself reaches
/// `Error::details` is asserted by [`aborted_retry_info_detail_reaches_adbc_error_details`].
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

/// (a″) `PERMISSION_DENIED` on `ExecuteStreamingSql` (as a caller lacking a Spanner IAM read
/// permission gets from real Spanner — the emulator never enforces IAM, so this is the only way to
/// exercise the path). The driver must surface a clean ADBC error whose message keeps the server's
/// text (which already names the missing `spanner.databases.select` permission) *and* gains the
/// fixed IAM guidance `src/error.rs`'s `PERMISSION_DENIED_GUIDANCE` appends: a generic "grant a role
/// that includes it" hint plus the IAM doc link (no permission re-parsing, no role lookup, and — for
/// BigQuery-driver parity — no specific role names). The numeric gRPC code (PERMISSION_DENIED = 7)
/// survives in `vendor_code`, and the status is `Unauthorized`.
#[test]
fn permission_denied_surfaces_iam_guidance() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "permission_denied_surfaces_iam_guidance",
    );

    let server = MockServer::start(|mock| {
        mock.expect_execute_streaming_sql().returning(|_| {
            Err(tonic::Status::permission_denied(
                "Caller is missing IAM permission spanner.databases.select on resource \
                 projects/mock-project/instances/mock-instance/databases/mock-db.",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let error = statement
        .execute()
        .err()
        .expect("a PERMISSION_DENIED query must fail, not hang or succeed");

    // PERMISSION_DENIED (7) → Unauthorized, code preserved in vendor_code.
    assert_eq!(error.status, AdbcStatus::Unauthorized, "got error: {error}");
    assert_eq!(
        error.vendor_code, 7,
        "vendor_code must carry PERMISSION_DENIED = 7; got error: {error}"
    );
    // The server's original message survives...
    assert!(
        error
            .message
            .contains("Caller is missing IAM permission spanner.databases.select"),
        "the server's message must survive, got: {}",
        error.message
    );
    // ...and the fixed IAM guidance is appended: a generic role hint plus the IAM doc link.
    assert!(
        error.message.contains("grant an IAM role that includes it"),
        "expected the appended IAM guidance, got: {}",
        error.message
    );
    assert!(
        error
            .message
            .contains("https://cloud.google.com/spanner/docs/iam"),
        "the guidance must link the Spanner IAM docs, got: {}",
        error.message
    );
    // No specific role is named (BigQuery-driver parity).
    assert!(
        !error.message.contains("roles/spanner."),
        "guidance must name no specific IAM role, got: {}",
        error.message
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

/// (c′) The timeout twin of the silent-stream cancel test above, on the **DDL** path:
/// `spanner.rpc.timeout_seconds.update` bounds `run_ddl` — the admin `UpdateDatabaseDdl` call and
/// its long-running-operation poll loop, which sit inside the same `with_timeout`, so bounding the
/// wrapper bounds the poll loop that used to run unbounded.
///
/// Do not "simplify" this back to racing a tiny deadline against a real emulator DDL (TEST-12):
/// tokio's timer wheel has ~1ms granularity, so a sub-millisecond deadline fires up to ~1ms late,
/// while the emulator answers DDL over a warm local connection in well under that — whichever won
/// decided the result. Here there is nothing to race. The admin endpoint is a black hole ([`GatedEndpoint`]), so the
/// DDL can never complete and the driver's deadline is the *only* way the call can return quickly
/// — the deadline's value affects how long the test takes, not whether it passes. Unwiring the
/// update timeout from `run_ddl` fails this test on the status assertion: the request then falls
/// through to the admin client's *own* ~60s default request deadline and surfaces as `Internal`,
/// not `Timeout` (the `Watchdog` is only a backstop for a genuine hang).
#[test]
fn ddl_update_timeout_fires_on_a_silent_admin_endpoint() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "ddl_update_timeout_fires_on_a_silent_admin_endpoint",
    );
    // No RPC is scripted: the DDL path never touches the data plane, and `CreateSession` (served
    // by the harness) is the only thing that must get through before the gate closes.
    let server = MockServer::start(|_mock| {});
    let gate = GatedEndpoint::start(&server.endpoint);
    let mut connection = gate.connect();
    // From here on nothing can ever answer the admin client.
    gate.go_silent();

    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_RPC_TIMEOUT_UPDATE.into()),
            OptionValue::Double(0.25),
        )
        .expect("set the update deadline");
    statement
        .set_sql_query("CREATE TABLE T (Id INT64) PRIMARY KEY (Id)")
        .unwrap();

    let started = Instant::now();
    let error = statement
        .execute_update()
        .expect_err("an unanswerable DDL must expire its update deadline");
    let elapsed = started.elapsed();

    assert_eq!(error.status, AdbcStatus::Timeout, "got error: {error}");
    assert!(
        error
            .message
            .contains(adbc_spanner::OPTION_RPC_TIMEOUT_UPDATE),
        "the DDL timeout error must name the update option: {}",
        error.message
    );
    // The deadline — not some unrelated transport failure racing it — is what ended the call: a
    // connection-refused or reset would come back in single-digit milliseconds.
    assert!(
        elapsed >= Duration::from_millis(250),
        "returned in {elapsed:?}, before the 0.25s deadline could fire — the DDL failed for some \
         other reason"
    );
}

/// A new operation on the statement must not **un-cancel** a live streamed reader from an earlier
/// `execute`. With the old shared resettable signal, the new operation's `reset()` cleared the
/// latch a `cancel()` had set between two chunk fetches, so the old reader either resumed
/// streaming or — if the prefetch task had already exited with a chunk still buffered — yielded
/// that chunk and then a clean end-of-stream: a silently **truncated** result. With per-operation
/// signals the old reader keeps its own latched signal forever, so its next fetch must fail with
/// `Status::Cancelled`, while the new operation (on a fresh signal) runs to completion.
#[test]
fn new_operation_does_not_uncancel_an_earlier_streamed_reader() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "new_operation_does_not_uncancel_an_earlier_streamed_reader",
    );

    // Every query gets the same complete three-row stream; with one row per batch the first
    // reader has fetches outstanding when the cancel lands (the prefetch buffers row 2 and is
    // fetching/holding row 3 — exactly the full-channel shape of the truncation failure mode).
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql().returning(move |_| {
            Ok(stream_of(vec![Ok(partial_result_set(
                true,
                &["v1", "v2", "v3"],
                b"rt-1",
                true,
            ))]))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(1),
        )
        .expect("set rows_per_batch");
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    let mut old_reader = statement.execute().expect("first execute");

    // The first batch was fetched by `execute` itself (it settles the schema) — consume it.
    let first = old_reader
        .next()
        .expect("first batch exists")
        .expect("first batch is clean");
    assert_eq!(first.num_rows(), 1);

    // Cancel between two chunk fetches of the old reader, then start a NEW operation on the same
    // statement before the old reader observes the cancel.
    statement.cancel().expect("cancel");
    let new_batches: Vec<_> = statement
        .execute()
        .expect("a new operation after a cancel must start uncancelled")
        .collect::<Result<Vec<_>, _>>()
        .expect("the new operation's reader must stream to completion");
    assert_eq!(
        new_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        3,
        "the new reader must deliver the full result"
    );

    // The old reader must surface the cancel — not resume streaming (row 2 as a clean batch) and
    // not end cleanly truncated (`None`).
    let item = old_reader
        .next()
        .expect("the cancelled reader must yield an error item, not a clean end of stream");
    let error = item.expect_err("the cancelled reader must not resume streaming rows");
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
    // And the cancel is sticky for the old reader: iteration stays ended (no stale row 3).
    assert!(
        old_reader.next().is_none(),
        "a cancelled reader must not yield further batches"
    );
}

/// (d) **Self-healing bulk ingest.** An autocommit ingest chunk the driver sized as "safe"
/// (rows × columns under its 20k budget) can still overshoot Spanner's *true* per-commit mutation
/// cap once invisible secondary-index entries are counted. The mock rejects any `Commit` carrying
/// more than 40 mutations with the real "too many mutations" `INVALID_ARGUMENT`, and accepts the
/// rest. The driver must react by **bisecting** the failing chunk and retrying its halves down to a
/// size the server accepts — so all 100 rows land, the returned count is exact, and the server saw
/// more than one `Commit` (the retries with smaller batches).
#[test]
fn ingest_bisects_a_chunk_that_overshoots_the_mutation_limit() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "ingest_bisects_a_chunk_that_overshoots_the_mutation_limit",
    );

    // Every scripted commit records the mutation count it saw and fails-big / succeeds-small on it,
    // so the outcome is decided by chunk *size*, not call order — deterministic under the bisect.
    let commit_sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let sizes_in_mock = commit_sizes.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        mock.expect_commit().returning(move |request| {
            let mutations = request.into_inner().mutations.len();
            sizes_in_mock.lock().unwrap().push(mutations);
            if mutations > 40 {
                Err(too_many_mutations_status())
            } else {
                commit_ok()
            }
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    // `append` mode inserts into a pre-existing table, so no admin/DDL client is built (the mock
    // serves only data-plane RPCs).
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set ingest mode append");
    statement.bind(ingest_batch(100)).expect("bind ingest data");

    let count = statement
        .execute_update()
        .expect("the ingest must self-heal, not fail on the over-limit chunk");

    assert_eq!(
        count,
        Some(100),
        "every bound row must land once the chunk is bisected under the limit"
    );

    let sizes = commit_sizes.lock().unwrap();
    // The single 100-row chunk overshoots (>40) and is bisected: 100 → 50 → 25. So the server saw
    // several commits, the last (accepted) ones each carried at most 40 mutations, and the accepted
    // commits sum to exactly the 100 rows.
    assert!(
        sizes.len() > 1,
        "the driver must have retried with smaller commits; saw commit sizes {sizes:?}"
    );
    let accepted: usize = sizes.iter().copied().filter(|&n| n <= 40).sum();
    assert_eq!(
        accepted, 100,
        "the accepted (<=40-mutation) commits must cover all 100 rows; saw {sizes:?}"
    );
    assert!(
        sizes.iter().any(|&n| n > 40),
        "the test must actually exercise an over-limit commit; saw {sizes:?}"
    );
}

/// (d′) The negative companion: a `Commit` failure that is **not** the mutation-limit rejection
/// must propagate unchanged — no bisecting. The mock fails every commit with `ALREADY_EXISTS` (a
/// duplicate primary key), and the driver must surface that status after exactly **one** commit,
/// with the append remap naming the target table. Proves the bisect predicate is narrow: only the
/// specific "too many mutations" error triggers a retry.
#[test]
fn ingest_does_not_bisect_a_non_mutation_limit_error() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "ingest_does_not_bisect_a_non_mutation_limit_error",
    );

    let commits = Arc::new(AtomicUsize::new(0));
    let commits_in_mock = commits.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        mock.expect_commit().returning(move |_| {
            commits_in_mock.fetch_add(1, Ordering::SeqCst);
            Err(tonic::Status::already_exists(
                "Row [v0] in table MockTable already exists",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set ingest mode append");
    statement.bind(ingest_batch(100)).expect("bind ingest data");

    let error = statement
        .execute_update()
        .expect_err("a duplicate-key commit must fail the ingest, not be bisected away");

    // AlreadyExists propagates (remapped by the append contract to name the table), and there was
    // exactly one commit attempt — the driver did not split-and-retry a non-limit error.
    assert_eq!(
        error.status,
        AdbcStatus::AlreadyExists,
        "got error: {error}"
    );
    assert!(
        error.message.contains("MockTable"),
        "the append remap should name the target table; got: {}",
        error.message
    );
    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "a non-mutation-limit error must not trigger the bisect retry"
    );
}

/// (d″) **A failed exists-probe must not mask the ingest error** (IDIO-9). When an `append` commit
/// fails with anything other than `AlreadyExists`, the driver probes `INFORMATION_SCHEMA.TABLES` to
/// choose between the contract's `NotFound` (table absent) and `AlreadyExists` (schema mismatch).
/// Here the probe *itself* fails — as it would for a principal that may write but not read
/// `INFORMATION_SCHEMA`, or during a transport blip — and the driver must surface the original
/// commit error untouched rather than the probe's. The probe only *refines* a failure the user's own
/// operation already produced, so a probe that answers nothing may not replace it with an error
/// about a metadata query the caller never issued.
///
/// The probe-call assertion keeps this honest: without it the test would pass vacuously if the
/// remap never probed at all.
#[test]
fn ingest_append_keeps_the_original_error_when_the_exists_probe_fails() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "ingest_append_keeps_the_original_error_when_the_exists_probe_fails",
    );

    let probes = Arc::new(AtomicUsize::new(0));
    let probes_in_mock = probes.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        // Fail the insert for a reason the append contract wants reinterpreted: not `AlreadyExists`
        // (annotated without probing) and not the "too many mutations" rejection (bisected instead).
        mock.expect_commit().returning(|_| {
            Err(tonic::Status::invalid_argument(
                "Invalid value for column c in table MockTable",
            ))
        });
        // The exists-probe is the only query this ingest issues — the rows ship as mutations.
        mock.expect_execute_streaming_sql().returning(move |_| {
            probes_in_mock.fetch_add(1, Ordering::SeqCst);
            Err(tonic::Status::permission_denied(
                "spanner.databases.select denied on INFORMATION_SCHEMA",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set ingest mode append");
    statement.bind(ingest_batch(1)).expect("bind ingest data");

    let error = statement
        .execute_update()
        .expect_err("the failing commit must fail the ingest");

    assert_eq!(
        probes.load(Ordering::SeqCst),
        1,
        "the append remap must have run the exists probe — otherwise this test is vacuous"
    );
    // The original commit error survives whole: status, message and vendor_code.
    assert_eq!(
        error.status,
        AdbcStatus::InvalidArguments,
        "the probe's own failure must not replace the ingest error; got: {error}"
    );
    assert!(
        error.message.contains("Invalid value for column c"),
        "the original commit error's message must survive; got: {}",
        error.message
    );
    assert!(
        !error.message.contains("INFORMATION_SCHEMA"),
        "the probe's error must not surface in the ingest error; got: {}",
        error.message
    );
    assert_eq!(
        error.vendor_code, 3,
        "the untouched original error keeps INVALID_ARGUMENT = 3; got: {error}"
    );
}

/// (d‴) **BatchWrite per-group failure.** The `spanner.ingest.batch_write` transport reports a
/// failed mutation group *in band* — a `google.rpc.Status` embedded in a streamed
/// `BatchWriteResponse`, not a gRPC trailer — so it never passes through `from_spanner`. This is the
/// only wire coverage of that second error path (`error::from_status_parts`), and it must be
/// indistinguishable from the write-only commit path's:
///
/// - the numeric code maps the same way (`ALREADY_EXISTS` → `AlreadyExists`, 6 in `vendor_code`);
/// - the ingest `append` remap still fires, naming the target table;
/// - and the status' **structured details** reach [`adbc_core::error::Error::details`] under the
///   contract `from_spanner` documents — key = lowercased proto type name, value = ProtoJSON.
///
/// The details assertion is what pins COR-8: before the fix only `code` + `message` were forwarded
/// on this path, so an `ErrorInfo` a caller needs to distinguish *why* a group failed was silently
/// dropped. Driving it through the wire also exercises the client's own
/// `prost Any → wkt::Any` decode of the embedded status, which the `src/error.rs` unit tests
/// (which build the details directly) cannot.
#[test]
fn batch_write_group_failure_forwards_status_details() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "batch_write_group_failure_forwards_status_details",
    );

    let batch_writes = Arc::new(AtomicUsize::new(0));
    let writes_in_mock = batch_writes.clone();
    let server = MockServer::start(move |mock| {
        // BatchWrite needs no BeginTransaction/Commit: the RPC owns its own writes.
        mock.expect_batch_write().returning(move |_| {
            writes_in_mock.fetch_add(1, Ordering::SeqCst);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.try_send(Ok(batch_write_group_already_exists()))
                .expect("scripted stream channel sized to fit");
            Ok(tonic::Response::new(rx))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    // `append` into a pre-existing table: no DDL, so no admin client is built (the mock serves only
    // data-plane RPCs). The AlreadyExists remap keeps the status and just names the table.
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set ingest mode append");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_INGEST_BATCH_WRITE.into()),
            OptionValue::String("true".into()),
        )
        .expect("route the ingest through BatchWrite");
    // One row ⇒ one MutationGroup ⇒ the single failing group the mock scripts.
    statement.bind(ingest_batch(1)).expect("bind ingest data");

    let error = statement
        .execute_update()
        .expect_err("a failed mutation group must fail the ingest");

    assert_eq!(
        batch_writes.load(Ordering::SeqCst),
        1,
        "the ingest must have gone through the BatchWrite RPC, not a write-only Commit"
    );
    // The in-band group status maps exactly as the commit path's gRPC status would.
    assert_eq!(
        error.status,
        AdbcStatus::AlreadyExists,
        "got error: {error}"
    );
    assert_eq!(
        error.vendor_code, 6,
        "vendor_code must carry ALREADY_EXISTS = 6; got error: {error}"
    );
    assert!(
        error.message.contains("MockTable"),
        "the append remap should name the target table; got: {}",
        error.message
    );

    // COR-8: the group status' details survive the whole stack — the client's decode of the
    // embedded `google.rpc.Status`, `from_status_parts`, and the ingest append remap.
    let details = error.details.as_ref().unwrap_or_else(|| {
        panic!(
            "the failing group carried a google.rpc.ErrorInfo detail; Error.details must be Some"
        )
    });
    let (_, value) = details
        .iter()
        .find(|(key, _)| key == "google.rpc.errorinfo")
        .unwrap_or_else(|| {
            panic!(
                "expected a `google.rpc.errorinfo` detail, got keys: {:?}",
                details.iter().map(|(k, _)| k).collect::<Vec<_>>()
            )
        });
    let json: serde_json::Value =
        serde_json::from_slice(value).expect("the detail value is UTF-8 ProtoJSON");
    assert_eq!(
        json["@type"], "type.googleapis.com/google.rpc.ErrorInfo",
        "the detail must self-describe as an ErrorInfo, got: {json}"
    );
    assert_eq!(
        json["reason"], "DUPLICATE_KEY",
        "the ErrorInfo's reason must round-trip what the mock sent, got: {json}"
    );
    assert_eq!(
        json["domain"], "spanner.googleapis.com",
        "the ErrorInfo's domain must round-trip what the mock sent, got: {json}"
    );
}

/// (d⁗) **BatchWrite same-chunk applied rows (COR-5).** BatchWrite applies mutation groups
/// non-atomically, so groups *before* a failing one within the same chunk stay committed. The error
/// annotation must therefore report those same-chunk applied rows — not just whole earlier chunks —
/// so the caller learns the true table state. The mock streams two OK groups then a failing one in a
/// single chunk; before the fix `applied` was discarded and the count would be zero (no annotation).
#[test]
fn batch_write_folds_same_chunk_applied_rows_into_committed_count() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "batch_write_folds_same_chunk_applied_rows_into_committed_count",
    );

    let server = MockServer::start(move |mock| {
        mock.expect_batch_write().returning(move |_| {
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            // Two groups apply, then a third fails — all within this one chunk.
            tx.try_send(Ok(batch_write_groups_ok(vec![0, 1])))
                .expect("scripted stream channel sized to fit");
            tx.try_send(Ok(batch_write_group_already_exists()))
                .expect("scripted stream channel sized to fit");
            Ok(tonic::Response::new(rx))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set ingest mode append");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_INGEST_BATCH_WRITE.into()),
            OptionValue::String("true".into()),
        )
        .expect("route the ingest through BatchWrite");
    // Three rows ⇒ three MutationGroups in one chunk; the mock scripts two applied + one failed.
    statement.bind(ingest_batch(3)).expect("bind ingest data");

    let error = statement
        .execute_update()
        .expect_err("a failed mutation group must fail the ingest");

    // The underlying failure survives the annotation (status + remap naming the table).
    assert_eq!(
        error.status,
        AdbcStatus::AlreadyExists,
        "got error: {error}"
    );
    assert!(
        error.message.contains("MockTable"),
        "the append remap should name the target table; got: {}",
        error.message
    );
    // COR-5: the two groups that applied within this same chunk are folded into the count.
    assert!(
        error
            .message
            .contains("2 row(s) from this bulk ingest were already committed"),
        "the same-chunk applied groups must be reported; got: {}",
        error.message
    );
}

/// (d″) **Manual-mode ingest atomicity.** A manual-transaction ingest whose *later* row fails
/// Arrow→Spanner conversion (row 1 here: a Date32 far outside Spanner's
/// 0001-01-01..9999-12-31 range) must leave the transaction buffer completely untouched — not
/// buffer rows `0..k` and then error, which a later `commit` would silently apply as a partial
/// batch atomically with the rest of the transaction.
///
/// Three assertions pin the buffer state after the failed ingest, each deterministic offline:
/// 1. a query runs — a partially-buffered batch would have fixed the transaction's kind to DML,
///    and the kind-exclusive guard rejects queries in a DML transaction, so success proves the
///    buffer is empty (the query fixes the kind to queries; the test rolls back afterwards so
///    the re-ingest starts a fresh transaction);
/// 2. a re-ingest of the fixed data buffers cleanly (`None`);
/// 3. the `commit` reaches the mock with **exactly** the fixed batch's two mutations — no
///    stragglers from the failed batch.
///
/// (In `append` mode the conversion failure is remapped by the ingest-append contract: the mock's
/// unbounded query expectation also serves the remap's `table_exists` probe, whose one-row answer
/// turns the error into the contract's `AlreadyExists` with the original out-of-range message
/// folded in.)
#[test]
fn manual_ingest_conversion_failure_leaves_txn_buffer_untouched() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "manual_ingest_conversion_failure_leaves_txn_buffer_untouched",
    );

    let commit_sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let sizes_in_mock = commit_sizes.clone();
    let server = MockServer::start(move |mock| {
        // Serves both the append remap's `table_exists` probe (one row → "exists") and the
        // post-failure guard query — which begins the manual transaction's shared read-only
        // transaction inline, so the helper echoes a transaction id back; unbounded times.
        serve_streaming_sql_begin_aware(mock, &["v0"], None);
        // The manual-mode commit begins a read/write transaction and commits the buffered
        // mutations; record how many each commit carries.
        serve_begin_transaction(mock);
        mock.expect_commit().returning(move |request| {
            sizes_in_mock
                .lock()
                .unwrap()
                .push(request.into_inner().mutations.len());
            commit_ok()
        });
    });

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("enter manual transaction mode");

    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set ingest mode append");

    let date_schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
    // Row 0 converts fine (1970-01-01); row 1 is out of Spanner's DATE range, so the conversion
    // fails only after the first row has already been built.
    let poisoned = RecordBatch::try_new(
        date_schema.clone(),
        vec![Arc::new(Date32Array::from(vec![0, i32::MAX]))],
    )
    .expect("build poisoned ingest batch");
    statement.bind(poisoned).expect("bind poisoned batch");

    let error = statement
        .execute_update()
        .expect_err("an out-of-range date must fail the ingest");
    assert!(
        error.message.contains("out of range"),
        "the conversion failure must surface (folded into the append remap): {}",
        error.message
    );

    // 1. The buffer must be untouched: a partially-buffered batch would have fixed the
    //    transaction's kind to DML, and the kind-exclusive guard rejects queries in a DML
    //    transaction — so a successful query proves nothing from the failed batch was kept.
    let mut probe = connection.new_statement().expect("new statement");
    probe.set_sql_query("SELECT c FROM MockTable").unwrap();
    let batches: Vec<_> = probe
        .execute()
        .expect("a query after the failed ingest must run — nothing may be buffered")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect probe batches");
    assert_eq!(batches[0].num_rows(), 1);
    // The probe fixed the transaction's kind to queries; end it so the re-ingest below starts a
    // fresh transaction (a read-only transaction rolls back by drop, no RPC).
    connection
        .rollback()
        .expect("end the probe's query transaction");

    // 2. Re-ingest the fixed data (both dates in range); manual mode buffers it (`None`).
    let fixed = RecordBatch::try_new(date_schema, vec![Arc::new(Date32Array::from(vec![0, 1]))])
        .expect("build fixed ingest batch");
    statement.bind(fixed).expect("bind fixed batch");
    assert_eq!(
        statement
            .execute_update()
            .expect("re-ingest of fixed data must buffer"),
        None,
        "a manual-mode ingest buffers (returns None), not commits"
    );

    // 3. Commit: the mock must see exactly one commit carrying exactly the fixed batch's two
    //    mutations — any third mutation would be a straggler from the failed batch.
    connection.commit().expect("commit the fixed batch");
    assert_eq!(
        *commit_sizes.lock().unwrap(),
        [2],
        "the commit must carry only the fixed batch's rows, none from the failed batch"
    );
}

/// (d) Commit-statistics capture: with `spanner.commit_stats=true` the driver must set
/// `return_commit_stats` on the `CommitRequest` and thread the server's `mutation_count` out of the
/// `CommitResponse` into `spanner.commit_stats.mutation_count`.
///
/// This is the only **gating** coverage that asserts a *positive* mutation count: the emulator
/// returns `commit_stats = None`, so its integration test (`commit_stats_reports_mutation_count` in
/// tests/integration.rs) can only assert a positive count against real Spanner — a non-gating,
/// nightly path. A regression that stopped threading the count would pass every gating check but
/// this one.
///
/// An `append` bulk ingest is the cleanest committing operation to script here: its autocommit
/// write-only transaction is a plain `BeginTransaction` + `Commit` (no `ExecuteBatchDml` result-set
/// to script), and an append *success* never probes the target table's existence (the
/// `table_exists` probe only fires to remap an *error*). The scripted `CommitResponse` carries a
/// distinctive `mutation_count` the driver could not derive from the two ingested rows, proving it
/// reads the server's value verbatim rather than counting rows.
///
/// TEST-5 rides along here: `spanner.commit.max_delay` is the other option
/// `RequestConfig::apply_to_write_only` puts on this very `CommitRequest`, so the same scripted
/// ingest asserts it arrives as the duration it was set to. The runner commit sites — and the
/// negative (unset ⇒ no delay on the wire) — are covered by
/// [`max_commit_delay_reaches_the_wire_on_runner_commits`].
#[test]
fn commit_stats_mutation_count_is_captured_from_the_commit_response() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "commit_stats_mutation_count_is_captured_from_the_commit_response",
    );

    // A value the driver cannot infer from the two ingested rows — it must come from the server.
    const SCRIPTED_MUTATION_COUNT: i64 = 4242;

    // Every CommitRequest the mock receives, for the two commit-option wire assertions below
    // (`return_commit_stats` and `max_commit_delay`).
    let commits: Arc<Mutex<Vec<v1::CommitRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record_commits = commits.clone();
    let server = MockServer::start(move |mock| {
        // The write-only ingest transaction begins a read/write transaction...
        mock.expect_begin_transaction().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: b"mock-txn-1".to_vec(),
                ..Default::default()
            }))
        });
        // ...then commits the insert mutations. Record the request (the commit options ride it),
        // and return a CommitResponse carrying the scripted mutation count. No precommit token is
        // set, so the client's write-only path commits exactly once (no precommit-token retry).
        mock.expect_commit().returning(move |request| {
            record_commits.lock().unwrap().push(request.into_inner());
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_stats: Some(v1::commit_response::CommitStats {
                    mutation_count: SCRIPTED_MUTATION_COUNT,
                }),
                ..Default::default()
            }))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    // `append` into a (notionally) pre-existing table: a pure write-only commit, no DDL and no
    // table_exists probe on the success path.
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set append ingest mode");
    // Request commit stats: this is what makes the driver call `set_return_commit_stats(true)` and
    // capture the returned mutation count.
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_COMMIT_STATS.into()),
            OptionValue::String("true".into()),
        )
        .expect("enable commit stats");
    // TEST-5: the other commit option applied at this same write-only site — it must reach the
    // CommitRequest as a 100ms `max_commit_delay` (asserted below).
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_MAX_COMMIT_DELAY.into()),
            OptionValue::String("100ms".into()),
        )
        .expect("set the commit delay");

    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )
    .expect("build ingest batch");
    statement.bind(rows).expect("bind ingest rows");

    // The ingest reports the number of rows it applied (the chunk length, 2) — deliberately *not*
    // the server's mutation count, so the two assertions can't accidentally alias.
    assert_eq!(
        statement.execute_update().expect("append ingest"),
        Some(2),
        "the ingest reports the number of rows it applied"
    );

    {
        let commits = commits.lock().unwrap();
        assert_eq!(commits.len(), 1, "the ingest chunk commits exactly once");
        assert!(
            commits[0].return_commit_stats,
            "spanner.commit_stats=true must make the driver set return_commit_stats on the \
             CommitRequest"
        );
        // TEST-5: the delay must arrive as the duration it was set to, not merely be present.
        assert_eq!(
            commits[0].max_commit_delay,
            Some(prost_types::Duration {
                seconds: 0,
                nanos: 100_000_000,
            }),
            "spanner.commit.max_delay=100ms must reach the write-only ingest CommitRequest verbatim"
        );
    }
    // The count read back must be exactly what the server put in the CommitResponse.
    assert_eq!(
        statement
            .get_option_int(OptionStatement::Other(
                adbc_spanner::OPTION_COMMIT_STATS_MUTATION_COUNT.into()
            ))
            .expect("mutation count must be readable after a stats-bearing commit"),
        SCRIPTED_MUTATION_COUNT,
        "the driver must read back the server's mutation_count verbatim"
    );
}

/// TEST-5 (wire): `spanner.commit.max_delay` is parse/round-trip tested offline (`src/request.rs`),
/// but nothing proved the parsed duration leaves the driver. `RequestConfig::apply_to_runner` puts
/// it on the read/write **runner** commits — autocommit DML and the manual-mode commit — which
/// `commit_stats_mutation_count_is_captured_from_the_commit_response` (the write-only ingest site)
/// does not touch.
///
/// Four commits on one connection, in order, all captured off the wire:
/// 1. **Unset** (the negative that makes the rest meaningful): an autocommit DML before any
///    `spanner.commit.max_delay` is set must carry **no** `max_commit_delay` — otherwise an
///    assertion that the field is populated could never fail.
/// 2. **Inherited**: with the connection-level option at `100ms`, a statement created afterwards
///    inherits it and its autocommit DML commit carries exactly 100ms.
/// 3. **Overridden**: a statement-level `250ms` wins over the connection's 100ms.
/// 4. **Manual commit**: `Connection::commit` replays the buffered DML through the runner with the
///    *connection's* 100ms (the statement's override does not leak into it).
#[test]
fn max_commit_delay_reaches_the_wire_on_runner_commits() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "max_commit_delay_reaches_the_wire_on_runner_commits",
    );

    const DML_SQL: &str = "INSERT INTO MockTable (c) VALUES ('x')";

    /// The wire form of a `spanner.commit.max_delay` of `millis` milliseconds.
    fn delay_millis(millis: i32) -> Option<prost_types::Duration> {
        Some(prost_types::Duration {
            seconds: 0,
            nanos: millis * 1_000_000,
        })
    }

    // Every CommitRequest the server saw, in order.
    let commits: Arc<Mutex<Vec<v1::CommitRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record = commits.clone();
    let server = MockServer::start(move |mock| {
        // In case the client begins the read/write transaction explicitly rather than inline.
        serve_begin_transaction(mock);
        // Autocommit DML and the manual commit both replay through ExecuteBatchDml; echo a
        // transaction id back when the batch begins the transaction inline.
        mock.expect_execute_batch_dml().returning(move |request| {
            let request = request.into_inner();
            let inline_begin = matches!(
                request
                    .transaction
                    .as_ref()
                    .and_then(|t| t.selector.as_ref()),
                Some(v1::transaction_selector::Selector::Begin(_))
            );
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: inline_begin.then(|| v1::Transaction {
                            id: b"dml-txn".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(move |request| {
            record.lock().unwrap().push(request.into_inner());
            commit_ok()
        });
    });

    let mut connection = server.connect();

    let run_autocommit_dml = |connection: &mut SpannerConnection, delay: Option<&str>| {
        let mut statement = connection.new_statement().expect("new statement");
        if let Some(value) = delay {
            statement
                .set_option(
                    OptionStatement::Other(adbc_spanner::OPTION_MAX_COMMIT_DELAY.into()),
                    OptionValue::String(value.into()),
                )
                .expect("set statement-level commit delay");
        }
        statement.set_sql_query(DML_SQL).unwrap();
        assert_eq!(
            statement.execute_update().expect("autocommit DML"),
            Some(1),
            "the scripted DML affects one row"
        );
    };

    // 1. The negative: no commit delay configured anywhere yet.
    run_autocommit_dml(&mut connection, None);

    // 2. Statements created after this inherit the connection-level delay.
    connection
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_MAX_COMMIT_DELAY.into()),
            OptionValue::String("100ms".into()),
        )
        .expect("set connection-level commit delay");
    run_autocommit_dml(&mut connection, None);

    // 3. ...and may override it.
    run_autocommit_dml(&mut connection, Some("250ms"));

    // 4. The manual-mode commit uses the connection's own config.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("enter manual transaction mode");
    let mut buffered = connection.new_statement().expect("new statement");
    buffered.set_sql_query(DML_SQL).unwrap();
    assert_eq!(
        buffered.execute_update().expect("manual-mode DML buffers"),
        None,
        "manual-mode DML buffers (returns None), not commits"
    );
    connection.commit().expect("commit the buffered DML");

    let commits = commits.lock().unwrap();
    assert_eq!(
        commits.len(),
        4,
        "one commit per transaction: three autocommit DMLs, then the manual commit"
    );
    assert_eq!(
        commits[0].max_commit_delay, None,
        "with spanner.commit.max_delay unset, the driver must send no max_commit_delay at all"
    );
    assert_eq!(
        commits[1].max_commit_delay,
        delay_millis(100),
        "an autocommit DML must commit with the connection's inherited 100ms delay"
    );
    assert_eq!(
        commits[2].max_commit_delay,
        delay_millis(250),
        "the statement-level 250ms must override the connection's 100ms"
    );
    assert_eq!(
        commits[3].max_commit_delay,
        delay_millis(100),
        "the manual-mode commit must carry the connection's 100ms delay"
    );
}

/// TEST-3 (wire): the standard `adbc.connection.transaction.isolation_level` option is parse- and
/// round-trip-tested offline (`src/connection.rs`), but nothing proved a level — least of all a
/// **promoted** one — ever leaves the driver. `apply_isolation` puts the level on the read/write
/// runner, whose transaction the client begins *inline*, so it rides the `TransactionOptions` of the
/// `ExecuteBatchDml` request's `transaction.begin` selector — which is what this test captures off
/// an autocommit DML, one per case, on one connection, in this order:
///
/// 1. **Unset** (the negative that makes the rest meaningful): with no isolation level set, the
///    begin must carry `ISOLATION_LEVEL_UNSPECIFIED` — the database default stands. Without it, an
///    assertion that a level is populated could never fail.
/// 2. **Natively supported**: `serializable`, `repeatable_read` and `snapshot` map 1:1 onto
///    Spanner's own two levels — `snapshot` included, because Spanner implements `REPEATABLE_READ`
///    *as* snapshot isolation (SPEC-7), so it is an exact match rather than a promotion.
/// 3. **Promoted** (SPEC-4, the deliberate deviation worth pinning): each of the three spec levels
///    Spanner does not expose arrives as the weakest supported level that satisfies it —
///    `read_uncommitted`/`read_committed` → `REPEATABLE_READ`, `linearizable` → `SERIALIZABLE`.
///    Nothing on the wire is ever an unsupported level, and nothing is dropped.
/// 4. **Back to `default`**: it resets the connection, so the begin is unspecified again.
///
/// Every case asserts the exact wire enum value, not mere presence.
#[test]
fn isolation_level_reaches_transaction_options_on_the_begin() {
    use adbc_core::constants::*;
    use v1::transaction_options::IsolationLevel;

    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "isolation_level_reaches_transaction_options_on_the_begin",
    );

    const DML_SQL: &str = "INSERT INTO MockTable (c) VALUES ('x')";

    // Every ExecuteBatchDml the server saw, in order — the isolation level rides its inline begin.
    let batches: Arc<Mutex<Vec<v1::ExecuteBatchDmlRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record = batches.clone();
    let server = MockServer::start(move |mock| {
        // In case the client begins the read/write transaction explicitly rather than inline.
        serve_begin_transaction(mock);
        mock.expect_execute_batch_dml().returning(move |request| {
            let request = request.into_inner();
            let inline_begin = matches!(
                request
                    .transaction
                    .as_ref()
                    .and_then(|t| t.selector.as_ref()),
                Some(v1::transaction_selector::Selector::Begin(_))
            );
            record.lock().unwrap().push(request);
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: inline_begin.then(|| v1::Transaction {
                            id: b"dml-txn".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(|_| commit_ok());
    });

    // The requested ADBC level (`None` = never set) and the level its transaction must begin with.
    let cases: [(Option<&str>, IsolationLevel); 8] = [
        (None, IsolationLevel::Unspecified),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE),
            IsolationLevel::Serializable,
        ),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ),
            IsolationLevel::RepeatableRead,
        ),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED),
            IsolationLevel::RepeatableRead,
        ),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED),
            IsolationLevel::RepeatableRead,
        ),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT),
            IsolationLevel::RepeatableRead,
        ),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE),
            IsolationLevel::Serializable,
        ),
        (
            Some(ADBC_OPTION_ISOLATION_LEVEL_DEFAULT),
            IsolationLevel::Unspecified,
        ),
    ];

    let mut connection = server.connect();
    for (requested, _) in &cases {
        if let Some(level) = requested {
            connection
                .set_option(
                    OptionConnection::IsolationLevel,
                    OptionValue::String((*level).into()),
                )
                .expect("set the connection's isolation level");
        }
        // Statements inherit the connection's isolation level at creation.
        let mut statement = connection.new_statement().expect("new statement");
        statement.set_sql_query(DML_SQL).unwrap();
        assert_eq!(
            statement.execute_update().expect("autocommit DML"),
            Some(1),
            "the scripted DML affects one row"
        );
    }

    let batches = batches.lock().unwrap();
    assert_eq!(
        batches.len(),
        cases.len(),
        "one ExecuteBatchDml per autocommit DML"
    );
    for ((requested, expected), request) in cases.iter().zip(batches.iter()) {
        let Some(v1::transaction_selector::Selector::Begin(options)) = request
            .transaction
            .as_ref()
            .and_then(|t| t.selector.as_ref())
        else {
            panic!("an autocommit DML must begin its read/write transaction inline: {request:?}");
        };
        assert_eq!(
            options.isolation_level,
            *expected as i32,
            "isolation level {requested:?} must reach TransactionOptions as {}",
            expected.as_str_name()
        );
    }
}

/// SPAN-6 regression (wire): a manual transaction that buffered **only mutations** (bulk ingests,
/// no DML) must commit through the client's replay-protected **write-only** transaction —
/// `WriteOnlyTransaction::write` begins the transaction with a `mutation_key` (the replay-
/// protection marker) and never touches `ExecuteBatchDml` — while a transaction that buffered DML
/// must keep the read/write runner (whose `ExecuteBatchDml` carries the statements and whose
/// commit applies the buffered mutations).
///
/// Two commits on one connection, split on the wire:
/// 1. **Mutations-only** (an `append` ingest): exactly one `BeginTransaction`, carrying a
///    `mutation_key`, then a `Commit` by transaction id with the ingest's two mutations — and no
///    `ExecuteBatchDml` at all (an unexpected one would also hit the unscripted-RPC catch-all).
/// 2. **DML + mutations**: `ExecuteBatchDml` runs the buffered statement (inline-beginning the
///    read/write transaction), and its `Commit` carries the buffered mutation; any explicit begin
///    on this path has no `mutation_key`.
#[test]
fn mutations_only_manual_commit_uses_the_write_only_path() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "mutations_only_manual_commit_uses_the_write_only_path",
    );

    let begins: Arc<Mutex<Vec<v1::BeginTransactionRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let commits: Arc<Mutex<Vec<v1::CommitRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let batch_dml_count = Arc::new(AtomicUsize::new(0));
    let record_begins = begins.clone();
    let record_commits = commits.clone();
    let record_batch_dml = batch_dml_count.clone();
    let server = MockServer::start(move |mock| {
        // The write-only path (and a runner electing an explicit begin) starts here; record the
        // request — the write-only begin is recognizable by its `mutation_key`.
        mock.expect_begin_transaction().returning(move |request| {
            record_begins.lock().unwrap().push(request.into_inner());
            Ok(tonic::Response::new(v1::Transaction {
                id: b"mock-txn".to_vec(),
                ..Default::default()
            }))
        });
        // The read/write runner's DML batch; echo a transaction id when it inline-begins.
        mock.expect_execute_batch_dml().returning(move |request| {
            let request = request.into_inner();
            record_batch_dml.fetch_add(1, Ordering::SeqCst);
            let inline_begin = matches!(
                request
                    .transaction
                    .as_ref()
                    .and_then(|t| t.selector.as_ref()),
                Some(v1::transaction_selector::Selector::Begin(_))
            );
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: inline_begin.then(|| v1::Transaction {
                            id: b"dml-txn".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(move |request| {
            record_commits.lock().unwrap().push(request.into_inner());
            commit_ok()
        });
    });

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("enter manual transaction mode");

    let mut ingest = connection.new_statement().expect("new statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    ingest
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set ingest mode append");

    // 1. Mutations-only: buffer a two-row ingest (`None` — manual mode buffers) and commit.
    ingest.bind(ingest_batch(2)).expect("bind ingest rows");
    assert_eq!(
        ingest.execute_update().expect("manual-mode ingest buffers"),
        None,
        "a manual-mode ingest buffers (returns None), not commits"
    );
    connection
        .commit()
        .expect("commit the mutations-only transaction");

    {
        let begins = begins.lock().unwrap();
        assert_eq!(
            begins.len(),
            1,
            "a mutations-only commit begins exactly one (write-only) transaction"
        );
        assert!(
            begins[0].mutation_key.is_some(),
            "the write-only begin must carry a mutation_key — the replay-protection marker: {:?}",
            begins[0]
        );
    }
    assert_eq!(
        batch_dml_count.load(Ordering::SeqCst),
        0,
        "a mutations-only commit must not issue ExecuteBatchDml"
    );
    {
        let commits = commits.lock().unwrap();
        assert_eq!(commits.len(), 1, "exactly one commit so far");
        assert_eq!(
            commits[0].mutations.len(),
            2,
            "the write-only commit must carry the ingest's two mutations"
        );
        assert!(
            matches!(
                commits[0].transaction,
                Some(v1::commit_request::Transaction::TransactionId(_))
            ),
            "the replay-protected write commits by transaction id (never single-use): {:?}",
            commits[0].transaction
        );
    }

    // 2. DML + mutations: buffer an UPDATE and a one-row ingest, then commit — this transaction
    //    has statements to execute, so it must keep the read/write runner.
    let mut dml = connection.new_statement().expect("new statement");
    dml.set_sql_query("UPDATE MockTable SET c = 'x' WHERE TRUE")
        .expect("set DML");
    assert_eq!(
        dml.execute_update().expect("manual-mode DML buffers"),
        None,
        "manual-mode DML buffers (returns None), not commits"
    );
    ingest.bind(ingest_batch(1)).expect("bind second ingest");
    assert_eq!(
        ingest.execute_update().expect("second ingest buffers"),
        None
    );
    connection.commit().expect("commit the DML transaction");

    assert_eq!(
        batch_dml_count.load(Ordering::SeqCst),
        1,
        "a commit with buffered DML must run it via ExecuteBatchDml (the read/write runner)"
    );
    {
        let begins = begins.lock().unwrap();
        assert!(
            begins.iter().skip(1).all(|b| b.mutation_key.is_none()),
            "only the write-only path begins with a mutation_key; the runner's begin has none"
        );
    }
    {
        let commits = commits.lock().unwrap();
        assert_eq!(commits.len(), 2, "the DML transaction adds a second commit");
        assert_eq!(
            commits[1].mutations.len(),
            1,
            "the runner's commit must carry the second ingest's buffered mutation"
        );
    }
}

/// (f) `adbc.connection.readonly` must gate the **commit** paths, not just the statement write
/// paths: DML buffered *before* the flag is set must not reach the wire through `commit()` — nor
/// through re-enabling `adbc.connection.autocommit`, which commits pending work as a side effect.
///
/// This is a wire assertion by necessity: the driver-side rejection is unit-tested in
/// `src/connection.rs` (`read_only_rejects_a_commit_that_would_write`), but only the mock proves
/// that *no* `ExecuteBatchDml`/`Commit` leaves the driver — the whole point of the guard. It also
/// pins the surrounding contract the fix must not break: the buffer stays replayable (clearing the
/// flag and committing again applies exactly it), `rollback` is never gated, and a read-only
/// connection still commits a query transaction (which writes nothing) and still runs queries.
#[test]
fn read_only_connection_rejects_the_commit_of_buffered_dml() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "read_only_connection_rejects_the_commit_of_buffered_dml",
    );

    let batch_dml: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let commits = Arc::new(AtomicUsize::new(0));
    let record_batch_dml = batch_dml.clone();
    let count_commits = commits.clone();
    let server = MockServer::start(move |mock| {
        // The read-only connection's queries (and the query transaction below, which inline-begins
        // its shared multi-use read-only transaction).
        serve_streaming_sql_begin_aware(mock, &["v0"], None);
        serve_begin_transaction(mock);
        mock.expect_execute_batch_dml().returning(move |request| {
            let request = request.into_inner();
            // The runner begins its read/write transaction inline with the batch; the client needs
            // the created transaction's id echoed back in the first result set's metadata.
            let inline_begin = matches!(
                request
                    .transaction
                    .as_ref()
                    .and_then(|t| t.selector.as_ref()),
                Some(v1::transaction_selector::Selector::Begin(_))
            );
            record_batch_dml.lock().unwrap().extend(
                request
                    .statements
                    .into_iter()
                    .map(|statement| statement.sql),
            );
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: inline_begin.then(|| v1::Transaction {
                            id: b"dml-txn".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(move |_| {
            count_commits.fetch_add(1, Ordering::SeqCst);
            commit_ok()
        });
    });

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("enter manual transaction mode");

    // 1. Buffer DML while the connection is still writable, then turn it read-only.
    let mut dml = connection.new_statement().expect("new statement");
    dml.set_sql_query("UPDATE MockTable SET c = 'x' WHERE TRUE")
        .expect("set DML");
    assert_eq!(
        dml.execute_update().expect("manual-mode DML buffers"),
        None,
        "manual-mode DML buffers (returns None), not commits"
    );
    let set_read_only = |connection: &mut SpannerConnection, value: &str| {
        connection
            .set_option(
                OptionConnection::ReadOnly,
                OptionValue::String(value.into()),
            )
            .expect("set adbc.connection.readonly");
    };
    set_read_only(&mut connection, "true");

    // 2. `commit()` must be rejected, with nothing on the wire.
    let error = connection
        .commit()
        .expect_err("a read-only connection must not commit buffered DML");
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert!(
        error.message.contains("read-only"),
        "the rejection must name the read-only flag: {}",
        error.message
    );

    // 3. So must re-enabling autocommit, which would otherwise commit the pending work as a side
    //    effect — and the failure must leave the connection in manual mode, transaction intact.
    let error = connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect_err("enabling autocommit must not commit buffered DML on a read-only connection");
    assert_eq!(error.status, AdbcStatus::InvalidState);
    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .expect("read back autocommit"),
        "false",
        "a rejected toggle must leave the connection in manual mode"
    );

    assert!(
        batch_dml.lock().unwrap().is_empty(),
        "no DML may reach the wire while the connection is read-only: {:?}",
        batch_dml.lock().unwrap()
    );
    assert_eq!(
        commits.load(Ordering::SeqCst),
        0,
        "no CommitRequest may reach the wire while the connection is read-only"
    );

    // 4. The buffer stayed replayable: clearing the flag and committing applies exactly it.
    set_read_only(&mut connection, "false");
    connection
        .commit()
        .expect("a writable connection commits the still-buffered DML");
    assert_eq!(
        *batch_dml.lock().unwrap(),
        ["UPDATE MockTable SET c = 'x' WHERE TRUE"],
        "the commit must replay exactly the DML buffered before the flag was set"
    );
    assert_eq!(commits.load(Ordering::SeqCst), 1, "exactly one commit");

    // 5. `rollback` is never gated — discarding buffered work writes nothing.
    dml.execute_update().expect("buffer DML again");
    set_read_only(&mut connection, "true");
    connection
        .rollback()
        .expect("rollback must work on a read-only connection");

    // 6. A read-only connection still queries, and still commits the resulting query transaction:
    //    it applies nothing (the snapshot is simply dropped).
    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT c FROM MockTable")
        .expect("set query");
    let batches: Vec<_> = query
        .execute()
        .expect("queries still run on a read-only connection")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect query batches");
    assert_eq!(batches[0].num_rows(), 1);
    connection
        .commit()
        .expect("committing a query transaction writes nothing, so read-only must allow it");

    assert_eq!(
        *batch_dml.lock().unwrap(),
        ["UPDATE MockTable SET c = 'x' WHERE TRUE"],
        "the rolled-back DML must never reach the wire"
    );
    assert_eq!(
        commits.load(Ordering::SeqCst),
        1,
        "neither the rollback nor the query transaction's commit issues a CommitRequest"
    );
}

// ---------------------------------------------------------------------------
// Read-option wire assertions: spanner.read.staleness + spanner.directed_read
// ---------------------------------------------------------------------------
//
// Both options are exhaustively parse/round-trip tested offline (src/staleness.rs,
// src/directed_read.rs), but those unit tests never prove the parsed value leaves the driver. The
// tests below capture the actual `ExecuteSqlRequest`s the server receives — the same
// request-capture pattern as `mock_server_round_trips_a_query` — so a regression that silently
// dropped either option (serving strong reads, or ignoring the replica selection) fails gating CI.

/// `2026-07-07T00:00:00Z` (the timestamp the `read:`/`min:` staleness forms use below) as Unix
/// seconds, for asserting the wire `prost_types::Timestamp`.
const READ_TIMESTAMP_RFC3339: &str = "2026-07-07T00:00:00Z";
const READ_TIMESTAMP_UNIX: i64 = 1_783_382_400;

/// Unwrap the read-only timestamp bound out of a query's `single_use` transaction selector,
/// panicking with the actual shape when the query is not a single-use read-only transaction.
fn single_use_read_only_bound(
    selector: Option<&v1::TransactionSelector>,
) -> v1::transaction_options::read_only::TimestampBound {
    let selector = selector
        .and_then(|s| s.selector.as_ref())
        .expect("the query must carry a transaction selector");
    let v1::transaction_selector::Selector::SingleUse(options) = selector else {
        panic!("a plain autocommit query must run single-use, got: {selector:?}");
    };
    read_only_bound(options)
}

/// Unwrap the timestamp bound out of read-only `TransactionOptions`, panicking with the actual
/// shape when the options are not read-only or carry no bound.
fn read_only_bound(
    options: &v1::TransactionOptions,
) -> v1::transaction_options::read_only::TimestampBound {
    let Some(v1::transaction_options::Mode::ReadOnly(read_only)) = &options.mode else {
        panic!("expected read-only transaction options, got: {options:?}");
    };
    read_only
        .timestamp_bound
        .expect("the read-only transaction options must carry a timestamp bound")
}

/// TEST-1 (wire): `spanner.read.staleness` must reach Spanner as the matching non-strong
/// timestamp bound in the `ExecuteSqlRequest`'s single-use transaction selector — for each of the
/// four prefix forms (`exact:`/`max:`/`read:`/`min:`). Also pins the option's two levels: the
/// first query *inherits* the connection-level value, the second *overrides* it on the statement.
#[test]
fn read_staleness_reaches_the_wire_on_single_use_queries() {
    use v1::transaction_options::read_only::TimestampBound as WireBound;

    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "read_staleness_reaches_the_wire_on_single_use_queries",
    );

    // The transaction selector of every ExecuteStreamingSql request the server sees, in order.
    let selectors: Arc<Mutex<Vec<Option<v1::TransactionSelector>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let record = selectors.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                record
                    .lock()
                    .unwrap()
                    .push(request.into_inner().transaction);
                Ok(stream_of(vec![Ok(partial_result_set(
                    true,
                    &["v1"],
                    b"st-1",
                    true,
                ))]))
            });
    });

    let mut connection = server.connect();
    // Connection-level value; statements inherit it at creation (and may override it).
    connection
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_READ_STALENESS.into()),
            OptionValue::String("exact:10s".into()),
        )
        .expect("set connection-level staleness");

    let mut run_query = |staleness: Option<&str>| {
        let mut statement = connection.new_statement().expect("new statement");
        if let Some(value) = staleness {
            statement
                .set_option(
                    OptionStatement::Other(adbc_spanner::OPTION_READ_STALENESS.into()),
                    OptionValue::String(value.into()),
                )
                .expect("set statement-level staleness");
        }
        statement.set_sql_query("SELECT c FROM MockTable").unwrap();
        statement
            .execute()
            .expect("query against mock server")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect batches");
    };
    run_query(None); // inherits the connection's exact:10s
    run_query(Some("max:500ms")); // statement overrides the connection value
    run_query(Some(&format!("read:{READ_TIMESTAMP_RFC3339}")));
    run_query(Some(&format!("min:{READ_TIMESTAMP_RFC3339}")));

    let selectors = selectors.lock().unwrap();
    assert_eq!(selectors.len(), 4, "one request per staleness form");
    let bounds: Vec<WireBound> = selectors
        .iter()
        .map(|s| single_use_read_only_bound(s.as_ref()))
        .collect();
    assert_eq!(
        bounds[0],
        WireBound::ExactStaleness(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        "exact:10s (inherited from the connection) must arrive as exact_staleness"
    );
    assert_eq!(
        bounds[1],
        WireBound::MaxStaleness(prost_types::Duration {
            seconds: 0,
            nanos: 500_000_000,
        }),
        "max:500ms (the statement's override of the connection value) must arrive as max_staleness"
    );
    assert_eq!(
        bounds[2],
        WireBound::ReadTimestamp(prost_types::Timestamp {
            seconds: READ_TIMESTAMP_UNIX,
            nanos: 0,
        }),
        "read:<rfc3339> must arrive as read_timestamp"
    );
    assert_eq!(
        bounds[3],
        WireBound::MinReadTimestamp(prost_types::Timestamp {
            seconds: READ_TIMESTAMP_UNIX,
            nanos: 0,
        }),
        "min:<rfc3339> must arrive as min_read_timestamp"
    );
}

/// Run one two-row bound (parameterized) query with the given `spanner.read.staleness` against its
/// own mock server, returning the transaction selector of every `ExecuteSqlRequest` the server
/// saw. Two bound rows force the multi-use read-only transaction path; the client begins it
/// *inline*, so the first request carries `transaction.begin` (whose bound is asserted on by the
/// caller) and expects the created transaction's id back in the result metadata, which the later
/// per-row request then references by id.
fn bound_query_transaction_selectors(staleness: &str) -> Vec<Option<v1::TransactionSelector>> {
    let selectors: Arc<Mutex<Vec<Option<v1::TransactionSelector>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let record = selectors.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                let request = request.into_inner();
                let inline_begin = matches!(
                    request
                        .transaction
                        .as_ref()
                        .and_then(|t| t.selector.as_ref()),
                    Some(v1::transaction_selector::Selector::Begin(_))
                );
                record.lock().unwrap().push(request.transaction);
                let mut first = partial_result_set(true, &["v"], b"bq-1", true);
                if inline_begin {
                    first
                        .metadata
                        .as_mut()
                        .expect("first message carries metadata")
                        .transaction = Some(v1::Transaction {
                        id: b"bound-txn".to_vec(),
                        ..Default::default()
                    });
                }
                Ok(stream_of(vec![Ok(first)]))
            });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_READ_STALENESS.into()),
            OptionValue::String(staleness.into()),
        )
        .expect("set staleness");
    statement
        .set_sql_query("SELECT c FROM MockTable WHERE c = @val")
        .expect("set query");
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["a", "b"]))],
    )
    .expect("build bound batch");
    statement.bind(rows).expect("bind rows");
    let total: usize = statement
        .execute()
        .expect("bound query against mock server")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 2, "one result row per bound row ({staleness})");

    let seen = selectors.lock().unwrap().clone();
    drop(server); // shut the mock down before handing the captured selectors back
    seen
}

/// TEST-1 (wire, multi-use pinning): a bound query over several bound rows runs all its per-row
/// statements in **one** multi-use read-only transaction, and Spanner accepts the
/// bounded-staleness kinds only on single-use transactions — so the driver must pin `max:`/`min:`
/// to their most-stale legal equivalent when beginning it (`max:<d>` → exact staleness `<d>`,
/// `min:<t>` → read timestamp `<t>`; `ReadBound::pinned_for_multi_use` in src/staleness.rs).
/// Asserts the pinned bound on the wire `transaction.begin`, and that the later per-row statement
/// reuses the begun transaction by id (i.e. the bound really is shared, not re-sent).
#[test]
fn bounded_staleness_is_pinned_for_multi_use_bound_queries() {
    use v1::transaction_options::read_only::TimestampBound as WireBound;

    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "bounded_staleness_is_pinned_for_multi_use_bound_queries",
    );

    let cases = [
        (
            "max:500ms",
            WireBound::ExactStaleness(prost_types::Duration {
                seconds: 0,
                nanos: 500_000_000,
            }),
        ),
        (
            &format!("min:{READ_TIMESTAMP_RFC3339}") as &str,
            WireBound::ReadTimestamp(prost_types::Timestamp {
                seconds: READ_TIMESTAMP_UNIX,
                nanos: 0,
            }),
        ),
    ];
    for (staleness, expected_pin) in cases {
        let selectors = bound_query_transaction_selectors(staleness);
        assert_eq!(
            selectors.len(),
            2,
            "two bound rows must produce two statements ({staleness})"
        );

        // The first statement begins the multi-use transaction, carrying the *pinned* bound.
        let first = selectors[0]
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .expect("the first statement must carry a transaction selector");
        let v1::transaction_selector::Selector::Begin(options) = first else {
            panic!("the first bound-row statement must begin the transaction, got: {first:?}");
        };
        assert_eq!(
            read_only_bound(options),
            expected_pin,
            "{staleness} must be pinned to its most-stale legal multi-use equivalent"
        );

        // The second statement reuses that transaction by id — one shared snapshot for all rows.
        let second = selectors[1]
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .expect("the second statement must carry a transaction selector");
        assert_eq!(
            second,
            &v1::transaction_selector::Selector::Id(b"bound-txn".to_vec()),
            "later bound-row statements must reuse the begun transaction ({staleness})"
        );
    }
}

/// TEST-2 (wire): `spanner.directed_read` must land on `ExecuteSqlRequest.directed_read_options`
/// for read-only queries, and must NOT ride along on DML — Spanner rejects directed reads on a
/// read/write transaction with `INVALID_ARGUMENT`, so a regression here breaks every write while
/// the option is set. Plain DML goes out as `ExecuteBatchDml`, whose request proto has no
/// directed-read field at all (asserted via the RPC choice below); the DML shape that *could*
/// regress is `THEN RETURN`, which shares `ExecuteSqlRequest` with the query path inside a
/// read/write transaction — that is the negative asserted on the wire. Also pins the option's two
/// levels: the plain query *inherits* the connection-level value, the second query *overrides* it
/// on the statement.
#[test]
fn directed_read_reaches_the_wire_on_queries_but_never_on_dml() {
    use v1::directed_read_options as dro;

    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "directed_read_reaches_the_wire_on_queries_but_never_on_dml",
    );

    const QUERY_SQL: &str = "SELECT c FROM MockTable";
    const OVERRIDE_QUERY_SQL: &str = "SELECT c FROM OtherMockTable";
    const INSERT_SQL: &str = "INSERT INTO MockTable (c) VALUES ('x')";
    const RETURNING_SQL: &str = "INSERT INTO MockTable (c) VALUES ('y') THEN RETURN c";

    /// `(sql, directed_read_options)` of one `ExecuteStreamingSql` request the server saw.
    type SeenExecuteSql = (String, Option<v1::DirectedReadOptions>);
    let streaming: Arc<Mutex<Vec<SeenExecuteSql>>> = Arc::new(Mutex::new(Vec::new()));
    // Every ExecuteBatchDml request (the plain-DML path).
    let batch_dml: Arc<Mutex<Vec<v1::ExecuteBatchDmlRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record_streaming = streaming.clone();
    let record_batch_dml = batch_dml.clone();
    let server = MockServer::start(move |mock| {
        // In case the client begins the read/write transaction explicitly rather than inline.
        serve_begin_transaction(mock);
        mock.expect_execute_streaming_sql()
            .returning(move |request| {
                let request = request.into_inner();
                let inline_begin = matches!(
                    request
                        .transaction
                        .as_ref()
                        .and_then(|t| t.selector.as_ref()),
                    Some(v1::transaction_selector::Selector::Begin(_))
                );
                record_streaming
                    .lock()
                    .unwrap()
                    .push((request.sql.clone(), request.directed_read_options));
                let mut first = partial_result_set(true, &["v"], b"dr-1", true);
                if inline_begin {
                    first
                        .metadata
                        .as_mut()
                        .expect("first message carries metadata")
                        .transaction = Some(v1::Transaction {
                        id: b"dml-txn".to_vec(),
                        ..Default::default()
                    });
                }
                Ok(stream_of(vec![Ok(first)]))
            });
        mock.expect_execute_batch_dml().returning(move |request| {
            let request = request.into_inner();
            let inline_begin = matches!(
                request
                    .transaction
                    .as_ref()
                    .and_then(|t| t.selector.as_ref()),
                Some(v1::transaction_selector::Selector::Begin(_))
            );
            record_batch_dml.lock().unwrap().push(request);
            let metadata = v1::ResultSetMetadata {
                transaction: inline_begin.then(|| v1::Transaction {
                    id: b"dml-txn".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    metadata: Some(metadata),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(|_| commit_ok());
    });

    let mut connection = server.connect();
    // Connection-level value; statements inherit it at creation (and may override it).
    connection
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_DIRECTED_READ.into()),
            OptionValue::String("include:us-east1:read_only".into()),
        )
        .expect("set connection-level directed read");

    // 1. A plain query, inheriting the connection-level directed read.
    let mut query = connection.new_statement().expect("new statement");
    query.set_sql_query(QUERY_SQL).unwrap();
    query
        .execute()
        .expect("query against mock server")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");

    // 2. A query whose statement overrides the connection-level value.
    let mut override_query = connection.new_statement().expect("new statement");
    override_query
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_DIRECTED_READ.into()),
            OptionValue::String("exclude:eu-west1".into()),
        )
        .expect("set statement-level directed read");
    override_query.set_sql_query(OVERRIDE_QUERY_SQL).unwrap();
    override_query
        .execute()
        .expect("override query against mock server")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");

    // 3. Plain DML: rides ExecuteBatchDml inside a read/write transaction.
    let mut insert = connection.new_statement().expect("new statement");
    insert.set_sql_query(INSERT_SQL).unwrap();
    assert_eq!(
        insert.execute_update().expect("autocommit insert"),
        Some(1),
        "the scripted batch-DML row count must surface"
    );

    // 4. THEN RETURN DML: rides ExecuteSql inside a read/write transaction — the request shape
    //    that could regress into carrying directed reads.
    let mut returning = connection.new_statement().expect("new statement");
    returning.set_sql_query(RETURNING_SQL).unwrap();
    assert_eq!(
        returning.execute_update().expect("THEN RETURN insert"),
        Some(1),
        "the drained THEN RETURN row count must surface"
    );

    let streaming = streaming.lock().unwrap();
    let directed_for = |sql: &str| -> Option<v1::DirectedReadOptions> {
        streaming
            .iter()
            .find(|(seen, _)| seen == sql)
            .unwrap_or_else(|| {
                panic!(
                    "no ExecuteSql request seen for {sql:?}; saw: {:?}",
                    streaming.iter().map(|(seen, _)| seen).collect::<Vec<_>>()
                )
            })
            .1
            .clone()
    };

    // The plain query carries the connection's include list, replica type resolved to READ_ONLY.
    assert_eq!(
        directed_for(QUERY_SQL),
        Some(v1::DirectedReadOptions {
            replicas: Some(dro::Replicas::IncludeReplicas(dro::IncludeReplicas {
                replica_selections: vec![dro::ReplicaSelection {
                    location: "us-east1".to_string(),
                    r#type: dro::replica_selection::Type::ReadOnly as i32,
                }],
                auto_failover_disabled: false,
            })),
        }),
        "the connection-level include list must land on ExecuteSqlRequest.directed_read_options"
    );

    // The second query carries the statement's override (an exclude list), not the inherited one.
    assert_eq!(
        directed_for(OVERRIDE_QUERY_SQL),
        Some(v1::DirectedReadOptions {
            replicas: Some(dro::Replicas::ExcludeReplicas(dro::ExcludeReplicas {
                replica_selections: vec![dro::ReplicaSelection {
                    location: "eu-west1".to_string(),
                    r#type: dro::replica_selection::Type::Unspecified as i32,
                }],
            })),
        }),
        "a statement-level value must override the inherited connection-level one on the wire"
    );

    // The negative: THEN RETURN DML shares ExecuteSqlRequest with the query path but must carry
    // no directed-read options — Spanner rejects them on a read/write transaction.
    assert_eq!(
        directed_for(RETURNING_SQL),
        None,
        "DML must never carry directed-read options, even with the connection option set"
    );

    // Plain DML went out as ExecuteBatchDml (whose request proto cannot carry directed reads).
    let batch_dml = batch_dml.lock().unwrap();
    assert_eq!(
        batch_dml.len(),
        1,
        "the plain INSERT must ride a single ExecuteBatchDml"
    );
    assert_eq!(batch_dml[0].statements.len(), 1);
    assert_eq!(batch_dml[0].statements[0].sql, INSERT_SQL);
}

/// SPAN-7 (wire): every mutation-free autocommit `ExecuteBatchDml` batch is by construction the
/// transaction's *entire* content — nothing follows it before the commit — so the driver must
/// flag it as the transaction's last request (`ExecuteBatchDmlRequest.last_statements = true`)
/// for a multi-statement `;`-batch (the dbt-style `DELETE …; INSERT …` shape) just as for a
/// single statement. The negative: buffered manual-mode DML replayed at commit must go out with
/// the flag OFF — that commit may still apply buffered mutations *after* the batch executes, so
/// the batch is not the transaction's last request there.
#[test]
fn autocommit_batch_dml_is_flagged_last_statements_but_manual_commit_is_not() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "autocommit_batch_dml_is_flagged_last_statements_but_manual_commit_is_not",
    );

    const BATCH_SQL: &str =
        "DELETE FROM MockTable WHERE TRUE; INSERT INTO MockTable (c) VALUES ('x')";

    // Every ExecuteBatchDml request the server saw, in order.
    let batch_dml: Arc<Mutex<Vec<v1::ExecuteBatchDmlRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record = batch_dml.clone();
    let server = MockServer::start(move |mock| {
        // In case the client begins the read/write transaction explicitly rather than inline.
        serve_begin_transaction(mock);
        mock.expect_execute_batch_dml().returning(move |request| {
            let request = request.into_inner();
            let inline_begin = matches!(
                request
                    .transaction
                    .as_ref()
                    .and_then(|t| t.selector.as_ref()),
                Some(v1::transaction_selector::Selector::Begin(_))
            );
            let statements = request.statements.len();
            record.lock().unwrap().push(request);
            // One result set per statement (row count 1 each); the first echoes the begun
            // transaction id back when the batch began the transaction inline.
            let result_sets = (0..statements)
                .map(|i| v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: (i == 0 && inline_begin).then(|| v1::Transaction {
                            id: b"dml-txn".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .collect();
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets,
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(|_| commit_ok());
    });

    let mut connection = server.connect();

    // 1. Autocommit: a multi-statement `;`-batch runs immediately as one ExecuteBatchDml — the
    //    whole read/write transaction — and reports the summed affected-row count.
    let mut batch = connection.new_statement().expect("new statement");
    batch.set_sql_query(BATCH_SQL).unwrap();
    assert_eq!(
        batch.execute_update().expect("autocommit `;`-batch"),
        Some(2),
        "the summed batch-DML row count must surface"
    );

    // 2. Manual mode: the same `;`-batch buffers (no RPC), and commit replays it in one
    //    read/write transaction.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");
    let mut buffered = connection.new_statement().expect("new statement");
    buffered.set_sql_query(BATCH_SQL).unwrap();
    assert_eq!(
        buffered.execute_update().expect("buffered `;`-batch"),
        None,
        "DML in manual mode buffers (returns None), not commits"
    );
    connection.commit().expect("commit the buffered DML");

    let seen = batch_dml.lock().unwrap();
    assert_eq!(
        seen.len(),
        2,
        "one ExecuteBatchDml per transaction: the autocommit batch, then the manual commit"
    );
    assert_eq!(
        seen[0].statements.len(),
        2,
        "both statements ride one batch"
    );
    assert!(
        seen[0].last_statements,
        "a mutation-free autocommit `;`-batch is the transaction's entire content, so it must \
         be flagged as the transaction's last request"
    );
    assert_eq!(seen[1].statements.len(), 2, "the commit replays the buffer");
    assert!(
        !seen[1].last_statements,
        "buffered manual-mode DML replayed at commit must NOT be flagged — the commit may still \
         apply buffered mutations after the batch executes"
    );
}

/// SPAN-8 (wire): `spanner.request.priority` must reach the `ExecuteBatchDml` RPC itself — the
/// path *all* plain autocommit DML takes — and not only the transaction's commit. The client's
/// `BatchDmlBuilder` gained the priority setter upstream (UP-4,
/// googleapis/google-cloud-rust#6047); before it, the batch went out with
/// `PRIORITY_UNSPECIFIED` while the caller had asked for `low`, so Spanner scheduled the actual
/// DML work at the default priority. The request tag rides the same `RequestOptions`, so it is
/// asserted here too.
#[test]
fn batch_dml_carries_the_request_priority_and_tag() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "batch_dml_carries_the_request_priority_and_tag",
    );

    let batch_dml: Arc<Mutex<Vec<v1::ExecuteBatchDmlRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record = batch_dml.clone();
    let server = MockServer::start(move |mock| {
        serve_begin_transaction(mock);
        mock.expect_execute_batch_dml().returning(move |request| {
            record.lock().unwrap().push(request.into_inner());
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: b"dml-txn".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });
        mock.expect_commit().returning(|_| commit_ok());
    });

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_REQUEST_PRIORITY.into()),
            OptionValue::String("low".into()),
        )
        .expect("set the request priority");
    connection
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_REQUEST_TAG.into()),
            OptionValue::String("etl-batch".into()),
        )
        .expect("set the request tag");

    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_sql_query("INSERT INTO MockTable (c) VALUES ('x')")
        .unwrap();
    assert_eq!(statement.execute_update().expect("autocommit DML"), Some(1));

    let seen = batch_dml.lock().unwrap();
    assert_eq!(seen.len(), 1, "the INSERT rides a single ExecuteBatchDml");
    let options = seen[0]
        .request_options
        .as_ref()
        .expect("the batch must carry RequestOptions");
    assert_eq!(
        options.priority,
        v1::request_options::Priority::Low as i32,
        "spanner.request.priority must reach the ExecuteBatchDml request itself"
    );
    assert_eq!(options.request_tag, "etl-batch");
}

/// UP-5 (wire): the `spanner.ingest.batch_write` firehose path must carry
/// `spanner.request.priority` and `spanner.transaction.tag` on its `BatchWrite` request — the
/// client's `BatchWriteTransactionBuilder` gained those setters upstream
/// (googleapis/google-cloud-rust#6073), before which a BatchWrite ingest silently ignored both.
///
/// The negative half is the contract's other side: `spanner.request.tag` must **not** appear.
/// Spanner ignores per-request tags on BatchWrite (the reason the client exposes no setter for
/// it), so the driver deliberately drops it rather than sending a tag that does nothing.
#[test]
fn batch_write_carries_the_priority_and_transaction_tag_but_not_the_request_tag() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "batch_write_carries_the_priority_and_transaction_tag_but_not_the_request_tag",
    );

    let requests: Arc<Mutex<Vec<v1::BatchWriteRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let record = requests.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_batch_write().returning(move |request| {
            record.lock().unwrap().push(request.into_inner());
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.try_send(Ok(v1::BatchWriteResponse {
                indexes: vec![0],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
            .expect("scripted stream channel sized to fit");
            Ok(tonic::Response::new(rx))
        });
    });

    let mut connection = server.connect();
    connection
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_REQUEST_PRIORITY.into()),
            OptionValue::String("high".into()),
        )
        .expect("set the request priority");
    connection
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_TRANSACTION_TAG.into()),
            OptionValue::String("nightly-etl".into()),
        )
        .expect("set the transaction tag");
    connection
        .set_option(
            OptionConnection::Other(adbc_spanner::OPTION_REQUEST_TAG.into()),
            OptionValue::String("ignored-by-batch-write".into()),
        )
        .expect("set the request tag");

    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("MockTable".into()),
        )
        .expect("set target table");
    statement
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .expect("set ingest mode append");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_INGEST_BATCH_WRITE.into()),
            OptionValue::String("true".into()),
        )
        .expect("route the ingest through BatchWrite");
    statement.bind(ingest_batch(1)).expect("bind ingest data");
    assert_eq!(
        statement.execute_update().expect("BatchWrite ingest"),
        Some(1)
    );

    let seen = requests.lock().unwrap();
    assert_eq!(seen.len(), 1, "one chunk ⇒ one BatchWrite request");
    let options = seen[0]
        .request_options
        .as_ref()
        .expect("the BatchWrite must carry RequestOptions");
    assert_eq!(
        options.priority,
        v1::request_options::Priority::High as i32,
        "spanner.request.priority must reach the BatchWrite request"
    );
    assert_eq!(
        options.transaction_tag, "nightly-etl",
        "spanner.transaction.tag must tag the transactions the BatchWrite creates"
    );
    assert_eq!(
        options.request_tag, "",
        "spanner.request.tag must NOT be sent — Spanner ignores per-request tags on BatchWrite"
    );
    assert!(
        !seen[0].exclude_txn_from_change_streams,
        "no driver option exposes change-stream exclusion yet, so the default must go out"
    );
}

// ---------------------------------------------------------------------------
// Shared client stack (SPAN-1)
// ---------------------------------------------------------------------------

/// Like [`MockServer::start`], but with a **counting** `CreateSession` handler in place of
/// [`serve_sessions`]: every session-creation RPC increments `sessions`. Since the pinned client
/// issues exactly one `CreateSession` (its multiplexed session) when a `DatabaseClient` is built,
/// the counter counts how many client stacks were actually built. No other RPC is scripted — the
/// trailing catch-alls reject anything else.
fn start_counting_sessions(sessions: Arc<AtomicUsize>) -> MockServer {
    let mut mock = MockSpanner::new();
    mock.expect_create_session().returning(move |request| {
        sessions.fetch_add(1, Ordering::SeqCst);
        let database = request.into_inner().database;
        Ok(tonic::Response::new(v1::Session {
            name: format!(
                "{database}/sessions/mock-session-{}",
                sessions.load(Ordering::SeqCst)
            ),
            multiplexed: true,
            ..Default::default()
        }))
    });
    reject_unscripted_rpcs(&mut mock);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build mock-server runtime");
    let (endpoint, server) = runtime
        .block_on(spanner_grpc_mock::start("127.0.0.1:0", mock))
        .expect("start mock Spanner server");
    MockServer {
        endpoint,
        server,
        _runtime: runtime,
    }
}

/// **SPAN-1** — connections share one client stack. Building the Spanner client stack is
/// expensive (a 4-channel gRPC pool, credential resolution, a `CreateSession` RPC, and a
/// background session-maintenance task), and it is a per-*database* cost: the `SpannerDatabase`
/// caches the stack built for its first connection and hands cheap clones (shared session +
/// channels) to every later one. Setting **any** database option invalidates the cache, since
/// options affect the endpoint/credentials/database path the stack was built from. The
/// `CreateSession` count observed by the mock is a direct proxy for "how many stacks were built".
#[test]
fn connections_share_one_client_stack_until_an_option_is_set() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "connections_share_one_client_stack_until_an_option_is_set",
    );

    let sessions = Arc::new(AtomicUsize::new(0));
    let server = start_counting_sessions(sessions.clone());

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let mut database = driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String(format!("spanner:///{DATABASE}")),
            ),
            (
                OptionDatabase::Other(adbc_spanner::OPTION_ENDPOINT.into()),
                OptionValue::String(server.endpoint.clone()),
            ),
            (
                OptionDatabase::Other(adbc_spanner::OPTION_EMULATOR.into()),
                OptionValue::String("true".into()),
            ),
        ])
        .expect("create database");

    // The stack is lazy: merely configuring the database builds nothing.
    assert_eq!(
        sessions.load(Ordering::SeqCst),
        0,
        "no client stack may be built before the first connection"
    );

    let _first = database.new_connection().expect("first connection");
    let _second = database.new_connection().expect("second connection");
    assert_eq!(
        sessions.load(Ordering::SeqCst),
        1,
        "two connections on one database must share one client stack (one CreateSession)"
    );

    // The cached stack renders presence-only in Debug — never the client types' own Debug output.
    let rendered = format!("{database:?}");
    assert!(
        rendered.contains(r#"connected: Some("<client stack>")"#),
        "the cached stack must render presence-only: {rendered}"
    );
    assert!(
        !rendered.contains("DatabaseClient"),
        "Debug must not delegate to the client's own Debug: {rendered}"
    );

    // Setting any database option (here: re-setting the endpoint to the same value) invalidates
    // the cache, so the next connection rebuilds the stack — a second CreateSession.
    database
        .set_option(
            OptionDatabase::Other(adbc_spanner::OPTION_ENDPOINT.into()),
            OptionValue::String(server.endpoint.clone()),
        )
        .expect("re-set endpoint");
    let _third = database.new_connection().expect("third connection");
    assert_eq!(
        sessions.load(Ordering::SeqCst),
        2,
        "set_option must invalidate the cached stack (second CreateSession on the next connection)"
    );
}

// ---------------------------------------------------------------------------
// SPEC-2: `adbc.statement.exec.incremental` accepts its spec default
// ---------------------------------------------------------------------------

/// `adbc.statement.exec.incremental` at its spec default (DISABLED, `"false"`) must be an
/// accept-default no-op — generic clients (e.g. driver-manager shims) write every option's
/// default unconditionally, so rejecting the default breaks them. Enabling it (`"true"`) stays
/// `NotImplemented`, and the getter reports the effective `"false"` rather than `NotFound`
/// (the `adbc.ingest.temporary` pattern). Options are handled entirely client-side, so no RPC
/// beyond the connection's `CreateSession` is scripted.
#[test]
fn exec_incremental_spec_default_is_a_no_op() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "exec_incremental_spec_default_is_a_no_op",
    );

    let server = MockServer::start(|_| {});
    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");

    // The spec default (`false`) is accepted as a no-op.
    statement
        .set_option(
            OptionStatement::Incremental,
            OptionValue::String("false".into()),
        )
        .expect("setting adbc.statement.exec.incremental=false is a no-op");

    // The getter reports the (only possible) effective value instead of NotFound.
    assert_eq!(
        statement
            .get_option_string(OptionStatement::Incremental)
            .expect("incremental getter must report the default"),
        "false"
    );

    // Enabling incremental execution stays NotImplemented, and the message names the option.
    let error = statement
        .set_option(
            OptionStatement::Incremental,
            OptionValue::String("true".into()),
        )
        .expect_err("adbc.statement.exec.incremental=true must be rejected");
    assert_eq!(error.status, AdbcStatus::NotImplemented);
    assert!(
        error.message.contains("adbc.statement.exec.incremental"),
        "{error}"
    );
}

// ---------------------------------------------------------------------------
// Retry-limit accounting per RPC path (UP-14)
// ---------------------------------------------------------------------------

// The pinned client runs two *different* retry loops, and they account for attempts differently —
// so `spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds` do not mean the same thing
// on every path. These tests pin the exact, observed numbers on one path of each kind, so the
// asymmetry is a checked fact rather than a claim in a comment, and so a `google-cloud-rust` rev
// bump that fixes it upstream (UP-14) fails here loudly instead of silently changing what a
// caller's option means. See `src/retry.rs`'s module doc for the user-facing statement of this.
//
// - **Unary** RPCs (`ExecuteBatchDml`, `ExecuteSql`, `BeginTransaction`, `Commit`) go through gax's
//   `retry_loop`, which increments `RetryState::attempt_count` *before* each attempt and pins
//   `RetryState::start` to the real loop start. Both limits are then exact.
// - **Server-streaming** `ExecuteStreamingSql` is dispatched outside `retry_loop`; the client
//   hand-rolls stream resumption in `ResultSet::check_retry`, seeding `RetryState` with its own
//   `retry_count` (retries *so far* — 0 on the first failure) and a fresh `Instant::now()` start.
//   So the attempt limit permits one attempt too many, and the elapsed-time limit never fires.

/// How many attempts each probe's mock serves before giving up with a permanent error. A retry
/// limit that never fires stops here rather than hanging the test.
const RETRY_PROBE_CAP: usize = 20;

/// Set the retry knobs a probe shares: a constant 10ms backoff, so an attempt budget is spent in
/// milliseconds rather than the client's default 1s-doubling backoff.
fn set_constant_backoff(statement: &mut impl Statement) {
    for (key, value) in [
        (adbc_spanner::OPTION_RETRY_BACKOFF_INITIAL_SECONDS, 0.01),
        (adbc_spanner::OPTION_RETRY_BACKOFF_MULTIPLIER, 1.0),
    ] {
        statement
            .set_option(
                OptionStatement::Other(key.into()),
                OptionValue::Double(value),
            )
            .expect("set backoff knob");
    }
}

/// Count the `ExecuteStreamingSql` attempts a query makes when every attempt fails, under the
/// retry option `key` = `value`.
///
/// The fault is delivered **inside** the result stream, and that is load-bearing: the pinned client
/// dispatches server-streaming RPCs outside gax's retry loop (`server_streaming/builder.rs`'s
/// `send()` issues the RPC with no retry loop of its own), so an `UNAVAILABLE` returned as the
/// *initial* RPC status is never retried no matter what the policy says — scripting it that way
/// would make this probe vacuous. Only a status arriving mid-stream reaches `ResultSet::check_retry`
/// and therefore the configured policy.
fn streaming_attempts(key: &str, value: OptionValue) -> usize {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in_mock = calls.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_streaming_sql().returning(move |_| {
            let n = calls_in_mock.fetch_add(1, Ordering::SeqCst);
            if n >= RETRY_PROBE_CAP {
                return Err(tonic::Status::internal("mock server: probe cap reached"));
            }
            // One row, then the stream dies. Spanner sends the row type exactly once, on the first
            // message of the first attempt — a resumed stream must not repeat it.
            Ok(stream_of(vec![
                Ok(partial_result_set(n == 0, &["v1"], b"rt-1", false)),
                Err(tonic::Status::unavailable(
                    "mock server: connection lost mid-stream",
                )),
            ]))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(OptionStatement::Other(key.into()), value)
        .expect("set retry option");
    set_constant_backoff(&mut statement);
    statement.set_sql_query("SELECT c FROM MockTable").unwrap();
    statement
        .execute()
        .err()
        .expect("every attempt fails, so the query must fail");
    calls.load(Ordering::SeqCst)
}

/// Count the `ExecuteBatchDml` attempts a DML statement makes when every attempt fails with an
/// `UNAVAILABLE`, under the retry option `key` = `value`. `ExecuteBatchDml` is unary, so the
/// initial status *is* what the gax retry loop sees — the mirror image of [`streaming_attempts`].
fn unary_attempts(key: &str, value: OptionValue) -> usize {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in_mock = calls.clone();
    let server = MockServer::start(move |mock| {
        mock.expect_execute_batch_dml().returning(move |_| {
            let n = calls_in_mock.fetch_add(1, Ordering::SeqCst);
            if n >= RETRY_PROBE_CAP {
                return Err(tonic::Status::internal("mock server: probe cap reached"));
            }
            Err(tonic::Status::unavailable(
                "mock server: backend unavailable",
            ))
        });
    });

    let mut connection = server.connect();
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(OptionStatement::Other(key.into()), value)
        .expect("set retry option");
    set_constant_backoff(&mut statement);
    statement
        .set_sql_query("UPDATE MockTable SET c = 'x' WHERE TRUE")
        .unwrap();
    statement
        .execute_update()
        .expect_err("every attempt fails, so the DML must fail");
    calls.load(Ordering::SeqCst)
}

/// On the **unary** RPC paths `spanner.retry.max_attempts` means exactly what it says: `N` permits
/// `N` attempts, and `1` really does disable retrying.
#[test]
fn retry_max_attempts_is_exact_on_unary_rpcs() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "retry_max_attempts_is_exact_on_unary_rpcs",
    );

    for max_attempts in [1_i64, 2, 3] {
        let attempts = unary_attempts(
            adbc_spanner::OPTION_RETRY_MAX_ATTEMPTS,
            OptionValue::Int(max_attempts),
        );
        assert_eq!(
            attempts, max_attempts as usize,
            "max_attempts={max_attempts} must permit exactly {max_attempts} ExecuteBatchDml attempts"
        );
    }
}

/// On the **streaming** query path the same option permits `N + 1` attempts — one too many — because
/// the client seeds the retry policy with its own `retry_count` (retries so far, `0` on the first
/// failure) where gax's own loop would pass the 1-based attempt count. `1` therefore does *not*
/// disable retrying here. Upstream bug (UP-14), pinned here as observed behaviour; `src/retry.rs`
/// documents it. The `N + 1` shape (not a constant) is what proves the option reaches the streaming
/// retry loop at all rather than being ignored.
#[test]
fn retry_max_attempts_permits_one_extra_attempt_on_the_streaming_path() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "retry_max_attempts_permits_one_extra_attempt_on_the_streaming_path",
    );

    for max_attempts in [1_i64, 2, 3] {
        let attempts = streaming_attempts(
            adbc_spanner::OPTION_RETRY_MAX_ATTEMPTS,
            OptionValue::Int(max_attempts),
        );
        assert_eq!(
            attempts,
            max_attempts as usize + 1,
            "max_attempts={max_attempts} currently permits {} ExecuteStreamingSql attempts \
             (UP-14); a change here means the pinned client's stream-resume accounting moved — \
             update src/retry.rs, docs/options.md and REVIEW.md's UP-14 to match",
            max_attempts + 1
        );
    }
}

/// `spanner.retry.max_elapsed_seconds` bounds the unary paths, but is **inert** on the streaming
/// query path: the client builds a fresh `RetryState` (hence `start = Instant::now()`) for every
/// resume decision, so the gax elapsed-time decorator always compares now against a deadline one
/// budget in the future and never exhausts. Same upstream root cause as the attempt off-by-one
/// (UP-14). A streaming caller who wants a wall-clock bound has a working one in the separate
/// `spanner.rpc.timeout_seconds.{query,fetch}` family.
#[test]
fn retry_max_elapsed_seconds_bounds_unary_rpcs_but_is_inert_on_the_streaming_path() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(240),
        "retry_max_elapsed_seconds_bounds_unary_rpcs_but_is_inert_on_the_streaming_path",
    );

    // A 50ms budget against a 10ms backoff: the unary loop gives up well inside the probe cap.
    let unary = unary_attempts(
        adbc_spanner::OPTION_RETRY_MAX_ELAPSED_SECONDS,
        OptionValue::Double(0.05),
    );
    assert!(
        unary < RETRY_PROBE_CAP,
        "a 50ms elapsed budget must exhaust the unary retry loop, but it ran {unary} attempts \
         (the probe cap is {RETRY_PROBE_CAP})"
    );

    // The streaming loop runs until the mock stops it: the budget never fires.
    let streaming = streaming_attempts(
        adbc_spanner::OPTION_RETRY_MAX_ELAPSED_SECONDS,
        OptionValue::Double(0.05),
    );
    assert_eq!(
        streaming,
        RETRY_PROBE_CAP + 1,
        "a 50ms elapsed budget currently never exhausts the streaming retry loop (UP-14), so the \
         mock's own cap is what stops it; a change here means the pinned client's stream-resume \
         accounting moved — update src/retry.rs, docs/options.md and REVIEW.md's UP-14 to match"
    );
}

/// SPAN-5: `get_statistics` must take its `INFORMATION_SCHEMA` discovery reads *and* every
/// per-table aggregate scan at ONE consistent snapshot, so a table created between discovery and
/// its scan cannot fail the call and every count is taken at the same timestamp. The driver does
/// this by running them all on a single multi-use read-only transaction (as `get_objects` does),
/// rather than a fresh single-use transaction per query.
///
/// This captures every `ExecuteStreamingSql` transaction selector and asserts the first query
/// begins ONE read-only transaction inline and every later query — the rest of discovery and all
/// aggregate scans — reuses it by id. The previous per-query single-use implementation would show a
/// fresh `single_use` selector on every request instead.
#[test]
fn get_statistics_shares_one_read_only_transaction() {
    let _watchdog = Watchdog::arm(
        Duration::from_secs(120),
        "get_statistics_shares_one_read_only_transaction",
    );

    let selectors: Arc<Mutex<Vec<Option<v1::TransactionSelector>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let server = {
        let record = selectors.clone();
        MockServer::start(move |mock| {
            let record = record.clone();
            mock.expect_execute_streaming_sql()
                .returning(move |request| {
                    let request = request.into_inner();
                    let inline_begin = matches!(
                        request
                            .transaction
                            .as_ref()
                            .and_then(|t| t.selector.as_ref()),
                        Some(v1::transaction_selector::Selector::Begin(_))
                    );
                    record.lock().unwrap().push(request.transaction);
                    // Shape the result to whichever query the collector issued (it reads columns
                    // positionally, so only the count and types matter, not the names).
                    let (fields, values): (Vec<(&str, v1::TypeCode)>, Vec<&str>) =
                        if request.sql.contains("INFORMATION_SCHEMA.TABLES") {
                            (
                                vec![
                                    ("TABLE_SCHEMA", v1::TypeCode::String),
                                    ("TABLE_NAME", v1::TypeCode::String),
                                ],
                                vec!["", "MockTable"],
                            )
                        } else if request.sql.contains("INFORMATION_SCHEMA.COLUMNS") {
                            (
                                vec![
                                    ("TABLE_SCHEMA", v1::TypeCode::String),
                                    ("TABLE_NAME", v1::TypeCode::String),
                                    ("COLUMN_NAME", v1::TypeCode::String),
                                    ("SPANNER_TYPE", v1::TypeCode::String),
                                ],
                                vec!["", "MockTable", "c", "STRING(MAX)"],
                            )
                        } else {
                            // The single-scan aggregate for one groupable STRING column `c`:
                            // COUNT(*), COUNTIF(c IS NULL), COUNT(DISTINCT c).
                            (
                                vec![
                                    ("n", v1::TypeCode::Int64),
                                    ("nulls", v1::TypeCode::Int64),
                                    ("distinct", v1::TypeCode::Int64),
                                ],
                                vec!["3", "0", "3"],
                            )
                        };
                    let mut metadata = result_metadata(&fields);
                    if inline_begin {
                        metadata.transaction = Some(v1::Transaction {
                            id: b"stats-ro-txn".to_vec(),
                            ..Default::default()
                        });
                    }
                    let result = v1::PartialResultSet {
                        metadata: Some(metadata),
                        values: values.iter().map(|s| string_value(s)).collect(),
                        resume_token: b"stat-1".to_vec(),
                        last: true,
                        ..Default::default()
                    };
                    Ok(stream_of(vec![Ok(result)]))
                });
        })
    };

    let connection = server.connect();
    let reader = connection
        .get_statistics(None, None, None, false)
        .expect("get_statistics");
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect statistics");
    assert_eq!(batches.len(), 1, "one catalog batch");

    let seen = selectors.lock().unwrap();
    assert!(
        seen.len() >= 3,
        "discovery (TABLES + COLUMNS) plus at least one aggregate scan, got {}",
        seen.len()
    );
    // The first query begins ONE read-only transaction inline.
    let first = seen[0]
        .as_ref()
        .and_then(|s| s.selector.as_ref())
        .expect("the first query must carry a transaction selector");
    let v1::transaction_selector::Selector::Begin(options) = first else {
        panic!("the first get_statistics query must begin the transaction, got: {first:?}");
    };
    assert!(
        matches!(
            options.mode,
            Some(v1::transaction_options::Mode::ReadOnly(_))
        ),
        "the begun transaction must be read-only: {options:?}"
    );
    // Every later query reuses it by id — one shared snapshot across discovery and all scans, not a
    // fresh single-use transaction per query.
    for (i, selector) in seen.iter().enumerate().skip(1) {
        let sel = selector
            .as_ref()
            .and_then(|s| s.selector.as_ref())
            .unwrap_or_else(|| panic!("query {i} must carry a transaction selector"));
        assert_eq!(
            sel,
            &v1::transaction_selector::Selector::Id(b"stats-ro-txn".to_vec()),
            "query {i} must reuse the begun read-only transaction, not open its own"
        );
    }
}

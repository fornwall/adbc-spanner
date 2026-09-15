//! FFI / stream-lifecycle battery for the ADBC driver's C ABI export.
//!
//! Driver-manager-level regression tests for the crash class that dominated other ADBC drivers'
//! bug reports (DuckDB's 2026 stream-lifetime fixes, e.g. the release race fixed in DuckDB PR
//! #21800; the C++ `adbc_validation` suite's TestResultIndependence / TestResultInvalidation
//! checks): a result stream handed across the C ABI is a standalone object, so consuming or
//! releasing it must never crash regardless of what has happened to the statement / connection /
//! database that produced it. Erroring is acceptable per the ADBC spec; crashing is not.
//!
//! These live in their own binary rather than in `tests/integration.rs` because they are
//! *structurally* independent of everything there: every query generates its rows server-side with
//! `GENERATE_ARRAY`, so the battery touches no table, writes nothing, and needs none of that
//! suite's schema serialization (`serial_guard`). All it needs is a reachable database and a built
//! cdylib.
//!
//! Like `tests/integration.rs` the file **skips itself** unless `SPANNER_EMULATOR_HOST` or
//! `SPANNER_GCP_DATABASE` is set, so a plain `cargo test` stays green:
//!
//! ```sh
//! scripts/with-emulator.sh cargo test --test ffi_lifecycle -- --nocapture
//! ```
//!
//! The raw-C-ABI test additionally needs the cdylib built next to the test binary
//! (`cargo build`); it skips when it is absent, unless `ADBC_TEST_REQUIRE_TARGET` is set.

use std::sync::Mutex;

use adbc_core::error::Status;
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_manager::ManagedDriver;
use adbc_spanner::{SpannerConnection, SpannerDatabase, SpannerDriver};
use arrow_array::{RecordBatch, RecordBatchReader};
use google_cloud_lro::Poller;
use google_cloud_spanner::client::Spanner;
use google_cloud_spanner_admin_instance_v1::model::Instance;

// Identifiers used against the emulator, matching `tests/integration.rs` so both binaries drive the
// same emulator database interchangeably, whichever runs first.
const PROJECT: &str = "test-project";
const INSTANCE: &str = "test-instance";
const DATABASE: &str = "adbc-test";

/// Where these tests should run, resolved from the environment.
struct TestTarget {
    project: String,
    instance: String,
    database: String,
    /// `true` when pointed at the local emulator (via `SPANNER_EMULATOR_HOST`), `false` for a real
    /// Cloud Spanner database reached with Application Default Credentials.
    is_emulator: bool,
}

impl TestTarget {
    fn database_path(&self) -> String {
        format!(
            "projects/{}/instances/{}/databases/{}",
            self.project, self.instance, self.database
        )
    }

    /// The database path wrapped as a `spanner://` connection URI, the form the `uri` option
    /// requires (a bare path is rejected).
    fn database_uri(&self) -> String {
        format!("spanner:///{}", self.database_path())
    }

    fn instance_path(&self) -> String {
        format!("projects/{}/instances/{}", self.project, self.instance)
    }
}

/// Whether the `ADBC_TEST_REQUIRE_TARGET` env var demands a configured target (CI sets it).
///
/// When set to a truthy value, the "no target configured" / "no cdylib built" skip branches
/// `panic!` instead of quietly returning, so a broken env wiring (e.g. a dropped or misspelled
/// `env:` line in a workflow refactor) fails the run loudly rather than passing vacuously with zero
/// behavioral coverage.
fn require_target() -> bool {
    matches!(
        std::env::var("ADBC_TEST_REQUIRE_TARGET").ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

/// Resolve the test target from the environment, or `None` to skip — panicking instead when
/// `ADBC_TEST_REQUIRE_TARGET` is truthy, so this suite cannot silently skip in CI.
fn test_target() -> Option<TestTarget> {
    let target = resolve_test_target();
    if target.is_none() && require_target() {
        panic!(
            "ADBC_TEST_REQUIRE_TARGET is set but neither SPANNER_EMULATOR_HOST nor \
             SPANNER_GCP_DATABASE is configured — the Spanner target env wiring is missing, so \
             this suite would skip all behavioral coverage. Refusing to pass vacuously."
        );
    }
    target
}

/// Inner resolver for [`test_target`]; returns `None` when no target env is set.
fn resolve_test_target() -> Option<TestTarget> {
    if std::env::var("SPANNER_EMULATOR_HOST").is_ok_and(|s| !s.is_empty()) {
        return Some(TestTarget {
            project: PROJECT.to_string(),
            instance: INSTANCE.to_string(),
            database: DATABASE.to_string(),
            is_emulator: true,
        });
    }

    let spec = std::env::var("SPANNER_GCP_DATABASE")
        .ok()
        .filter(|s| !s.is_empty())?;
    // Spanner project / instance / database ids cannot themselves contain a dot, so a plain split
    // unambiguously recovers the three components.
    match spec.split('.').collect::<Vec<_>>().as_slice() {
        [project, instance, database] => Some(TestTarget {
            project: project.to_string(),
            instance: instance.to_string(),
            database: database.to_string(),
            is_emulator: false,
        }),
        _ => {
            panic!("SPANNER_GCP_DATABASE must be in 'project.instance.database' form, got {spec:?}")
        }
    }
}

/// Create the emulator instance and the test database if they do not already exist.
///
/// Deliberately creates **no tables**: every query in this file is a table-less `GENERATE_ARRAY`.
/// `tests/integration.rs` reconciles its own schema unconditionally, so whichever binary `cargo
/// test` happens to run first, the other still finds what it needs. `create_instance` /
/// `create_database` are best-effort: on an already-populated target they fail with `AlreadyExists`,
/// which is intentionally ignored. A **real** instance is assumed to exist already (creating one is
/// slow and billable).
async fn ensure_database(target: &TestTarget) {
    // The client auto-detects `SPANNER_EMULATOR_HOST` (anonymous credentials) and otherwise uses
    // Application Default Credentials — the same resolution the driver under test performs.
    let spanner = Spanner::builder()
        .build()
        .await
        .expect("failed to build Spanner client for setup");

    if target.is_emulator {
        let instance_admin = spanner
            .instance_admin_builder()
            .build()
            .await
            .expect("failed to build instance admin client");
        let _ = instance_admin
            .create_instance()
            .set_parent(format!("projects/{}", target.project))
            .set_instance_id(&target.instance)
            .set_instance(
                Instance::new()
                    .set_config(format!(
                        "projects/{}/instanceConfigs/emulator-config",
                        target.project
                    ))
                    .set_display_name("ADBC test instance")
                    .set_node_count(1),
            )
            .poller()
            .until_done()
            .await;
    }

    let database_admin = spanner
        .database_admin_builder()
        .build()
        .await
        .expect("failed to build database admin client");
    let _ = database_admin
        .create_database()
        .set_parent(target.instance_path())
        .set_create_statement(format!("CREATE DATABASE `{}`", target.database))
        .poller()
        .until_done()
        .await;
}

/// Run [`ensure_database`] exactly once for the whole test binary.
///
/// These tests run in parallel and share one database, so letting several drive the admin setup
/// concurrently races (the emulator can report `Instance not found` if two `create_instance` calls
/// overlap). Guard it behind a mutex + "already done" flag.
fn ensure_database_once(target: &TestTarget) {
    static DONE: Mutex<bool> = Mutex::new(false);
    // Ignore poisoning: if the first arrival's setup panicked, that failure should surface on its
    // own rather than cascade into every later test as a "poisoned lock" panic. `done` is still
    // false then, so the next test simply retries the idempotent setup.
    let mut done = DONE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if !*done {
        // A throwaway runtime just for the async admin setup.
        tokio::runtime::Runtime::new()
            .expect("failed to build setup runtime")
            .block_on(ensure_database(target));
        *done = true;
    }
}

/// Open a connection, retrying briefly: a freshly-created emulator database can be momentarily
/// invisible to the data plane right after the admin `create_database` returns.
fn connect_with_retry(database: &SpannerDatabase) -> SpannerConnection {
    let mut last_err = None;
    for _ in 0..20 {
        match database.new_connection() {
            Ok(connection) => return connection,
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        }
    }
    panic!("create connection failed after retries: {last_err:?}");
}

/// Locate the built `cdylib` (`libadbc_spanner.so` / `.dylib` / `.dll`) next to the test binary.
fn cdylib_path() -> Option<std::path::PathBuf> {
    // The test binary lives in `target/<profile>/deps/`; the cdylib is in `target/<profile>/`.
    let dir = std::env::current_exe()
        .ok()?
        .parent()?
        .parent()?
        .to_path_buf();
    let name = if cfg!(target_os = "windows") {
        "adbc_spanner.dll"
    } else if cfg!(target_os = "macos") {
        "libadbc_spanner.dylib"
    } else {
        "libadbc_spanner.so"
    };
    let path = dir.join(name);
    path.exists().then_some(path)
}

/// Locate the built cdylib, or `None` to skip — but panic instead of skipping when
/// `ADBC_TEST_REQUIRE_TARGET` is set, so a missing build cannot silently pass the FFI tests in CI.
fn required_cdylib_path() -> Option<std::path::PathBuf> {
    let path = cdylib_path();
    if path.is_none() && require_target() {
        panic!(
            "ADBC_TEST_REQUIRE_TARGET is set but the cdylib \
             (libadbc_spanner.so / .dylib / adbc_spanner.dll) is not built next to the test \
             binary — run `cargo build` first. Refusing to skip the FFI test vacuously."
        );
    }
    path
}

/// SQL producing `n` ordered INT64 rows without touching any table.
fn rows_sql(n: usize) -> String {
    format!("SELECT n FROM UNNEST(GENERATE_ARRAY(1, {n})) AS n ORDER BY n")
}

/// Load the driver through the driver manager and open a database + connection, retrying the
/// connection briefly (a freshly-created emulator database can lag behind the admin API).
fn ffi_connect(
    cdylib: &std::path::Path,
    target: &TestTarget,
) -> (
    ManagedDriver,
    adbc_driver_manager::ManagedDatabase,
    adbc_driver_manager::ManagedConnection,
) {
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        cdylib,
        Some(b"AdbcSpannerInit"),
        AdbcVersion::V110,
    )
    .expect("load driver via the driver manager");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_uri()),
        )])
        .expect("new_database via FFI");
    let mut connection = None;
    let mut last = None;
    for _ in 0..20 {
        match database.new_connection() {
            Ok(c) => {
                connection = Some(c);
                break;
            }
            Err(e) => {
                last = Some(e);
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        }
    }
    let connection = connection.unwrap_or_else(|| panic!("FFI connect failed: {last:?}"));
    (driver, database, connection)
}

/// New statement over `connection` querying `rows` generated rows with `spanner.rows_per_batch`
/// set to `batch`, so the result streams across several lazily-fetched chunks.
fn ffi_streaming_statement(
    connection: &mut adbc_driver_manager::ManagedConnection,
    rows: usize,
    batch: i64,
) -> adbc_driver_manager::ManagedStatement {
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(batch),
        )
        .expect("set rows_per_batch");
    statement.set_sql_query(rows_sql(rows)).unwrap();
    statement
}

/// Total row count of a fully-consumed reader, failing the test on any stream error.
fn drain_rows(reader: Box<dyn RecordBatchReader + Send>) -> usize {
    reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect result stream")
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

/// (a) TestResultIndependence, through the C ABI: a live result stream must keep working — or at
/// worst error, never crash — after the statement, connection and database that produced it have
/// all been released. In particular, releasing the hierarchy must not tear down the shared Tokio
/// runtime while the stream's reader still blocks on it (the exported reader owns `Arc` handles
/// to the runtime and client precisely so it can outlive its producers).
#[test]
fn ffi_stream_survives_statement_connection_database_release() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping ffi_stream_survives_… test");
        return;
    };
    let Some(cdylib) = required_cdylib_path() else {
        eprintln!("cdylib not built — skipping FFI lifecycle test (run `cargo build` first)");
        return;
    };
    ensure_database_once(&target);

    let (driver, database, mut connection) = ffi_connect(&cdylib, &target);
    let mut statement = ffi_streaming_statement(&mut connection, 1000, 100);
    let mut reader = statement.execute().expect("execute via FFI");
    // Pull the first batch while everything is still alive, so the stream is genuinely in flight.
    let first = reader.next().expect("first batch").expect("first batch ok");
    assert_eq!(first.num_rows(), 100);

    // Release the entire producing hierarchy under the live stream. The `ManagedDriver` handle
    // itself must outlive the stream: dropping the last one unloads the shared library, and with
    // it the code the stream's function pointers point into — the same contract as the C driver
    // manager, which keeps the library loaded until `AdbcDriverRelease`.
    drop(statement);
    drop(connection);
    drop(database);

    // Keep consuming. This driver keeps orphaned streams fully functional (the reader holds the
    // runtime and client alive), so assert complete success rather than merely "no crash".
    let rest = drain_rows(reader);
    assert_eq!(first.num_rows() + rest, 1000);
    drop(driver);
}

/// Pure-Rust (non-FFI) variant of
/// [`ffi_stream_survives_statement_connection_database_release`]: the reader returned by
/// `execute` keeps the shared runtime alive after statement, connection, database *and driver*
/// are all dropped.
#[test]
fn stream_survives_statement_connection_database_release() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping stream_survives_… test");
        return;
    };
    ensure_database_once(&target);

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_uri()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(100),
        )
        .expect("set rows_per_batch");
    statement.set_sql_query(rows_sql(1000)).unwrap();

    let mut reader = statement.execute().expect("execute");
    let first = reader.next().expect("first batch").expect("first batch ok");
    assert_eq!(first.num_rows(), 100);

    // Unlike the FFI variant there is no shared library to keep loaded, so *everything* can go.
    drop(statement);
    drop(connection);
    drop(database);
    drop(driver);

    let rest = drain_rows(reader);
    assert_eq!(first.num_rows() + rest, 1000);
}

/// (b) TestResultInvalidation, through the C ABI: executing again on a statement whose previous
/// result stream is still open must not crash. The second result must be complete and correct;
/// the first stream may be invalidated (erroring on its next pull) or keep working — this driver
/// hands out independent streams — but either way draining it afterwards must be safe.
#[test]
fn ffi_execute_twice_with_first_stream_still_open() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping ffi_execute_twice_… test");
        return;
    };
    let Some(cdylib) = required_cdylib_path() else {
        eprintln!("cdylib not built — skipping FFI lifecycle test (run `cargo build` first)");
        return;
    };
    ensure_database_once(&target);

    let (_driver, _database, mut connection) = ffi_connect(&cdylib, &target);
    let mut statement = ffi_streaming_statement(&mut connection, 600, 50);
    let mut first_reader = statement.execute().expect("first execute");
    let batch = first_reader
        .next()
        .expect("first batch")
        .expect("first batch ok");
    assert_eq!(batch.num_rows(), 50);

    // Re-execute with the first stream still open, and consume the new stream to completion.
    let second_reader = statement.execute().expect("second execute");
    assert_eq!(drain_rows(second_reader), 600);

    // Draining the invalidation candidate may yield rows or an error; crashing is the only
    // unacceptable outcome.
    for item in first_reader {
        if item.is_err() {
            break;
        }
    }
}

/// (c) Cross-thread release race (the DuckDB PR #21800 bug class): a second thread releases a
/// live result stream while the main thread starts and consumes a new query on the same
/// connection. ~50 iterations to give the race room to bite.
#[test]
fn ffi_stream_release_races_new_query() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping ffi_stream_release_races_new_query");
        return;
    };
    let Some(cdylib) = required_cdylib_path() else {
        eprintln!("cdylib not built — skipping FFI lifecycle test (run `cargo build` first)");
        return;
    };
    ensure_database_once(&target);

    let (_driver, _database, mut connection) = ffi_connect(&cdylib, &target);
    for _ in 0..50 {
        let mut statement = ffi_streaming_statement(&mut connection, 500, 50);
        let reader = statement.execute().expect("execute racing stream");
        // Release the stream (its drop runs the C release callback, which tears down the
        // driver-side reader and its prefetch task) from another thread…
        let releaser = std::thread::spawn(move || drop(reader));
        // …while this thread runs a fresh query on the same connection.
        let mut probe = connection.new_statement().expect("new statement");
        probe.set_sql_query("SELECT 1 AS one").unwrap();
        assert_eq!(drain_rows(probe.execute().expect("racing query")), 1);
        releaser
            .join()
            .expect("stream releaser thread must not panic");
    }
}

/// (e) Unhappy paths must surface specific errors — not crashes — through the C ABI: executing
/// with no SQL set (query and update paths), deriving a parameter schema with nothing to derive
/// it from, and preparing an empty statement. The statement must stay usable afterwards.
/// (Cancel-after-release, the remaining InvalidState-class path, needs raw FFI calls and lives in
/// `ffi_double_release_and_error_struct_reuse`.)
#[test]
fn ffi_unhappy_paths_error_instead_of_crash() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping ffi_unhappy_paths_… test");
        return;
    };
    let Some(cdylib) = required_cdylib_path() else {
        eprintln!("cdylib not built — skipping FFI lifecycle test (run `cargo build` first)");
        return;
    };
    ensure_database_once(&target);

    let (_driver, _database, mut connection) = ffi_connect(&cdylib, &target);
    let mut statement = connection.new_statement().expect("new statement");

    let Err(error) = statement.execute() else {
        panic!("execute without SQL must fail");
    };
    assert_eq!(error.status, Status::InvalidState, "{error:?}");
    let error = statement
        .execute_update()
        .expect_err("execute_update without SQL must fail");
    assert_eq!(error.status, Status::InvalidState, "{error:?}");
    let error = statement
        .get_parameter_schema()
        .expect_err("get_parameter_schema without SQL must fail");
    assert_eq!(error.status, Status::InvalidState, "{error:?}");
    let error = statement
        .prepare()
        .expect_err("prepare without SQL must fail");
    assert_eq!(error.status, Status::InvalidState, "{error:?}");

    // With SQL set but *before* `prepare`, the parameter schema is well-defined for this driver
    // (Spanner plans server-side; prepare is a no-op) — it must succeed rather than crash.
    statement.set_sql_query("SELECT @p AS v").unwrap();
    let schema = statement
        .get_parameter_schema()
        .expect("parameter schema before prepare");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "p");

    // The failed calls must not have wedged the statement: it still executes.
    statement.set_sql_query("SELECT 1 AS one").unwrap();
    assert_eq!(
        drain_rows(statement.execute().expect("execute after failures")),
        1
    );
}

/// (f) Abandoning a half-consumed stream, through the C ABI: read one batch of a large result,
/// release the stream mid-flight (which must also clean up the driver's background prefetch task
/// — `spawn_prefetch` in `src/runtime.rs`; its `JoinHandle` is aborted on reader drop), and
/// verify the statement and connection keep working. Repeated a few times so leaked tasks or a
/// wedged runtime would compound and surface.
#[test]
fn ffi_drop_half_consumed_stream_then_connection_still_works() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping ffi_drop_half_consumed_… test");
        return;
    };
    let Some(cdylib) = required_cdylib_path() else {
        eprintln!("cdylib not built — skipping FFI lifecycle test (run `cargo build` first)");
        return;
    };
    ensure_database_once(&target);

    let (_driver, _database, mut connection) = ffi_connect(&cdylib, &target);
    for _ in 0..5 {
        // 10k rows / 500-row batches: 20 chunks, of which only the first is consumed.
        // (GENERATE_ARRAY caps at 16000 elements, so this cannot grow past that.)
        let mut statement = ffi_streaming_statement(&mut connection, 10_000, 500);
        let mut reader = statement.execute().expect("execute large query");
        let first = reader.next().expect("first batch").expect("first batch ok");
        assert_eq!(first.num_rows(), 500);
        // Abandon the stream mid-flight, prefetch task and all.
        drop(reader);
        // The same statement handle must remain usable after its stream was abandoned.
        statement.set_sql_query("SELECT 1 AS one").unwrap();
        assert_eq!(drain_rows(statement.execute().expect("re-execute")), 1);
    }
    // And the connection as a whole still streams a fresh result to completion.
    let mut probe = ffi_streaming_statement(&mut connection, 1000, 100);
    assert_eq!(drain_rows(probe.execute().expect("fresh query")), 1000);
}

/// Pure-Rust (non-FFI) variant of [`ffi_drop_half_consumed_stream_then_connection_still_works`].
#[test]
fn drop_half_consumed_reader_then_connection_still_works() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping drop_half_consumed_… test");
        return;
    };
    ensure_database_once(&target);

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_uri()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    for _ in 0..5 {
        let mut statement = connection.new_statement().expect("new statement");
        statement
            .set_option(
                OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
                OptionValue::Int(500),
            )
            .expect("set rows_per_batch");
        // 10k rows / 500-row batches — see the FFI variant for the sizing rationale.
        statement.set_sql_query(rows_sql(10_000)).unwrap();
        let mut reader = statement.execute().expect("execute large query");
        let first = reader.next().expect("first batch").expect("first batch ok");
        assert_eq!(first.num_rows(), 500);
        drop(reader); // abandons the stream with the prefetch task mid-flight
    }

    let mut probe = connection.new_statement().expect("new statement");
    probe.set_sql_query(rows_sql(1000)).unwrap();
    assert_eq!(drain_rows(probe.execute().expect("fresh query")), 1000);
}

/// (d) Double-release and error-struct reuse at the raw C ABI — paths the managed Rust wrappers
/// never exercise, because their `Drop` impls release exactly once. Releasing a statement /
/// connection / database / driver twice must yield a clean `INVALID_STATE` error, not a crash or
/// double-free; one `FFI_AdbcError` struct is reused across every call with its embedded
/// `release` callback invoked (deliberately twice — it must be idempotent) between uses, the
/// standard C consumer idiom. This is the regression test for the fork's idempotent
/// `release_ffi_error` fix (see the `adbc_ffi` pin note in Cargo.toml) ahead of the eventual
/// unpin to a crates.io arrow-adbc release, and it also covers cancel-after-release from (e).
#[cfg(feature = "ffi")]
#[test]
fn ffi_double_release_and_error_struct_reuse() {
    use std::ffi::{CStr, CString};
    use std::os::raw::c_void;

    use adbc_core::constants::{ADBC_STATUS_INVALID_STATE, ADBC_STATUS_OK, ADBC_VERSION_1_1_0};
    use adbc_ffi::{
        FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcDriverInitFunc,
        FFI_AdbcError, FFI_AdbcStatement,
    };

    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping ffi_double_release_… test");
        return;
    };
    let Some(cdylib) = required_cdylib_path() else {
        eprintln!("cdylib not built — skipping raw FFI test (run `cargo build` first)");
        return;
    };
    ensure_database_once(&target);

    /// Release `error`'s contents through its embedded callback, twice: the second call is the
    /// idempotency check (pre-fix, it double-freed the message `CString`).
    fn release_error_twice(error: &mut FFI_AdbcError) {
        assert!(
            error.release.is_some(),
            "a failed call must set a releasable error"
        );
        for _ in 0..2 {
            if let Some(release) = error.release {
                unsafe { release(error) };
            }
        }
        assert!(error.message.is_null(), "release must null the message");
    }

    /// The error message currently held by `error`, for asserting on specific errors.
    fn error_message(error: &FFI_AdbcError) -> String {
        if error.message.is_null() {
            return String::new();
        }
        unsafe { CStr::from_ptr(error.message) }
            .to_string_lossy()
            .into_owned()
    }

    // Load the shared library directly (as a C driver manager would), so the raw entrypoints can
    // be driven in ways the managed wrappers never do. Declared before every FFI object below so
    // it is dropped last: the driver's function pointers and the error's release callback point
    // into this library's code.
    let library = unsafe { libloading::Library::new(&cdylib) }.expect("load cdylib");
    let init: libloading::Symbol<FFI_AdbcDriverInitFunc> =
        unsafe { library.get(b"AdbcSpannerInit") }.expect("resolve AdbcSpannerInit");

    // The single error struct reused across every call below.
    let mut error = FFI_AdbcError::default();

    let mut driver = FFI_AdbcDriver::default();
    let status = unsafe {
        init(
            ADBC_VERSION_1_1_0,
            &mut driver as *mut _ as *mut c_void,
            &mut error,
        )
    };
    assert_eq!(status, ADBC_STATUS_OK);

    // Database: new → set uri → init.
    let mut database = FFI_AdbcDatabase::default();
    let status = unsafe { driver.DatabaseNew.unwrap()(&mut database, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let key = CString::new("uri").unwrap();
    let value = CString::new(target.database_uri()).unwrap();
    let status = unsafe {
        driver.DatabaseSetOption.unwrap()(&mut database, key.as_ptr(), value.as_ptr(), &mut error)
    };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { driver.DatabaseInit.unwrap()(&mut database, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);

    // Connection: new → init, retrying briefly (a fresh emulator database can lag). Each failed
    // attempt also exercises the release-and-reuse cycle on the shared error struct.
    let mut connection = FFI_AdbcConnection::default();
    let status = unsafe { driver.ConnectionNew.unwrap()(&mut connection, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let mut status = ADBC_STATUS_OK;
    for _ in 0..20 {
        status =
            unsafe { driver.ConnectionInit.unwrap()(&mut connection, &mut database, &mut error) };
        if status == ADBC_STATUS_OK {
            break;
        }
        release_error_twice(&mut error);
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    assert_eq!(status, ADBC_STATUS_OK, "raw FFI connect failed");

    let mut statement = FFI_AdbcStatement::default();
    let status =
        unsafe { driver.StatementNew.unwrap()(&mut connection, &mut statement, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);

    // --- Statement: release, release again, then cancel-after-release. ---
    let statement_release = driver.StatementRelease.unwrap();
    let status = unsafe { statement_release(&mut statement, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { statement_release(&mut statement, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "double statement release must error, not crash"
    );
    assert!(
        error_message(&error).contains("already released"),
        "unexpected double-release error: {:?}",
        error_message(&error)
    );
    release_error_twice(&mut error);
    // (e) Cancel on a released statement: a specific InvalidState-class error, not a crash.
    let status = unsafe { driver.StatementCancel.unwrap()(&mut statement, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "cancel after release must error, not crash"
    );
    release_error_twice(&mut error);

    // --- Connection released twice, reusing the same error struct. ---
    let connection_release = driver.ConnectionRelease.unwrap();
    let status = unsafe { connection_release(&mut connection, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { connection_release(&mut connection, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "double connection release must error, not crash"
    );
    release_error_twice(&mut error);

    // --- Database released twice. ---
    let database_release = driver.DatabaseRelease.unwrap();
    let status = unsafe { database_release(&mut database, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { database_release(&mut database, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "double database release must error, not crash"
    );
    release_error_twice(&mut error);

    // --- Driver released twice (through a saved callback: the first release clears the field,
    // which is also what makes the struct's eventual `Drop` a safe no-op). ---
    let driver_release = driver.release.unwrap();
    let status = unsafe { driver_release(&mut driver, &mut error) };
    assert_eq!(status, ADBC_STATUS_OK);
    let status = unsafe { driver_release(&mut driver, &mut error) };
    assert_eq!(
        status, ADBC_STATUS_INVALID_STATE,
        "double driver release must error, not crash"
    );
    release_error_twice(&mut error);
}

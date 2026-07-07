//! End-to-end integration test that runs the ADBC driver against Cloud Spanner.
//!
//! The test is **skipped automatically** unless one of two environment variables is set, so a plain
//! `cargo test` stays green without any external dependency:
//!
//! - `SPANNER_EMULATOR_HOST` — run against a local Spanner **emulator**. The helper script starts
//!   the emulator, exports the variable and runs the test:
//!
//!   ```sh
//!   scripts/with-emulator.sh cargo test --test integration -- --nocapture
//!   ```
//!
//! - `SPANNER_GCP_DATABASE` — run against a **real** Cloud Spanner database, reached with
//!   Application Default Credentials (`gcloud auth application-default login`, a service-account
//!   key via `GOOGLE_APPLICATION_CREDENTIALS`, or the ambient GCP identity). The value is the target
//!   database in `project.instance.database` form, e.g.
//!
//!   ```sh
//!   SPANNER_GCP_DATABASE=my-project.my-instance.my-db cargo test --test integration -- --nocapture
//!   ```
//!
//!   The instance must already exist; the test best-effort creates the database and the tables it
//!   needs (the `Singers` table and the property-test tables, all idempotent), and clears or drops
//!   its scratch data, so it is safe to re-run against a persistent database. If both variables are
//!   set, the emulator wins.
//!
//! Setup (creating the database and table) uses the Spanner admin clients directly; the actual query
//! and DML round-trip goes through the `adbc-spanner` driver being tested.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use adbc_core::options::{
    AdbcVersion, InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
    OptionValue,
};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_manager::ManagedDriver;
use adbc_spanner::{SpannerConnection, SpannerDatabase, SpannerDriver};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int64Array, ListArray, RecordBatch, RecordBatchReader, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampNanosecondArray, UnionArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, SecondsFormat};
use google_cloud_lro::Poller;
use google_cloud_spanner::client::Spanner;
use google_cloud_spanner_admin_instance_v1::model::Instance;
use proptest::prelude::*;

// Identifiers used against the emulator, which starts empty and lets us create everything.
const PROJECT: &str = "test-project";
const INSTANCE: &str = "test-instance";
const DATABASE: &str = "adbc-test";

/// Where the integration tests should run, resolved from the environment.
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

/// Resolve the test target from the environment, or `None` to skip.
///
/// `SPANNER_EMULATOR_HOST` selects the emulator with fixed identifiers. `SPANNER_GCP_DATABASE`, in
/// `project.instance.database` form, points the tests at a real Cloud Spanner database reached with
/// Application Default Credentials. The emulator takes precedence if both are set.
///
/// When `ADBC_TEST_REQUIRE_TARGET` is truthy (CI) and no target is configured, this panics instead
/// of returning `None`, so the suite cannot silently skip all of its behavioral coverage.
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
    if std::env::var("SPANNER_EMULATOR_HOST")
        .ok()
        .filter(|s| !s.is_empty())
        .is_some()
    {
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

/// DDL for the `Singers` table the tests read from. `if_not_exists` guards it for a pre-existing
/// real database, where `create_database`'s extra statements do not run.
fn singers_ddl(if_not_exists: bool) -> String {
    let guard = if if_not_exists { "IF NOT EXISTS " } else { "" };
    format!(
        "CREATE TABLE {guard}Singers (\
             SingerId INT64 NOT NULL, \
             Name STRING(MAX), \
             Active BOOL, \
             Score FLOAT64\
         ) PRIMARY KEY (SingerId)"
    )
}

/// DDL for the tables the property-based round-trip tests write to. Created during one-time setup
/// (not in the test bodies) so the parallel tests never issue concurrent schema changes, which the
/// emulator rejects database-wide. Column names avoid Spanner reserved words (e.g. `BY`).
fn prop_tables_ddl(if_not_exists: bool) -> Vec<String> {
    let g = if if_not_exists { "IF NOT EXISTS " } else { "" };
    vec![
        format!(
            "CREATE TABLE {g}AdbcPropBind \
                 (Id INT64, IntCol INT64, FloatCol FLOAT64, BoolCol BOOL, \
                  StrCol STRING(MAX), BytesCol BYTES(MAX), \
                  DateCol DATE, TsCol TIMESTAMP, NumCol NUMERIC) \
             PRIMARY KEY (Id)"
        ),
        format!(
            "CREATE TABLE {g}AdbcPropTypes \
                 (Id INT64, D DATE, T TIMESTAMP, N NUMERIC) PRIMARY KEY (Id)"
        ),
    ]
}

/// Create the test database and `Singers` table if they do not already exist.
///
/// `create_instance` / `create_database` are best-effort: on a re-run against an already-populated
/// target they fail with `AlreadyExists`, which we intentionally ignore.
///
/// The **emulator** starts empty, so we also create the instance. A **real** instance is assumed to
/// exist already (creating one is slow and billable); there we additionally issue a
/// `CREATE TABLE IF NOT EXISTS Singers`, because on a pre-existing database `create_database`'s
/// extra statements never run.
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
    let mut create_statements = vec![singers_ddl(false)];
    create_statements.extend(prop_tables_ddl(false));
    let _ = database_admin
        .create_database()
        .set_parent(target.instance_path())
        .set_create_statement(format!("CREATE DATABASE `{}`", target.database))
        .set_extra_statements(create_statements)
        .poller()
        .until_done()
        .await;

    // On a pre-existing real database the create above is a no-op (`AlreadyExists`), so reconcile
    // the `Singers` table separately. Skipped for the emulator, whose fresh database always gets it
    // from the extra statements above.
    if !target.is_emulator {
        let mut reconcile = vec![singers_ddl(true)];
        reconcile.extend(prop_tables_ddl(true));
        let _ = database_admin
            .update_database_ddl()
            .set_database(target.database_path())
            .set_statements(reconcile)
            .poller()
            .until_done()
            .await;
    }
}

/// Run [`ensure_database`] exactly once for the whole test binary.
///
/// The two integration tests run in parallel and share one database, so letting both drive the
/// admin setup concurrently races (the emulator can report `Instance not found` if two
/// `create_instance` calls overlap). Guard it behind a mutex + "already done" flag so the first test
/// to arrive performs the setup and the second reuses it.
fn ensure_database_once(target: &TestTarget) {
    static DONE: Mutex<bool> = Mutex::new(false);
    // Like `serial_guard`, ignore poisoning: if the first arrival's setup panicked, that failure
    // should surface on its own rather than cascade into every later test as a "poisoned lock"
    // panic. `done` is still false then, so the next test simply retries the idempotent setup.
    let mut done = DONE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if !*done {
        // A throwaway runtime just for the async admin setup.
        tokio::runtime::Runtime::new()
            .expect("failed to build setup runtime")
            .block_on(ensure_database(target));
        *done = true;
    }
}

#[test]
fn query_and_dml_round_trip() {
    let Some(target) = test_target() else {
        eprintln!(
            "neither SPANNER_EMULATOR_HOST nor SPANNER_GCP_DATABASE set — \
             skipping Spanner integration test"
        );
        return;
    };

    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    // The driver reports the table types Spanner supports without hitting the network.
    let table_types = connection.get_table_types().expect("get_table_types");
    assert_eq!(table_types.schema().field(0).name(), "table_type");

    // Idempotency: clear any rows left over from a previous run.
    let mut delete = connection.new_statement().expect("new statement");
    delete
        .set_sql_query("DELETE FROM Singers WHERE true")
        .unwrap();
    delete.execute_update().expect("delete");

    // Insert two rows via DML and assert the affected-row count.
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(
            "INSERT INTO Singers (SingerId, Name, Active, Score) \
             VALUES (1, 'Alice', true, 4.5), (2, 'Bob', false, 3.25)",
        )
        .unwrap();
    assert_eq!(insert.execute_update().expect("insert"), Some(2));

    // Read the rows back through the driver as Arrow.
    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT SingerId, Name, Active, Score FROM Singers ORDER BY SingerId")
        .unwrap();
    let reader = query.execute().expect("query");

    // The Arrow schema should reflect the Spanner column types.
    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "SingerId");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
    assert_eq!(schema.field(3).data_type(), &DataType::Float64);

    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "expected two rows back");

    let batch = &batches[0];
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let active = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let score = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(
        (
            ids.value(0),
            names.value(0),
            active.value(0),
            score.value(0)
        ),
        (1, "Alice", true, 4.5)
    );
    assert_eq!(
        (
            ids.value(1),
            names.value(1),
            active.value(1),
            score.value(1)
        ),
        (2, "Bob", false, 3.25)
    );

    // --- get_table_schema reflects the table's column types ---

    let singers_schema = connection
        .get_table_schema(None, None, "Singers")
        .expect("get_table_schema");
    assert_eq!(singers_schema.fields().len(), 4);
    assert_eq!(singers_schema.field(0).name(), "SingerId");
    assert_eq!(singers_schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(singers_schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(singers_schema.field(2).data_type(), &DataType::Boolean);
    assert_eq!(singers_schema.field(3).data_type(), &DataType::Float64);

    // A name with an embedded backtick is escaped rather than interpolated into the SQL (which
    // used to break the probe query), so it cleanly reports NotFound like any other absent table.
    let hostile = connection
        .get_table_schema(None, None, "no`such`table")
        .expect_err("hostile table name must not resolve");
    assert_eq!(hostile.status, adbc_core::error::Status::NotFound);

    // The catalog argument is honoured: Spanner's single catalog is the empty string, so `Some("")`
    // behaves like `None`, while any other catalog is NotFound (nothing can exist in it).
    let empty_catalog = connection
        .get_table_schema(Some(""), None, "Singers")
        .expect("get_table_schema with the default empty catalog");
    assert_eq!(empty_catalog, singers_schema);
    let bogus_catalog = connection
        .get_table_schema(Some("nosuchcatalog"), None, "Singers")
        .expect_err("a named catalog does not exist in Spanner");
    assert_eq!(bogus_catalog.status, adbc_core::error::Status::NotFound);

    // --- DDL through the driver (routed to the admin UpdateDatabaseDdl API) ---

    // A batch of two DDL statements submitted as one near-atomic schema change; idempotent so the
    // test can be re-run against a persistent emulator.
    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query(
        "DROP TABLE IF EXISTS AdbcDdl; \
         CREATE TABLE AdbcDdl (Id INT64, Note STRING(MAX)) PRIMARY KEY (Id)",
    )
    .unwrap();
    assert_eq!(ddl.execute_update().expect("ddl"), None); // DDL reports no affected-row count

    // The freshly-created table is immediately usable via the data plane.
    let mut ins = connection.new_statement().expect("new statement");
    ins.set_sql_query("INSERT INTO AdbcDdl (Id, Note) VALUES (7, 'hello')")
        .unwrap();
    assert_eq!(
        ins.execute_update().expect("insert into ddl table"),
        Some(1)
    );

    let mut check = connection.new_statement().expect("new statement");
    check
        .set_sql_query("SELECT Note FROM AdbcDdl WHERE Id = 7")
        .unwrap();
    let ddl_rows: Vec<_> = check
        .execute()
        .expect("select from ddl table")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(ddl_rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    // Drop the scratch table like every other section, so re-runs against a persistent
    // `SPANNER_GCP_DATABASE` don't accumulate leftovers.
    let mut drop_ddl = connection.new_statement().expect("new statement");
    drop_ddl.set_sql_query("DROP TABLE AdbcDdl").unwrap();
    drop_ddl.execute_update().expect("drop ddl table");

    // --- Manual multi-statement transactions ---

    let mut txn_ddl = connection.new_statement().expect("new statement");
    txn_ddl
        .set_sql_query(
            "DROP TABLE IF EXISTS AdbcTxn; CREATE TABLE AdbcTxn (Id INT64) PRIMARY KEY (Id)",
        )
        .unwrap();
    txn_ddl.execute_update().expect("create txn table");

    // Enter manual transaction mode.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");

    // Buffered DML returns None (count unknown until commit) and is not yet visible.
    for id in [1, 2] {
        let mut s = connection.new_statement().expect("new statement");
        s.set_sql_query(format!("INSERT INTO AdbcTxn (Id) VALUES ({id})"))
            .unwrap();
        assert_eq!(s.execute_update().expect("buffered insert"), None);
    }
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        0,
        "buffered rows must not be visible before commit"
    );

    // Commit applies the whole batch atomically.
    connection.commit().expect("commit");
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        2,
        "rows must be visible after commit"
    );

    // A buffered insert followed by rollback leaves no trace.
    let mut rolled = connection.new_statement().expect("new statement");
    rolled
        .set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (3)")
        .unwrap();
    assert_eq!(rolled.execute_update().expect("buffered insert"), None);
    connection.rollback().expect("rollback");
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        2,
        "rolled-back row must not appear"
    );

    // Parameterized DML must honour manual-transaction buffering too. Regression test: a bound
    // INSERT used to bypass the buffer and commit immediately in its own transaction, so it was
    // visible before commit and survived a rollback.
    let buffer_param_insert = |connection: &mut SpannerConnection, id: i64| {
        let row = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![id]))],
        )
        .unwrap();
        let mut s = connection.new_statement().expect("new statement");
        s.set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (@Id)")
            .unwrap();
        s.bind(row).expect("bind param");
        assert_eq!(
            s.execute_update().expect("buffered param insert"),
            None,
            "parameterized DML in manual mode must buffer (return None), not commit immediately"
        );
    };

    // Buffered, then committed: the bound row appears only after commit.
    buffer_param_insert(&mut connection, 3);
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        2,
        "buffered parameterized row must not be visible before commit"
    );
    connection.commit().expect("commit param");
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        3,
        "parameterized row must be visible after commit"
    );

    // Buffered, then rolled back: the bound row leaves no trace.
    buffer_param_insert(&mut connection, 4);
    connection.rollback().expect("rollback param");
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        3,
        "rolled-back parameterized row must not appear"
    );

    // Re-enabling autocommit COMMITS any pending buffered work — it must never discard it. This
    // is the data-loss path: buffer a row, toggle autocommit back on, and the row has to be there.
    let mut pending = connection.new_statement().expect("new statement");
    pending
        .set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (5)")
        .unwrap();
    assert_eq!(pending.execute_update().expect("buffered insert"), None);
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        3,
        "the buffered row must not be visible before the autocommit toggle"
    );
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("enable autocommit");
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        4,
        "re-enabling autocommit must commit the buffered DML, not discard it"
    );

    // The toggle is idempotent (no pending work to re-commit), and per-statement commit is back:
    // DML applies immediately, and commit/rollback without a transaction are errors again.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("re-enable autocommit is a no-op");
    let mut immediate = connection.new_statement().expect("new statement");
    immediate
        .set_sql_query("INSERT INTO AdbcTxn (Id) VALUES (6)")
        .unwrap();
    assert_eq!(
        immediate.execute_update().expect("autocommit insert"),
        Some(1),
        "back in autocommit mode DML reports its count and applies immediately"
    );
    assert_eq!(count_rows(&mut connection, "AdbcTxn"), 5);
    assert!(
        connection.commit().is_err(),
        "commit without an active manual transaction must fail"
    );

    // A FAILED commit must keep the buffered DML: the transaction stays open so the caller can
    // retry (a genuine replay) or roll back. Regression test: the buffer used to be taken
    // *before* the apply, so the DML was lost on error and a retried commit saw an empty batch
    // and reported success with nothing written. Force the failure with DML that buffers fine
    // (buffering never talks to Spanner) but cannot execute — an unknown table.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");
    let buffer_sql = |connection: &mut SpannerConnection, sql: &str| {
        let mut s = connection.new_statement().expect("new statement");
        s.set_sql_query(sql).unwrap();
        assert_eq!(s.execute_update().expect("buffer DML"), None);
    };
    buffer_sql(&mut connection, "INSERT INTO AdbcTxn (Id) VALUES (7)");
    buffer_sql(
        &mut connection,
        "INSERT INTO AdbcTxnNoSuchTable (Id) VALUES (7)",
    );
    assert!(
        connection.commit().is_err(),
        "committing a batch with an unknown table must fail"
    );
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        5,
        "a failed commit must apply nothing (the batch is atomic)"
    );
    assert!(
        connection.commit().is_err(),
        "a retried failed commit must replay the buffer and fail again — not report success \
         on an emptied buffer"
    );
    // The failing statement is still buffered, so enabling autocommit (an implicit commit) must
    // fail too and leave the connection in manual mode with the buffer intact.
    assert!(
        connection
            .set_option(
                OptionConnection::AutoCommit,
                OptionValue::String("true".into()),
            )
            .is_err(),
        "enabling autocommit must fail while the buffered batch cannot commit"
    );
    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .expect("get autocommit"),
        "false",
        "a failed implicit commit must not flip the connection into autocommit mode"
    );
    // Rollback discards the failed batch; after that the retry path is clean again.
    connection.rollback().expect("rollback failed batch");
    buffer_sql(&mut connection, "INSERT INTO AdbcTxn (Id) VALUES (7)");
    connection.commit().expect("commit after rollback");
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        6,
        "the replacement batch must commit normally after the failed one was rolled back"
    );
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("re-enable autocommit");

    let mut drop_txn = connection.new_statement().expect("new statement");
    drop_txn.set_sql_query("DROP TABLE AdbcTxn").unwrap();
    drop_txn.execute_update().expect("drop txn table");

    // --- Native Arrow types for DATE / TIMESTAMP / NUMERIC ---

    let mut types_ddl = connection.new_statement().expect("new statement");
    types_ddl
        .set_sql_query(
            "DROP TABLE IF EXISTS AdbcTypes; \
             CREATE TABLE AdbcTypes (Id INT64, D DATE, T TIMESTAMP, N NUMERIC) PRIMARY KEY (Id)",
        )
        .unwrap();
    types_ddl.execute_update().expect("create types table");

    let mut types_ins = connection.new_statement().expect("new statement");
    types_ins
        .set_sql_query(
            "INSERT INTO AdbcTypes (Id, D, T, N) VALUES \
             (1, DATE '2024-01-15', TIMESTAMP '2024-01-15T12:34:56.789012Z', NUMERIC '1.5')",
        )
        .unwrap();
    assert_eq!(types_ins.execute_update().expect("insert types"), Some(1));

    let mut types_q = connection.new_statement().expect("new statement");
    types_q
        .set_sql_query("SELECT D, T, N FROM AdbcTypes WHERE Id = 1")
        .unwrap();
    let types_reader = types_q.execute().expect("types query");
    let types_schema = types_reader.schema();
    assert_eq!(types_schema.field(0).data_type(), &DataType::Date32);
    assert_eq!(
        types_schema.field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
    assert_eq!(
        types_schema.field(2).data_type(),
        &DataType::Decimal128(38, 9)
    );

    let types_batches = types_reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect types");
    let tb = &types_batches[0];
    let date = tb.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
    let ts = tb
        .column(1)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let num = tb
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(date.value(0), 19737); // days from 1970-01-01 to 2024-01-15
    assert_eq!(ts.value(0), 1_705_322_096_789_012_000); // nanos since epoch
    assert_eq!(num.value(0), 1_500_000_000); // 1.5 unscaled at scale 9

    let mut drop_types = connection.new_statement().expect("new statement");
    drop_types.set_sql_query("DROP TABLE AdbcTypes").unwrap();
    drop_types.execute_update().expect("drop types table");

    // --- Parameter binding and bulk ingest ---

    let mut bind_ddl = connection.new_statement().expect("new statement");
    bind_ddl
        .set_sql_query(
            "DROP TABLE IF EXISTS AdbcBind; \
             CREATE TABLE AdbcBind (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
        )
        .unwrap();
    bind_ddl.execute_update().expect("create bind table");

    // Bulk ingest: two rows of an Arrow batch inserted into the target table.
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();
    let mut ingest = connection.new_statement().expect("new statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcBind".into()),
        )
        .unwrap();
    ingest
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    // Statement options round-trip through get_option (ingest mode reported in canonical form).
    assert_eq!(
        ingest
            .get_option_string(OptionStatement::TargetTable)
            .unwrap(),
        "AdbcBind"
    );
    assert_eq!(
        ingest
            .get_option_string(OptionStatement::IngestMode)
            .unwrap(),
        "adbc.ingest.mode.append"
    );
    // `adbc.ingest.temporary`: the spec default (`false`) is accepted as a no-op and round-trips
    // as "false"; `true` is rejected — Spanner has no temporary tables.
    ingest
        .set_option(
            OptionStatement::Temporary,
            OptionValue::String("false".into()),
        )
        .expect("setting adbc.ingest.temporary=false is a no-op");
    assert_eq!(
        ingest
            .get_option_string(OptionStatement::Temporary)
            .unwrap(),
        "false"
    );
    let temporary_err = ingest
        .set_option(
            OptionStatement::Temporary,
            OptionValue::String("true".into()),
        )
        .expect_err("adbc.ingest.temporary=true must be rejected");
    assert_eq!(
        temporary_err.status,
        adbc_core::error::Status::NotImplemented
    );
    ingest.bind(rows).expect("bind ingest rows");
    assert_eq!(ingest.execute_update().expect("ingest"), Some(2));
    assert_eq!(count_rows(&mut connection, "AdbcBind"), 2);

    // Append onto a missing target table must surface as the ADBC-mandated NotFound (not a generic
    // mapped Spanner INVALID_ARGUMENT). The driver probes INFORMATION_SCHEMA on the failure path and
    // remaps: table absent → NotFound.
    let missing_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut ingest_missing = connection.new_statement().expect("new statement");
    ingest_missing
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcNoSuchIngestTable".into()),
        )
        .unwrap();
    ingest_missing
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    ingest_missing
        .bind(missing_rows)
        .expect("bind rows for missing-table ingest");
    let missing_err = ingest_missing
        .execute_update()
        .expect_err("append onto a missing table must fail");
    assert_eq!(
        missing_err.status,
        adbc_core::error::Status::NotFound,
        "append onto a missing table must be NotFound, got: {missing_err:?}"
    );

    // Append with a schema incompatible with the existing table must surface as AlreadyExists per
    // the ADBC bulk-ingest contract. `AdbcBind` exists but has no `NoSuchColumn`, so the INSERT
    // fails; the probe finds the table present and remaps the failure to AlreadyExists.
    let mismatch_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "NoSuchColumn",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut ingest_mismatch = connection.new_statement().expect("new statement");
    ingest_mismatch
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcBind".into()),
        )
        .unwrap();
    ingest_mismatch
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    ingest_mismatch
        .bind(mismatch_rows)
        .expect("bind rows for schema-mismatch ingest");
    let mismatch_err = ingest_mismatch
        .execute_update()
        .expect_err("append with an incompatible schema must fail");
    assert_eq!(
        mismatch_err.status,
        adbc_core::error::Status::AlreadyExists,
        "append with an incompatible schema must be AlreadyExists, got: {mismatch_err:?}"
    );
    // The rejected appends changed nothing.
    assert_eq!(count_rows(&mut connection, "AdbcBind"), 2);

    // DML issued through the query entry point (`execute`, not the Rust-only `execute_update`) must
    // run on the read/write path and succeed. Every ADBC client — the Python DBAPI, R, etc. — issues
    // DML this way, since the C ABI exposes only `ExecuteQuery`. Regression test for routing it to a
    // read-only single-use transaction, which Spanner rejects ("DML statements may not be performed
    // in single-use transactions").
    let mut dml_via_execute = connection.new_statement().expect("new statement");
    dml_via_execute
        .set_sql_query("INSERT INTO AdbcBind (Id, Name) VALUES (3, 'Carol')")
        .unwrap();
    let dml_rows = dml_via_execute
        .execute()
        .expect("DML via execute()")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        dml_rows.iter().all(|b| b.num_rows() == 0),
        "DML via execute() must yield an empty result set"
    );
    assert_eq!(count_rows(&mut connection, "AdbcBind"), 3);

    // Bound data is consumed by the execute that uses it. A client that reuses one statement handle
    // (as the Python DBAPI does: adbc_ingest binds a stream, then the next cursor.execute is a query)
    // must not replay the stale bound rows — which previously ran the follow-up query once per bound
    // row, tripling its result.
    let mut reuse_ddl = connection.new_statement().expect("new statement");
    reuse_ddl
        .set_sql_query(
            "DROP TABLE IF EXISTS AdbcReuse; \
             CREATE TABLE AdbcReuse (Id INT64) PRIMARY KEY (Id)",
        )
        .unwrap();
    reuse_ddl.execute_update().expect("create reuse table");
    let reuse_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
    )
    .unwrap();
    let mut reuse = connection.new_statement().expect("new statement");
    reuse
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcReuse".into()),
        )
        .unwrap();
    reuse
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    reuse.bind(reuse_rows).expect("bind reuse rows");
    assert_eq!(reuse.execute_update().expect("ingest reuse"), Some(3));
    // Same handle, now a query: the ingest consumed the bound rows, so it runs exactly once.
    reuse
        .set_sql_query("SELECT Id FROM AdbcReuse ORDER BY Id")
        .unwrap();
    let reuse_out = reuse
        .execute()
        .expect("reuse query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        reuse_out.iter().map(|b| b.num_rows()).sum::<usize>(),
        3,
        "stale bound rows must not replay the follow-up query"
    );
    let mut drop_reuse = connection.new_statement().expect("new statement");
    drop_reuse.set_sql_query("DROP TABLE AdbcReuse").unwrap();
    drop_reuse.execute_update().expect("drop reuse table");

    // Bulk ingest driven through the query entry point (`execute`, not `execute_update`): an ADBC
    // FFI caller supplying a non-null stream out-pointer must get the ingest performed and an empty
    // stream back, not InvalidState ("no SQL query set"). Regression test for ingest only triggering
    // through `execute_update`.
    let mut ingest_via_execute = connection.new_statement().expect("new statement");
    ingest_via_execute
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcBind".into()),
        )
        .unwrap();
    ingest_via_execute
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    let ingest_via_execute_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["Dave"])),
        ],
    )
    .unwrap();
    ingest_via_execute
        .bind(ingest_via_execute_rows)
        .expect("bind rows for ingest via execute");
    let ingest_stream = ingest_via_execute
        .execute()
        .expect("ingest via execute()")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        ingest_stream.iter().all(|b| b.num_rows() == 0),
        "ingest via execute() must yield an empty result set"
    );
    assert_eq!(count_rows(&mut connection, "AdbcBind"), 4);

    // Statement handle reused for an ingest after a SQL query — the pattern the Python DBAPI
    // `Cursor` produces (one statement per cursor: `cur.execute("CREATE TABLE …")` sets the query,
    // then `cur.adbc_ingest(…)` sets the ingest target without clearing it). Setting the target must
    // win over the stale query, so the bound rows are ingested rather than the query re-run.
    // Regression test for the ingest branch being skipped while a prior query is still set.
    let mut reuse_query_then_ingest = connection.new_statement().expect("new statement");
    reuse_query_then_ingest
        .set_sql_query("SELECT 1")
        .expect("set stale query");
    reuse_query_then_ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcBind".into()),
        )
        .unwrap();
    reuse_query_then_ingest
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    let reuse_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(StringArray::from(vec!["Erin"])),
        ],
    )
    .unwrap();
    reuse_query_then_ingest
        .bind(reuse_rows)
        .expect("bind rows for reuse ingest");
    reuse_query_then_ingest
        .execute_update()
        .expect("ingest after a prior query on a reused statement");
    assert_eq!(count_rows(&mut connection, "AdbcBind"), 5);

    // Create-mode bulk ingest: the driver builds the table from the bound Arrow schema (with a
    // synthetic UUID primary key), so no CREATE TABLE is needed first. Exercises create/append/replace.
    let mut drop_create = connection.new_statement().expect("new statement");
    drop_create
        .set_sql_query("DROP TABLE IF EXISTS AdbcCreate")
        .unwrap();
    drop_create.execute_update().expect("pre-drop create table");
    let create_rows = || {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("Id", DataType::Int64, false),
                Field::new("Label", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap()
    };
    // Bind `create_rows()` into `table` with the given ingest mode, returning the raw result so
    // callers can assert either the affected count or an error status.
    let ingest_into = |connection: &mut SpannerConnection, table: &str, mode: &str| {
        let mut s = connection.new_statement().expect("new statement");
        s.set_option(
            OptionStatement::TargetTable,
            OptionValue::String(table.into()),
        )
        .unwrap();
        s.set_option(
            OptionStatement::IngestMode,
            OptionValue::String(mode.into()),
        )
        .unwrap();
        s.bind(create_rows()).expect("bind ingest rows");
        s.execute_update()
    };
    let ingest_create = |connection: &mut SpannerConnection, mode: &str| {
        ingest_into(connection, "AdbcCreate", mode)
    };
    assert_eq!(
        ingest_create(&mut connection, "create").expect("create"),
        Some(2)
    ); // creates table + 2 rows
    assert_eq!(count_rows(&mut connection, "AdbcCreate"), 2);
    assert_eq!(
        ingest_create(&mut connection, "append").expect("append"),
        Some(2)
    ); // appends
    assert_eq!(count_rows(&mut connection, "AdbcCreate"), 4);
    assert_eq!(
        ingest_create(&mut connection, "replace").expect("replace"),
        Some(2)
    ); // drops + recreates
    assert_eq!(count_rows(&mut connection, "AdbcCreate"), 2);
    // The data columns read back even though the table also has the synthetic key column.
    let mut read_create = connection.new_statement().expect("new statement");
    read_create
        .set_sql_query("SELECT Id, Label FROM AdbcCreate ORDER BY Id")
        .unwrap();
    let created = read_create
        .execute()
        .expect("read created")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(created.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    // `create` mode on an already-existing table is the error path: the driver emits a
    // `CREATE TABLE` (no `IF NOT EXISTS`), which Spanner rejects because `AdbcCreate` still exists.
    // Unlike append-mode ingest, create-mode failures are not remapped, so the underlying DDL error
    // surfaces directly — we only assert that it fails and that nothing was inserted.
    let create_on_existing = ingest_into(&mut connection, "AdbcCreate", "create")
        .expect_err("create-mode ingest onto an existing table must fail");
    assert_eq!(
        count_rows(&mut connection, "AdbcCreate"),
        2,
        "a failed create-mode ingest must leave the table unchanged (got error: {create_on_existing:?})"
    );
    let mut drop_created = connection.new_statement().expect("new statement");
    drop_created.set_sql_query("DROP TABLE AdbcCreate").unwrap();
    drop_created.execute_update().expect("drop create table");

    // `create_append` mode end-to-end: it creates the table from the bound Arrow schema when absent
    // (like `create`), but — unlike `create` — is a no-op-on-conflict for the table itself, so a
    // second ingest into the now-existing table simply appends. Exercises both halves.
    let mut drop_create_append = connection.new_statement().expect("new statement");
    drop_create_append
        .set_sql_query("DROP TABLE IF EXISTS AdbcCreateAppend")
        .unwrap();
    drop_create_append
        .execute_update()
        .expect("pre-drop create_append table");
    // First ingest: table absent → created + 2 rows.
    assert_eq!(
        ingest_into(&mut connection, "AdbcCreateAppend", "create_append").expect("create_append"),
        Some(2)
    );
    assert_eq!(count_rows(&mut connection, "AdbcCreateAppend"), 2);
    // Second ingest: table now present → append (no error, unlike `create`).
    assert_eq!(
        ingest_into(&mut connection, "AdbcCreateAppend", "create_append")
            .expect("create_append onto an existing table appends"),
        Some(2)
    );
    assert_eq!(count_rows(&mut connection, "AdbcCreateAppend"), 4);
    // The data columns read back even though the table also carries the synthetic key column.
    let mut read_create_append = connection.new_statement().expect("new statement");
    read_create_append
        .set_sql_query("SELECT Id, Label FROM AdbcCreateAppend ORDER BY Id")
        .unwrap();
    let create_appended = read_create_append
        .execute()
        .expect("read create_append")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        create_appended.iter().map(|b| b.num_rows()).sum::<usize>(),
        4
    );
    let mut drop_create_appended = connection.new_statement().expect("new statement");
    drop_create_appended
        .set_sql_query("DROP TABLE AdbcCreateAppend")
        .unwrap();
    drop_create_appended
        .execute_update()
        .expect("drop create_append table");

    // Parameterized query: bind @Id and read the matching row back.
    let param = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .unwrap();
    let mut pq = connection.new_statement().expect("new statement");
    pq.set_sql_query("SELECT Name FROM AdbcBind WHERE Id = @Id")
        .unwrap();
    // Before binding, get_parameter_schema derives the parameter names from the SQL; Spanner does
    // not expose parameter types ahead of execution, so the type is Null (Arrow's "unknown").
    let ps = pq
        .get_parameter_schema()
        .expect("parameter schema from SQL");
    assert_eq!(ps.fields().len(), 1);
    assert_eq!(ps.field(0).name(), "Id");
    assert_eq!(ps.field(0).data_type(), &DataType::Null);
    pq.bind(param).expect("bind query param");
    // Once data is bound, the parameter schema reflects the bound column's real type.
    let ps = pq
        .get_parameter_schema()
        .expect("parameter schema from bound data");
    assert_eq!(ps.field(0).name(), "Id");
    assert_eq!(ps.field(0).data_type(), &DataType::Int64);
    let pq_batches = pq
        .execute()
        .expect("param query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(pq_batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        pq_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Bob"
    );

    // Positional binding: the bound column is *not* named after the query's `@parameter`, so the
    // driver binds the (sole) column to the (sole) parameter by position — the ADBC ordinal contract
    // that positional clients (and the Foundry validation suite) rely on. Reads Id = 2 -> "Bob".
    let positional = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .unwrap();
    let mut pp = connection.new_statement().expect("new statement");
    pp.set_sql_query("SELECT Name FROM AdbcBind WHERE Id = @p1")
        .unwrap();
    pp.bind(positional).expect("bind positional param");
    let pp_batches = pp
        .execute()
        .expect("positional query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(pp_batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        pp_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Bob"
    );

    // Parameterized DML: update by bound @Id / @Name.
    let upd = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alicia"])),
        ],
    )
    .unwrap();
    let mut pu = connection.new_statement().expect("new statement");
    pu.set_sql_query("UPDATE AdbcBind SET Name = @Name WHERE Id = @Id")
        .unwrap();
    pu.bind(upd).expect("bind update params");
    assert_eq!(pu.execute_update().expect("param update"), Some(1));

    let mut drop_bind = connection.new_statement().expect("new statement");
    drop_bind.set_sql_query("DROP TABLE AdbcBind").unwrap();
    drop_bind.execute_update().expect("drop bind table");

    // Preparing a statement before its query is set is an InvalidState error (ADBC precondition).
    let mut unprepared = connection.new_statement().expect("new statement");
    assert_eq!(
        unprepared.prepare().unwrap_err().status,
        adbc_core::error::Status::InvalidState,
    );
    // A bulk-ingest statement needs no SQL query, so preparing one (target set, no query) is OK.
    let mut ingest_prepare = connection.new_statement().expect("new statement");
    ingest_prepare
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcBind".into()),
        )
        .unwrap();
    ingest_prepare.prepare().expect("prepare ingest statement");

    // Bulk ingest must quote identifiers, so reserved words survive as table/column names. This is
    // the value of the ADBC suite's ingest-escaping tests, which we can only run in append mode
    // (Spanner requires a primary key, so it has no create-mode ingest). Table `create` and column
    // `index` are both reserved words.
    let mut esc_ddl = connection.new_statement().expect("new statement");
    esc_ddl
        .set_sql_query(
            "DROP TABLE IF EXISTS `create`; \
             CREATE TABLE `create` (`index` INT64) PRIMARY KEY (`index`)",
        )
        .unwrap();
    esc_ddl
        .execute_update()
        .expect("create reserved-word table");
    let esc_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "index",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![42, -42]))],
    )
    .unwrap();
    let mut esc_ingest = connection.new_statement().expect("new statement");
    esc_ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("create".into()),
        )
        .unwrap();
    esc_ingest.bind(esc_rows).expect("bind reserved-word rows");
    assert_eq!(
        esc_ingest.execute_update().expect("ingest reserved"),
        Some(2)
    );
    assert_eq!(count_rows(&mut connection, "`create`"), 2);
    let mut drop_esc = connection.new_statement().expect("new statement");
    drop_esc.set_sql_query("DROP TABLE `create`").unwrap();
    drop_esc.execute_update().expect("drop reserved-word table");

    // --- ARRAY<scalar> maps to a native Arrow List ---

    let mut arr_ddl = connection.new_statement().expect("new statement");
    arr_ddl
        .set_sql_query(
            "DROP TABLE IF EXISTS AdbcArr; \
             CREATE TABLE AdbcArr (Id INT64, Nums ARRAY<INT64>, Tags ARRAY<STRING(MAX)>) \
             PRIMARY KEY (Id)",
        )
        .unwrap();
    arr_ddl.execute_update().expect("create array table");

    let mut arr_ins = connection.new_statement().expect("new statement");
    arr_ins
        .set_sql_query("INSERT INTO AdbcArr (Id, Nums, Tags) VALUES (1, [10, 20, 30], ['a', 'b'])")
        .unwrap();
    assert_eq!(arr_ins.execute_update().expect("insert arrays"), Some(1));

    let mut arr_q = connection.new_statement().expect("new statement");
    arr_q
        .set_sql_query("SELECT Nums, Tags FROM AdbcArr WHERE Id = 1")
        .unwrap();
    let arr_reader = arr_q.execute().expect("array query");
    let arr_schema = arr_reader.schema();
    assert_eq!(
        arr_schema.field(0).data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
    );
    assert_eq!(
        arr_schema.field(1).data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    );
    let arr_batches = arr_reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect arrays");
    let ab = &arr_batches[0];
    let nums = ab.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let nums0 = nums.value(0);
    let nums0 = nums0.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(nums0.values(), &[10, 20, 30]);
    let tags = ab.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    let tags0 = tags.value(0);
    let tags0 = tags0.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!((tags0.value(0), tags0.value(1)), ("a", "b"));

    let mut drop_arr = connection.new_statement().expect("new statement");
    drop_arr.set_sql_query("DROP TABLE AdbcArr").unwrap();
    drop_arr.execute_update().expect("drop array table");

    // --- STRUCT → native Arrow Struct (Spanner only returns structs inside an ARRAY) ---

    // Build the array-of-structs with `ARRAY(SELECT AS STRUCT ...)`. A top-level array *literal* of
    // `STRUCT(...)` constructions (`SELECT [STRUCT(1 AS a, ...), ...]`) is accepted by the emulator
    // but rejected by real Spanner with UNIMPLEMENTED ("Spanner does not yet support returning STRUCT
    // except as arrays-of-structs ... use ARRAY(SELECT AS STRUCT expr ...)"). `ARRAY(subquery)` has
    // no inherent row order, so an explicit ordering column keeps the two elements deterministic.
    let mut arr_struct_q = connection.new_statement().expect("new statement");
    arr_struct_q
        .set_sql_query(
            "SELECT ARRAY(\
                 SELECT AS STRUCT a, b FROM (\
                     SELECT 0 AS n, 1 AS a, 'x' AS b \
                     UNION ALL SELECT 1 AS n, 2 AS a, 'y' AS b\
                 ) ORDER BY n\
             ) AS arr",
        )
        .unwrap();
    let arr_struct_reader = arr_struct_q.execute().expect("array-of-struct query");

    // The element type is Struct<a: Int64, b: Utf8>, with field names from the metadata.
    match arr_struct_reader.schema().field(0).data_type() {
        DataType::List(field) => match field.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "a");
                assert_eq!(fields[0].data_type(), &DataType::Int64);
                assert_eq!(fields[1].name(), "b");
                assert_eq!(fields[1].data_type(), &DataType::Utf8);
            }
            other => panic!("expected List<Struct>, got List<{other:?}>"),
        },
        other => panic!("expected List, got {other:?}"),
    }

    let arr_struct_batches = arr_struct_reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect array-of-struct");
    let list = arr_struct_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let inner = list.value(0);
    let inner = inner.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(inner.len(), 2);
    let a = inner
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let b = inner
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!((a.value(0), a.value(1)), (1, 2));
    assert_eq!((b.value(0), b.value(1)), ("x", "y"));

    // --- Batched multi-statement DML in one execute_update (atomic DELETE; INSERT) ---

    let mut batch_ddl = connection.new_statement().expect("new statement");
    batch_ddl
        .set_sql_query(
            "DROP TABLE IF EXISTS AdbcBatch; CREATE TABLE AdbcBatch (Id INT64) PRIMARY KEY (Id)",
        )
        .unwrap();
    batch_ddl.execute_update().expect("create batch table");

    let mut seed = connection.new_statement().expect("new statement");
    seed.set_sql_query("INSERT INTO AdbcBatch (Id) VALUES (1)")
        .unwrap();
    assert_eq!(seed.execute_update().expect("seed"), Some(1));

    // A single execute_update running two statements: the delete and the insert commit together,
    // and the affected count is their sum (1 deleted + 2 inserted).
    let mut batch = connection.new_statement().expect("new statement");
    batch
        .set_sql_query(
            "DELETE FROM AdbcBatch WHERE true; INSERT INTO AdbcBatch (Id) VALUES (2), (3)",
        )
        .unwrap();
    assert_eq!(batch.execute_update().expect("batch dml"), Some(3));
    assert_eq!(count_rows(&mut connection, "AdbcBatch"), 2);

    let mut drop_batch = connection.new_statement().expect("new statement");
    drop_batch.set_sql_query("DROP TABLE AdbcBatch").unwrap();
    drop_batch.execute_update().expect("drop batch table");

    // --- execute_schema returns a query's schema without running it (incl. a top-level WITH) ---

    let mut schema_stmt = connection.new_statement().expect("new statement");
    schema_stmt
        .set_sql_query("WITH cte AS (SELECT 1 AS a, 'x' AS b) SELECT a, b FROM cte")
        .unwrap();
    let planned = schema_stmt.execute_schema().expect("execute_schema");
    assert_eq!(planned.fields().len(), 2);
    assert_eq!(planned.field(0).name(), "a");
    assert_eq!(planned.field(0).data_type(), &DataType::Int64);
    assert_eq!(planned.field(1).name(), "b");
    assert_eq!(planned.field(1).data_type(), &DataType::Utf8);

    // DML through execute_schema is rejected up front with a clear InvalidArguments error rather
    // than surfacing Spanner's raw read-only-transaction error from the PLAN probe.
    let mut dml_schema = connection.new_statement().expect("new statement");
    dml_schema
        .set_sql_query("INSERT INTO Singers (SingerId, Name) VALUES (999, 'x')")
        .unwrap();
    let error = dml_schema
        .execute_schema()
        .expect_err("execute_schema must reject DML");
    assert_eq!(error.status, adbc_core::error::Status::InvalidArguments);
    assert!(
        error.message.contains("only supports queries"),
        "unexpected message: {}",
        error.message
    );

    // --- get_objects: catalog → schema → table → columns from INFORMATION_SCHEMA ---

    let objects = connection
        .get_objects(
            ObjectDepth::All,
            None,
            Some(""),
            Some("Singers"),
            None,
            None,
        )
        .expect("get_objects");
    let object_batches = objects
        .collect::<Result<Vec<_>, _>>()
        .expect("collect objects");
    let ob = &object_batches[0];
    assert_eq!(ob.num_rows(), 1, "single catalog");

    // catalog_db_schemas: List<Struct{db_schema_name, db_schema_tables}> — only the "" schema.
    let schemas = ob.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    let schemas = schemas.value(0);
    let schemas = schemas.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(schemas.len(), 1);

    // db_schema_tables (field 1): List<Struct{table_name, table_type, table_columns, ...}>.
    let tables = schemas
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let tables = tables.value(0);
    let tables = tables.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(tables.len(), 1);
    let table_name = tables
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(table_name.value(0), "Singers");

    // table_columns (field 2): List<Struct{column_name, ...}> — the four Singers columns.
    let columns = tables
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let columns = columns.value(0);
    let columns = columns.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(columns.len(), 4);
    let column_name = columns
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(column_name.value(0), "SingerId");

    // --- get_objects at Catalogs depth: the single unnamed catalog with a NULL db_schemas
    // list (this depth needs no INFORMATION_SCHEMA data and issues no queries at all).
    let catalogs = connection
        .get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
        .expect("get_objects at Catalogs depth")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect catalogs");
    let cb = &catalogs[0];
    assert_eq!(cb.num_rows(), 1, "single catalog");
    let catalog_name = cb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        catalog_name.value(0),
        "",
        "Spanner's single unnamed catalog"
    );
    let cb_schemas = cb.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(
        cb_schemas.is_null(0),
        "catalog_db_schemas must be NULL at Catalogs depth"
    );

    // A catalog filter that excludes "" yields zero rows even at Catalogs depth.
    let none = connection
        .get_objects(ObjectDepth::Catalogs, Some("nope"), None, None, None, None)
        .expect("get_objects with excluding catalog filter")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect filtered catalogs");
    assert_eq!(
        none.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "a catalog filter excluding \"\" must match nothing"
    );

    // --- get_objects at Schemas depth: schemas are populated (the default "" schema is
    // present) but each schema's table list is NULL.
    let db_schemas = connection
        .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
        .expect("get_objects at Schemas depth")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect db schemas");
    let sb = &db_schemas[0];
    assert_eq!(sb.num_rows(), 1, "single catalog");
    let sb_schemas = sb.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(
        sb_schemas.is_valid(0),
        "catalog_db_schemas must be populated at DBSchemas depth"
    );
    let sb_schemas = sb_schemas.value(0);
    let sb_schemas = sb_schemas.as_any().downcast_ref::<StructArray>().unwrap();
    let schema_names = sb_schemas
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        (0..schema_names.len()).any(|i| schema_names.value(i).is_empty()),
        "the default \"\" schema must be reported"
    );
    let schema_tables = sb_schemas
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(
        (0..schema_tables.len()).all(|i| schema_tables.is_null(i)),
        "db_schema_tables must be NULL at DBSchemas depth"
    );

    // --- Round trip: every value get_table_types reports works as a get_objects table_type
    // filter. The ADBC spec says valid filter values come from get_table_types, so the two
    // vocabularies must agree — filtering on the reported type of a base table must find it.
    let reported_types: Vec<String> = connection
        .get_table_types()
        .expect("get_table_types")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect table types")
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            (0..col.len())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        reported_types.contains(&"BASE TABLE".to_string()),
        "get_table_types should report Spanner's INFORMATION_SCHEMA vocabulary: {reported_types:?}"
    );
    let filtered = connection
        .get_objects(
            ObjectDepth::Tables,
            None,
            None,
            Some("Singers"),
            Some(reported_types.iter().map(String::as_str).collect()),
            None,
        )
        .expect("get_objects with table_type filter");
    let filtered = filtered
        .collect::<Result<Vec<_>, _>>()
        .expect("collect filtered objects");
    assert_eq!(
        objects_table_names(&filtered),
        vec!["Singers".to_string()],
        "types reported by get_table_types must round-trip as get_objects filters"
    );
}

/// A bulk ingest big enough to cross the driver's per-chunk byte budget (~4 MiB) is split into
/// several `ExecuteBatchDml` transactions. The split must be invisible in the result: the returned
/// affected-row count is the sum across chunks, and every row lands exactly once with its full
/// payload. (The mutation-count arithmetic that also cuts chunks is unit-tested offline in
/// `src/statement.rs`; crossing it here would need thousands of rows, while the byte budget crosses
/// with a handful of wide ones.)
#[test]
fn bulk_ingest_chunks_past_the_byte_budget() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping bulk_ingest_chunks_past_the_byte_budget");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query(
        "DROP TABLE IF EXISTS AdbcChunked; \
         CREATE TABLE AdbcChunked (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    )
    .unwrap();
    ddl.execute_update().expect("create chunked table");

    // Six ~1.1 MB rows: ~6.6 MB of bound data guarantees at least two chunks under the ~4 MiB
    // budget without needing thousands of rows or a test-only override of the production constants,
    // and lands 3 rows (~3.3 MB) per chunk — comfortably away from both the budget edge and gRPC
    // 4 MB message-size defaults, whatever Arrow's buffer-capacity rounding does to the estimate.
    const ROWS: usize = 6;
    const VALUE_LEN: usize = 1_100_000;
    let names: Vec<String> = (0..ROWS)
        .map(|i| char::from(b'a' + i as u8).to_string().repeat(VALUE_LEN))
        .collect();
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                names.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let mut ingest = connection.new_statement().expect("new statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcChunked".into()),
        )
        .unwrap();
    ingest
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    ingest.bind(rows).expect("bind chunked ingest rows");
    assert_eq!(
        ingest.execute_update().expect("chunked ingest"),
        Some(ROWS as i64),
        "the affected-row count must sum across the ingest's chunk transactions"
    );

    // Every row landed exactly once with its full payload (no chunk dropped or double-applied).
    let mut read = connection.new_statement().expect("new statement");
    read.set_sql_query(
        "SELECT Id, CHAR_LENGTH(Name) AS Len, SUBSTR(Name, 1, 1) AS Head \
         FROM AdbcChunked ORDER BY Id",
    )
    .unwrap();
    let batches = read
        .execute()
        .expect("read chunked rows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut seen = 0_usize;
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lens = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let heads = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(ids.value(row), seen as i64);
            assert_eq!(lens.value(row), VALUE_LEN as i64);
            assert_eq!(heads.value(row), char::from(b'a' + seen as u8).to_string());
            seen += 1;
        }
    }
    assert_eq!(seen, ROWS, "all ingested rows must be readable back");

    let mut drop_chunked = connection.new_statement().expect("new statement");
    drop_chunked
        .set_sql_query("DROP TABLE AdbcChunked")
        .unwrap();
    drop_chunked.execute_update().expect("drop chunked table");
}

/// Flatten a collected `get_objects` result into the table names it contains, across all
/// catalogs and schemas.
fn objects_table_names(batches: &[RecordBatch]) -> Vec<String> {
    let mut names = Vec::new();
    for batch in batches {
        let schema_lists = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for c in 0..batch.num_rows() {
            let schemas = schema_lists.value(c);
            let schemas = schemas.as_any().downcast_ref::<StructArray>().unwrap();
            let table_lists = schemas
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for s in 0..schemas.len() {
                if table_lists.is_null(s) {
                    continue;
                }
                let tables = table_lists.value(s);
                let tables = tables.as_any().downcast_ref::<StructArray>().unwrap();
                let table_names = tables
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for r in 0..tables.len() {
                    names.push(table_names.value(r).to_string());
                }
            }
        }
    }
    names
}

/// Number of generated cases per property. Each case is a full DELETE + INSERT + SELECT round trip
/// over the network, so this is kept modest to bound the emulator wall-clock.
const PROP_CASES: u32 = 64;

/// A `ProptestConfig` for the emulator round-trips: a bounded case count and no on-disk regression
/// file (a persisted seed is useless anyway — reproducing it needs a live emulator).
fn prop_config() -> ProptestConfig {
    ProptestConfig {
        cases: PROP_CASES,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// Run a statement that returns no rows (DDL / DML), panicking on error.
fn run(connection: &mut SpannerConnection, sql: &str) {
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query(sql).unwrap();
    s.execute_update()
        .unwrap_or_else(|e| panic!("failed to run {sql:?}: {e:?}"));
}

/// Property: arbitrary values bound as parameters (the `bind.rs` write path) survive a round trip
/// through Spanner and come back byte-for-byte via the Arrow read path (`conversion.rs`), nulls
/// included. Covers every Arrow type the bind path supports: Int64, Float64, Bool, Utf8, Binary,
/// Date32, Timestamp (bound as Microsecond, read back as Nanosecond), and Decimal128 across
/// NUMERIC's full range. Timestamps are confined to the Arrow nanosecond-representable window
/// (~1678–2261) since the read path now returns `Timestamp(Nanosecond)`.
#[test]
fn prop_bind_round_trip() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping prop_bind_round_trip");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let connection = RefCell::new(connect_with_retry(&database));
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    // Finite floats only: NaN/Inf don't compare with `==` and take a separate string wire path.
    let float = any::<f64>().prop_filter("finite", |f| f.is_finite());
    // Any non-control Unicode, up to 24 chars.
    let text = proptest::string::string_regex("\\PC{0,24}").unwrap();
    let bytes = proptest::collection::vec(any::<u8>(), 0..24);
    // Date / timestamp components in Spanner's supported range; NUMERIC across its full range
    // (integer part < 10^28, well beyond what a 96-bit decimal could hold).
    let date = (1i32..=9999, 1u32..=12, 1u32..=28);
    // Timestamps are constrained to the Arrow nanosecond-representable window (~1677-09-21 to
    // 2262-04-11), well inside years 1678..=2261, so the nanosecond read path never overflows.
    let ts = (
        1678i32..=2261,
        1u32..=12,
        1u32..=28,
        0u32..24,
        0u32..60,
        0u32..60,
        0u32..1_000_000,
    );
    let numeric = (any::<bool>(), 0u128..10u128.pow(28), 0u32..1_000_000_000);

    proptest!(prop_config(), |(
        oi in proptest::option::of(any::<i64>()),
        of in proptest::option::of(float),
        ob in proptest::option::of(any::<bool>()),
        os in proptest::option::of(text),
        oby in proptest::option::of(bytes),
        od in proptest::option::of(date),
        ot in proptest::option::of(ts),
        on in proptest::option::of(numeric),
    )| {
        let mut conn = connection.borrow_mut();

        // Derive the exact Arrow encodings the read path should return for each temporal value.
        let exp_days = od.map(|(y, m, d)| {
            (NaiveDate::from_ymd_opt(y, m, d).unwrap() - epoch).num_days() as i32
        });
        let exp_micros = ot.map(|(y, mo, d, h, mi, s, us)| {
            NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_micro_opt(h, mi, s, us)
                .unwrap()
                .and_utc()
                .timestamp_micros()
        });
        let exp_unscaled = on.map(|(neg, int_mag, frac)| {
            let mag = int_mag as i128 * 1_000_000_000 + frac as i128;
            if neg { -mag } else { mag }
        });

        let num_col = Decimal128Array::from(vec![exp_unscaled])
            .with_precision_and_scale(38, 9)
            .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("Id", DataType::Int64, false),
                Field::new("IntCol", DataType::Int64, true),
                Field::new("FloatCol", DataType::Float64, true),
                Field::new("BoolCol", DataType::Boolean, true),
                Field::new("StrCol", DataType::Utf8, true),
                Field::new("BytesCol", DataType::Binary, true),
                Field::new("DateCol", DataType::Date32, true),
                Field::new(
                    "TsCol",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new("NumCol", DataType::Decimal128(38, 9), true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int64Array::from(vec![oi])),
                Arc::new(Float64Array::from(vec![of])),
                Arc::new(BooleanArray::from(vec![ob])),
                Arc::new(StringArray::from(vec![os.as_deref()])),
                Arc::new(BinaryArray::from_opt_vec(vec![oby.as_deref()])),
                Arc::new(Date32Array::from(vec![exp_days])),
                Arc::new(TimestampMicrosecondArray::from(vec![exp_micros])),
                Arc::new(num_col),
            ],
        )
        .unwrap();

        run(&mut conn, "DELETE FROM AdbcPropBind WHERE true");
        let mut ins = conn.new_statement().expect("new statement");
        ins.set_sql_query(
            "INSERT INTO AdbcPropBind \
                 (Id, IntCol, FloatCol, BoolCol, StrCol, BytesCol, DateCol, TsCol, NumCol) \
             VALUES (@Id, @IntCol, @FloatCol, @BoolCol, @StrCol, @BytesCol, @DateCol, @TsCol, @NumCol)",
        )
        .unwrap();
        ins.bind(batch).expect("bind row");
        prop_assert_eq!(ins.execute_update().expect("insert"), Some(1));

        let mut q = conn.new_statement().expect("new statement");
        q.set_sql_query(
            "SELECT IntCol, FloatCol, BoolCol, StrCol, BytesCol, DateCol, TsCol, NumCol \
             FROM AdbcPropBind WHERE Id = 1",
        )
        .unwrap();
        let batches = q
            .execute()
            .expect("select")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        prop_assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let b = &batches[0];

        let i = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let f = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let bo = b.column(2).as_any().downcast_ref::<BooleanArray>().unwrap();
        let s = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let by = b.column(4).as_any().downcast_ref::<BinaryArray>().unwrap();
        let d = b.column(5).as_any().downcast_ref::<Date32Array>().unwrap();
        let t = b
            .column(6)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let n = b
            .column(7)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        match oi {
            Some(v) => prop_assert_eq!(i.value(0), v),
            None => prop_assert!(i.is_null(0)),
        }
        match of {
            Some(v) => prop_assert_eq!(f.value(0), v),
            None => prop_assert!(f.is_null(0)),
        }
        match ob {
            Some(v) => prop_assert_eq!(bo.value(0), v),
            None => prop_assert!(bo.is_null(0)),
        }
        match &os {
            Some(v) => prop_assert_eq!(s.value(0), v.as_str()),
            None => prop_assert!(s.is_null(0)),
        }
        match &oby {
            Some(v) => prop_assert_eq!(by.value(0), v.as_slice()),
            None => prop_assert!(by.is_null(0)),
        }
        match exp_days {
            Some(v) => prop_assert_eq!(d.value(0), v),
            None => prop_assert!(d.is_null(0)),
        }
        // The value was bound at microsecond precision; the read path returns nanoseconds, so the
        // expected nanosecond count is exactly the microsecond count scaled by 1000.
        match exp_micros {
            Some(v) => prop_assert_eq!(t.value(0), v * 1_000),
            None => prop_assert!(t.is_null(0)),
        }
        match exp_unscaled {
            Some(v) => prop_assert_eq!(n.value(0), v),
            None => prop_assert!(n.is_null(0)),
        }
    });
}

/// Property: arbitrary DATE / TIMESTAMP / NUMERIC values, inserted as SQL literals (the bind path
/// doesn't accept these Arrow types), come back through the read path as the exact Arrow encoding —
/// epoch days, epoch nanos, and unscaled scale-9 `i128` respectively. Values are confined to
/// Spanner's supported ranges (dates years 1..=9999, NUMERIC magnitude < 10^28); timestamps are
/// further confined to the Arrow nanosecond-representable window (~1678–2261). Nulls included.
#[test]
fn prop_temporal_round_trip() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping prop_temporal_round_trip");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let connection = RefCell::new(connect_with_retry(&database));
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    proptest!(prop_config(), |(
        od in proptest::option::of((1i32..=9999, 1u32..=12, 1u32..=28)),
        ot in proptest::option::of((1678i32..=2261, 1u32..=12, 1u32..=28, 0u32..24, 0u32..60, 0u32..60, 0u32..1_000_000)),
        on in proptest::option::of((any::<bool>(), 0u128..10u128.pow(28), 0u32..1_000_000_000)),
    )| {
        let mut conn = connection.borrow_mut();

        // Build each column's literal and the expected Arrow value.
        let (d_lit, exp_days) = match od {
            Some((y, m, d)) => {
                let date = NaiveDate::from_ymd_opt(y, m, d).unwrap();
                let days = (date - epoch).num_days() as i32;
                (format!("DATE '{}'", date.format("%Y-%m-%d")), Some(days))
            }
            None => ("NULL".to_string(), None),
        };
        let (t_lit, exp_nanos) = match ot {
            Some((y, mo, d, h, mi, s, us)) => {
                let dt = NaiveDate::from_ymd_opt(y, mo, d)
                    .unwrap()
                    .and_hms_micro_opt(h, mi, s, us)
                    .unwrap()
                    .and_utc();
                (
                    format!("TIMESTAMP '{}'", dt.to_rfc3339_opts(SecondsFormat::Micros, true)),
                    // In range by construction, so `timestamp_nanos_opt` is always `Some`.
                    Some(dt.timestamp_nanos_opt().unwrap()),
                )
            }
            None => ("NULL".to_string(), None),
        };
        let (n_lit, exp_unscaled) = match on {
            Some((neg, int_mag, frac)) => {
                let mag = int_mag as i128 * 1_000_000_000 + frac as i128;
                let unscaled = if neg { -mag } else { mag };
                let sign = if neg { "-" } else { "" };
                (format!("NUMERIC '{sign}{int_mag}.{frac:09}'"), Some(unscaled))
            }
            None => ("NULL".to_string(), None),
        };

        run(&mut conn, "DELETE FROM AdbcPropTypes WHERE true");
        run(
            &mut conn,
            &format!(
                "INSERT INTO AdbcPropTypes (Id, D, T, N) VALUES (1, {d_lit}, {t_lit}, {n_lit})"
            ),
        );

        let mut q = conn.new_statement().expect("new statement");
        q.set_sql_query("SELECT D, T, N FROM AdbcPropTypes WHERE Id = 1")
            .unwrap();
        let batches = q
            .execute()
            .expect("select")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        prop_assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let b = &batches[0];

        let d = b.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        let t = b
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let n = b
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        match exp_days {
            Some(v) => prop_assert_eq!(d.value(0), v),
            None => prop_assert!(d.is_null(0)),
        }
        match exp_nanos {
            Some(v) => prop_assert_eq!(t.value(0), v),
            None => prop_assert!(t.is_null(0)),
        }
        match exp_unscaled {
            Some(v) => prop_assert_eq!(n.value(0), v),
            None => prop_assert!(n.is_null(0)),
        }
    });
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

/// Load the driver through the ADBC **driver manager** (i.e. via the C ABI / `AdbcSpannerInit`
/// entrypoint of the built shared library) and run a query — a smoke test of the FFI export that
/// the trait-level tests bypass.
#[test]
fn ffi_driver_manager_smoke() {
    let Some(target) = test_target() else {
        eprintln!(
            "neither SPANNER_EMULATOR_HOST nor SPANNER_GCP_DATABASE set — skipping FFI smoke test"
        );
        return;
    };
    let Some(cdylib) = required_cdylib_path() else {
        eprintln!("cdylib not built — skipping FFI smoke test (run `cargo build` first)");
        return;
    };

    ensure_database_once(&target);

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        &cdylib,
        Some(b"AdbcSpannerInit"),
        AdbcVersion::V110,
    )
    .expect("load driver via the driver manager");

    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("new_database via FFI");

    // The freshly-created emulator database can lag; retry the connection briefly.
    let mut connection = {
        let mut conn = None;
        let mut last = None;
        for _ in 0..20 {
            match database.new_connection() {
                Ok(c) => {
                    conn = Some(c);
                    break;
                }
                Err(e) => {
                    last = Some(e);
                    std::thread::sleep(std::time::Duration::from_millis(250));
                }
            }
        }
        conn.unwrap_or_else(|| panic!("FFI connect failed: {last:?}"))
    };

    let mut statement = connection.new_statement().expect("new_statement via FFI");
    statement.set_sql_query("SELECT 1 AS one").unwrap();
    let reader = statement.execute().expect("execute via FFI");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect via FFI");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(value, 1);
}

/// Drive the driver through the ADBC **driver manager** (the real C-ABI path a language binding
/// uses) across the metadata / query / DML / transaction / ingest surface, asserting the canonical
/// ADBC result schemas — a conformance smoke test in the spirit of the `adbc_driver_manager`
/// project's own reusable driver checks. Complements `query_and_dml_round_trip`, which exercises the
/// same surface at the Rust-trait level.
#[test]
fn conformance_via_driver_manager() {
    use adbc_core::schemas::{GET_INFO_SCHEMA, GET_OBJECTS_SCHEMA, GET_TABLE_TYPES_SCHEMA};

    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping conformance_via_driver_manager");
        return;
    };
    let Some(cdylib) = required_cdylib_path() else {
        eprintln!("cdylib not built — skipping conformance test (run `cargo build` first)");
        return;
    };
    ensure_database_once(&target);
    // Holds for the whole body: this test issues DDL/DML, which Spanner will not run concurrently
    // with a schema change from the other DDL/DML-heavy tests.
    let _guard = serial_guard();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        &cdylib,
        Some(b"AdbcSpannerInit"),
        AdbcVersion::V110,
    )
    .expect("load driver via the driver manager");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("new_database via FFI");
    // The freshly-created emulator database can lag; retry the connection briefly.
    let mut connection = {
        let mut conn = None;
        for _ in 0..20 {
            match database.new_connection() {
                Ok(c) => {
                    conn = Some(c);
                    break;
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(250)),
            }
        }
        conn.expect("connect via FFI")
    };

    // --- get_info: canonical schema, all reported codes, and a filtered subset. ---
    let reader = connection.get_info(None).expect("get_info(None)");
    assert_eq!(reader.schema(), GET_INFO_SCHEMA.clone());
    let all_info = reader.collect::<Result<Vec<_>, _>>().expect("collect info");
    assert!(
        all_info.iter().map(|b| b.num_rows()).sum::<usize>() >= 4,
        "get_info should report at least the core codes"
    );
    let subset = connection
        .get_info(Some([InfoCode::VendorName, InfoCode::DriverName].into()))
        .expect("get_info(subset)");
    assert_eq!(subset.schema(), GET_INFO_SCHEMA.clone());
    let subset_rows: usize = subset
        .collect::<Result<Vec<_>, _>>()
        .expect("collect subset")
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(subset_rows, 2, "one row per explicitly requested code");

    // --- get_table_types: canonical schema, BASE TABLE and VIEW present. ---
    let reader = connection.get_table_types().expect("get_table_types");
    assert_eq!(reader.schema(), GET_TABLE_TYPES_SCHEMA.clone());
    let types: Vec<String> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect table types")
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            (0..col.len())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(types.contains(&"BASE TABLE".to_string()) && types.contains(&"VIEW".to_string()));

    // --- Scratch table for the metadata / DML / ingest checks. ---
    run_ffi(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcConf; \
         CREATE TABLE AdbcConf (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    // get_objects: canonical schema and one catalog row (Spanner's single unnamed catalog).
    let reader = connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .expect("get_objects");
    assert_eq!(reader.schema(), GET_OBJECTS_SCHEMA.clone());
    let objects = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect objects");
    assert_eq!(objects.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    // get_table_schema: the scratch table's columns come back with the right names/types.
    let schema = connection
        .get_table_schema(None, None, "AdbcConf")
        .expect("get_table_schema");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["Id", "Name"]);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    // --- execute (query) and execute_update (DML) through the FFI path. ---
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query("INSERT INTO AdbcConf (Id, Name) VALUES (1, 'a')")
        .unwrap();
    assert_eq!(s.execute_update().expect("insert"), Some(1));
    assert_eq!(ffi_count(&mut connection, "AdbcConf"), 1);

    // --- Manual transaction: buffered DML is invisible until commit, discarded on rollback. ---
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query("INSERT INTO AdbcConf (Id, Name) VALUES (2, 'b')")
        .unwrap();
    assert_eq!(s.execute_update().expect("buffered insert"), None);
    assert_eq!(
        ffi_count(&mut connection, "AdbcConf"),
        1,
        "not visible pre-commit"
    );
    connection.commit().expect("commit");
    assert_eq!(
        ffi_count(&mut connection, "AdbcConf"),
        2,
        "visible after commit"
    );
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query("INSERT INTO AdbcConf (Id, Name) VALUES (3, 'c')")
        .unwrap();
    assert_eq!(s.execute_update().expect("buffered insert"), None);
    connection.rollback().expect("rollback");
    assert_eq!(
        ffi_count(&mut connection, "AdbcConf"),
        2,
        "rollback discards"
    );
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("enable autocommit");

    // --- Bulk ingest through the FFI path. ---
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();
    let mut ingest = connection.new_statement().expect("new statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcConf".into()),
        )
        .unwrap();
    ingest.bind(rows).expect("bind ingest rows");
    assert_eq!(ingest.execute_update().expect("ingest"), Some(2));
    assert_eq!(ffi_count(&mut connection, "AdbcConf"), 4);

    run_ffi(&mut connection, "DROP TABLE AdbcConf");
}

/// Run a statement for its side effect through a driver-manager connection.
fn run_ffi(connection: &mut adbc_driver_manager::ManagedConnection, sql: &str) {
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query(sql).unwrap();
    s.execute_update()
        .unwrap_or_else(|e| panic!("run {sql:?}: {e:?}"));
}

/// Count rows in `table` through a driver-manager connection.
fn ffi_count(connection: &mut adbc_driver_manager::ManagedConnection, table: &str) -> i64 {
    let mut q = connection.new_statement().expect("new statement");
    q.set_sql_query(format!("SELECT COUNT(*) AS n FROM {table}"))
        .unwrap();
    let batches: Vec<_> = q
        .execute()
        .expect("count query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

/// Serialize the schema-mutating / DML-heavy tests against each other. Spanner rejects a schema
/// change while a read-write transaction is in progress on the database, so the DDL-heavy
/// `query_and_dml_round_trip` cannot run concurrently with the DML-heavy property tests. Each holds
/// this guard for its whole body. Lock poisoning is ignored so one test's failure surfaces on its
/// own rather than cascading into the others. (`ffi_driver_manager_smoke` only reads, so it is
/// exempt — read-only transactions do not block schema changes.)
fn serial_guard() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

/// Count the rows in `table` through a driver query.
fn count_rows(connection: &mut SpannerConnection, table: &str) -> i64 {
    let mut q = connection.new_statement().expect("new statement");
    q.set_sql_query(format!("SELECT COUNT(*) AS n FROM {table}"))
        .unwrap();
    let batches: Vec<_> = q
        .execute()
        .expect("count query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

/// Extract (table, column, key, value) tuples from a get_statistics result batch.
fn extract_statistics(batch: &RecordBatch) -> Vec<(String, Option<String>, i16, i64)> {
    let mut out = Vec::new();
    let db_schemas_list = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let db_schemas = db_schemas_list.value(0);
    let db_schemas = db_schemas.as_any().downcast_ref::<StructArray>().unwrap();
    let stats_list = db_schemas
        .column_by_name("db_schema_statistics")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    for i in 0..db_schemas.len() {
        let stats = stats_list.value(i);
        let stats = stats.as_any().downcast_ref::<StructArray>().unwrap();
        let table = stats
            .column_by_name("table_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let column = stats
            .column_by_name("column_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let key = stats
            .column_by_name("statistic_key")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        let value = stats
            .column_by_name("statistic_value")
            .unwrap()
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        for r in 0..stats.len() {
            let col = if column.is_null(r) {
                None
            } else {
                Some(column.value(r).to_string())
            };
            let v = value.value(r);
            let v = v.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
            out.push((table.value(r).to_string(), col, key.value(r), v));
        }
    }
    out
}

#[test]
fn get_statistics_reports_real_counts() {
    use adbc_core::constants::{
        ADBC_STATISTIC_DISTINCT_COUNT_KEY, ADBC_STATISTIC_NULL_COUNT_KEY,
        ADBC_STATISTIC_ROW_COUNT_KEY,
    };
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping get_statistics_reports_real_counts");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcStats; \
         CREATE TABLE AdbcStats (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );
    run(
        &mut connection,
        "INSERT INTO AdbcStats (Id, Name) VALUES (1, 'a'), (2, 'a'), (3, NULL)",
    );

    // Exact statistics.
    let batches = connection
        .get_statistics(None, None, Some("AdbcStats"), false)
        .expect("get_statistics")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect statistics");
    let stats = extract_statistics(&batches[0]);
    let has = |col: Option<&str>, key: i16, val: i64| {
        stats
            .iter()
            .any(|(t, c, k, v)| t == "AdbcStats" && c.as_deref() == col && *k == key && *v == val)
    };
    assert!(
        has(None, ADBC_STATISTIC_ROW_COUNT_KEY, 3),
        "row count 3: {stats:?}"
    );
    assert!(
        has(Some("Name"), ADBC_STATISTIC_NULL_COUNT_KEY, 1),
        "Name null 1: {stats:?}"
    );
    assert!(
        has(Some("Name"), ADBC_STATISTIC_DISTINCT_COUNT_KEY, 1),
        "Name distinct 1: {stats:?}"
    );
    assert!(
        has(Some("Id"), ADBC_STATISTIC_NULL_COUNT_KEY, 0),
        "Id null 0: {stats:?}"
    );
    assert!(
        has(Some("Id"), ADBC_STATISTIC_DISTINCT_COUNT_KEY, 3),
        "Id distinct 3: {stats:?}"
    );

    // approximate=true yields nothing (Spanner has no cheap statistics).
    let approx = connection
        .get_statistics(None, None, Some("AdbcStats"), true)
        .expect("get_statistics approx")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect approx");
    assert!(extract_statistics(&approx[0]).is_empty());

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcStats").unwrap();
    drop.execute_update().expect("drop stats table");
}

/// JSON and FLOAT32 columns round-trip through the driver: JSON keeps `Utf8` storage but is tagged
/// with the canonical `arrow.json` extension in the field metadata, FLOAT32 maps to Arrow
/// `Float32`, and NULLs in both survive.
#[test]
fn json_and_float32_round_trip() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping json_and_float32_round_trip");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcJson; \
         CREATE TABLE AdbcJson (Id INT64, Doc JSON, Ratio FLOAT32) PRIMARY KEY (Id)",
    );
    run(
        &mut connection,
        r#"INSERT INTO AdbcJson (Id, Doc, Ratio) VALUES
           (1, JSON '{"a":1,"b":"x"}', 1.5), (2, NULL, NULL)"#,
    );

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT Doc, Ratio FROM AdbcJson ORDER BY Id")
        .unwrap();
    let reader = query.execute().expect("query json/float32");

    let schema = reader.schema();
    let doc_field = schema.field(0);
    assert_eq!(doc_field.data_type(), &DataType::Utf8);
    assert_eq!(
        doc_field
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.json"),
        "JSON column must carry the arrow.json extension: {doc_field:?}"
    );
    assert_eq!(schema.field(1).data_type(), &DataType::Float32);

    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect json/float32 batches");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let batch = &batches[0];
    let docs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ratios = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();

    // Spanner stores JSON normalized; assert on key/value fragments rather than the exact text so
    // the check is robust to whitespace/ordering differences between backends.
    let doc = docs.value(0);
    assert!(
        doc.contains(r#""a":1"#) && doc.contains(r#""b":"x""#),
        "unexpected JSON text: {doc:?}"
    );
    assert!(docs.is_null(1), "NULL JSON must come back null");
    assert_eq!(ratios.value(0), 1.5f32);
    assert!(ratios.is_null(1), "NULL FLOAT32 must come back null");

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcJson").unwrap();
    drop.execute_update().expect("drop json table");
}

#[test]
fn execute_streams_in_batches() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping execute_streams_in_batches");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcStream; \
         CREATE TABLE AdbcStream (Id INT64) PRIMARY KEY (Id)",
    );
    // 2500 rows in one DML via GENERATE_ARRAY.
    run(
        &mut connection,
        "INSERT INTO AdbcStream (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 2500)) AS n",
    );

    let mut query = connection.new_statement().expect("new statement");
    // A small batch size so the 2500 rows span several batches.
    query
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(1000),
        )
        .expect("set rows_per_batch");
    assert_eq!(
        query
            .get_option_int(OptionStatement::Other(
                adbc_spanner::OPTION_ROWS_PER_BATCH.into()
            ))
            .expect("get rows_per_batch"),
        1000
    );
    query
        .set_sql_query("SELECT Id FROM AdbcStream ORDER BY Id")
        .unwrap();
    let reader = query.execute().expect("streaming query");
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect streamed batches");

    // 2500 rows at 1000 per batch → three batches (1000, 1000, 500).
    assert_eq!(batches.len(), 3, "expected three streamed batches");
    assert!(batches.iter().all(|b| b.schema() == schema));
    let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(sizes, vec![1000, 1000, 500]);

    // The concatenation is exactly 1..=2500 in order.
    let total: usize = sizes.iter().sum();
    assert_eq!(total, 2500);
    let mut expected = 1i64;
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..ids.len() {
            assert_eq!(ids.value(i), expected);
            expected += 1;
        }
    }

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcStream").unwrap();
    drop.execute_update().expect("drop stream table");
}

/// A cancel that lands while the streamed reader is *between* chunk fetches — no `block_on` parked
/// on the signal — must still cancel the next fetch: the signal is sticky rather than a transient
/// wake-up. And a subsequent execute on the same statement must run normally, because starting a
/// new operation resets the latch.
#[test]
fn cancel_between_stream_chunks_cancels_the_next_fetch() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping cancel_between_stream_chunks");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcCancel; \
         CREATE TABLE AdbcCancel (Id INT64) PRIMARY KEY (Id)",
    );
    run(
        &mut connection,
        "INSERT INTO AdbcCancel (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 300)) AS n",
    );

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(100),
        )
        .expect("set rows_per_batch");
    query
        .set_sql_query("SELECT Id FROM AdbcCancel ORDER BY Id")
        .unwrap();
    let mut reader = query.execute().expect("streaming query");

    // The first chunk was prefetched by `execute` (it settles the schema), so this consumes it
    // without touching the signal — leaving the stream idle between fetches.
    let first = reader
        .next()
        .expect("first batch")
        .expect("first batch is ok");
    assert_eq!(first.num_rows(), 100);

    // Cancel with no fetch in flight — exactly the window where a non-sticky signal was lost.
    query.cancel().expect("cancel");

    // The next chunk fetch must observe the latched cancel instead of streaming to completion.
    let error = reader
        .next()
        .expect("the cancelled fetch yields an item")
        .expect_err("the fetch after cancel must fail");
    assert!(
        error.to_string().to_lowercase().contains("cancel"),
        "expected a cancellation error, got: {error}"
    );

    // Starting a new operation on the same statement clears the latch and runs normally.
    let batches = query
        .execute()
        .expect("re-execute after cancel")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect re-executed batches");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 300);

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcCancel").unwrap();
    drop.execute_update().expect("drop cancel table");
}

/// The view-layout and remaining narrow Arrow types bind end-to-end: `Utf8View`/`BinaryView`
/// (what polars and newer pyarrow emit by default), `Int8` (widened to INT64), and `Date64`
/// (ms-at-day-boundary → DATE). Values inserted through bound parameters read back exactly.
#[test]
fn view_and_narrow_types_bind_round_trip() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping view_and_narrow_types_bind_round_trip");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcView; \
         CREATE TABLE AdbcView (Id INT64, S STRING(MAX), B BYTES(MAX), D DATE) PRIMARY KEY (Id)",
    );

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int8, false),
            Field::new("s", DataType::Utf8View, true),
            Field::new("b", DataType::BinaryView, true),
            Field::new("d", DataType::Date64, true),
        ])),
        vec![
            Arc::new(arrow_array::Int8Array::from(vec![1i8, 2])),
            Arc::new(arrow_array::StringViewArray::from(vec![
                Some("view-hello"),
                None,
            ])),
            Arc::new(arrow_array::BinaryViewArray::from(vec![
                Some(b"view-bytes".as_ref()),
                None,
            ])),
            // 19737 days = 2024-01-15, at the exact millisecond day boundary.
            Arc::new(arrow_array::Date64Array::from(vec![
                Some(19_737i64 * 86_400_000),
                None,
            ])),
        ],
    )
    .unwrap();

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO AdbcView (Id, S, B, D) VALUES (@id, @s, @b, @d)")
        .unwrap();
    insert.bind(batch).expect("bind view-typed batch");
    assert_eq!(insert.execute_update().expect("insert"), Some(2));

    let mut query = connection.new_statement().expect("new statement");
    query
        .set_sql_query("SELECT S, B, D FROM AdbcView ORDER BY Id")
        .unwrap();
    let batches = query
        .execute()
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");
    let all = &batches[0];
    assert_eq!(all.num_rows(), 2);
    let s = all
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(s.value(0), "view-hello");
    assert!(s.is_null(1));
    let b = all
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(b.value(0), b"view-bytes");
    assert!(b.is_null(1));
    let d = all
        .column(2)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(d.value(0), 19_737);
    assert!(d.is_null(1));

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcView").unwrap();
    drop.execute_update().expect("drop view table");
}

/// DML behind a leading `@{…}` statement hint is still classified as DML and routed to the
/// read/write path. Previously `first_keyword` saw no keyword at all, so hinted DML entering via
/// `execute()` was sent to a read-only single-use transaction, which Spanner rejects.
#[test]
fn hinted_dml_routes_to_the_read_write_path() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping hinted_dml_routes_to_the_read_write_path");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcHint; \
         CREATE TABLE AdbcHint (Id INT64, N INT64) PRIMARY KEY (Id)",
    );

    // Through the query entry point (`execute`), exactly as ADBC clients issue DML.
    // `LOCK_SCANNED_RANGES` is a documented statement hint for read/write transactions, accepted
    // by both real Cloud Spanner and the emulator (which rejects most other statement hints as
    // "Unsupported hint"). Before the fix this INSERT never reached a read/write transaction:
    // `first_keyword` saw no keyword behind the hint, so the statement went to the read-only
    // single-use query path and failed.
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(
            "@{LOCK_SCANNED_RANGES=exclusive} INSERT INTO AdbcHint (Id, N) VALUES (1, 0)",
        )
        .unwrap();
    insert.execute().expect("hinted INSERT via execute");

    // And through execute_update, with the affected-row count intact.
    let mut update = connection.new_statement().expect("new statement");
    update
        .set_sql_query("@{LOCK_SCANNED_RANGES=exclusive} UPDATE AdbcHint SET N = 1 WHERE true")
        .unwrap();
    assert_eq!(
        update.execute_update().expect("hinted UPDATE"),
        Some(1),
        "the hinted INSERT committed exactly one row"
    );

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcHint").unwrap();
    drop.execute_update().expect("drop hint table");
}

/// DML with a `THEN RETURN` clause returns its rows through `execute()` (previously they were
/// silently discarded as an empty result), reports the stats-based affected count through
/// `execute_update()`, fans out over bound parameters, and is rejected up front in manual
/// transaction mode, where its rows would be unobtainable (commit goes through `ExecuteBatchDml`,
/// which does not support `THEN RETURN`).
#[test]
fn dml_then_return_round_trip() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping dml_then_return_round_trip");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcReturn; \
         CREATE TABLE AdbcReturn (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );

    // execute(): the THEN RETURN rows come back as a typed Arrow result.
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(
            "INSERT INTO AdbcReturn (Id, Name) VALUES (1, 'a'), (2, 'b') THEN RETURN Id, Name",
        )
        .unwrap();
    let reader = insert.execute().expect("insert then return");
    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "Id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).name(), "Name");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect returned rows");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2, "one returned row per inserted row");
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((ids.value(0), ids.value(1)), (1, 2));

    // execute_update(): the rows are discarded, but the affected count is still reported.
    let mut update = connection.new_statement().expect("new statement");
    update
        .set_sql_query("UPDATE AdbcReturn SET Name = 'x' WHERE Id <= 2 THEN RETURN Id")
        .unwrap();
    assert_eq!(
        update.execute_update().expect("update then return"),
        Some(2)
    );

    // Bound parameters fan out one execution per row; the returned batches concatenate.
    let mut bound = connection.new_statement().expect("new statement");
    bound
        .set_sql_query("INSERT INTO AdbcReturn (Id, Name) VALUES (@id, @name) THEN RETURN Id")
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();
    bound.bind(batch).expect("bind");
    let rows: usize = bound
        .execute()
        .expect("bound insert then return")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bound returned rows")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 2);

    // Manual transaction mode: rejected up front with a clear error; the buffer stays usable.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");
    let mut manual = connection.new_statement().expect("new statement");
    manual
        .set_sql_query("DELETE FROM AdbcReturn WHERE true THEN RETURN Id")
        .unwrap();
    let error = manual
        .execute_update()
        .expect_err("THEN RETURN must be rejected in manual transaction mode");
    assert_eq!(error.status, adbc_core::error::Status::InvalidState);
    assert!(error.message.contains("THEN RETURN"), "{}", error.message);
    connection.rollback().expect("rollback");
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("re-enable autocommit");

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcReturn").unwrap();
    drop.execute_update().expect("drop return table");
}

/// Partitioned execution: `Statement::execute_partitions` splits a query into opaque partition
/// descriptors, and `Connection::read_partition` reads each one back as Arrow. The union of all
/// partitions must reproduce the full result set exactly once.
#[test]
fn execute_partitions_round_trip() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping execute_partitions_round_trip");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcPartition; \
         CREATE TABLE AdbcPartition (Id INT64) PRIMARY KEY (Id)",
    );
    // 200 rows, so the query has enough data for the server to (potentially) split it.
    run(
        &mut connection,
        "INSERT INTO AdbcPartition (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 200)) AS n",
    );

    let mut statement = connection.new_statement().expect("new statement");
    // The Data Boost and max-partitions options round-trip through get_option. Data Boost is baked
    // into each descriptor at partition-creation time (below), so it travels with the token.
    let data_boost_key = || OptionStatement::Other(adbc_spanner::OPTION_DATA_BOOST.into());
    let max_partitions_key = || OptionStatement::Other(adbc_spanner::OPTION_MAX_PARTITIONS.into());
    statement
        .set_option(data_boost_key(), OptionValue::String("true".into()))
        .expect("set data_boost");
    statement
        .set_option(max_partitions_key(), OptionValue::Int(4))
        .expect("set max_partitions");
    assert_eq!(
        statement
            .get_option_string(data_boost_key())
            .expect("get data_boost"),
        "true"
    );
    assert_eq!(
        statement
            .get_option_int(max_partitions_key())
            .expect("get max_partitions"),
        4
    );

    // A simple single-table scan is root-partitionable. No ORDER BY: ordering is not partitionable,
    // and partitions carry no inherent order relative to one another.
    statement
        .set_sql_query("SELECT Id FROM AdbcPartition")
        .unwrap();
    let partitioned = statement.execute_partitions().expect("execute_partitions");

    // The schema is known up front, independent of how many partitions come back.
    assert_eq!(partitioned.schema.fields().len(), 1);
    assert_eq!(partitioned.schema.field(0).name(), "Id");
    assert_eq!(partitioned.schema.field(0).data_type(), &DataType::Int64);
    assert!(
        !partitioned.partitions.is_empty(),
        "expected at least one partition"
    );
    // A read query reports no affected-row count.
    assert_eq!(partitioned.rows_affected, -1);

    // Read every partition back through the connection and union the ids.
    let mut seen: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    for token in &partitioned.partitions {
        let reader = connection.read_partition(token).expect("read_partition");
        assert_eq!(reader.schema().field(0).name(), "Id");
        for batch in reader {
            let batch = batch.expect("partition batch");
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..ids.len() {
                assert!(
                    seen.insert(ids.value(i)),
                    "id {} appeared in more than one partition",
                    ids.value(i)
                );
            }
        }
    }
    // Every row appears exactly once across all partitions.
    assert_eq!(seen.len(), 200);
    assert_eq!(*seen.iter().next().unwrap(), 1);
    assert_eq!(*seen.iter().next_back().unwrap(), 200);

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcPartition").unwrap();
    drop.execute_update().expect("drop partition table");
}

#[test]
fn query_with_trailing_semicolons_returns_rows() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping query_with_trailing_semicolons_returns_rows");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    // A run of trailing statement terminators on a single query (mirroring the ADBC conformance
    // case `SqlQueryTrailingSemicolons`, `SELECT current_date;;;`) is stripped by the driver on the
    // query path — Spanner's single-use query API otherwise rejects the trailing `;` with
    // "Expected end of input but got ;". The query still runs and returns its row.
    let mut statement = connection.new_statement().expect("new statement");
    statement.set_sql_query("SELECT 1 AS n;;;").unwrap();
    let reader = statement.execute().expect("query with trailing semicolons");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "expected one row back");
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(n.value(0), 1);

    // A `;` inside a string literal is not a terminator: it is preserved, not stripped.
    let mut str_stmt = connection.new_statement().expect("new statement");
    str_stmt.set_sql_query("SELECT ';' AS s;").unwrap();
    let reader = str_stmt
        .execute()
        .expect("query with a semicolon string literal");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");
    let s = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(s.value(0), ";");
}

/// The spec `adbc.connection.readonly` option. Covers the four dimensions the review asks for:
/// **round-trip** (defaults to `false`, and set values read back through `get_option`), **allow**
/// (a SELECT still runs), **deny** (DML, DDL and bulk ingest each fail with `InvalidState`), and
/// **toggle/live** (the flag is shared live with every statement — not snapshotted at creation —
/// so flipping the connection option immediately affects existing statements in both directions).
/// Regression guard for the four read-only enforcement branches in `src/statement.rs`, which
/// previously had no coverage.
#[test]
fn readonly_connection_rejects_writes() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping readonly_connection_rejects_writes");
        return;
    };
    ensure_database_once(&target);
    let _serial = serial_guard();

    use adbc_core::error::Status;

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    // --- round-trip: the option defaults to false, and set values read back ---
    assert_eq!(
        connection
            .get_option_string(OptionConnection::ReadOnly)
            .expect("read default readonly"),
        "false",
        "adbc.connection.readonly defaults to false"
    );
    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("true".into()),
        )
        .expect("enable readonly");
    assert_eq!(
        connection
            .get_option_string(OptionConnection::ReadOnly)
            .expect("read readonly back"),
        "true",
        "setting adbc.connection.readonly=true round-trips through get_option"
    );

    // --- allow: a SELECT still runs on a read-only connection ---
    let mut query = connection.new_statement().expect("new statement");
    query.set_sql_query("SELECT 1 AS n").unwrap();
    let rows: usize = query
        .execute()
        .expect("SELECT on a read-only connection must run")
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(
        rows, 1,
        "a query on a read-only connection returns its rows"
    );

    // --- deny: DML is rejected with InvalidState (nothing is touched — the WHERE never runs) ---
    let mut dml = connection.new_statement().expect("new statement");
    dml.set_sql_query("DELETE FROM Singers WHERE true").unwrap();
    let dml_err = dml
        .execute_update()
        .expect_err("DML on a read-only connection must fail");
    assert_eq!(
        dml_err.status,
        Status::InvalidState,
        "DML on a read-only connection must be InvalidState, got: {dml_err:?}"
    );

    // --- deny: DDL is rejected with InvalidState (the table is never created) ---
    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query("CREATE TABLE AdbcReadOnlyDenied (Id INT64) PRIMARY KEY (Id)")
        .unwrap();
    let ddl_err = ddl
        .execute_update()
        .expect_err("DDL on a read-only connection must fail");
    assert_eq!(
        ddl_err.status,
        Status::InvalidState,
        "DDL on a read-only connection must be InvalidState, got: {ddl_err:?}"
    );

    // --- deny: bulk ingest is rejected with InvalidState ---
    let ingest_rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "SingerId",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut ingest = connection.new_statement().expect("new statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("Singers".into()),
        )
        .unwrap();
    ingest.bind(ingest_rows).expect("bind ingest rows");
    let ingest_err = ingest
        .execute_update()
        .expect_err("ingest on a read-only connection must fail");
    assert_eq!(
        ingest_err.status,
        Status::InvalidState,
        "ingest on a read-only connection must be InvalidState, got: {ingest_err:?}"
    );

    // --- toggle / live: the flag is read at execution time, not snapshotted at creation ---
    // Create a statement while the connection is still read-only, then flip the connection back to
    // writable. The existing statement must become writable immediately — the flag is shared live
    // with every statement, so a toggle applies to statements created before it.
    let mut live = connection.new_statement().expect("new statement");
    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("false".into()),
        )
        .expect("disable readonly");
    assert_eq!(
        connection
            .get_option_string(OptionConnection::ReadOnly)
            .expect("read readonly back"),
        "false",
        "setting adbc.connection.readonly=false round-trips through get_option"
    );
    live.set_sql_query("DELETE FROM Singers WHERE false")
        .unwrap();
    assert_eq!(
        live.execute_update()
            .expect("a pre-existing statement can write once readonly is cleared"),
        Some(0),
        "the read-only flag is live: clearing it on the connection immediately frees a statement \
         created while it was set"
    );

    // ... and the other direction: re-enabling read-only immediately locks the same pre-existing
    // statement out of writes again (nothing is touched — the WHERE never runs).
    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("true".into()),
        )
        .expect("re-enable readonly");
    live.set_sql_query("DELETE FROM Singers WHERE true")
        .unwrap();
    let relock_err = live
        .execute_update()
        .expect_err("re-enabling readonly must immediately affect an existing statement");
    assert_eq!(
        relock_err.status,
        Status::InvalidState,
        "the read-only flag is live: re-enabling it on the connection immediately locks a \
         pre-existing statement, got: {relock_err:?}"
    );

    // Leave the connection writable for any later use of the shared database.
    connection
        .set_option(
            OptionConnection::ReadOnly,
            OptionValue::String("false".into()),
        )
        .expect("disable readonly again");
}

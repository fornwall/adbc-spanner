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

use adbc_core::error::Status;
use adbc_core::options::{
    AdbcVersion, InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
    OptionValue,
};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_manager::ManagedDriver;
use adbc_spanner::{SpannerConnection, SpannerDatabase, SpannerDriver};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int64Array, ListArray, RecordBatch, RecordBatchIterator, RecordBatchReader,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampNanosecondArray, UnionArray,
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

/// Connect through a `spanner:` **connection URI** — the database path as the URI path and the
/// driver options as query parameters — instead of a bare path plus individual options, and run a
/// query. Against the emulator the URI carries `spanner.endpoint` (percent-encoded, to exercise
/// decoding) and `spanner.emulator=true`; against a real database the URI is just the path (ADC as
/// usual).
#[test]
fn connect_via_connection_uri() {
    let Some(target) = test_target() else {
        eprintln!(
            "neither SPANNER_EMULATOR_HOST nor SPANNER_GCP_DATABASE set — \
             skipping Spanner integration test"
        );
        return;
    };

    ensure_database_once(&target);
    let _serial = serial_guard();

    let uri = if target.is_emulator {
        // The same endpoint the driver would derive from SPANNER_EMULATOR_HOST, but passed
        // explicitly through the URI's query parameters (`://` percent-encoded).
        let host = std::env::var("SPANNER_EMULATOR_HOST").unwrap();
        format!(
            "spanner:///{}?spanner.endpoint=http%3A%2F%2F{host}&spanner.emulator=true",
            target.database_path()
        )
    } else {
        format!("spanner:///{}", target.database_path())
    };

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, OptionValue::String(uri))])
        .expect("create database from connection URI");

    // The URI expands into the underlying options: `uri` reads back as the bare database path, and
    // the query parameters round-trip under their own option keys.
    assert_eq!(
        database.get_option_string(OptionDatabase::Uri).unwrap(),
        target.database_path()
    );
    if target.is_emulator {
        let host = std::env::var("SPANNER_EMULATOR_HOST").unwrap();
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other("spanner.endpoint".into()))
                .unwrap(),
            format!("http://{host}")
        );
        assert_eq!(
            database
                .get_option_string(OptionDatabase::Other("spanner.emulator".into()))
                .unwrap(),
            "true"
        );
    }

    let mut connection = connect_with_retry(&database);
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_sql_query("SELECT 1 AS one")
        .expect("set query");
    let batches: Vec<_> = statement
        .execute()
        .expect("execute over URI-configured connection")
        .collect::<Result<Vec<_>, _>>()
        .expect("read batches");
    assert_eq!(batches.len(), 1);
    let ones = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ones.value(0), 1);
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

    // --- single-statement autocommit DML carries the `last_statement` optimization ---
    //
    // A single-statement autocommit UPDATE is the entire read/write transaction, so the driver
    // flags its `ExecuteBatchDml` batch as the transaction's last request (`last_statements=true`),
    // letting Spanner release the transaction without a separate Commit RPC. The flag must not
    // change the observable result: the statement still reports its exact affected-row count and
    // the write is durably committed. Update one row, assert the count, and read it back.
    let mut update = connection.new_statement().expect("new statement");
    update
        .set_sql_query("UPDATE Singers SET Score = 9.5 WHERE SingerId = 1")
        .unwrap();
    assert_eq!(
        update.execute_update().expect("single-statement update"),
        Some(1),
        "a single-statement autocommit UPDATE must still report its affected-row count"
    );

    let mut check = connection.new_statement().expect("new statement");
    check
        .set_sql_query("SELECT Score FROM Singers WHERE SingerId = 1")
        .unwrap();
    let check_batches = check
        .execute()
        .expect("read back updated row")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect updated row");
    let updated_score = check_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        updated_score.value(0),
        9.5,
        "the last_statement-optimized UPDATE must be durably committed"
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
    // Assert the stored value round-trips through the freshly-created table, not merely that a row
    // exists: the `Note` column must read back as the exact string that was inserted.
    let ddl_note = ddl_rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ddl_note.value(0), "hello");

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

    // Bulk ingest inside a manual transaction: the rows' insert mutations must buffer (returning
    // None, invisible before commit) and commit atomically in the SAME transaction as buffered
    // DML; rollback must discard them.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit for ingest");
    let buffer_ingest = |connection: &mut SpannerConnection, ids: &[i64]| {
        let rows = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap();
        let mut s = connection.new_statement().expect("new statement");
        s.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcTxn".into()),
        )
        .unwrap();
        s.bind(rows).expect("bind manual-mode ingest rows");
        assert_eq!(
            s.execute_update().expect("buffered ingest"),
            None,
            "a manual-mode ingest must buffer its mutations (return None), not commit immediately"
        );
    };
    buffer_ingest(&mut connection, &[100, 101]);
    buffer_sql(&mut connection, "INSERT INTO AdbcTxn (Id) VALUES (102)");
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        6,
        "buffered ingest mutations must not be visible before commit"
    );
    connection.commit().expect("commit ingest + DML");
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        9,
        "commit must apply the buffered DML and the buffered ingest mutations atomically"
    );
    // A buffered ingest followed by rollback leaves no trace.
    buffer_ingest(&mut connection, &[103]);
    connection.rollback().expect("rollback buffered ingest");
    assert_eq!(
        count_rows(&mut connection, "AdbcTxn"),
        9,
        "rolled-back ingest mutations must not appear"
    );
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("re-enable autocommit after ingest");

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
    // Assert the replaced values, not just the count: `replace` drops + recreates, so the table
    // holds exactly one copy of `create_rows()` — (10,"x"),(20,"y") — rather than the duplicated
    // four rows an `append` would have left behind.
    let created_ids = created[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let created_labels = created[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (0..created[0].num_rows())
            .map(|i| (created_ids.value(i), created_labels.value(i)))
            .collect::<Vec<_>>(),
        vec![(10, "x"), (20, "y")],
        "replace-mode ingest must leave exactly the replacement rows"
    );
    // `create` mode on an already-existing table is the ADBC-contractual error path: the driver
    // emits a `CREATE TABLE` (no `IF NOT EXISTS`), Spanner rejects it because `AdbcCreate` still
    // exists, and the driver remaps the DDL failure onto `AlreadyExists` — naming the table — so
    // consumers can branch on the status (e.g. to fall back to append). Nothing may be inserted.
    let create_on_existing = ingest_into(&mut connection, "AdbcCreate", "create")
        .expect_err("create-mode ingest onto an existing table must fail");
    assert_eq!(
        create_on_existing.status,
        adbc_core::error::Status::AlreadyExists,
        "create onto an existing table must be AlreadyExists, got: {create_on_existing:?}"
    );
    assert!(
        create_on_existing.message.contains("AdbcCreate"),
        "the error must name the target table: {create_on_existing:?}"
    );
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

    // Create-mode ingest keyed on an existing column (`spanner.ingest.primary_key`): the table is
    // built with that column as the primary key and NO synthetic `adbc_ingest_key` is added.
    let mut drop_pk = connection.new_statement().expect("new statement");
    drop_pk
        .set_sql_query("DROP TABLE IF EXISTS AdbcIngestPk")
        .unwrap();
    drop_pk
        .execute_update()
        .expect("pre-drop primary-key table");
    let pk_ingest = |connection: &mut SpannerConnection, key: &str, mode: &str| {
        let mut s = connection.new_statement().expect("new statement");
        s.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcIngestPk".into()),
        )
        .unwrap();
        s.set_option(
            OptionStatement::Other(adbc_spanner::OPTION_INGEST_PRIMARY_KEY.into()),
            OptionValue::String(key.into()),
        )
        .unwrap();
        s.set_option(
            OptionStatement::IngestMode,
            OptionValue::String(mode.into()),
        )
        .unwrap();
        // The option round-trips through get_option as the comma-joined column list.
        assert_eq!(
            s.get_option_string(OptionStatement::Other(
                adbc_spanner::OPTION_INGEST_PRIMARY_KEY.into()
            ))
            .unwrap(),
            key
        );
        s.bind(create_rows()).expect("bind primary-key ingest rows");
        s.execute_update()
    };
    assert_eq!(
        pk_ingest(&mut connection, "Id", "create").expect("create keyed on Id"),
        Some(2)
    );
    assert_eq!(count_rows(&mut connection, "AdbcIngestPk"), 2);
    // The created table's columns are exactly the data columns — no synthetic key was appended.
    let pk_schema = connection
        .get_table_schema(None, None, "AdbcIngestPk")
        .expect("get_table_schema for the keyed ingest table");
    let pk_cols: Vec<&str> = pk_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        pk_cols,
        vec!["Id", "Label"],
        "keying on an existing column must not add a synthetic column: {pk_cols:?}"
    );
    // Re-ingesting a row whose key duplicates an existing one is an insert-mutation conflict →
    // AlreadyExists (the same PK semantics as the synthetic key), proving `Id` really is the key.
    let dup_err = pk_ingest(&mut connection, "Id", "append")
        .expect_err("appending duplicate primary keys must fail");
    assert_eq!(
        dup_err.status,
        adbc_core::error::Status::AlreadyExists,
        "duplicate primary key must be AlreadyExists, got: {dup_err:?}"
    );
    let mut drop_pk_done = connection.new_statement().expect("new statement");
    drop_pk_done
        .set_sql_query("DROP TABLE AdbcIngestPk")
        .unwrap();
    drop_pk_done
        .execute_update()
        .expect("drop primary-key table");
    // A primary_key naming a column absent from the ingest data fails up front with
    // InvalidArguments — before any DDL is sent to Spanner.
    let mut bad_pk = connection.new_statement().expect("new statement");
    bad_pk
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcIngestBadPk".into()),
        )
        .unwrap();
    bad_pk
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_INGEST_PRIMARY_KEY.into()),
            OptionValue::String("NoSuchColumn".into()),
        )
        .unwrap();
    bad_pk
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("create".into()),
        )
        .unwrap();
    bad_pk
        .bind(create_rows())
        .expect("bind bad-primary-key rows");
    let bad_pk_err = bad_pk
        .execute_update()
        .expect_err("primary_key referencing a missing column must fail");
    assert_eq!(
        bad_pk_err.status,
        adbc_core::error::Status::InvalidArguments,
        "a primary_key column absent from the data must be InvalidArguments, got: {bad_pk_err:?}"
    );

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

    // Parameterized DML: update by bound @Id / @Name. The bound columns are named after the
    // parameters but in a different order than they appear in the SQL (@Name before @Id), so this
    // opts into by-name binding (`adbc.statement.bind_by_name=true`) rather than the default
    // positional pairing.
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
    pu.set_option(
        OptionStatement::Other(adbc_spanner::OPTION_BIND_BY_NAME.into()),
        OptionValue::String("true".into()),
    )
    .expect("set bind_by_name=true");
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

    // xdbc_type_name carries the Spanner-native type (INFORMATION_SCHEMA.COLUMNS.SPANNER_TYPE).
    let type_name = columns
        .column_by_name("xdbc_type_name")
        .expect("xdbc_type_name field")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let type_names: Vec<&str> = (0..type_name.len()).map(|i| type_name.value(i)).collect();
    assert_eq!(type_names, ["INT64", "STRING(MAX)", "BOOL", "FLOAT64"]);

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

    // --- Depth-boundary rule: a table_type filter matching nothing must still return the
    // catalog + db_schema skeleton, with each schema's db_schema_tables an EMPTY list — never
    // NULL, which is reserved for levels strictly below the requested depth. (The adbc-drivers
    // validation suite caught DuckDB shipping NULL here; see duckdb/duckdb PR #21018.)
    let no_such_type = connection
        .get_objects(
            ObjectDepth::Tables,
            None,
            None,
            None,
            Some(vec!["NO SUCH TYPE"]),
            None,
        )
        .expect("get_objects with a table_type filter matching nothing")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect type-filtered objects");
    let nb = &no_such_type[0];
    assert_eq!(nb.num_rows(), 1, "single catalog");
    let nb_schemas = nb.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(
        nb_schemas.is_valid(0),
        "catalog_db_schemas must not be NULL at Tables depth"
    );
    let nb_schemas = nb_schemas.value(0);
    let nb_schemas = nb_schemas.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(
        !nb_schemas.is_empty(),
        "the db_schema skeleton must survive a table_type filter matching nothing"
    );
    let nb_tables = nb_schemas
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    for i in 0..nb_tables.len() {
        assert!(
            nb_tables.is_valid(i),
            "db_schema_tables must be EMPTY, not NULL, when the filter matches nothing"
        );
        assert_eq!(nb_tables.value(i).len(), 0, "no tables may match");
    }
}

/// With `spanner.commit_stats` enabled, an autocommit DML commit captures Spanner's returned
/// mutation count, readable back on the statement via `spanner.commit_stats.mutation_count`
/// (`NotFound` before any such commit has run).
///
/// The **emulator does not populate commit statistics** (it returns `CommitResponse.commit_stats =
/// None` even when `return_commit_stats` is requested), so the positive-count assertion runs only
/// against a real Cloud Spanner target; on the emulator the mutation count stays `NotFound` after
/// the commit and that is asserted instead. The option plumbing (flag round-trip, read-only-key
/// rejection, pre-commit `NotFound`) is exercised on both.
#[test]
fn commit_stats_reports_mutation_count() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping commit_stats_reports_mutation_count");
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

    // Start from a known-empty table.
    let mut delete = connection.new_statement().expect("new statement");
    delete
        .set_sql_query("DELETE FROM Singers WHERE true")
        .unwrap();
    delete.execute_update().expect("delete");

    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_COMMIT_STATS.to_string()),
            OptionValue::String("true".to_string()),
        )
        .expect("enable commit stats");
    // The flag round-trips as the effective boolean.
    assert_eq!(
        insert
            .get_option_string(OptionStatement::Other(
                adbc_spanner::OPTION_COMMIT_STATS.to_string()
            ))
            .expect("read commit_stats flag"),
        "true"
    );
    // No commit has run yet, so the mutation count is NotFound.
    let before = insert
        .get_option_int(OptionStatement::Other(
            adbc_spanner::OPTION_COMMIT_STATS_MUTATION_COUNT.to_string(),
        ))
        .expect_err("mutation count must be NotFound before any commit");
    assert_eq!(before.status, Status::NotFound);

    insert
        .set_sql_query(
            "INSERT INTO Singers (SingerId, Name, Active, Score) \
             VALUES (1, 'Alice', true, 4.5), (2, 'Bob', false, 3.25)",
        )
        .unwrap();
    assert_eq!(insert.execute_update().expect("insert"), Some(2));

    let mutation_count = insert.get_option_int(OptionStatement::Other(
        adbc_spanner::OPTION_COMMIT_STATS_MUTATION_COUNT.to_string(),
    ));
    if target.is_emulator {
        // The emulator ignores `return_commit_stats`, so no count is captured.
        assert_eq!(
            mutation_count
                .expect_err("emulator returns no commit stats")
                .status,
            Status::NotFound
        );
    } else {
        // Real Spanner returns stats; the captured mutation count is positive (two rows × several
        // columns' worth of mutations — the exact number is Spanner's to decide, so only assert it
        // is meaningfully non-zero).
        let mutations = mutation_count.expect("mutation count present after a commit with stats");
        assert!(
            mutations > 0,
            "expected a positive mutation count, got {mutations}"
        );
    }

    // Setting the read-only mutation-count key is rejected.
    let rejected = insert.set_option(
        OptionStatement::Other(adbc_spanner::OPTION_COMMIT_STATS_MUTATION_COUNT.to_string()),
        OptionValue::Int(1),
    );
    assert_eq!(
        rejected
            .expect_err("mutation-count key is read-only")
            .status,
        Status::NotImplemented
    );

    // Clean up.
    let mut cleanup = connection.new_statement().expect("new statement");
    cleanup
        .set_sql_query("DELETE FROM Singers WHERE true")
        .unwrap();
    cleanup.execute_update().expect("cleanup delete");
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

/// `spanner.ingest.batch_write`: an autocommit bulk ingest routed through Spanner's BatchWrite RPC
/// (rather than a write-only transaction) lands every row and reports the exact affected-row count,
/// the option round-trips through `get_option`, and a duplicate primary key still surfaces as the
/// append-mode `AlreadyExists` remap — i.e. insert/count/error semantics are preserved across the
/// alternate transport. The emulator implements the BatchWrite RPC.
#[test]
fn bulk_ingest_via_batch_write() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping bulk_ingest_via_batch_write");
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
        "DROP TABLE IF EXISTS AdbcBatchWrite; \
         CREATE TABLE AdbcBatchWrite (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    )
    .unwrap();
    ddl.execute_update().expect("create batch-write table");

    const ROWS: usize = 5;
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..ROWS).map(|i| format!("row-{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    let mut ingest = connection.new_statement().expect("new statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcBatchWrite".into()),
        )
        .unwrap();
    ingest
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_INGEST_BATCH_WRITE.into()),
            OptionValue::String("true".into()),
        )
        .unwrap();
    // The option round-trips through get_option (and empty-string unsets back to the default).
    assert_eq!(
        ingest
            .get_option_string(OptionStatement::Other(
                adbc_spanner::OPTION_INGEST_BATCH_WRITE.into()
            ))
            .unwrap(),
        "true"
    );
    ingest.bind(rows.clone()).expect("bind batch-write rows");
    assert_eq!(
        ingest.execute_update().expect("batch-write ingest"),
        Some(ROWS as i64),
        "BatchWrite ingest must report the exact applied-row count"
    );

    // Every row landed exactly once, readable back.
    let mut read = connection.new_statement().expect("new statement");
    read.set_sql_query("SELECT Id, Name FROM AdbcBatchWrite ORDER BY Id")
        .unwrap();
    let batches = read
        .execute()
        .expect("read batch-write rows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut seen = 0_usize;
    for batch in &batches {
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
        for row in 0..batch.num_rows() {
            assert_eq!(ids.value(row), seen as i64);
            assert_eq!(names.value(row), format!("row-{seen}"));
            seen += 1;
        }
    }
    assert_eq!(
        seen, ROWS,
        "all BatchWrite-ingested rows must be readable back"
    );

    // A re-ingest of the same primary keys through the BatchWrite path still surfaces the
    // append-mode AlreadyExists remap (the per-group insert failure maps to ALREADY_EXISTS just as
    // the write-only path's does), naming the target table.
    let mut dup = connection.new_statement().expect("new statement");
    dup.set_option(
        OptionStatement::TargetTable,
        OptionValue::String("AdbcBatchWrite".into()),
    )
    .unwrap();
    dup.set_option(
        OptionStatement::Other(adbc_spanner::OPTION_INGEST_BATCH_WRITE.into()),
        OptionValue::String("true".into()),
    )
    .unwrap();
    dup.bind(rows).expect("bind duplicate batch-write rows");
    let error = dup
        .execute_update()
        .expect_err("a duplicate primary key must fail the BatchWrite ingest");
    assert_eq!(error.status, Status::AlreadyExists);
    assert!(
        error.message.contains("AdbcBatchWrite"),
        "the AlreadyExists error should name the target table: {error}"
    );

    let mut drop_bw = connection.new_statement().expect("new statement");
    drop_bw.set_sql_query("DROP TABLE AdbcBatchWrite").unwrap();
    drop_bw.execute_update().expect("drop batch-write table");
}

/// `spanner.max_timestamp_precision`: a stored TIMESTAMP outside Arrow's nanosecond range
/// (year 9999) errors loudly in the default mode — naming the column, the value and the escape
/// hatch — and reads back exactly in `microseconds` mode, where `execute_schema` advertises the
/// same `Timestamp(Microsecond, "UTC")` unit as the data path. Also covers connection-level
/// inheritance, per-statement override, and `""` reset.
#[test]
fn timestamp_precision_modes_round_trip() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping timestamp_precision_modes_round_trip");
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
        "DROP TABLE IF EXISTS AdbcTsPrecision; \
         CREATE TABLE AdbcTsPrecision (Id INT64, T TIMESTAMP) PRIMARY KEY (Id)",
    )
    .unwrap();
    ddl.execute_update().expect("create timestamp table");

    // Year 9999 is far outside the ~1677–2262 Arrow nanosecond window; the sub-microsecond digits
    // exercise the documented truncation. Year 1500 covers the pre-1677 side.
    let mut ins = connection.new_statement().expect("new statement");
    ins.set_sql_query(
        "INSERT INTO AdbcTsPrecision (Id, T) VALUES \
         (1, TIMESTAMP '9999-01-01T00:00:00.123456789Z'), \
         (2, TIMESTAMP '1500-06-15T12:34:56.789012Z'), \
         (3, NULL)",
    )
    .unwrap();
    assert_eq!(ins.execute_update().expect("insert timestamps"), Some(3));

    let micros_ty = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

    // Default mode: reading the out-of-range instant is a loud error pointing at the option.
    let mut q = connection.new_statement().expect("new statement");
    q.set_sql_query("SELECT T FROM AdbcTsPrecision WHERE Id = 1")
        .unwrap();
    // The failure may surface on `execute` (the first chunk settles the schema eagerly) or on the
    // first batch pulled from the reader; either way the message must be diagnosable.
    let msg = match q.execute() {
        Err(e) => e.message,
        Ok(reader) => match reader.collect::<Result<Vec<_>, _>>() {
            Err(e) => e.to_string(),
            Ok(_) => panic!("year-9999 TIMESTAMP must not decode as nanoseconds"),
        },
    };
    assert!(
        msg.contains("\"T\"")
            && msg.contains("9999-01-01")
            && msg.contains("spanner.max_timestamp_precision"),
        "error should name the column, the value and the escape hatch: {msg}"
    );

    // Connection-level microseconds mode: statements inherit it, the option round-trips, and the
    // full range reads back (with sub-microsecond digits truncated toward negative infinity).
    connection
        .set_option(
            OptionConnection::Other("spanner.max_timestamp_precision".into()),
            OptionValue::String("microseconds".into()),
        )
        .expect("set connection timestamp precision");
    assert_eq!(
        connection
            .get_option_string(OptionConnection::Other(
                "spanner.max_timestamp_precision".into()
            ))
            .unwrap(),
        "microseconds"
    );

    let mut q = connection.new_statement().expect("new statement");
    assert_eq!(
        q.get_option_string(OptionStatement::Other(
            "spanner.max_timestamp_precision".into()
        ))
        .unwrap(),
        "microseconds",
        "statement inherits the connection's mode"
    );
    q.set_sql_query("SELECT T FROM AdbcTsPrecision ORDER BY Id")
        .unwrap();

    // The advertised (PLAN-probe) schema must carry the same unit as the data the reader streams.
    let planned = q.execute_schema().expect("execute_schema in micros mode");
    assert_eq!(planned.field(0).data_type(), &micros_ty);

    let reader = q.execute().expect("query in micros mode");
    assert_eq!(reader.schema().field(0).data_type(), &micros_ty);
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect micros batches");
    let ts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts.value(0), 253_370_764_800_123_456); // .123456789 truncated to .123456
    assert_eq!(ts.value(1), -14_817_468_303_210_988); // 1500-06-15T12:34:56.789012Z
    assert!(ts.is_null(2));

    // The table-metadata surface honours the connection's mode too.
    let table_schema = connection
        .get_table_schema(None, None, "AdbcTsPrecision")
        .expect("get_table_schema");
    assert_eq!(
        table_schema
            .field_with_name("T")
            .expect("T column")
            .data_type(),
        &micros_ty
    );

    // A statement-level `""` resets to the driver default (nanoseconds), overriding the
    // connection's mode — and the out-of-range value errors again.
    let mut q = connection.new_statement().expect("new statement");
    q.set_option(
        OptionStatement::Other("spanner.max_timestamp_precision".into()),
        OptionValue::String(String::new()),
    )
    .expect("reset statement precision");
    assert_eq!(
        q.get_option_string(OptionStatement::Other(
            "spanner.max_timestamp_precision".into()
        ))
        .unwrap(),
        "nanoseconds_error_on_overflow"
    );
    q.set_sql_query("SELECT T FROM AdbcTsPrecision WHERE Id = 1")
        .unwrap();
    let planned = q.execute_schema().expect("execute_schema in default mode");
    assert_eq!(
        planned.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
    let failed = match q.execute() {
        Err(_) => true,
        Ok(reader) => reader.collect::<Result<Vec<_>, _>>().is_err(),
    };
    assert!(failed, "statement-level reset must restore the loud error");

    let mut drop_ts = connection.new_statement().expect("new statement");
    drop_ts.set_sql_query("DROP TABLE AdbcTsPrecision").unwrap();
    drop_ts.execute_update().expect("drop timestamp table");
}

/// Edge cases of the bulk-ingest surface, each mapped to a bug a peer ADBC driver shipped:
/// zero-row batches inside a bound stream (the Snowflake driver silently lost the rows after a
/// mid-stream empty batch, apache/arrow-adbc#1866), binding before setting the ingest options
/// (Flight SQL's `TestBulkIngestBindBeforeOptions`, fixed for all drivers in driver-manager
/// apache/arrow-adbc#4308), bound data with no destination at all, an ingest target with no bound
/// data, full reuse of one statement handle across ingest → DML → ingest (the DuckDB ADBC suite's
/// reuse chain), and a duplicate primary key surfacing as `AlreadyExists` naming the target table
/// (DuckDB's ingest errors shipped table-less until duckdb/duckdb#22146).
#[test]
fn bulk_ingest_edge_cases() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping bulk_ingest_edge_cases");
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
        "DROP TABLE IF EXISTS AdbcEdge; \
         CREATE TABLE AdbcEdge (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    )
    .unwrap();
    ddl.execute_update().expect("create edge table");

    let schema = Arc::new(Schema::new(vec![
        Field::new("Id", DataType::Int64, false),
        Field::new("Name", DataType::Utf8, false),
    ]));
    let batch = |ids: &[i64]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    ids.iter().map(|i| format!("name-{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    };
    let empty = || RecordBatch::new_empty(schema.clone());
    // A fresh statement pre-configured to append into AdbcEdge.
    let append_stmt = |connection: &mut SpannerConnection| {
        let mut s = connection.new_statement().expect("new statement");
        s.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcEdge".into()),
        )
        .unwrap();
        s.set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
        s
    };

    // --- Zero-row batches in a bound stream, in FIRST, MIDDLE and LAST position. Every row from
    // the non-empty batches must land — a mid-stream empty batch must not truncate the ingest.
    let mut zero_rows = append_stmt(&mut connection);
    zero_rows
        .bind_stream(Box::new(RecordBatchIterator::new(
            [empty(), batch(&[1, 2]), empty(), batch(&[3]), empty()].map(Ok),
            schema.clone(),
        )))
        .expect("bind stream with zero-row batches");
    assert_eq!(
        zero_rows
            .execute_update()
            .expect("ingest around zero-row batches"),
        Some(3),
        "all rows from the non-empty batches must land; empty batches contribute nothing"
    );
    assert_eq!(count_rows(&mut connection, "AdbcEdge"), 3);

    // A stream of only zero-row batches ingests zero rows successfully (no empty commit is sent —
    // the empty-chunk guard is unit-tested offline in src/statement.rs).
    let mut all_empty = append_stmt(&mut connection);
    all_empty
        .bind_stream(Box::new(RecordBatchIterator::new(
            [empty(), empty()].map(Ok),
            schema.clone(),
        )))
        .expect("bind all-empty stream");
    assert_eq!(
        all_empty
            .execute_update()
            .expect("all-empty ingest succeeds"),
        Some(0)
    );
    assert_eq!(count_rows(&mut connection, "AdbcEdge"), 3);

    // The same zero-row-batch stream in MANUAL transaction mode: buffered (None), all rows from
    // the non-empty batches applied on commit.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");
    let mut manual = append_stmt(&mut connection);
    manual
        .bind_stream(Box::new(RecordBatchIterator::new(
            [empty(), batch(&[4]), empty(), batch(&[5]), empty()].map(Ok),
            schema.clone(),
        )))
        .expect("bind manual-mode stream with zero-row batches");
    assert_eq!(
        manual.execute_update().expect("buffered ingest"),
        None,
        "a manual-mode ingest must buffer its mutations"
    );
    connection.commit().expect("commit buffered ingest");
    assert_eq!(
        count_rows(&mut connection, "AdbcEdge"),
        5,
        "every row around the zero-row batches must land on commit"
    );
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("re-enable autocommit");

    // Create-mode with a zero-row FIRST batch: the created table's schema comes from that empty
    // batch, and the later rows land.
    let mut create = connection.new_statement().expect("new statement");
    create
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcEdgeCreate".into()),
        )
        .unwrap();
    create
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("create".into()),
        )
        .unwrap();
    create
        .bind_stream(Box::new(RecordBatchIterator::new(
            [empty(), batch(&[1])].map(Ok),
            schema.clone(),
        )))
        .expect("bind create-mode stream with an empty first batch");
    assert_eq!(
        create
            .execute_update()
            .expect("create-mode ingest with an empty first batch"),
        Some(1),
        "the table schema must come from the zero-row first batch"
    );
    let mut drop_edge_create = connection.new_statement().expect("new statement");
    drop_edge_create
        .set_sql_query("DROP TABLE AdbcEdgeCreate")
        .unwrap();
    drop_edge_create
        .execute_update()
        .expect("drop AdbcEdgeCreate");

    // --- Bind BEFORE the ingest options: the bound data and the ingest options may arrive in
    // either order.
    let mut before = connection.new_statement().expect("new statement");
    before.bind(batch(&[6])).expect("bind before options");
    before
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcEdge".into()),
        )
        .unwrap();
    before
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    assert_eq!(
        before.execute_update().expect("bind-before-options ingest"),
        Some(1),
        "binding before setting the ingest target must work"
    );
    assert_eq!(count_rows(&mut connection, "AdbcEdge"), 6);

    // Bound data with NO destination at all (no ingest target, no SQL): a clean InvalidState that
    // names the missing ingest option, on BOTH entry points. The failed attempts must not consume
    // the bound data, so supplying the target afterwards still ingests it.
    let mut nowhere = connection.new_statement().expect("new statement");
    nowhere.bind(batch(&[7])).expect("bind without destination");
    let update_err = nowhere
        .execute_update()
        .expect_err("execute_update with bound data but no destination must fail");
    let execute_err = nowhere
        .execute()
        .err()
        .expect("execute with bound data but no destination must fail");
    for error in [update_err, execute_err] {
        assert_eq!(
            error.status,
            adbc_core::error::Status::InvalidState,
            "bound data without a destination must be InvalidState, got: {error:?}"
        );
        assert!(
            error.message.contains("adbc.ingest.target_table"),
            "the error must name the missing ingest option: {error:?}"
        );
    }
    nowhere
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcEdge".into()),
        )
        .unwrap();
    assert_eq!(
        nowhere
            .execute_update()
            .expect("ingest after supplying the destination"),
        Some(1),
        "the failed no-destination attempts must not have consumed the bound data"
    );
    assert_eq!(count_rows(&mut connection, "AdbcEdge"), 7);

    // --- An ingest target with NO bound data: InvalidState on both entry points.
    let mut nodata = append_stmt(&mut connection);
    let error = nodata
        .execute_update()
        .expect_err("ingest with no bound data must fail");
    assert_eq!(
        error.status,
        adbc_core::error::Status::InvalidState,
        "ingest with no bound data must be InvalidState, got: {error:?}"
    );
    assert!(
        error.message.contains("no data has been bound"),
        "unexpected message: {error:?}"
    );
    let error = nodata
        .execute()
        .err()
        .expect("ingest via execute with no bound data must fail");
    assert_eq!(
        error.status,
        adbc_core::error::Status::InvalidState,
        "ingest via execute with no bound data must be InvalidState, got: {error:?}"
    );

    // --- Full reuse of ONE statement handle: ingest → DML via execute_update → ingest again.
    // Each mode switch must clear the other's state, in both directions.
    let mut handle = append_stmt(&mut connection);
    handle
        .bind(batch(&[10, 11]))
        .expect("bind first reuse ingest");
    assert_eq!(
        handle.execute_update().expect("first reuse ingest"),
        Some(2)
    );
    // Switching to SQL clears the ingest target (observable through get_option)...
    handle
        .set_sql_query("INSERT INTO AdbcEdge (Id, Name) VALUES (12, 'dml')")
        .unwrap();
    assert!(
        handle
            .get_option_string(OptionStatement::TargetTable)
            .is_err(),
        "set_sql_query must clear the ingest target"
    );
    assert_eq!(
        handle.execute_update().expect("DML on the reused handle"),
        Some(1)
    );
    // ...and re-setting the target clears the stale DML, so the handle ingests again instead of
    // re-running the INSERT.
    handle
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcEdge".into()),
        )
        .unwrap();
    handle
        .bind(batch(&[13, 14]))
        .expect("bind second reuse ingest");
    assert_eq!(
        handle.execute_update().expect("second reuse ingest"),
        Some(2),
        "the re-set ingest target must win over the stale DML"
    );
    assert_eq!(count_rows(&mut connection, "AdbcEdge"), 12);

    // --- Duplicate primary key: insert mutations keep INSERT semantics, so re-ingesting an
    // existing key fails with AlreadyExists — naming the target table, and not misreported as a
    // schema mismatch.
    let mut dup = append_stmt(&mut connection);
    dup.bind(batch(&[1])).expect("bind duplicate-key row");
    let error = dup
        .execute_update()
        .expect_err("duplicate-PK ingest must fail");
    assert_eq!(
        error.status,
        adbc_core::error::Status::AlreadyExists,
        "a duplicate primary key must surface as AlreadyExists, got: {error:?}"
    );
    assert!(
        error.message.contains("AdbcEdge"),
        "the error must name the target table: {error:?}"
    );
    assert!(
        !error.message.contains("incompatible"),
        "a duplicate key must not be misreported as a schema mismatch: {error:?}"
    );
    assert_eq!(
        count_rows(&mut connection, "AdbcEdge"),
        12,
        "the rejected duplicate must not change the table"
    );

    let mut drop_edge = connection.new_statement().expect("new statement");
    drop_edge.set_sql_query("DROP TABLE AdbcEdge").unwrap();
    drop_edge.execute_update().expect("drop edge table");
}

/// A multi-chunk autocommit ingest that fails midway — a later chunk duplicates a primary key an
/// earlier chunk committed — must report how many rows those earlier chunks already committed.
/// The count is known exactly (the sum of the committed chunk sizes), so the caller learns the
/// table's actual state instead of guessing. The duplicate key also keeps its `AlreadyExists`
/// status through the append-failure remap, naming the table.
#[test]
fn bulk_ingest_mid_chunk_failure_reports_committed_rows() {
    let Some(target) = test_target() else {
        eprintln!(
            "no Spanner target set — skipping bulk_ingest_mid_chunk_failure_reports_committed_rows"
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

    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query(
        "DROP TABLE IF EXISTS AdbcMidFail; \
         CREATE TABLE AdbcMidFail (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    )
    .unwrap();
    ddl.execute_update().expect("create midfail table");

    // Six ~1.1 MB rows force at least two chunks under the ~4 MiB per-chunk byte budget (the same
    // shape as `bulk_ingest_chunks_past_the_byte_budget`). The LAST row reuses the FIRST row's
    // primary key, so wherever the chunk boundaries fall the duplicate sits in a later chunk than
    // its victim: the first chunk always commits, and the chunk holding the last row always fails.
    const ROWS: usize = 6;
    const VALUE_LEN: usize = 1_100_000;
    let mut ids: Vec<i64> = (0..ROWS as i64).collect();
    ids[ROWS - 1] = 0; // duplicate of the first row's key
    let names: Vec<String> = (0..ROWS)
        .map(|i| char::from(b'a' + i as u8).to_string().repeat(VALUE_LEN))
        .collect();
    let rows = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
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
            OptionValue::String("AdbcMidFail".into()),
        )
        .unwrap();
    ingest
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("append".into()),
        )
        .unwrap();
    ingest.bind(rows).expect("bind mid-fail ingest rows");
    let error = ingest
        .execute_update()
        .expect_err("the duplicate key in a later chunk must fail the ingest");

    // The duplicate key keeps AlreadyExists through the append remap, naming the table.
    assert_eq!(
        error.status,
        adbc_core::error::Status::AlreadyExists,
        "a duplicate key in a later chunk must be AlreadyExists, got: {error:?}"
    );
    assert!(
        error.message.contains("AdbcMidFail"),
        "the error must name the target table: {error:?}"
    );

    // The earlier chunks' rows stayed committed (per-chunk commits are not atomic as a whole),
    // and the error reports their exact count.
    let committed = count_rows(&mut connection, "AdbcMidFail");
    assert!(
        committed > 0 && committed < ROWS as i64,
        "a mid-ingest failure must leave exactly the earlier chunks' rows, found {committed}"
    );
    assert!(
        error.message.contains(&format!(
            "{committed} row(s) from this bulk ingest's earlier chunks were already committed"
        )),
        "the error must report the rows already committed ({committed} found in the table): {error:?}"
    );

    let mut drop_midfail = connection.new_statement().expect("new statement");
    drop_midfail
        .set_sql_query("DROP TABLE AdbcMidFail")
        .unwrap();
    drop_midfail.execute_update().expect("drop midfail table");
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

/// Tiny reference implementation of the ADBC `LIKE` pattern contract (`%` = any run, `_` = one
/// character, case-sensitive, **no** escape syntax), deliberately independent of the driver's
/// internal `like_match` so the push-down comparison below has an outside oracle.
fn adbc_like(pattern: &str, value: &str) -> bool {
    fn rec(p: &[char], v: &[char]) -> bool {
        match p.split_first() {
            None => v.is_empty(),
            Some((&'%', rest)) => (0..=v.len()).any(|k| rec(rest, &v[k..])),
            Some((&'_', rest)) => !v.is_empty() && rec(rest, &v[1..]),
            Some((c, rest)) => v.first() == Some(c) && rec(rest, &v[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    rec(&p, &v)
}

/// Extract the column names of `table` from a collected `get_objects` result, in reported order.
fn objects_column_names(batches: &[RecordBatch], table: &str) -> Vec<String> {
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
                let column_lists = tables
                    .column_by_name("table_columns")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                for r in 0..tables.len() {
                    if table_names.value(r) != table || column_lists.is_null(r) {
                        continue;
                    }
                    let columns = column_lists.value(r);
                    let columns = columns.as_any().downcast_ref::<StructArray>().unwrap();
                    let column_names = columns
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for k in 0..columns.len() {
                        names.push(column_names.value(k).to_string());
                    }
                }
            }
        }
    }
    names
}

/// `get_objects` pushes the ADBC pattern filters down into the `INFORMATION_SCHEMA` queries as
/// bound-parameter `LIKE` predicates. The push-down must be invisible: for every pattern shape,
/// the filtered result must equal the unfiltered result filtered client-side by an independent
/// implementation of the ADBC pattern contract. Also guards the semantics the push-down could
/// silently break: parent skeletons for all-excluding filters, and foreign keys whose referenced
/// table falls outside the filter.
#[test]
fn get_objects_filter_pushdown_matches_client_filtering() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping get_objects_filter_pushdown");
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

    // Distinctively-named tables covering the pattern shapes: a literal underscore in a name,
    // a one-character variant (`_` wildcard bait), and a foreign key from GoFilterChild to
    // GoFilterAlpha (parent deliberately NOT matched by the child-only filter below).
    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query(
        "DROP TABLE IF EXISTS GoFilterChild; \
         DROP TABLE IF EXISTS GoFilterAlpha; \
         DROP TABLE IF EXISTS GoFilter_Beta; \
         DROP TABLE IF EXISTS GoFilterXBeta; \
         CREATE TABLE GoFilterAlpha (Id INT64, NameOne STRING(MAX), Name_Two STRING(MAX), \
         NameXTwo STRING(MAX)) PRIMARY KEY (Id); \
         CREATE TABLE GoFilter_Beta (Id INT64) PRIMARY KEY (Id); \
         CREATE TABLE GoFilterXBeta (Id INT64) PRIMARY KEY (Id); \
         CREATE TABLE GoFilterChild (Id INT64, AlphaId INT64, \
         CONSTRAINT FK_GoFilterChild_Alpha FOREIGN KEY (AlphaId) \
         REFERENCES GoFilterAlpha (Id)) PRIMARY KEY (Id)",
    )
    .unwrap();
    ddl.execute_update().expect("create filter tables");

    let collect = |connection: &mut SpannerConnection,
                   table_name: Option<&str>,
                   column_name: Option<&str>| {
        connection
            .get_objects(ObjectDepth::All, None, None, table_name, None, column_name)
            .expect("get_objects")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect objects")
    };

    // The oracle: everything, filtered client-side per pattern by the independent matcher.
    let unfiltered = collect(&mut connection, None, None);
    let mut all_names = objects_table_names(&unfiltered);
    all_names.sort();

    let table_patterns = [
        "GoFilterAlpha", // exact name
        "GoFilter%",     // prefix
        "%Alpha%",       // substring
        "%ilter_B%",     // substring with `_` wildcard
        "GoFilter_Beta", // `_` wildcard: matches GoFilter_Beta AND GoFilterXBeta
        "GoFilterNone%", // matches nothing
    ];
    for pattern in table_patterns {
        let mut filtered = objects_table_names(&collect(&mut connection, Some(pattern), None));
        filtered.sort();
        let expected: Vec<String> = all_names
            .iter()
            .filter(|n| adbc_like(pattern, n))
            .cloned()
            .collect();
        assert_eq!(
            filtered, expected,
            "server-filtered result must equal client-filtered result for {pattern:?}"
        );
    }

    // `_` stayed a wildcard through the push-down: the pattern with a literal-looking underscore
    // matches both the underscore name and the X variant (ADBC patterns have no escape syntax).
    let mut wildcarded =
        objects_table_names(&collect(&mut connection, Some("GoFilter_Beta"), None));
    wildcarded.sort();
    assert_eq!(wildcarded, ["GoFilterXBeta", "GoFilter_Beta"]);

    // Column filters, same contract: compare each pattern against the unfiltered column list.
    let all_columns = objects_column_names(&unfiltered, "GoFilterAlpha");
    assert_eq!(all_columns, ["Id", "NameOne", "Name_Two", "NameXTwo"]);
    for pattern in ["Name%", "Name_Two", "%Two", "Id", "Zzz%"] {
        let filtered = collect(&mut connection, Some("GoFilterAlpha"), Some(pattern));
        let expected: Vec<String> = all_columns
            .iter()
            .filter(|n| adbc_like(pattern, n))
            .cloned()
            .collect();
        assert_eq!(
            objects_column_names(&filtered, "GoFilterAlpha"),
            expected,
            "column filter {pattern:?}"
        );
    }

    // Empty-vs-null skeletons survive the push-down: a table filter matching nothing keeps every
    // schema with an EMPTY (non-null) table list — the SCHEMATA query is not filtered by the
    // table pattern, only TABLES is.
    let none = collect(&mut connection, Some("GoFilterNone%"), None);
    assert!(objects_table_names(&none).is_empty());
    let batch = &none[0];
    let schema_lists = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(schema_lists.is_valid(0));
    let schemas = schema_lists.value(0);
    let schemas = schemas.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(!schemas.is_empty(), "schema skeletons must be kept");
    let table_lists = schemas
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    for s in 0..schemas.len() {
        assert!(
            table_lists.is_valid(s) && table_lists.value(s).is_empty(),
            "each kept schema must carry an empty, non-null table list"
        );
    }

    // A column filter matching nothing keeps the table with an empty, non-null column list.
    let no_columns = collect(&mut connection, Some("GoFilterAlpha"), Some("Zzz%"));
    assert_eq!(objects_table_names(&no_columns), ["GoFilterAlpha"]);
    assert!(objects_column_names(&no_columns, "GoFilterAlpha").is_empty());

    // Foreign keys resolve across the filter boundary: filtering to the child only, its
    // constraint_column_usage must still name the (excluded) parent's column — the
    // KEY_COLUMN_USAGE / REFERENTIAL_CONSTRAINTS queries are deliberately not filtered.
    let child_only = collect(&mut connection, Some("GoFilterChild"), None);
    assert_eq!(objects_table_names(&child_only), ["GoFilterChild"]);
    let batch = &child_only[0];
    let schemas = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let schemas = schemas.as_any().downcast_ref::<StructArray>().unwrap();
    let mut fk_usage = None;
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
        let constraint_lists = tables
            .column_by_name("table_constraints")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for r in 0..tables.len() {
            let constraints = constraint_lists.value(r);
            let constraints = constraints.as_any().downcast_ref::<StructArray>().unwrap();
            let ctype = constraints
                .column_by_name("constraint_type")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let usage_lists = constraints
                .column_by_name("constraint_column_usage")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for k in 0..constraints.len() {
                if ctype.value(k) == "FOREIGN KEY" {
                    assert!(usage_lists.is_valid(k), "FK usage list must be non-null");
                    let usage = usage_lists.value(k);
                    let usage = usage.as_any().downcast_ref::<StructArray>().unwrap();
                    assert_eq!(usage.len(), 1);
                    let fk_table = usage
                        .column_by_name("fk_table")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let fk_column = usage
                        .column_by_name("fk_column_name")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    fk_usage = Some((
                        fk_table.value(0).to_string(),
                        fk_column.value(0).to_string(),
                    ));
                }
            }
        }
    }
    assert_eq!(
        fk_usage,
        Some(("GoFilterAlpha".to_string(), "Id".to_string())),
        "the FK must resolve its parent column even though the filter excludes the parent table"
    );

    let mut drop_tables = connection.new_statement().expect("new statement");
    drop_tables
        .set_sql_query(
            "DROP TABLE GoFilterChild; DROP TABLE GoFilterAlpha; \
             DROP TABLE GoFilter_Beta; DROP TABLE GoFilterXBeta",
        )
        .unwrap();
    drop_tables.execute_update().expect("drop filter tables");
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

// -------------------------------------------------------------------------------------------
// FFI / stream lifecycle battery.
//
// Driver-manager-level regression tests for the crash class that dominated other ADBC drivers'
// bug reports (DuckDB's 2026 stream-lifetime fixes, e.g. the release race fixed in DuckDB PR
// #21800; the C++ `adbc_validation` suite's TestResultIndependence / TestResultInvalidation
// checks): a result stream handed across the C ABI is a standalone object, so consuming or
// releasing it must never crash regardless of what has happened to the statement / connection /
// database that produced it. Erroring is acceptable per the ADBC spec; crashing is not. Every
// query here generates rows via GENERATE_ARRAY, so the battery is read-only and — like
// `ffi_driver_manager_smoke` — exempt from `serial_guard`.
// -------------------------------------------------------------------------------------------

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
            OptionValue::String(target.database_path()),
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
            OptionValue::String(target.database_path()),
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
            OptionValue::String(target.database_path()),
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
    let value = CString::new(target.database_path()).unwrap();
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

/// Retry-tuning options (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`)
/// round-trip through `get_option` / `get_option_int` / `get_option_double`, inherit onto statements
/// then override, and — most importantly — a statement carrying a bounded retry policy still
/// executes a real query and DML successfully against the emulator (exercising the
/// `with_retry_policy` / `with_begin_retry_policy` / `with_commit_retry_policy` apply path). This is
/// read-only-plus-one-row so it does not need the schema serial guard.
#[test]
fn retry_tuning_round_trip_and_execute() {
    let Some(target) = test_target() else {
        eprintln!(
            "neither SPANNER_EMULATOR_HOST nor SPANNER_GCP_DATABASE set — \
             skipping retry-tuning integration test"
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

    // Connection-level round-trip, including the numeric accessors.
    let attempts_key = OptionConnection::Other("spanner.retry.max_attempts".into());
    let elapsed_key = OptionConnection::Other("spanner.retry.max_elapsed_seconds".into());
    connection
        .set_option(attempts_key.clone(), OptionValue::String("4".into()))
        .expect("set max_attempts");
    connection
        .set_option(elapsed_key.clone(), OptionValue::String("30".into()))
        .expect("set max_elapsed_seconds");
    assert_eq!(
        connection.get_option_string(attempts_key.clone()).unwrap(),
        "4"
    );
    assert_eq!(connection.get_option_int(attempts_key.clone()).unwrap(), 4);
    assert_eq!(
        connection.get_option_double(elapsed_key.clone()).unwrap(),
        30.0
    );

    // A statement inherits the connection's values, then overrides independently.
    let st_attempts = OptionStatement::Other("spanner.retry.max_attempts".into());
    let st_elapsed = OptionStatement::Other("spanner.retry.max_elapsed_seconds".into());
    let mut stmt = connection.new_statement().expect("new statement");
    assert_eq!(stmt.get_option_string(st_attempts.clone()).unwrap(), "4");
    assert_eq!(stmt.get_option_string(st_elapsed.clone()).unwrap(), "30");
    stmt.set_option(st_attempts.clone(), OptionValue::String("2".into()))
        .expect("override max_attempts");
    stmt.set_option(st_elapsed.clone(), OptionValue::String(String::new()))
        .expect("unset max_elapsed_seconds");
    assert_eq!(stmt.get_option_string(st_attempts.clone()).unwrap(), "2");
    assert_eq!(
        stmt.get_option_string(st_elapsed.clone())
            .unwrap_err()
            .status,
        Status::NotFound
    );

    // A bad value is rejected and leaves the stored value intact.
    assert_eq!(
        stmt.set_option(st_attempts.clone(), OptionValue::String("0".into()))
            .unwrap_err()
            .status,
        Status::InvalidArguments
    );
    assert_eq!(stmt.get_option_string(st_attempts.clone()).unwrap(), "2");

    // The statement's bounded retry policy is actually applied to a live query.
    stmt.set_sql_query("SELECT 1 AS one").unwrap();
    let reader = stmt.execute().expect("query with bounded retry policy");
    let rows: usize = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect")
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 1);

    // And to a write path (the runner's begin+commit RPCs): a no-op DELETE commits cleanly.
    let mut dml = connection.new_statement().expect("new statement");
    dml.set_sql_query("DELETE FROM Singers WHERE SingerId = -987654321")
        .unwrap();
    dml.execute_update()
        .expect("DML with bounded retry policy commits");
}

/// Spanner **property graphs** and **GQL graph queries** work through plain SQL: the graph is
/// declared with a `CREATE PROPERTY GRAPH` DDL statement over ordinary node/edge tables, and a
/// `GRAPH … MATCH … RETURN` query executes through the normal `execute` query path — no special
/// driver support is required, since GoogleSQL surfaces GQL as just another read-only query. This
/// exercises the full round-trip against the emulator: create the tables, declare the graph, insert
/// rows, then traverse an edge with `MATCH (a)-[e]->(b)` and assert the returned columns/rows.
#[test]
fn gql_graph_query_round_trip() {
    let Some(target) = test_target() else {
        eprintln!(
            "neither SPANNER_EMULATOR_HOST nor SPANNER_GCP_DATABASE set — \
             skipping GQL graph-query integration test"
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

    // Schema: a node table `GqlAccount` and an edge table `GqlTransfer` whose (source, destination)
    // keys reference it. All DDL is idempotent so the test can re-run against a persistent target.
    // The `CREATE PROPERTY GRAPH` declaration must come after its underlying tables exist, so it is
    // submitted as a second batch. Names are prefixed `Gql` to avoid colliding with other tests.
    let mut ddl = connection.new_statement().expect("new statement");
    ddl.set_sql_query(
        "DROP PROPERTY GRAPH IF EXISTS GqlFinGraph; \
         DROP TABLE IF EXISTS GqlTransfer; \
         DROP TABLE IF EXISTS GqlAccount; \
         CREATE TABLE GqlAccount (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (Id); \
         CREATE TABLE GqlTransfer ( \
             Id INT64 NOT NULL, ToId INT64 NOT NULL, Amount FLOAT64 \
         ) PRIMARY KEY (Id, ToId)",
    )
    .unwrap();
    assert_eq!(ddl.execute_update().expect("create graph tables"), None);

    let mut graph_ddl = connection.new_statement().expect("new statement");
    graph_ddl
        .set_sql_query(
            "CREATE PROPERTY GRAPH GqlFinGraph \
                 NODE TABLES (GqlAccount KEY (Id) LABEL Account PROPERTIES (Id, Name)) \
                 EDGE TABLES ( \
                     GqlTransfer \
                         KEY (Id, ToId) \
                         SOURCE KEY (Id) REFERENCES GqlAccount (Id) \
                         DESTINATION KEY (ToId) REFERENCES GqlAccount (Id) \
                         LABEL Transfer PROPERTIES (Amount) \
                 )",
        )
        .unwrap();
    assert_eq!(
        graph_ddl.execute_update().expect("create property graph"),
        None
    );

    // Populate three accounts and two transfers (1 -> 2 of 100.0, 2 -> 3 of 42.5).
    let mut ins = connection.new_statement().expect("new statement");
    ins.set_sql_query(
        "INSERT INTO GqlAccount (Id, Name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .unwrap();
    assert_eq!(ins.execute_update().expect("insert accounts"), Some(3));

    let mut ins_edges = connection.new_statement().expect("new statement");
    ins_edges
        .set_sql_query(
            "INSERT INTO GqlTransfer (Id, ToId, Amount) VALUES (1, 2, 100.0), (2, 3, 42.5)",
        )
        .unwrap();
    assert_eq!(
        ins_edges.execute_update().expect("insert transfers"),
        Some(2)
    );

    // The GQL graph query: traverse each transfer edge and return the endpoint names + amount.
    // Executed through the ordinary query path (`execute`), exactly like any GoogleSQL SELECT.
    let mut gql = connection.new_statement().expect("new statement");
    gql.set_sql_query(
        "GRAPH GqlFinGraph \
         MATCH (a:Account)-[t:Transfer]->(b:Account) \
         RETURN a.Name AS src, b.Name AS dst, t.Amount AS amount",
    )
    .unwrap();
    let reader = gql.execute().expect("execute GQL graph query");

    // The Arrow schema reflects the RETURN clause's projected columns and their GoogleSQL types.
    let schema = reader.schema();
    assert_eq!(schema.field(0).name(), "src");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "dst");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(2).name(), "amount");
    assert_eq!(schema.field(2).data_type(), &DataType::Float64);

    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect GQL result batches");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "expected two edges back from the graph match"
    );

    // Collect the returned edges into a set, since the graph-match traversal makes no ordering
    // guarantee without an explicit `ORDER BY`.
    let mut edges = Vec::new();
    for batch in &batches {
        let src = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dst = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amount = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            edges.push((
                src.value(i).to_string(),
                dst.value(i).to_string(),
                amount.value(i),
            ));
        }
    }
    edges.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());
    assert_eq!(
        edges,
        vec![
            ("Bob".to_string(), "Carol".to_string(), 42.5),
            ("Alice".to_string(), "Bob".to_string(), 100.0),
        ],
        "the two transfer edges must round-trip through the graph match"
    );

    // Clean up the scratch schema so a persistent `SPANNER_GCP_DATABASE` re-run stays tidy.
    let mut cleanup = connection.new_statement().expect("new statement");
    cleanup
        .set_sql_query(
            "DROP PROPERTY GRAPH IF EXISTS GqlFinGraph; \
             DROP TABLE IF EXISTS GqlTransfer; \
             DROP TABLE IF EXISTS GqlAccount",
        )
        .unwrap();
    cleanup.execute_update().expect("drop graph schema");
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

    // approximate=true serves the same exact statistics: approximate merely *allows* inexact
    // values, and Spanner has no cheaper source, so both modes run the same aggregate scans.
    let approx = connection
        .get_statistics(None, None, Some("AdbcStats"), true)
        .expect("get_statistics approx")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect approx");
    assert_eq!(
        extract_statistics(&approx[0]),
        stats,
        "approximate=true must serve the same exact statistics"
    );

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
         CREATE TABLE AdbcJson (Id INT64, Doc JSON, Ratio FLOAT32, Docs ARRAY<JSON>) \
         PRIMARY KEY (Id)",
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

    // --- Bind round trip: `arrow.json`-tagged parameters bind as JSON / ARRAY<JSON>. Spanner
    // rejects a plain STRING parameter in a JSON column, so the inserts below only succeed if the
    // driver sends the explicit JSON param type for tagged columns (this is also exactly what the
    // driver's own read path produces, so read → bind round-trips).
    let json_field = |name: &str| {
        Field::new(name, DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.json".to_string(),
        )]))
    };
    let docs_item = Arc::new(json_field("item"));
    let mut docs_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new())
            .with_field(docs_item.clone());
    docs_builder.values().append_value(r#"{"d":true}"#);
    docs_builder.values().append_null();
    docs_builder.append(true);
    docs_builder.append(false); // whole-cell NULL array for the second row
    let bind_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", DataType::Int64, false),
            json_field("Doc"),
            Field::new("Docs", DataType::List(docs_item), true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec![Some(r#"{"c":[1,2]}"#), None])),
            Arc::new(docs_builder.finish()),
        ],
    )
    .unwrap();
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO AdbcJson (Id, Doc, Docs) VALUES (@Id, @Doc, @Docs)")
        .unwrap();
    insert.bind(bind_batch.clone()).expect("bind json rows");
    insert.execute_update().expect("insert bound json rows");

    let mut verify = connection.new_statement().expect("new statement");
    verify
        .set_sql_query("SELECT Doc, Docs FROM AdbcJson WHERE Id >= 3 ORDER BY Id")
        .unwrap();
    let reader = verify.execute().expect("query bound json");
    // ARRAY<JSON> reads back with the extension tag on the list's item field.
    let bound_schema = reader.schema();
    let DataType::List(read_item) = bound_schema.field(1).data_type() else {
        panic!("Docs must read back as a List: {bound_schema:?}");
    };
    assert_eq!(
        read_item
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.json"),
        "ARRAY<JSON> item must carry the arrow.json extension"
    );
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bound json batches");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    let batch = &batches[0];
    let doc_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        doc_col.value(0).contains(r#""c":[1,2]"#),
        "unexpected bound JSON text: {:?}",
        doc_col.value(0)
    );
    assert!(doc_col.is_null(1), "bound NULL JSON must come back null");
    let docs_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let first_cell = docs_col.value(0);
    let first_cell = first_cell.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(first_cell.len(), 2);
    assert!(
        first_cell.value(0).contains(r#""d":true"#),
        "unexpected bound ARRAY<JSON> element: {:?}",
        first_cell.value(0)
    );
    assert!(first_cell.is_null(1), "NULL array element must survive");
    assert!(docs_col.is_null(1), "bound NULL array must come back null");

    // --- Create-mode ingest maps tagged fields to JSON / ARRAY<JSON> columns (a STRING(MAX)
    // column would reject the JSON-typed row params the ingest itself binds).
    let mut ingest = connection.new_statement().expect("new statement");
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("AdbcJsonIngest".into()),
        )
        .unwrap();
    ingest
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("create".into()),
        )
        .unwrap();
    ingest.bind(bind_batch).expect("bind json ingest rows");
    ingest.execute_update().expect("create-mode json ingest");
    let mut cols = connection.new_statement().expect("new statement");
    cols.set_sql_query(
        "SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'AdbcJsonIngest' AND COLUMN_NAME IN ('Doc', 'Docs') \
         ORDER BY COLUMN_NAME",
    )
    .unwrap();
    let cols_batches = cols
        .execute()
        .expect("query ingest column types")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect ingest column types");
    let cols_batch = &cols_batches[0];
    let names = cols_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let types = cols_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (names.value(0), types.value(0)),
        ("Doc", "JSON"),
        "tagged scalar ingest column"
    );
    assert_eq!(
        (names.value(1), types.value(1)),
        ("Docs", "ARRAY<JSON>"),
        "tagged array ingest column"
    );

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcJson; DROP TABLE AdbcJsonIngest")
        .unwrap();
    drop.execute_update().expect("drop json tables");
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

/// The `adbc.statement.bind_by_name` option (the ADBC SQLite reference driver's convention): the
/// default (`false`) binds positionally even when every bound column name coincidentally matches a
/// query parameter, and `true` forces strict by-name binding that rejects an unmatched column.
/// Also covers the `get_option` round-trip (`true`/`false`, defaulting to `false`).
#[test]
fn bind_by_name_modes() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping bind_by_name_modes");
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

    let bind_by_name_key = || OptionStatement::Other(adbc_spanner::OPTION_BIND_BY_NAME.into());
    // Two Int64 columns named after the query's parameters but in SWAPPED order: `b` (=10)
    // first, `a` (=20) second — the coincidental-name-match input where the binding mode is
    // observable in the result.
    let batch = |names: [&str; 2]| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(names[0], DataType::Int64, false),
                Field::new(names[1], DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![10i64])),
                Arc::new(Int64Array::from(vec![20i64])),
            ],
        )
        .unwrap()
    };
    // Run `SELECT @a, @b` with `rows` bound under the given bind_by_name value (None = leave the
    // option at its default) and return the (@a, @b) values Spanner saw.
    let query_pair = |connection: &mut SpannerConnection,
                      rows: RecordBatch,
                      mode: Option<&str>|
     -> Result<(i64, i64), adbc_core::error::Error> {
        let mut s = connection.new_statement().expect("new statement");
        if let Some(value) = mode {
            s.set_option(bind_by_name_key(), OptionValue::String(value.into()))
                .expect("set bind_by_name");
            assert_eq!(
                s.get_option_string(bind_by_name_key())
                    .expect("get bind_by_name"),
                value,
                "bind_by_name must round-trip through get_option"
            );
        } else {
            assert_eq!(
                s.get_option_string(bind_by_name_key())
                    .expect("bind_by_name reads its default"),
                "false",
                "bind_by_name defaults to false (positional)"
            );
        }
        s.set_sql_query("SELECT @a AS a_value, @b AS b_value")
            .unwrap();
        s.bind(rows).expect("bind rows");
        let reader = s.execute()?;
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("collect bound query result");
        assert_eq!(batches.len(), 1, "one bound row -> one result batch");
        let ints = |i: usize| {
            batches[0]
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        Ok((ints(0), ints(1)))
    };

    // Default (option left unset): strictly positional — the swapped batch binds column 0 -> @a,
    // column 1 -> @b, the coincidental name matches (and their order) ignored entirely.
    assert_eq!(
        query_pair(&mut connection, batch(["b", "a"]), None).expect("default positional binding"),
        (10, 20),
        "the default (bind_by_name unset) must bind positionally"
    );
    // false: the same positional binding, set explicitly.
    assert_eq!(
        query_pair(&mut connection, batch(["b", "a"]), Some("false")).expect("positional binding"),
        (10, 20),
        "bind_by_name=false must ignore coincidental name matches and bind positionally"
    );
    // true: strict by-name — order-independent, the swapped columns land on their namesakes.
    assert_eq!(
        query_pair(&mut connection, batch(["b", "a"]), Some("true")).expect("by-name binding"),
        (20, 10),
        "bind_by_name=true must bind matching columns by name"
    );
    // true with an unmatched column: a hard InvalidArguments error naming the parameter, instead
    // of a silent positional fallback.
    let error = query_pair(&mut connection, batch(["a", "x"]), Some("true"))
        .expect_err("bind_by_name=true must reject an unmatched column");
    assert_eq!(error.status, adbc_core::error::Status::InvalidArguments);
    assert!(
        error.message.contains("could not find parameter \"x\""),
        "error must name the missing parameter: {}",
        error.message
    );
    // The same partial match under the default (positional) binding succeeds: column 0 -> @a,
    // column 1 -> @b, names ignored.
    assert_eq!(
        query_pair(&mut connection, batch(["a", "x"]), None).expect("default positional binding"),
        (10, 20),
        "a partial name match under the default binding must bind positionally"
    );
    // An empty string is not a valid boolean and is rejected (the option has no unset state).
    let mut invalid = connection.new_statement().expect("new statement");
    assert_eq!(
        invalid
            .set_option(bind_by_name_key(), OptionValue::String(String::new()))
            .expect_err("empty bind_by_name must be rejected")
            .status,
        adbc_core::error::Status::InvalidArguments,
    );
}

/// A parameterized query over **several bound rows** streams through the same bounded-chunk
/// machinery as a plain query: each bound row's result is converted to Arrow in chunks of
/// `spanner.rows_per_batch` (not materialised whole), and all rows execute inside one shared
/// read-only snapshot (a multi-use read-only transaction) rather than one single-use transaction
/// per bound row.
#[test]
fn bound_query_streams_in_batches() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping bound_query_streams_in_batches");
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
        "DROP TABLE IF EXISTS AdbcBoundStream; \
         CREATE TABLE AdbcBoundStream (Id INT64, Grp INT64) PRIMARY KEY (Id)",
    );
    // 1500 rows split across three groups of 500 via GENERATE_ARRAY.
    run(
        &mut connection,
        "INSERT INTO AdbcBoundStream (Id, Grp) \
         SELECT n, MOD(n, 3) FROM UNNEST(GENERATE_ARRAY(1, 1500)) AS n",
    );

    let mut query = connection.new_statement().expect("new statement");
    // A small batch size so each bound row's 500-row result spans several batches.
    query
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(200),
        )
        .expect("set rows_per_batch");
    query
        .set_sql_query("SELECT Id FROM AdbcBoundStream WHERE Grp = @Grp ORDER BY Id")
        .unwrap();
    // Three bound parameter rows: the query runs once per bound row, all in one snapshot.
    let params = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Grp", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![0i64, 1, 2]))],
    )
    .unwrap();
    query.bind(params).expect("bind three parameter rows");

    let reader = query.execute().expect("bound streaming query");
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect streamed batches");

    // 500 rows per bound row at 200 per batch → (200, 200, 100) per bound row, nine batches in
    // total; the previous implementation materialised one monolithic batch per bound row.
    let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(sizes, vec![200, 200, 100, 200, 200, 100, 200, 200, 100]);
    assert!(batches.iter().all(|b| b.schema() == schema));

    // The concatenation is each group's ids ascending, groups in bound-row order.
    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let expected: Vec<i64> = (0..3)
        .flat_map(|group| (1..=1500i64).filter(move |id| id % 3 == group))
        .collect();
    assert_eq!(ids, expected);

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcBoundStream").unwrap();
    drop.execute_update().expect("drop bound stream table");
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

/// Dropping a streamed reader mid-stream — with the background prefetch fetch still in flight —
/// must abort the prefetch task cleanly (no leak, no runtime panic) and leave the statement and
/// connection fully usable.
#[test]
fn dropping_reader_mid_stream_aborts_the_prefetch() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping dropping_reader_mid_stream");
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
        "DROP TABLE IF EXISTS AdbcReaderDrop; \
         CREATE TABLE AdbcReaderDrop (Id INT64) PRIMARY KEY (Id)",
    );
    run(
        &mut connection,
        "INSERT INTO AdbcReaderDrop (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 500)) AS n",
    );

    let mut query = connection.new_statement().expect("new statement");
    // Small batches so several chunks remain unfetched when the reader is dropped.
    query
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(50),
        )
        .expect("set rows_per_batch");
    query
        .set_sql_query("SELECT Id FROM AdbcReaderDrop ORDER BY Id")
        .unwrap();
    let mut reader = query.execute().expect("streaming query");
    let first = reader
        .next()
        .expect("first batch")
        .expect("first batch is ok");
    assert_eq!(first.num_rows(), 50);
    // Drop with ~9 chunks unread and the prefetch of the next one racing this drop.
    drop(reader);

    // The statement (and its shared runtime) must be unaffected: a fresh execute streams the whole
    // result normally.
    let batches = query
        .execute()
        .expect("re-execute after dropping the reader")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect re-executed batches");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 500);

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcReaderDrop").unwrap();
    drop.execute_update().expect("drop reader-drop table");
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
        // Every partition's reader must agree with the schema `execute_partitions` reported up
        // front — a consumer routes each descriptor by that schema, so drift would be a bug.
        assert_eq!(*reader.schema(), partitioned.schema);
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

/// `rollback()` without an active manual transaction — i.e. in autocommit mode, the default — is
/// an `InvalidState` error, while `rollback()` inside a manual transaction with nothing buffered
/// is a harmless no-op that discards nothing and keeps the connection in manual mode.
#[test]
fn rollback_without_a_transaction_is_invalid_state() {
    use adbc_core::error::Status;

    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping rollback_without_a_transaction");
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

    // Autocommit (the default): there is no transaction to roll back.
    let error = connection
        .rollback()
        .expect_err("rollback in autocommit mode must fail");
    assert_eq!(error.status, Status::InvalidState, "got: {error:?}");
    assert!(
        error.message.contains("autocommit"),
        "the error should say why there is no transaction, got: {}",
        error.message
    );

    // Manual mode with an empty buffer: rollback succeeds as a no-op — there is an (empty)
    // transaction to discard — and it must not flip the connection back to autocommit.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");
    connection
        .rollback()
        .expect("rollback with nothing buffered is a no-op");
    connection
        .rollback()
        .expect("a repeated empty rollback is still a no-op");
    assert_eq!(
        connection
            .get_option_string(OptionConnection::AutoCommit)
            .expect("get autocommit"),
        "false",
        "rollback discards buffered work, not the transaction mode"
    );

    // Back in autocommit mode the error returns.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("re-enable autocommit");
    assert_eq!(
        connection
            .rollback()
            .expect_err("rollback must fail again in autocommit mode")
            .status,
        Status::InvalidState
    );
}

/// `get_statistic_names` returns an *empty* result set with exactly the canonical ADBC schema —
/// Spanner exposes no portable named statistics (`get_statistics` computes its standard ones on
/// demand instead).
#[test]
fn get_statistic_names_is_empty_and_correctly_typed() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping get_statistic_names");
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
    let connection = connect_with_retry(&database);

    let mut reader = connection
        .get_statistic_names()
        .expect("get_statistic_names");
    let schema = reader.schema();
    assert_eq!(
        schema,
        adbc_core::schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
        "the reader must carry the canonical GET_STATISTIC_NAMES_SCHEMA"
    );
    // Spell out the shape too, so a drift in the upstream constant cannot silently pass.
    assert_eq!(schema.field(0).name(), "statistic_name");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "statistic_key");
    assert_eq!(schema.field(1).data_type(), &DataType::Int16);
    assert!(!schema.field(1).is_nullable());
    assert!(
        reader.next().is_none(),
        "expected an empty result set (no batches at all)"
    );
}

/// `read_partition` with a descriptor that is not one of ours must fail cleanly with
/// `InvalidArguments` — no panic, and no RPC: the decode step rejects it before anything executes.
/// (The decode itself is unit-tested offline in `src/connection.rs`; this exercises the same
/// inputs through the public connection method.)
#[test]
fn read_partition_rejects_garbage_descriptors() {
    use adbc_core::error::Status;

    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping read_partition_rejects_garbage_descriptors");
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
    let connection = connect_with_retry(&database);

    let cases: [&[u8]; 4] = [
        b"",                      // empty descriptor
        b"\xffnot json",          // non-JSON garbage
        br#"{"hello": "world"}"#, // valid JSON that is not a partition descriptor
        b"[1, 2, 3]",             // valid JSON that is not even an object
    ];
    for descriptor in cases {
        let Err(error) = connection.read_partition(descriptor) else {
            panic!("descriptor {descriptor:?} must be rejected");
        };
        assert_eq!(
            error.status,
            Status::InvalidArguments,
            "descriptor {descriptor:?} → {error:?}"
        );
        assert!(
            error.message.contains("invalid partition descriptor"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
}

/// `Connection::cancel` mirrors the statement-level semantics on the connection's own signal: the
/// latch is sticky, so a cancel that lands *between* two chunk fetches of a `read_partition`
/// stream (nothing parked on the signal) still cancels the next fetch; it does not touch
/// statements, which carry their own signal; and the connection's next operation clears it.
/// Deterministic — the cancel is always latched *before* the operation it must affect.
#[test]
fn connection_cancel_is_sticky_until_the_next_operation() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping connection_cancel_is_sticky");
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
        "DROP TABLE IF EXISTS AdbcConnCancel; \
         CREATE TABLE AdbcConnCancel (Id INT64) PRIMARY KEY (Id)",
    );
    run(
        &mut connection,
        "INSERT INTO AdbcConnCancel (Id) \
         SELECT n FROM UNNEST(GENERATE_ARRAY(1, 200)) AS n",
    );

    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_sql_query("SELECT Id FROM AdbcConnCancel")
        .unwrap();
    let partitioned = statement.execute_partitions().expect("execute_partitions");
    let descriptor = partitioned
        .partitions
        .first()
        .expect("at least one partition");

    // Stream one partition and consume the prefetched first chunk, leaving the stream idle
    // between fetches — then cancel with nothing parked on the signal, exactly the window where
    // a non-sticky signal would be lost.
    let mut reader = connection
        .read_partition(descriptor)
        .expect("read_partition");
    let first = reader
        .next()
        .expect("first batch")
        .expect("first batch is ok");
    assert!(first.num_rows() <= 200);
    connection.cancel().expect("cancel");

    // The next chunk fetch must observe the latched cancel instead of running to completion.
    let error = reader
        .next()
        .expect("the cancelled fetch yields an item")
        .expect_err("the fetch after cancel must fail");
    assert!(
        error.to_string().to_lowercase().contains("cancel"),
        "expected a cancellation error, got: {error}"
    );

    // The connection-level latch must not leak into statements: they have their own signal, so a
    // statement query on this connection still runs while the connection latch is set.
    assert_eq!(count_rows(&mut connection, "AdbcConnCancel"), 200);

    // Starting the connection's next operation clears the latch: reading every partition back now
    // succeeds and reproduces the full result set (including the partition whose earlier read was
    // cancelled mid-stream).
    let mut total = 0usize;
    for token in &partitioned.partitions {
        let reader = connection
            .read_partition(token)
            .expect("read_partition after cancel");
        for batch in reader {
            total += batch.expect("partition batch").num_rows();
        }
    }
    assert_eq!(
        total, 200,
        "after the reset the connection must stream every partition normally"
    );

    // A cancel latched with nothing at all in flight must not pre-empt an unrelated metadata
    // operation either — every connection entry point resets the signal first.
    connection.cancel().expect("cancel with nothing in flight");
    let schema = connection
        .get_table_schema(None, None, "AdbcConnCancel")
        .expect("get_table_schema after a stale cancel");
    assert_eq!(schema.fields().len(), 1);

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcConnCancel").unwrap();
    drop.execute_update().expect("drop conn-cancel table");
}

/// Request priority and request/transaction tags (`spanner.request.priority` /
/// `spanner.request.tag` / `spanner.transaction.tag`): the options round-trip through
/// `get_option`, statements inherit the connection's values and can override the priority and
/// request tag, bad values are rejected, and a query plus DML run end-to-end with all three set
/// (the emulator accepts and ignores priorities/tags, so this proves the wiring sends valid
/// requests rather than asserting on scheduler behaviour).
#[test]
fn request_priority_and_tags() {
    use adbc_core::error::Status;
    use adbc_spanner::{OPTION_REQUEST_PRIORITY, OPTION_REQUEST_TAG, OPTION_TRANSACTION_TAG};

    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping request_priority_and_tags");
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

    let conn_key = |k: &str| OptionConnection::Other(k.into());
    let stmt_key = |k: &str| OptionStatement::Other(k.into());

    // Unset options read back as NotFound.
    for key in [
        OPTION_REQUEST_PRIORITY,
        OPTION_REQUEST_TAG,
        OPTION_TRANSACTION_TAG,
    ] {
        let error = connection
            .get_option_string(conn_key(key))
            .expect_err("unset option must be NotFound");
        assert_eq!(error.status, Status::NotFound, "{key}");
    }

    // Set all three at connection level; the priority is case-insensitive and reported canonically.
    connection
        .set_option(
            conn_key(OPTION_REQUEST_PRIORITY),
            OptionValue::String("MEDIUM".into()),
        )
        .expect("set connection priority");
    connection
        .set_option(
            conn_key(OPTION_REQUEST_TAG),
            OptionValue::String("adbc-test-request".into()),
        )
        .expect("set connection request tag");
    connection
        .set_option(
            conn_key(OPTION_TRANSACTION_TAG),
            OptionValue::String("adbc-test-txn".into()),
        )
        .expect("set connection transaction tag");
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_REQUEST_PRIORITY))
            .unwrap(),
        "medium"
    );
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_REQUEST_TAG))
            .unwrap(),
        "adbc-test-request"
    );
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_TRANSACTION_TAG))
            .unwrap(),
        "adbc-test-txn"
    );

    // A bad priority is rejected with InvalidArguments and leaves the stored value untouched.
    let error = connection
        .set_option(
            conn_key(OPTION_REQUEST_PRIORITY),
            OptionValue::String("urgent".into()),
        )
        .expect_err("bad priority must be rejected");
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_REQUEST_PRIORITY))
            .unwrap(),
        "medium"
    );

    // Statements inherit the connection's effective values, and may override or unset them.
    let mut statement = connection.new_statement().expect("new statement");
    assert_eq!(
        statement
            .get_option_string(stmt_key(OPTION_REQUEST_PRIORITY))
            .unwrap(),
        "medium",
        "the statement must inherit the connection's priority"
    );
    assert_eq!(
        statement
            .get_option_string(stmt_key(OPTION_REQUEST_TAG))
            .unwrap(),
        "adbc-test-request",
        "the statement must inherit the connection's request tag"
    );
    statement
        .set_option(
            stmt_key(OPTION_REQUEST_PRIORITY),
            OptionValue::String("high".into()),
        )
        .expect("override priority on the statement");
    statement
        .set_option(
            stmt_key(OPTION_REQUEST_TAG),
            OptionValue::String(String::new()),
        )
        .expect("unset the inherited request tag with an empty value");
    assert_eq!(
        statement
            .get_option_string(stmt_key(OPTION_REQUEST_PRIORITY))
            .unwrap(),
        "high"
    );
    let error = statement
        .get_option_string(stmt_key(OPTION_REQUEST_TAG))
        .expect_err("the unset request tag must be NotFound");
    assert_eq!(error.status, Status::NotFound);
    // The statement-level override does not leak back to the connection.
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_REQUEST_PRIORITY))
            .unwrap(),
        "medium"
    );
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_REQUEST_TAG))
            .unwrap(),
        "adbc-test-request"
    );

    // End-to-end with all three options set on the connection: DDL + DML (a tagged read/write
    // transaction) + a query (a tagged read) all succeed, and the results are correct.
    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcReqOpts; \
         CREATE TABLE AdbcReqOpts (Id INT64) PRIMARY KEY (Id)",
    );
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query("INSERT INTO AdbcReqOpts (Id) VALUES (1), (2)")
        .unwrap();
    assert_eq!(insert.execute_update().expect("tagged insert"), Some(2));
    assert_eq!(count_rows(&mut connection, "AdbcReqOpts"), 2);

    // A query on the overriding statement (priority high, request tag unset) also runs fine.
    statement
        .set_sql_query("SELECT Id FROM AdbcReqOpts ORDER BY Id")
        .unwrap();
    let batches = statement
        .execute()
        .expect("tagged query")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    // A tagged parameterized DML (bound rows go through the same builders).
    let row = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![3]))],
    )
    .unwrap();
    let mut bound = connection.new_statement().expect("new statement");
    bound
        .set_sql_query("INSERT INTO AdbcReqOpts (Id) VALUES (@Id)")
        .unwrap();
    bound.bind(row).expect("bind param");
    assert_eq!(
        bound.execute_update().expect("tagged bound insert"),
        Some(1)
    );
    assert_eq!(count_rows(&mut connection, "AdbcReqOpts"), 3);

    // Unsetting at the connection level round-trips back to NotFound.
    for key in [
        OPTION_REQUEST_PRIORITY,
        OPTION_REQUEST_TAG,
        OPTION_TRANSACTION_TAG,
    ] {
        connection
            .set_option(conn_key(key), OptionValue::String(String::new()))
            .expect("unset with an empty value");
        let error = connection
            .get_option_string(conn_key(key))
            .expect_err("an unset option must be NotFound");
        assert_eq!(error.status, Status::NotFound, "{key}");
    }
    // The transaction tag is connection-level only: a statement rejects it.
    let error = statement
        .set_option(
            stmt_key(OPTION_TRANSACTION_TAG),
            OptionValue::String("nope".into()),
        )
        .expect_err("the transaction tag must not be settable on a statement");
    assert_eq!(error.status, Status::NotImplemented);

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcReqOpts").unwrap();
    drop.execute_update().expect("drop reqopts table");
}

/// One expected column of the schema-fidelity projection: a GoogleSQL expression producing a value
/// of a mapped Spanner type, and the exact Arrow [`Field`] every schema path must map it to.
struct SchemaFidelityCase {
    /// Column alias in the projection (also the expected Arrow field name).
    name: &'static str,
    /// GoogleSQL expression of the column's type.
    expr: &'static str,
    /// The fully-typed Arrow field (name, data type, nullability, extension metadata).
    field: Field,
}

/// The table of (type, SQL expression) pairs covering **every** mapped Spanner type: all the
/// scalars, `ARRAY` of each scalar, and `ARRAY<STRUCT<..>>` including a nested `STRUCT` and a
/// nested `ARRAY`. Top-level (non-array) `STRUCT` columns are excluded because real Cloud Spanner
/// rejects returning structs except inside arrays ("Spanner does not yet support returning STRUCT
/// except as arrays-of-structs"), so `ARRAY(SELECT AS STRUCT ...)` is the only portable spelling —
/// it still exercises the whole recursive `STRUCT` mapping, one array level down.
fn schema_fidelity_cases() -> Vec<SchemaFidelityCase> {
    let json_metadata = || {
        std::collections::HashMap::from([
            ("ARROW:extension:name".to_string(), "arrow.json".to_string()),
            ("ARROW:extension:metadata".to_string(), String::new()),
        ])
    };
    let timestamp = || DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let list = |item: Field| DataType::List(Arc::new(item));
    let item = |data_type: DataType| Field::new("item", data_type, true);
    let case = |name: &'static str, expr: &'static str, data_type: DataType| SchemaFidelityCase {
        name,
        expr,
        field: Field::new(name, data_type, true),
    };
    vec![
        case("BoolCol", "TRUE", DataType::Boolean),
        case("Int64Col", "1", DataType::Int64),
        case("Float64Col", "1.5", DataType::Float64),
        case("Float32Col", "CAST(1.25 AS FLOAT32)", DataType::Float32),
        case("StringCol", "'x'", DataType::Utf8),
        case("BytesCol", "b'xyz'", DataType::Binary),
        case("DateCol", "DATE '2024-01-15'", DataType::Date32),
        case(
            "TsCol",
            "TIMESTAMP '2024-01-15T12:34:56.789012Z'",
            timestamp(),
        ),
        case("NumCol", "NUMERIC '1.5'", DataType::Decimal128(38, 9)),
        SchemaFidelityCase {
            name: "JsonCol",
            expr: r#"JSON '{"a":1}'"#,
            field: Field::new("JsonCol", DataType::Utf8, true).with_metadata(json_metadata()),
        },
        case("ArrBool", "[TRUE, FALSE]", list(item(DataType::Boolean))),
        case("ArrInt64", "[1, 2, 3]", list(item(DataType::Int64))),
        case("ArrFloat64", "[1.5, 2.5]", list(item(DataType::Float64))),
        case(
            "ArrFloat32",
            "[CAST(1.25 AS FLOAT32)]",
            list(item(DataType::Float32)),
        ),
        case("ArrStr", "['a', 'b']", list(item(DataType::Utf8))),
        case("ArrBytes", "[b'x']", list(item(DataType::Binary))),
        case(
            "ArrDate",
            "[DATE '2024-01-15']",
            list(item(DataType::Date32)),
        ),
        case(
            "ArrTs",
            "[TIMESTAMP '2024-01-15T12:34:56Z']",
            list(item(timestamp())),
        ),
        case(
            "ArrNum",
            "[NUMERIC '2.5']",
            list(item(DataType::Decimal128(38, 9))),
        ),
        SchemaFidelityCase {
            name: "ArrJson",
            expr: r#"[JSON '1', JSON '{"b":2}']"#,
            field: Field::new(
                "ArrJson",
                list(item(DataType::Utf8).with_metadata(json_metadata())),
                true,
            ),
        },
        case(
            "ArrStruct",
            "ARRAY(SELECT AS STRUCT 1 AS id, 'a' AS tag)",
            list(item(DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("tag", DataType::Utf8, true),
                ]
                .into(),
            ))),
        ),
        // A nested STRUCT and a nested ARRAY inside the array-of-structs element, so the recursive
        // struct-field mapping (struct-in-struct, list-in-struct) is covered on every schema path.
        case(
            "ArrNestedStruct",
            "ARRAY(SELECT AS STRUCT \
                 STRUCT(DATE '2024-01-15' AS d, [1, 2] AS xs) AS child, 2.5 AS ratio)",
            list(item(DataType::Struct(
                vec![
                    Field::new(
                        "child",
                        DataType::Struct(
                            vec![
                                Field::new("d", DataType::Date32, true),
                                Field::new("xs", list(item(DataType::Int64)), true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                    Field::new("ratio", DataType::Float64, true),
                ]
                .into(),
            ))),
        ),
    ]
}

/// Zero-row / metadata schema fidelity across every mapped Spanner type (the Snowflake ADBC
/// driver's recurring "schema rot" bug class: zero-row and metadata paths deriving a schema
/// differently from the data path).
///
/// The driver has two schema sources that could drift: the `QueryMode::Plan` probe behind
/// `execute_schema`, and the result-set-metadata schema the streaming readers build
/// (`build_schema` in `src/conversion.rs`, whose all-`Utf8` fallback kicks in if metadata were
/// ever absent). For one projection covering the full type table this asserts:
///
/// 1. `execute` on a **zero-row** result yields the same fully-typed schema as the identical
///    projection **with** rows — and both match the expected Arrow field exactly (name, type,
///    nullability, `arrow.json` extension metadata), so the two paths can't "agree" by both
///    degrading to the Utf8 fallback;
/// 2. `execute_schema` (the PLAN probe) reports that same schema, for the with-rows and the
///    zero-row statement alike;
/// 3. the bound-parameter path (`BoundQueryBatchReader`, several bound rows in one multi-use
///    read-only snapshot) reports that same schema even when every bound row matches nothing.
#[test]
fn zero_row_schema_fidelity() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping zero_row_schema_fidelity");
        return;
    };
    ensure_database_once(&target);
    // No serial guard: this test only runs read-only queries over literals (no DDL/DML, no
    // tables), and read-only transactions do not block schema changes.

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(target.database_path()),
        )])
        .expect("create database");
    let mut connection = connect_with_retry(&database);

    let cases = schema_fidelity_cases();
    let select_list = cases
        .iter()
        .map(|c| format!("{} AS {}", c.expr, c.name))
        .collect::<Vec<_>>()
        .join(", ");
    // The identical projection over a one-row and a zero-row source.
    let with_rows_sql = format!("SELECT {select_list} FROM UNNEST([1]) AS r");
    let zero_rows_sql = format!("SELECT {select_list} FROM UNNEST(ARRAY<INT64>[]) AS r");

    // Per-column comparison (rather than whole-schema equality) so a divergence names the exact
    // column and path. Field equality covers name, data type (recursively), nullability and the
    // extension metadata.
    let assert_expected_schema = |actual: &Schema, path: &str| {
        assert_eq!(
            actual.fields().len(),
            cases.len(),
            "{path}: column count diverges"
        );
        for (actual_field, case) in actual.fields().iter().zip(&cases) {
            assert_eq!(
                actual_field.as_ref(),
                &case.field,
                "{path}: column {} diverges from the expected Arrow field",
                case.name
            );
        }
    };

    // (1a) The data path, with rows: the reader schema and every batch's schema are fully typed.
    let mut with_rows = connection.new_statement().expect("new statement");
    with_rows.set_sql_query(&with_rows_sql).unwrap();
    let reader = with_rows.execute().expect("execute with-rows projection");
    assert_expected_schema(&reader.schema(), "execute (with rows) reader schema");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect with-rows batches");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    for batch in &batches {
        assert_expected_schema(&batch.schema(), "execute (with rows) batch schema");
    }

    // (1b) The data path, zero rows: no rows come back, but the stream's schema (and the schema of
    // any empty batch it yields) is still fully typed — never the all-Utf8 fallback.
    let mut zero_rows = connection.new_statement().expect("new statement");
    zero_rows.set_sql_query(&zero_rows_sql).unwrap();
    let reader = zero_rows.execute().expect("execute zero-row projection");
    assert_expected_schema(&reader.schema(), "execute (zero rows) reader schema");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect zero-row batches");
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "the zero-row projection must return no rows"
    );
    for batch in &batches {
        assert_expected_schema(&batch.schema(), "execute (zero rows) empty-batch schema");
    }

    // (2) The metadata path: execute_schema's PLAN probe agrees with the data path, for the
    // with-rows and the zero-row statement alike.
    let mut plan = connection.new_statement().expect("new statement");
    plan.set_sql_query(&with_rows_sql).unwrap();
    let schema = plan.execute_schema().expect("execute_schema (with rows)");
    assert_expected_schema(&schema, "execute_schema (with rows)");
    plan.set_sql_query(&zero_rows_sql).unwrap();
    let schema = plan.execute_schema().expect("execute_schema (zero rows)");
    assert_expected_schema(&schema, "execute_schema (zero rows)");

    // (3) The bound-parameter path: several bound rows stream through BoundQueryBatchReader (one
    // shared read-only snapshot, one execution per bound row). With no bound row matching
    // anything, the schema must still come out fully typed from the first (empty) result set's
    // metadata.
    let bound_sql = format!("SELECT {select_list} FROM UNNEST([1]) AS n WHERE n = @N");
    let bind_values = |values: Vec<i64>| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("N", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    };

    let mut bound_zero = connection.new_statement().expect("new statement");
    bound_zero.set_sql_query(&bound_sql).unwrap();
    bound_zero
        .bind(bind_values(vec![5, 6, 7]))
        .expect("bind three non-matching rows");
    let reader = bound_zero.execute().expect("bound zero-row query");
    assert_expected_schema(&reader.schema(), "bound query (zero rows) reader schema");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bound zero-row batches");
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        0,
        "no bound row matches, so the bound query must return no rows"
    );
    for batch in &batches {
        assert_expected_schema(
            &batch.schema(),
            "bound query (zero rows) empty-batch schema",
        );
    }

    // The same bound statement with matching rows produces the identical schema, so the zero-row
    // bound path cannot have drifted from the with-rows bound path either.
    let mut bound_hits = connection.new_statement().expect("new statement");
    bound_hits.set_sql_query(&bound_sql).unwrap();
    bound_hits
        .bind(bind_values(vec![1, 1]))
        .expect("bind two matching rows");
    let reader = bound_hits.execute().expect("bound with-rows query");
    assert_expected_schema(&reader.schema(), "bound query (with rows) reader schema");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bound with-rows batches");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    for batch in &batches {
        assert_expected_schema(&batch.schema(), "bound query (with rows) batch schema");
    }
}

/// RPC timeouts (`spanner.rpc.timeout_seconds.{query,update,fetch}`): round-trip through the
/// string **and** double getters at connection and statement level, statement inheritance and
/// override, validation of bad values, generous deadlines leaving real operations untouched, and —
/// the point of the feature — a deadline that actually fires against a live RPC surfacing ADBC
/// `Timeout` (instead of the pre-existing behaviour, blocking until `cancel`).
#[test]
fn rpc_timeouts() {
    use adbc_core::error::Status;
    use adbc_spanner::{
        OPTION_RPC_TIMEOUT_FETCH, OPTION_RPC_TIMEOUT_QUERY, OPTION_RPC_TIMEOUT_UPDATE,
    };

    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping rpc_timeouts");
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

    let conn_key = |k: &str| OptionConnection::Other(k.into());
    let stmt_key = |k: &str| OptionStatement::Other(k.into());
    const ALL: [&str; 3] = [
        OPTION_RPC_TIMEOUT_QUERY,
        OPTION_RPC_TIMEOUT_UPDATE,
        OPTION_RPC_TIMEOUT_FETCH,
    ];

    // Unset options read back as NotFound, through the string and double getters alike.
    for key in ALL {
        let error = connection
            .get_option_string(conn_key(key))
            .expect_err("unset option must be NotFound");
        assert_eq!(error.status, Status::NotFound, "{key}");
        let error = connection
            .get_option_double(conn_key(key))
            .expect_err("unset option must be NotFound via the double getter too");
        assert_eq!(error.status, Status::NotFound, "{key}");
    }

    // Set via a numeric string, an integer and a double; all round-trip through both getters.
    connection
        .set_option(
            conn_key(OPTION_RPC_TIMEOUT_QUERY),
            OptionValue::String("2.5".into()),
        )
        .expect("set query timeout from a string");
    connection
        .set_option(conn_key(OPTION_RPC_TIMEOUT_UPDATE), OptionValue::Int(30))
        .expect("set update timeout from an int");
    connection
        .set_option(
            conn_key(OPTION_RPC_TIMEOUT_FETCH),
            OptionValue::Double(0.75),
        )
        .expect("set fetch timeout from a double");
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_RPC_TIMEOUT_QUERY))
            .unwrap(),
        "2.5"
    );
    assert_eq!(
        connection
            .get_option_double(conn_key(OPTION_RPC_TIMEOUT_QUERY))
            .unwrap(),
        2.5
    );
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_RPC_TIMEOUT_UPDATE))
            .unwrap(),
        "30"
    );
    assert_eq!(
        connection
            .get_option_double(conn_key(OPTION_RPC_TIMEOUT_UPDATE))
            .unwrap(),
        30.0
    );
    assert_eq!(
        connection
            .get_option_double(conn_key(OPTION_RPC_TIMEOUT_FETCH))
            .unwrap(),
        0.75
    );

    // Bad values are rejected with InvalidArguments and leave the stored value untouched.
    for bad in [
        OptionValue::String("-1".into()),
        OptionValue::String("abc".into()),
        OptionValue::Double(f64::NAN),
        OptionValue::Double(f64::INFINITY),
        OptionValue::Int(-3),
    ] {
        let error = connection
            .set_option(conn_key(OPTION_RPC_TIMEOUT_QUERY), bad)
            .expect_err("bad timeout value must be rejected");
        assert_eq!(error.status, Status::InvalidArguments);
    }
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_RPC_TIMEOUT_QUERY))
            .unwrap(),
        "2.5"
    );

    // Statements inherit the connection's values at creation, and may override or unset each
    // independently without leaking back.
    let mut statement = connection.new_statement().expect("new statement");
    assert_eq!(
        statement
            .get_option_double(stmt_key(OPTION_RPC_TIMEOUT_QUERY))
            .unwrap(),
        2.5,
        "the statement must inherit the connection's query timeout"
    );
    assert_eq!(
        statement
            .get_option_string(stmt_key(OPTION_RPC_TIMEOUT_UPDATE))
            .unwrap(),
        "30"
    );
    statement
        .set_option(
            stmt_key(OPTION_RPC_TIMEOUT_QUERY),
            OptionValue::Double(1.25),
        )
        .expect("override the query timeout on the statement");
    statement
        .set_option(
            stmt_key(OPTION_RPC_TIMEOUT_FETCH),
            OptionValue::String(String::new()),
        )
        .expect("unset the inherited fetch timeout with an empty value");
    assert_eq!(
        statement
            .get_option_string(stmt_key(OPTION_RPC_TIMEOUT_QUERY))
            .unwrap(),
        "1.25"
    );
    let error = statement
        .get_option_string(stmt_key(OPTION_RPC_TIMEOUT_FETCH))
        .expect_err("the unset fetch timeout must be NotFound");
    assert_eq!(error.status, Status::NotFound);
    assert_eq!(
        connection
            .get_option_string(conn_key(OPTION_RPC_TIMEOUT_QUERY))
            .unwrap(),
        "2.5",
        "statement-level overrides must not leak back to the connection"
    );
    // `0` disables the deadline but still round-trips.
    statement
        .set_option(stmt_key(OPTION_RPC_TIMEOUT_UPDATE), OptionValue::Int(0))
        .expect("zero disables");
    assert_eq!(
        statement
            .get_option_double(stmt_key(OPTION_RPC_TIMEOUT_UPDATE))
            .unwrap(),
        0.0
    );

    // End-to-end with generous deadlines on the connection: DDL (now the update deadline, covering
    // its long-running-operation poll loop), DML (the update deadline) and a multi-chunk streamed
    // query (query deadline for the first chunk, fetch deadline for each later chunk inside the
    // prefetch task) all succeed under 30-second limits.
    for key in ALL {
        connection
            .set_option(conn_key(key), OptionValue::Int(30))
            .expect("set a generous deadline");
    }
    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcRpcTimeout; \
         CREATE TABLE AdbcRpcTimeout (Id INT64) PRIMARY KEY (Id)",
    );
    let mut insert = connection.new_statement().expect("new statement");
    insert
        .set_sql_query(
            "INSERT INTO AdbcRpcTimeout (Id) SELECT n FROM UNNEST(GENERATE_ARRAY(1, 100)) AS n",
        )
        .unwrap();
    assert_eq!(
        insert.execute_update().expect("bounded insert"),
        Some(100),
        "a generous update deadline must not affect a fast DML"
    );
    let mut query = connection.new_statement().expect("new statement");
    query
        .set_option(
            OptionStatement::Other(adbc_spanner::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(40),
        )
        .expect("small batches so the fetch deadline path is exercised across chunks");
    query
        .set_sql_query("SELECT Id FROM AdbcRpcTimeout ORDER BY Id")
        .unwrap();
    let batches = query
        .execute()
        .expect("bounded streaming query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect bounded streamed batches");
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        100
    );
    assert!(
        batches.len() > 1,
        "expected several chunks so the fetch deadline actually bounded later fetches"
    );

    // A deadline that fires: a microsecond query timeout cannot be met by any real RPC, so the
    // statement must fail with ADBC `Timeout` naming the option — not hang, not panic.
    let mut tiny = connection.new_statement().expect("new statement");
    tiny.set_option(
        stmt_key(OPTION_RPC_TIMEOUT_QUERY),
        OptionValue::String("0.000001".into()),
    )
    .expect("set a microsecond query deadline");
    tiny.set_sql_query("SELECT Id FROM AdbcRpcTimeout ORDER BY Id")
        .unwrap();
    // The query deadline covers the initial execution (through the first chunk), so `execute`
    // itself fails — no reader is produced.
    let Err(error) = tiny.execute() else {
        panic!("a microsecond query deadline must expire");
    };
    assert_eq!(error.status, Status::Timeout, "{error:?}");
    assert!(
        error.message.contains(OPTION_RPC_TIMEOUT_QUERY),
        "the timeout error must name the responsible option: {}",
        error.message
    );

    // The update deadline fires on the write path too, as a plain ADBC error with Timeout status.
    let mut tiny_dml = connection.new_statement().expect("new statement");
    tiny_dml
        .set_option(
            stmt_key(OPTION_RPC_TIMEOUT_UPDATE),
            OptionValue::Double(0.000001),
        )
        .expect("set a microsecond update deadline");
    tiny_dml
        .set_sql_query("UPDATE AdbcRpcTimeout SET Id = Id WHERE FALSE")
        .unwrap();
    let error = tiny_dml
        .execute_update()
        .expect_err("a microsecond update deadline must expire");
    assert_eq!(error.status, Status::Timeout, "{error:?}");
    assert!(
        error.message.contains(OPTION_RPC_TIMEOUT_UPDATE),
        "{}",
        error.message
    );

    // Raising the deadline on the same statements makes them work again — an expired deadline
    // poisons nothing.
    tiny.set_option(stmt_key(OPTION_RPC_TIMEOUT_QUERY), OptionValue::Int(30))
        .expect("raise the query deadline");
    let batches = tiny
        .execute()
        .expect("query after a raised deadline")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        100
    );
    tiny_dml
        .set_option(stmt_key(OPTION_RPC_TIMEOUT_UPDATE), OptionValue::Int(30))
        .expect("raise the update deadline");
    assert_eq!(
        tiny_dml
            .execute_update()
            .expect("DML after a raised deadline"),
        Some(0)
    );

    // The update deadline also bounds DDL — the admin `UpdateDatabaseDdl` call plus its
    // long-running-operation poll loop, which used to poll unboundedly. A microsecond update
    // deadline on the statement makes even a trivial DDL fail fast with `Timeout` naming the
    // option, rather than hanging in the poller.
    let mut tiny_ddl = connection.new_statement().expect("new statement");
    tiny_ddl
        .set_option(
            stmt_key(OPTION_RPC_TIMEOUT_UPDATE),
            OptionValue::String("0.000001".into()),
        )
        .expect("set a microsecond update deadline for DDL");
    tiny_ddl
        .set_sql_query("CREATE TABLE AdbcRpcTimeoutDdl (Id INT64) PRIMARY KEY (Id)")
        .unwrap();
    let error = tiny_ddl
        .execute_update()
        .expect_err("a microsecond update deadline must expire on DDL");
    assert_eq!(error.status, Status::Timeout, "{error:?}");
    assert!(
        error.message.contains(OPTION_RPC_TIMEOUT_UPDATE),
        "the DDL timeout error must name the update option: {}",
        error.message
    );
    // Best-effort cleanup in case the DDL nonetheless applied server-side before the poll gave up
    // (a timed-out LRO may still complete): drop it if present, ignoring any error.
    let mut drop_ddl = connection.new_statement().expect("new statement");
    if drop_ddl
        .set_sql_query("DROP TABLE IF EXISTS AdbcRpcTimeoutDdl")
        .is_ok()
    {
        let _ = drop_ddl.execute_update();
    }

    // The query deadline bounds the driver-internal metadata reads too. A microsecond query
    // deadline on the connection makes `get_objects` fail with `Timeout` rather than blocking in
    // the INFORMATION_SCHEMA scan; raising it back lets the same call succeed.
    connection
        .set_option(
            conn_key(OPTION_RPC_TIMEOUT_QUERY),
            OptionValue::String("0.000001".into()),
        )
        .expect("set a microsecond query deadline on the connection");
    let error = connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .err()
        .expect("a microsecond query deadline must expire on a metadata read");
    assert_eq!(error.status, Status::Timeout, "{error:?}");
    assert!(
        error.message.contains(OPTION_RPC_TIMEOUT_QUERY),
        "the metadata timeout error must name the query option: {}",
        error.message
    );
    connection
        .set_option(conn_key(OPTION_RPC_TIMEOUT_QUERY), OptionValue::Int(30))
        .expect("raise the connection query deadline");
    connection
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .expect("get_objects after a raised deadline")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect get_objects batches");

    // Unsetting at the connection level round-trips back to NotFound.
    for key in ALL {
        connection
            .set_option(conn_key(key), OptionValue::String(String::new()))
            .expect("unset with an empty value");
        let error = connection
            .get_option_string(conn_key(key))
            .expect_err("an unset option must be NotFound");
        assert_eq!(error.status, Status::NotFound, "{key}");
    }

    let mut drop = connection.new_statement().expect("new statement");
    drop.set_sql_query("DROP TABLE AdbcRpcTimeout").unwrap();
    drop.execute_update().expect("drop rpc-timeout table");
}

/// Spanner **change streams** are usable through the driver's ordinary SQL paths — no dedicated
/// driver support is needed. This exercises the full plain-SQL surface end-to-end:
///
/// 1. `CREATE CHANGE STREAM … FOR <table>` and `DROP CHANGE STREAM` run through the driver's DDL
///    path (`execute_update`), just like any other DDL.
/// 2. The stream is introspectable through `INFORMATION_SCHEMA.CHANGE_STREAMS` /
///    `CHANGE_STREAM_TABLES` as ordinary read queries.
/// 3. The generated `READ_<stream>` table-valued function runs through the ordinary query path and
///    the driver maps its richly-nested `ChangeRecord` result (a `List<Struct<data_change_record,
///    heartbeat_record, child_partitions_record>>`) natively to Arrow.
///
/// The `READ_` TVF is a *tailing* read: its `start_timestamp` must be at or after the change
/// stream's earliest read timestamp, which the emulator tracks at ~now (it keeps no historical
/// change data), so we read a small window that begins in the very near future. The initial call
/// with `partition_token => NULL` deterministically yields the stream's child-partition record;
/// surfacing an actual `data_change_record` would require following those partition tokens in a
/// streaming follow-up read (timing-sensitive), which is out of scope here — we assert on the fully
/// mapped result *schema* plus a non-empty result instead, which is deterministic.
#[test]
fn change_stream_via_plain_sql() {
    let Some(target) = test_target() else {
        eprintln!("no Spanner target set — skipping change_stream_via_plain_sql");
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

    // --- (1) DDL: create the watched table and a change stream over it, via plain SQL. ---
    run(
        &mut connection,
        "DROP TABLE IF EXISTS AdbcChangeStream; \
         CREATE TABLE AdbcChangeStream (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)",
    );
    run(
        &mut connection,
        "CREATE CHANGE STREAM AdbcChangeStreamCs FOR AdbcChangeStream",
    );

    // --- (2) The stream is introspectable through INFORMATION_SCHEMA as ordinary read queries. ---
    let mut streams = connection.new_statement().expect("new statement");
    streams
        .set_sql_query(
            "SELECT CHANGE_STREAM_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAMS \
             WHERE CHANGE_STREAM_NAME = 'AdbcChangeStreamCs'",
        )
        .unwrap();
    let stream_batches: Vec<_> = streams
        .execute()
        .expect("query change stream metadata")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect change stream metadata");
    let stream_rows: usize = stream_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        stream_rows, 1,
        "the change stream should be listed exactly once"
    );
    let names = stream_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "AdbcChangeStreamCs");

    // The change stream must be associated with the watched table.
    let mut tables = connection.new_statement().expect("new statement");
    tables
        .set_sql_query(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES \
             WHERE CHANGE_STREAM_NAME = 'AdbcChangeStreamCs'",
        )
        .unwrap();
    let table_batches: Vec<_> = tables
        .execute()
        .expect("query change stream tables")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect change stream tables");
    let watched: Vec<String> = table_batches
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            (0..col.len())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        watched,
        vec!["AdbcChangeStream".to_string()],
        "the change stream should watch exactly the AdbcChangeStream table"
    );

    // Seed a write so the stream has data to have captured (before the tailing window).
    run(
        &mut connection,
        "INSERT INTO AdbcChangeStream (Id, Name) VALUES (1, 'alpha'), (2, 'beta')",
    );

    // --- (3) Read the change stream through its generated READ_ TVF as an ordinary query. ---
    //
    // A short tailing window starting in the near future (the emulator's earliest-read timestamp
    // tracks ~now). The initial NULL-partition read returns the stream's child-partition record.
    let start = chrono::Utc::now() + chrono::Duration::milliseconds(500);
    let end = chrono::Utc::now() + chrono::Duration::seconds(3);
    let start_lit = start.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let end_lit = end.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let read_sql = format!(
        "SELECT ChangeRecord FROM READ_AdbcChangeStreamCs (\
             start_timestamp => TIMESTAMP '{start_lit}', \
             end_timestamp => TIMESTAMP '{end_lit}', \
             partition_token => NULL, \
             heartbeat_milliseconds => 2000)"
    );
    let mut reader_stmt = connection.new_statement().expect("new statement");
    reader_stmt.set_sql_query(read_sql).unwrap();
    let reader = reader_stmt.execute().expect("read change stream TVF");
    let schema = reader.schema();

    // The driver maps the whole ChangeRecord type natively: a List<Struct<...>> whose element struct
    // carries the three change-record variants.
    assert_eq!(schema.fields().len(), 1);
    let change_field = schema.field(0);
    assert_eq!(change_field.name(), "ChangeRecord");
    let DataType::List(elem) = change_field.data_type() else {
        panic!(
            "ChangeRecord should map to a List, got {:?}",
            change_field.data_type()
        );
    };
    let DataType::Struct(record_fields) = elem.data_type() else {
        panic!(
            "ChangeRecord element should be a Struct, got {:?}",
            elem.data_type()
        );
    };
    let record_field_names: Vec<&str> = record_fields.iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        record_field_names,
        vec![
            "data_change_record",
            "heartbeat_record",
            "child_partitions_record"
        ],
        "the change record struct should expose all three record variants"
    );

    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect change stream records");
    let record_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        record_rows >= 1,
        "the initial change-stream read should yield at least the child-partition record"
    );

    // --- Cleanup: the stream must be dropped before the table it watches. ---
    run(&mut connection, "DROP CHANGE STREAM AdbcChangeStreamCs");
    run(&mut connection, "DROP TABLE AdbcChangeStream");
}

/// Opt-in **end-to-end auth** tests: drive the `spanner.keyfile` and
/// `spanner.impersonate.target_principal` credential paths against a **real** Cloud Spanner database
/// and prove each one authenticates by running a trivial `SELECT 1`.
///
/// The offline unit tests in `src/driver.rs` only assert option parsing / mutual-exclusion / the
/// emulator guards; they never actually authenticate. These tests close that gap, but they need real
/// credentials (which normal CI does not have), so they **self-skip cleanly** — green, no failure —
/// whenever their env vars are unset, exactly like the `SPANNER_GCP_DATABASE` tests above. That keeps
/// a plain `cargo test` green everywhere.
///
/// The emulator refuses keyfile/impersonation credentials (see the driver's emulator guard), so these
/// tests read `SPANNER_GCP_DATABASE` directly and never touch the emulator. Env vars:
///
/// - `SPANNER_GCP_DATABASE` (`project.instance.database`) — the real target database, shared with the
///   other real-backend tests. Required for both.
/// - `SPANNER_TEST_KEYFILE` — filesystem path to a service-account JSON key. Enables
///   `keyfile_auth_end_to_end`.
/// - `SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL` — a service-account email to impersonate (the base
///   credentials come from ADC). Enables `impersonation_auth_end_to_end`.
mod auth_end_to_end {
    use super::*;
    use adbc_spanner::{OPTION_IMPERSONATE_TARGET_PRINCIPAL, OPTION_KEYFILE};

    /// Resolve the **real** Cloud Spanner target from `SPANNER_GCP_DATABASE` alone.
    ///
    /// Unlike [`test_target`], this ignores `SPANNER_EMULATOR_HOST` (the emulator can't exercise
    /// these credentials) and never consults `ADBC_TEST_REQUIRE_TARGET` — the auth tests are opt-in
    /// beyond the normal target and must skip silently when their extra credentials are absent.
    fn gcp_database_target() -> Option<TestTarget> {
        let spec = std::env::var("SPANNER_GCP_DATABASE")
            .ok()
            .filter(|s| !s.is_empty())?;
        match spec.split('.').collect::<Vec<_>>().as_slice() {
            [project, instance, database] => Some(TestTarget {
                project: project.to_string(),
                instance: instance.to_string(),
                database: database.to_string(),
                is_emulator: false,
            }),
            _ => panic!(
                "SPANNER_GCP_DATABASE must be in 'project.instance.database' form, got {spec:?}"
            ),
        }
    }

    /// Read a non-empty env var, or `None` (so the caller can self-skip).
    fn non_empty_env(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|s| !s.is_empty())
    }

    /// Run `SELECT 1` over the connection and assert it returns the single value `1` — enough to
    /// prove the credential path authenticated end-to-end (a bad credential fails to connect or
    /// errors here rather than returning a row).
    fn assert_select_one(connection: &mut SpannerConnection) {
        let mut statement = connection.new_statement().expect("new statement");
        statement
            .set_sql_query("SELECT 1 AS one")
            .expect("set query");
        let batches: Vec<_> = statement
            .execute()
            .expect("execute SELECT 1 over the authenticated connection")
            .collect::<Result<Vec<_>, _>>()
            .expect("read batches");
        assert_eq!(batches.len(), 1);
        let ones = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ones.value(0), 1);
    }

    /// Authenticate with a service-account key file (`spanner.keyfile`) and run `SELECT 1`.
    ///
    /// Skips unless both `SPANNER_GCP_DATABASE` and `SPANNER_TEST_KEYFILE` are set.
    #[test]
    fn keyfile_auth_end_to_end() {
        let Some(target) = gcp_database_target() else {
            eprintln!("SPANNER_GCP_DATABASE not set — skipping keyfile auth end-to-end test");
            return;
        };
        let Some(keyfile) = non_empty_env("SPANNER_TEST_KEYFILE") else {
            eprintln!("SPANNER_TEST_KEYFILE not set — skipping keyfile auth end-to-end test");
            return;
        };

        // The database only needs to exist for `SELECT 1`; setup uses ADC, independent of the
        // keyfile credential under test.
        ensure_database_once(&target);

        let mut driver = SpannerDriver::try_new().expect("create driver");
        let database = driver
            .new_database_with_opts([
                (
                    OptionDatabase::Uri,
                    OptionValue::String(target.database_path()),
                ),
                (
                    OptionDatabase::Other(OPTION_KEYFILE.into()),
                    OptionValue::String(keyfile),
                ),
            ])
            .expect("create database with keyfile credentials");
        let mut connection = connect_with_retry(&database);
        assert_select_one(&mut connection);
    }

    /// Authenticate via service-account impersonation (`spanner.impersonate.target_principal`,
    /// layered on ADC base credentials) and run `SELECT 1`.
    ///
    /// Skips unless both `SPANNER_GCP_DATABASE` and `SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL` are
    /// set. The ambient identity (ADC) must hold the *Token Creator* role on the target principal.
    #[test]
    fn impersonation_auth_end_to_end() {
        let Some(target) = gcp_database_target() else {
            eprintln!("SPANNER_GCP_DATABASE not set — skipping impersonation auth end-to-end test");
            return;
        };
        let Some(principal) = non_empty_env("SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL") else {
            eprintln!(
                "SPANNER_TEST_IMPERSONATE_TARGET_PRINCIPAL not set — skipping impersonation auth \
                 end-to-end test"
            );
            return;
        };

        ensure_database_once(&target);

        let mut driver = SpannerDriver::try_new().expect("create driver");
        let database = driver
            .new_database_with_opts([
                (
                    OptionDatabase::Uri,
                    OptionValue::String(target.database_path()),
                ),
                (
                    OptionDatabase::Other(OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()),
                    OptionValue::String(principal),
                ),
            ])
            .expect("create database with impersonation credentials");
        let mut connection = connect_with_retry(&database);
        assert_select_one(&mut connection);
    }
}

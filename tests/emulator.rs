//! End-to-end integration test that runs the ADBC driver against the Cloud Spanner emulator.
//!
//! The test is **skipped automatically** unless the `SPANNER_EMULATOR_HOST` environment variable is
//! set, so a plain `cargo test` stays green without any external dependency. To run it against a
//! local emulator use the helper script, which starts the emulator, exports the variable and runs
//! the test:
//!
//! ```sh
//! scripts/with-emulator.sh cargo test --test emulator -- --nocapture
//! ```
//!
//! Setup (creating the instance, database and table) uses the Spanner admin clients directly; the
//! actual query and DML round-trip goes through the `adbc-spanner` driver being tested.

use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_spanner::{SpannerConnection, SpannerDriver};
use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatchReader, StringArray};
use arrow_schema::DataType;
use google_cloud_lro::Poller;
use google_cloud_spanner::client::Spanner;
use google_cloud_spanner_admin_instance_v1::model::Instance;

const PROJECT: &str = "test-project";
const INSTANCE: &str = "test-instance";
const DATABASE: &str = "adbc-test";

fn database_path() -> String {
    format!("projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}")
}

fn emulator_configured() -> bool {
    std::env::var("SPANNER_EMULATOR_HOST")
        .ok()
        .filter(|s| !s.is_empty())
        .is_some()
}

/// Create the test instance, database and `Singers` table if they do not already exist.
///
/// `create_instance` / `create_database` are best-effort: on a re-run against an already-populated
/// emulator they fail with `AlreadyExists`, which we intentionally ignore.
async fn ensure_database() {
    // The client auto-detects `SPANNER_EMULATOR_HOST` and uses anonymous credentials.
    let spanner = Spanner::builder()
        .build()
        .await
        .expect("failed to build Spanner client for setup");

    let instance_admin = spanner
        .instance_admin_builder()
        .build()
        .await
        .expect("failed to build instance admin client");
    let _ = instance_admin
        .create_instance()
        .set_parent(format!("projects/{PROJECT}"))
        .set_instance_id(INSTANCE)
        .set_instance(
            Instance::new()
                .set_config(format!(
                    "projects/{PROJECT}/instanceConfigs/emulator-config"
                ))
                .set_display_name("ADBC test instance")
                .set_node_count(1),
        )
        .poller()
        .until_done()
        .await;

    let database_admin = spanner
        .database_admin_builder()
        .build()
        .await
        .expect("failed to build database admin client");
    let _ = database_admin
        .create_database()
        .set_parent(format!("projects/{PROJECT}/instances/{INSTANCE}"))
        .set_create_statement(format!("CREATE DATABASE `{DATABASE}`"))
        .set_extra_statements(vec!["CREATE TABLE Singers (\
                 SingerId INT64 NOT NULL, \
                 Name STRING(MAX), \
                 Active BOOL, \
                 Score FLOAT64\
             ) PRIMARY KEY (SingerId)"
            .to_string()])
        .poller()
        .until_done()
        .await;
}

#[test]
fn query_and_dml_round_trip() {
    if !emulator_configured() {
        eprintln!("SPANNER_EMULATOR_HOST not set — skipping Spanner emulator integration test");
        return;
    }

    // A throwaway runtime just for the async admin setup.
    tokio::runtime::Runtime::new()
        .expect("failed to build setup runtime")
        .block_on(ensure_database());

    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, OptionValue::String(database_path()))])
        .expect("create database");
    let mut connection = database.new_connection().expect("create connection");

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

    // Re-enabling autocommit commits any pending work (none here) and restores per-statement commit.
    connection
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("enable autocommit");

    let mut drop_txn = connection.new_statement().expect("new statement");
    drop_txn.set_sql_query("DROP TABLE AdbcTxn").unwrap();
    drop_txn.execute_update().expect("drop txn table");
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

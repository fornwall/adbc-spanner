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

use std::sync::Arc;

use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_spanner::{SpannerConnection, SpannerDriver};
use arrow_array::{
    BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, RecordBatch,
    RecordBatchReader, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
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
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
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
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let num = tb
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(date.value(0), 19737); // days from 1970-01-01 to 2024-01-15
    assert_eq!(ts.value(0), 1_705_322_096_789_012); // micros since epoch
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
    ingest.bind(rows).expect("bind ingest rows");
    assert_eq!(ingest.execute_update().expect("ingest"), Some(2));
    assert_eq!(count_rows(&mut connection, "AdbcBind"), 2);

    // Parameterized query: bind @Id and read the matching row back.
    let param = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .unwrap();
    let mut pq = connection.new_statement().expect("new statement");
    pq.set_sql_query("SELECT Name FROM AdbcBind WHERE Id = @Id")
        .unwrap();
    pq.bind(param).expect("bind query param");
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

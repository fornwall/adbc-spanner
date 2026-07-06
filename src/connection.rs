//! The [`SpannerConnection`] — an ADBC connection backed by a Spanner [`DatabaseClient`].
//!
//! ## Transactions
//!
//! By default the connection is in **autocommit** mode: every statement runs in its own Spanner
//! transaction (a single-use read-only transaction for queries, a read/write transaction for DML).
//!
//! Setting the `adbc.connection.autocommit` option to `false` begins **manual** transaction mode.
//! Because Spanner's client only exposes read/write transactions through a closure-based runner
//! (there is no public begin/commit handle), the driver implements manual transactions by
//! *buffering* DML statements and applying the whole batch atomically in a single read/write
//! transaction on [`Connection::commit`] — which also makes the retry-on-abort safe, since the
//! buffer is simply replayed. [`Connection::rollback`] discards the buffer.
//!
//! Consequences of this model, which callers should be aware of:
//! - In manual mode, `execute_update` on DML returns `None` (the affected-row count is not known
//!   until commit).
//! - Queries (`execute`) and DDL always run immediately (DDL is never transactional in Spanner), so
//!   a query does not observe DML buffered earlier in the same manual transaction.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use google_cloud_spanner::builder::BatchDmlBuilder;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::ReadWriteTransaction;

use crate::conversion::result_set_to_batch;
use crate::driver::Connected;
use crate::error::{err, from_spanner, invalid_argument, invalid_state, not_implemented};
use crate::runtime::SharedRuntime;
use crate::statement::SpannerStatement;

/// Transaction state shared between a connection and the statements it creates.
#[derive(Debug)]
pub(crate) struct TxnState {
    /// When false, the connection is in manual transaction mode and DML is buffered.
    autocommit: bool,
    /// DML statements buffered while in manual mode, applied atomically on commit. Built
    /// statements (not raw SQL) so that parameterized DML — which carries bound values — buffers
    /// just like a plain `;`-batch does.
    pending: Vec<SpannerSql>,
}

impl TxnState {
    fn new() -> Self {
        Self {
            autocommit: true,
            pending: Vec::new(),
        }
    }

    /// Whether the connection is currently in autocommit mode.
    pub(crate) fn autocommit(&self) -> bool {
        self.autocommit
    }

    /// Buffer a DML statement to be applied on the next commit.
    pub(crate) fn buffer(&mut self, statement: SpannerSql) {
        self.pending.push(statement);
    }
}

/// A handle to a connection's transaction state, shared with its statements.
pub(crate) type SharedTxn = Arc<Mutex<TxnState>>;

/// An ADBC connection to a Spanner database.
pub struct SpannerConnection {
    runtime: SharedRuntime,
    client: DatabaseClient,
    spanner: Spanner,
    database: String,
    read_only: bool,
    txn: SharedTxn,
}

impl SpannerConnection {
    pub(crate) fn new(runtime: SharedRuntime, connected: Connected) -> Self {
        Self {
            runtime,
            client: connected.client,
            spanner: connected.spanner,
            database: connected.database,
            read_only: false,
            txn: Arc::new(Mutex::new(TxnState::new())),
        }
    }

    /// Apply the buffered DML statements atomically in one read/write transaction, discarding the
    /// affected-row count (a commit reports no count).
    fn apply_transaction(&self, statements: Vec<SpannerSql>) -> Result<()> {
        run_batch_dml(&self.runtime, &self.client, statements)?;
        Ok(())
    }

    /// Query `INFORMATION_SCHEMA` and assemble the schema→table→column hierarchy for `get_objects`,
    /// applying the ADBC `LIKE`/type filters and the requested depth.
    fn collect_objects(
        &self,
        depth: ObjectDepth,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: &Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Vec<crate::objects::DbSchema>> {
        let populate_tables = matches!(
            depth,
            ObjectDepth::All | ObjectDepth::Tables | ObjectDepth::Columns
        );
        let populate_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);
        let client = self.client.clone();

        let (schema_batch, table_batch, column_batch) = self.runtime.block_on(async move {
            let schemas =
                query_batch(&client, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA").await?;
            let tables = if populate_tables {
                Some(
                    query_batch(
                        &client,
                        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES",
                    )
                    .await?,
                )
            } else {
                None
            };
            let columns = if populate_columns {
                Some(
                    query_batch(
                        &client,
                        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE \
                         FROM INFORMATION_SCHEMA.COLUMNS \
                         ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
                    )
                    .await?,
                )
            } else {
                None
            };
            Ok::<_, Error>((schemas, tables, columns))
        })?;

        let schema_names = str_col(&schema_batch, 0)?;
        let mut result = Vec::new();
        for i in 0..schema_batch.num_rows() {
            let schema_name = schema_names.value(i);
            if db_schema.is_some_and(|p| !like_match(p, schema_name)) {
                continue;
            }
            let mut tables = Vec::new();
            if let Some(batch) = &table_batch {
                let (ts, tn, tt) = (str_col(batch, 0)?, str_col(batch, 1)?, str_col(batch, 2)?);
                for r in 0..batch.num_rows() {
                    if ts.value(r) != schema_name {
                        continue;
                    }
                    let name = tn.value(r);
                    if table_name.is_some_and(|p| !like_match(p, name)) {
                        continue;
                    }
                    let ttype = tt.value(r).to_string();
                    if table_type
                        .as_ref()
                        .is_some_and(|types| !types.iter().any(|t| *t == ttype))
                    {
                        continue;
                    }
                    let columns = match &column_batch {
                        Some(cb) => collect_columns(cb, schema_name, name, column_name)?,
                        None => Vec::new(),
                    };
                    tables.push(crate::objects::Table {
                        name: name.to_string(),
                        table_type: ttype,
                        columns,
                    });
                }
            }
            result.push(crate::objects::DbSchema {
                name: schema_name.to_string(),
                tables,
            });
        }
        Ok(result)
    }
}

/// Apply DML `statements` atomically in one read/write transaction via Spanner's `ExecuteBatchDml`
/// (a single RPC), returning the total affected-row count.
///
/// The runner may retry the closure on abort, so the (cloned) statement list is replayed on each
/// attempt. Shared by autocommit `execute_update` and the manual-mode commit path.
pub(crate) fn run_batch_dml(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    statements: Vec<SpannerSql>,
) -> Result<i64> {
    if statements.is_empty() {
        return Ok(0);
    }
    let client = client.clone();
    runtime.block_on(async move {
        let runner = client
            .read_write_transaction()
            .build()
            .await
            .map_err(from_spanner)?;
        let outcome = runner
            .run(move |transaction: ReadWriteTransaction| {
                let statements = statements.clone();
                async move {
                    let mut batch = BatchDmlBuilder::new();
                    for statement in statements {
                        batch = batch.add_statement(statement);
                    }
                    let counts = transaction.execute_batch_update(batch.build()).await?;
                    Ok(counts.into_iter().sum::<i64>())
                }
            })
            .await
            .map_err(from_spanner)?;
        Ok::<i64, Error>(outcome.result)
    })
}

/// Run a query and return its single materialised record batch.
async fn query_batch(client: &DatabaseClient, sql: &str) -> Result<RecordBatch> {
    let transaction = client.single_use().build();
    let result_set = transaction
        .execute_query(SpannerSql::builder(sql).build())
        .await
        .map_err(from_spanner)?;
    let (_schema, batch) = result_set_to_batch(result_set).await?;
    Ok(batch)
}

fn str_col(batch: &RecordBatch, index: usize) -> Result<&StringArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            err(
                format!("INFORMATION_SCHEMA column {index} is not a string"),
                Status::Internal,
            )
        })
}

fn collect_columns(
    batch: &RecordBatch,
    schema: &str,
    table: &str,
    filter: Option<&str>,
) -> Result<Vec<crate::objects::Column>> {
    let (ts, tn, cn, nul) = (
        str_col(batch, 0)?,
        str_col(batch, 1)?,
        str_col(batch, 2)?,
        str_col(batch, 4)?,
    );
    let ordinal = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| err("ORDINAL_POSITION is not an integer", Status::Internal))?;
    let mut columns = Vec::new();
    for r in 0..batch.num_rows() {
        if ts.value(r) != schema || tn.value(r) != table {
            continue;
        }
        let name = cn.value(r);
        if filter.is_some_and(|p| !like_match(p, name)) {
            continue;
        }
        columns.push(crate::objects::Column {
            name: name.to_string(),
            ordinal: ordinal.value(r) as i32,
            nullable: nul.value(r).eq_ignore_ascii_case("YES"),
        });
    }
    Ok(columns)
}

/// Match an ADBC `LIKE` pattern (`%` = any run, `_` = one char) against a value, case-sensitively.
///
/// Iterative with backtrack pointers (O(pattern × value), no recursion) so adversarial patterns
/// like `%a%a%a…` cannot cause exponential blowup or stack overflow.
pub(crate) fn like_match(pattern: &str, value: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    let (mut pi, mut vi) = (0usize, 0usize);
    // Position in the pattern/value to backtrack to after the most recent `%`.
    let mut star: Option<(usize, usize)> = None;
    while vi < v.len() {
        // `%` must be tested before the literal/`_` branch: otherwise a `%` in the pattern that
        // happens to equal the current value char (e.g. both are `%`) would be consumed as a
        // literal instead of acting as a wildcard.
        if pi < p.len() && p[pi] == '%' {
            star = Some((pi, vi));
            pi += 1;
        } else if pi < p.len() && (p[pi] == '_' || p[pi] == v[vi]) {
            pi += 1;
            vi += 1;
        } else if let Some((sp, sv)) = star {
            // Let the last `%` consume one more character and retry.
            pi = sp + 1;
            vi = sv + 1;
            star = Some((sp, sv + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod like_tests {
    use super::like_match;

    #[test]
    fn like_matching() {
        assert!(like_match("", ""));
        assert!(like_match("%", ""));
        assert!(like_match("%", "anything"));
        assert!(like_match("Singers", "Singers"));
        assert!(!like_match("Singers", "singers")); // case-sensitive
        assert!(like_match("Sing%", "Singers"));
        assert!(like_match("%ers", "Singers"));
        assert!(like_match("S_ngers", "Singers"));
        assert!(like_match("%a%a%", "banana"));
        assert!(!like_match("%x%", "banana"));
        assert!(!like_match("", "x"));
        // A pattern `%` must stay a wildcard even when the value has a literal `%` where the
        // wildcard begins matching — the value starts with `%`, or a `%` follows matched literals.
        // The literal branch used to mis-consume it there, so these all failed. Found by the `like`
        // fuzz target's differential regex oracle.
        assert!(like_match("%", "%foo"));
        assert!(like_match("%", "%^%?"));
        assert!(like_match("a%", "a%b"));
    }
}

impl Optionable for SpannerConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::AutoCommit => {
                let enable = parse_bool(value)?;
                let currently = self.txn.lock().unwrap().autocommit;
                if enable && !currently {
                    // Enabling autocommit commits any active manual transaction.
                    let pending = std::mem::take(&mut self.txn.lock().unwrap().pending);
                    self.apply_transaction(pending)?;
                }
                self.txn.lock().unwrap().autocommit = enable;
            }
            OptionConnection::ReadOnly => self.read_only = parse_bool(value)?,
            other => {
                return Err(invalid_argument(format!(
                    "unsupported Spanner connection option: {}",
                    connection_option_name(other)
                )))
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionConnection::AutoCommit => Ok(self.txn.lock().unwrap().autocommit.to_string()),
            OptionConnection::ReadOnly => Ok(self.read_only.to_string()),
            other => Err(err(
                format!("option {} is not set", connection_option_name(other)),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Ok(self.get_option_string(key)?.into_bytes())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(err(
            format!("option {} is not an integer", connection_option_name(&key)),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(err(
            format!("option {} is not a double", connection_option_name(&key)),
            Status::NotFound,
        ))
    }
}

impl Connection for SpannerConnection {
    type StatementType = SpannerStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(SpannerStatement::new(
            self.runtime.clone(),
            self.client.clone(),
            self.spanner.clone(),
            self.database.clone(),
            self.read_only,
            self.txn.clone(),
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(not_implemented("Connection::cancel"))
    }

    /// Driver / vendor metadata, sourced entirely from static driver constants (no Spanner RPC).
    ///
    /// `codes = None` returns the set of codes the driver has a meaningful value for; an explicit
    /// set returns one row per requested code (a null value for codes it cannot answer).
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let batch = crate::info::build(codes)?;
        let schema = batch.schema();
        Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
    }

    /// Catalog/schema/table/column introspection, sourced from Spanner `INFORMATION_SCHEMA`.
    ///
    /// A Spanner database is a single, unnamed catalog (`""`). Name arguments are ADBC `LIKE`
    /// patterns (`%`/`_`); `depth` bounds how far the hierarchy is populated.
    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let out_schema = adbc_core::schemas::GET_OBJECTS_SCHEMA.clone();
        // Spanner has a single catalog (""); a catalog filter that excludes it yields no rows.
        if catalog.is_some_and(|c| !like_match(c, "")) {
            return Ok(Box::new(RecordBatchIterator::new(Vec::new(), out_schema)));
        }
        let schemas =
            self.collect_objects(depth, db_schema, table_name, &table_type, column_name)?;
        let batch = crate::objects::build(depth, schemas)?;
        Ok(Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            out_schema,
        )))
    }

    /// Return the Arrow schema of a table.
    ///
    /// Implemented by running a zero-row `SELECT * FROM <table> LIMIT 0` and mapping the result-set
    /// column metadata to Arrow (the same mapping used for query results). Spanner has no catalog
    /// concept, so `catalog` is ignored.
    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        let table = qualified_table(db_schema, table_name);
        let sql = format!("SELECT * FROM {table} LIMIT 0");
        let client = self.client.clone();
        let (schema, _batch) = self.runtime.block_on(async move {
            let transaction = client.single_use().build();
            let result_set = transaction
                .execute_query(SpannerSql::builder(sql).build())
                .await
                .map_err(from_spanner)?;
            result_set_to_batch(result_set).await
        })?;
        Ok((*schema).clone())
    }

    /// Return the table types supported by Spanner as a single-column (`table_type: utf8`) batch,
    /// per the ADBC specification.
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let array = Arc::new(StringArray::from(vec!["TABLE", "VIEW"])) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).map_err(|e| {
            err(
                format!("failed to build table types batch: {e}"),
                Status::Internal,
            )
        })?;
        Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
    }

    /// Spanner exposes no portable per-table statistics, so this returns an empty (but correctly
    /// typed) result set — i.e. "no statistic names".
    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(Box::new(RecordBatchIterator::new(
            Vec::new(),
            adbc_core::schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
        )))
    }

    /// Spanner exposes no portable per-table statistics, so this returns an empty (but correctly
    /// typed) result set.
    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(Box::new(RecordBatchIterator::new(
            Vec::new(),
            adbc_core::schemas::GET_STATISTICS_SCHEMA.clone(),
        )))
    }

    fn commit(&mut self) -> Result<()> {
        let pending = {
            let mut st = self.txn.lock().unwrap();
            if st.autocommit {
                return Err(invalid_state(
                    "commit invoked with autocommit enabled; no active transaction",
                ));
            }
            std::mem::take(&mut st.pending)
        };
        self.apply_transaction(pending)
    }

    fn rollback(&mut self) -> Result<()> {
        let mut st = self.txn.lock().unwrap();
        if st.autocommit {
            return Err(invalid_state(
                "rollback invoked with autocommit enabled; no active transaction",
            ));
        }
        st.pending.clear();
        Ok(())
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // Pairs with Statement::execute_partitions; see the note there.
        Err(not_implemented(
            "partitioned execution: Spanner's Partition APIs are session-bound and unsupported by \
             the emulator, so it is not implemented",
        ))
    }
}

fn parse_bool(value: OptionValue) -> Result<bool> {
    match value {
        OptionValue::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            other => Err(invalid_argument(format!(
                "expected a boolean, got {other:?}"
            ))),
        },
        OptionValue::Int(i) => Ok(i != 0),
        _ => Err(invalid_argument("expected a boolean option value")),
    }
}

fn connection_option_name(key: &OptionConnection) -> String {
    key.as_ref().to_string()
}

/// Backtick-quote a table name, optionally qualified by a (named) schema.
fn qualified_table(db_schema: Option<&str>, table_name: &str) -> String {
    match db_schema.filter(|s| !s.is_empty()) {
        Some(schema) => format!("`{schema}`.`{table_name}`"),
        None => format!("`{table_name}`"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualifies_table_names() {
        assert_eq!(qualified_table(None, "Users"), "`Users`");
        assert_eq!(qualified_table(Some(""), "Users"), "`Users`");
        assert_eq!(qualified_table(Some("app"), "Users"), "`app`.`Users`");
    }
}

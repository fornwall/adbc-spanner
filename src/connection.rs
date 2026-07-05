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
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
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
    /// DML statements buffered while in manual mode, applied atomically on commit.
    pending: Vec<String>,
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
    pub(crate) fn buffer(&mut self, sql: String) {
        self.pending.push(sql);
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

    /// Apply a batch of buffered DML statements atomically in one read/write transaction.
    ///
    /// The runner may retry the closure on abort, so the statements are rebuilt from the (cloned)
    /// buffer on each attempt.
    fn apply_transaction(&self, statements: Vec<String>) -> Result<()> {
        if statements.is_empty() {
            return Ok(());
        }
        let client = self.client.clone();
        self.runtime.block_on(async move {
            let runner = client
                .read_write_transaction()
                .build()
                .await
                .map_err(from_spanner)?;
            runner
                .run(move |transaction: ReadWriteTransaction| {
                    let statements = statements.clone();
                    async move {
                        for sql in statements {
                            transaction
                                .execute_update(SpannerSql::builder(sql).build())
                                .await?;
                        }
                        Ok(())
                    }
                })
                .await
                .map_err(from_spanner)?;
            Ok::<(), Error>(())
        })
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

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("Connection::get_info"))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("Connection::get_objects"))
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

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("Connection::get_statistic_names"))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(not_implemented("Connection::get_statistics"))
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
        Err(not_implemented("Connection::read_partition"))
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

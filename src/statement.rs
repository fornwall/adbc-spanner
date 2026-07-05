//! The [`SpannerStatement`] — an ADBC statement that runs SQL against Spanner and returns Arrow.
//!
//! A statement holds a SQL string set via [`Statement::set_sql_query`]. Calling
//! [`Statement::execute`] runs it as a query in a single-use read-only transaction and streams the
//! result back as an Arrow [`RecordBatch`]. Calling [`Statement::execute_update`] runs it as DML
//! inside a read/write transaction and returns the number of affected rows.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};
use google_cloud_lro::Poller as _;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::ReadWriteTransaction;

use crate::connection::SharedTxn;
use crate::conversion::result_set_to_batch;
use crate::error::{err, from_spanner, invalid_state, not_implemented};
use crate::runtime::SharedRuntime;

/// An ADBC statement bound to a Spanner [`DatabaseClient`].
pub struct SpannerStatement {
    runtime: SharedRuntime,
    client: DatabaseClient,
    spanner: Spanner,
    database: String,
    read_only: bool,
    txn: SharedTxn,
    sql: Option<String>,
}

impl SpannerStatement {
    pub(crate) fn new(
        runtime: SharedRuntime,
        client: DatabaseClient,
        spanner: Spanner,
        database: String,
        read_only: bool,
        txn: SharedTxn,
    ) -> Self {
        Self {
            runtime,
            client,
            spanner,
            database,
            read_only,
            txn,
            sql: None,
        }
    }

    /// Apply one or more DDL statements as a single Spanner `UpdateDatabaseDdl` schema change.
    ///
    /// Batching all statements into one call makes a multi-step change (for example dbt's
    /// intermediate-table build followed by a rename swap) near-atomic.
    fn run_ddl(&self, statements: Vec<String>) -> Result<()> {
        if self.read_only {
            return Err(invalid_state(
                "cannot execute DDL: the connection is read-only",
            ));
        }
        let spanner = self.spanner.clone();
        let database = self.database.clone();
        self.runtime.block_on(async move {
            let admin = spanner
                .database_admin_builder()
                .build()
                .await
                .map_err(from_spanner)?;
            admin
                .update_database_ddl()
                .set_database(database)
                .set_statements(statements)
                .poller()
                .until_done()
                .await
                .map_err(from_spanner)?;
            Ok::<(), Error>(())
        })
    }

    fn sql(&self) -> Result<String> {
        self.sql
            .clone()
            .ok_or_else(|| invalid_state("no SQL query set on statement; call set_sql_query first"))
    }
}

impl Optionable for SpannerStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, _value: OptionValue) -> Result<()> {
        Err(not_implemented(&format!(
            "statement option {}",
            key.as_ref()
        )))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        Err(err(
            format!("option {} is not set", key.as_ref()),
            Status::NotFound,
        ))
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(err(
            format!("option {} is not set", key.as_ref()),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(err(
            format!("option {} is not set", key.as_ref()),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(err(
            format!("option {} is not set", key.as_ref()),
            Status::NotFound,
        ))
    }
}

impl Statement for SpannerStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Err(not_implemented("Statement::bind (parameter binding)"))
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(not_implemented("Statement::bind_stream (bulk ingest)"))
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let sql = self.sql()?;
        if crate::ddl::is_ddl(&sql) {
            self.run_ddl(crate::ddl::split_statements(&sql))?;
            // DDL has no result set — return an empty reader with an empty schema.
            let schema = Arc::new(Schema::empty());
            let empty: Vec<std::result::Result<RecordBatch, ArrowError>> = Vec::new();
            return Ok(Box::new(RecordBatchIterator::new(empty, schema)));
        }
        let client = self.client.clone();
        let (schema, batch) = self.runtime.block_on(async move {
            let transaction = client.single_use().build();
            let statement = SpannerSql::builder(sql).build();
            let result_set = transaction
                .execute_query(statement)
                .await
                .map_err(from_spanner)?;
            result_set_to_batch(result_set).await
        })?;
        Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let sql = self.sql()?;
        if crate::ddl::is_ddl(&sql) {
            self.run_ddl(crate::ddl::split_statements(&sql))?;
            // DDL does not report an affected-row count.
            return Ok(None);
        }
        if self.read_only {
            return Err(invalid_state(
                "cannot execute DML: the connection is read-only",
            ));
        }
        // In manual transaction mode, buffer the DML to be applied atomically on commit; the
        // affected-row count is not known until then.
        if !self.txn.lock().unwrap().autocommit() {
            self.txn.lock().unwrap().buffer(sql);
            return Ok(None);
        }
        let client = self.client.clone();
        let affected = self.runtime.block_on(async move {
            let runner = client
                .read_write_transaction()
                .build()
                .await
                .map_err(from_spanner)?;
            // The runner may retry the closure if Spanner aborts the transaction, so rebuild the
            // statement from the (cloned) SQL on each attempt.
            let outcome = runner
                .run(move |transaction: ReadWriteTransaction| {
                    let sql = sql.clone();
                    async move {
                        let statement = SpannerSql::builder(sql).build();
                        transaction.execute_update(statement).await
                    }
                })
                .await
                .map_err(from_spanner)?;
            Ok::<i64, Error>(outcome.result)
        })?;
        Ok(Some(affected))
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(not_implemented("Statement::execute_schema"))
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(not_implemented("Statement::execute_partitions"))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(not_implemented("Statement::get_parameter_schema"))
    }

    fn prepare(&mut self) -> Result<()> {
        // Spanner prepares/plans statements server-side on execution, so this is a no-op.
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(not_implemented("Statement::set_substrait_plan"))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(not_implemented("Statement::cancel"))
    }
}

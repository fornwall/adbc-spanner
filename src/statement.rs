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
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use google_cloud_lro::Poller as _;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::execute_sql_request::QueryMode;
use google_cloud_spanner::statement::Statement as SpannerSql;

use crate::bind;
use crate::connection::SharedTxn;
use crate::conversion::result_set_to_batch;
use crate::error::{err, from_spanner, invalid_argument, invalid_state, not_implemented};
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
    /// Parameter / bulk-ingest data bound via [`Statement::bind`] or [`Statement::bind_stream`].
    bound: Vec<RecordBatch>,
    /// Target table for bulk ingest (`adbc.ingest.target_table`), if set.
    target_table: Option<String>,
    /// Ingest mode (`adbc.ingest.mode`), stored in canonical form once set so it round-trips
    /// through `get_option`. Only `append` is accepted.
    ingest_mode: Option<String>,
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
            bound: Vec::new(),
            target_table: None,
            ingest_mode: None,
        }
    }

    /// Build one Spanner statement per bound row, binding its columns as named parameters.
    fn build_bound_statements(&self, sql: &str) -> Result<Vec<SpannerSql>> {
        let mut statements = Vec::new();
        for batch in &self.bound {
            for row in 0..batch.num_rows() {
                statements.push(bind::bind_row(SpannerSql::builder(sql), batch, row)?.build());
            }
        }
        Ok(statements)
    }

    /// Build one `INSERT` statement per bound row for bulk ingest into `table`.
    fn build_ingest_statements(&self, table: &str) -> Result<Vec<SpannerSql>> {
        let mut statements = Vec::new();
        for batch in &self.bound {
            let columns: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let sql = bind::insert_sql(table, &columns);
            for row in 0..batch.num_rows() {
                statements.push(bind::bind_row(SpannerSql::builder(&sql), batch, row)?.build());
            }
        }
        Ok(statements)
    }

    /// Apply DML `statements` honouring the connection's transaction mode.
    ///
    /// In autocommit mode they run immediately in one atomic read/write transaction and the
    /// affected-row count is returned. In manual mode they are buffered for the next `commit` and
    /// `None` is returned (the count is unknown until commit). Routing every DML form — plain
    /// `;`-batches, parameterized DML and bulk ingest — through here keeps them all consistent with
    /// the buffer-and-commit model.
    fn run_or_buffer(&self, statements: Vec<SpannerSql>) -> Result<Option<i64>> {
        {
            let mut txn = self.txn.lock().unwrap();
            if !txn.autocommit() {
                for statement in statements {
                    txn.buffer(statement);
                }
                return Ok(None);
            }
        }
        let count = crate::connection::run_batch_dml(&self.runtime, &self.client, statements)?;
        Ok(Some(count))
    }

    /// Run a parameterized query once per bound row, concatenating the result batches.
    fn execute_bound_query(
        &self,
        sql: &str,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let statements = self.build_bound_statements(sql)?;
        let client = self.client.clone();
        let (schema, batches): (Option<SchemaRef>, Vec<RecordBatch>) =
            self.runtime.block_on(async move {
                let mut schema = None;
                let mut batches = Vec::new();
                for statement in statements {
                    let transaction = client.single_use().build();
                    let result_set = transaction
                        .execute_query(statement)
                        .await
                        .map_err(from_spanner)?;
                    let (sch, batch) = result_set_to_batch(result_set).await?;
                    schema.get_or_insert(sch);
                    batches.push(batch);
                }
                Ok::<_, Error>((schema, batches))
            })?;
        let schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        let batches: Vec<std::result::Result<RecordBatch, ArrowError>> =
            batches.into_iter().map(Ok).collect();
        Ok(Box::new(RecordBatchIterator::new(batches, schema)))
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

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionStatement::TargetTable => self.target_table = Some(string_option(value)?),
            OptionStatement::IngestMode => match string_option(value)?.as_str() {
                // Only appending into an existing table is supported.
                "adbc.ingest.mode.append" | "append" => {
                    self.ingest_mode = Some("adbc.ingest.mode.append".to_string());
                }
                other => return Err(not_implemented(&format!("ingest mode {other:?}"))),
            },
            other => {
                return Err(not_implemented(&format!(
                    "statement option {}",
                    other.as_ref()
                )))
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let value = match &key {
            OptionStatement::TargetTable => self.target_table.clone(),
            OptionStatement::IngestMode => self.ingest_mode.clone(),
            _ => None,
        };
        value.ok_or_else(|| {
            err(
                format!("option {} is not set", key.as_ref()),
                Status::NotFound,
            )
        })
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Ok(self.get_option_string(key)?.into_bytes())
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
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound = vec![batch];
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|e| {
                err(
                    format!("failed to read bound stream: {e}"),
                    Status::InvalidData,
                )
            })?);
        }
        self.bound = batches;
        Ok(())
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
        // Parameterized query: run once per bound row.
        if !self.bound.is_empty() {
            return self.execute_bound_query(&sql);
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
        // Bulk ingest: insert the bound rows into the target table (needs no SQL query).
        if let Some(table) = self.target_table.clone() {
            if self.read_only {
                return Err(invalid_state("cannot ingest: the connection is read-only"));
            }
            let statements = self.build_ingest_statements(&table)?;
            return self.run_or_buffer(statements);
        }

        let sql = self.sql()?;
        if crate::ddl::is_ddl(&sql) {
            self.run_ddl(crate::ddl::split_statements(&sql))?;
            // DDL does not report an affected-row count (and is never transactional in Spanner, so
            // it always runs immediately rather than buffering).
            return Ok(None);
        }
        if self.read_only {
            return Err(invalid_state(
                "cannot execute DML: the connection is read-only",
            ));
        }
        // Build the statements to apply: one per bound row for parameterized DML, otherwise a
        // `;`-separated batch (e.g. dbt's DELETE; INSERT) split into individual statements so the
        // whole batch is applied atomically. `run_or_buffer` then either runs them (autocommit) or
        // buffers them for commit (manual mode).
        let statements = if !self.bound.is_empty() {
            self.build_bound_statements(&sql)?
        } else {
            crate::ddl::split_statements(&sql)
                .into_iter()
                .map(|s| SpannerSql::builder(s).build())
                .collect()
        };
        self.run_or_buffer(statements)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let sql = self.sql()?;
        if crate::ddl::is_ddl(&sql) {
            return Err(invalid_state("execute_schema is only valid for queries"));
        }
        let client = self.client.clone();
        let bound = self.bound.clone();
        let schema = self.runtime.block_on(async move {
            let transaction = client.single_use().build();
            // QueryMode::Plan analyses the query and returns its column metadata without scanning
            // any data, so dbt can introspect a model's output columns without wrapping it in a
            // `SELECT ... WHERE false` subquery.
            let mut builder = SpannerSql::builder(sql).set_query_mode(QueryMode::Plan);
            // Bind parameters if any were provided (values are irrelevant to the schema) so that
            // `@param` references resolve.
            if let Some(batch) = bound.first() {
                if batch.num_rows() > 0 {
                    builder = bind::bind_row(builder, batch, 0)?;
                }
            }
            let result_set = transaction
                .execute_query(builder.build())
                .await
                .map_err(from_spanner)?;
            let (schema, _batch) = result_set_to_batch(result_set).await?;
            Ok::<SchemaRef, Error>(schema)
        })?;
        Ok((*schema).clone())
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        // Spanner does support partitioned queries via a batch read-only transaction, but the
        // partitions are only valid within that live, session-bound transaction — which does not
        // map onto ADBC's opaque-token / read_partition model — and the Spanner emulator does not
        // implement the Partition RPCs, so this is intentionally left unimplemented.
        Err(not_implemented("Statement::execute_partitions"))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        // If parameter (or bulk-ingest) data has already been bound, each column *is* a parameter,
        // so its schema is the parameter schema — carrying real, known types.
        if let Some(batch) = self.bound.first() {
            return Ok((*batch.schema()).clone());
        }
        // Otherwise derive the parameters from the statement's `@name` references. Spanner infers
        // parameter types from the surrounding SQL at execution time and exposes no way to
        // introspect them beforehand, so each parameter is typed as `Null` — Arrow's convention for
        // an unknown/any type — with the parameter name preserved.
        let sql = self.sql()?;
        let fields: Vec<Field> = bind::named_parameters(&sql)
            .into_iter()
            .map(|name| Field::new(name, DataType::Null, true))
            .collect();
        Ok(Schema::new(fields))
    }

    fn prepare(&mut self) -> Result<()> {
        // Spanner prepares/plans statements server-side on execution, so there is nothing to do
        // beyond the ADBC precondition that a query must have been set first.
        self.sql()?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        // Spanner has no Substrait support (it executes GoogleSQL / PostgreSQL text), so there is
        // nothing to execute a Substrait plan against.
        Err(not_implemented(
            "Substrait: Spanner does not support Substrait plans",
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(not_implemented("Statement::cancel"))
    }
}

fn string_option(value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(invalid_argument("statement option requires a string value")),
    }
}

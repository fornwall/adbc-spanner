//! The [`SpannerStatement`] — an ADBC statement that runs SQL against Spanner and returns Arrow.
//!
//! A statement holds a SQL string set via [`Statement::set_sql_query`]. Calling
//! [`Statement::execute`] runs it as a query in a single-use read-only transaction and returns a
//! streaming Arrow [`RecordBatchReader`]: rows are pulled from Spanner and converted to Arrow in
//! bounded chunks (see [`OPTION_ROWS_PER_BATCH`](crate::OPTION_ROWS_PER_BATCH)) as the consumer
//! iterates, so a large result set is not fully materialised in memory. Calling
//! [`Statement::execute_update`] runs it as DML inside a read/write transaction and returns the
//! number of affected rows.
//!
//! DML with a `THEN RETURN` clause returns rows: through [`Statement::execute`] they come back as
//! an Arrow result (running via `ExecuteSql` in a read/write transaction, since `ExecuteBatchDml`
//! does not support `THEN RETURN`); through [`Statement::execute_update`] the rows are discarded
//! and the affected-row count is reported from the result-set stats.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use google_cloud_lro::Poller as _;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::execute_sql_request::QueryMode;
use google_cloud_spanner::model::transaction_options::IsolationLevel;
use google_cloud_spanner::model::PartitionOptions;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::ReadWriteTransaction;

use crate::bind;
use crate::connection::{apply_isolation, SharedTxn};
use crate::conversion::{result_set_to_batch, stream_query};
use crate::error::{
    err, from_builder, from_spanner, invalid_argument, invalid_state, not_implemented,
};
use crate::runtime::{block_on_cancellable, CancelSignal, SharedRuntime};
use crate::staleness::ReadStaleness;

/// Default number of rows converted into each streamed Arrow batch (see
/// [`OPTION_ROWS_PER_BATCH`](crate::OPTION_ROWS_PER_BATCH)). Also used by
/// [`Connection::read_partition`](adbc_core::Connection::read_partition), which has no per-statement
/// batch-size option.
pub(crate) const DEFAULT_ROWS_PER_BATCH: usize = 8192;

/// The result of routing DML through [`SpannerStatement::run_dml`].
enum DmlOutcome {
    /// Plain DML via `ExecuteBatchDml`: the affected-row count, or `None` when the statements
    /// were buffered for a manual-transaction commit.
    Plain(Option<i64>),
    /// DML with `THEN RETURN`: the returned rows and the affected-row count from the stats.
    Returning {
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        affected: i64,
    },
}

/// An ADBC statement bound to a Spanner [`DatabaseClient`].
pub struct SpannerStatement {
    runtime: SharedRuntime,
    client: DatabaseClient,
    spanner: Spanner,
    database: String,
    read_only: bool,
    /// Isolation level for this statement's read/write transactions, inherited from the connection
    /// at creation time (see the standard `adbc.connection.transaction.isolation_level` option).
    isolation: IsolationLevel,
    txn: SharedTxn,
    sql: Option<String>,
    /// Parameter / bulk-ingest data bound via [`Statement::bind`] or [`Statement::bind_stream`].
    bound: Vec<RecordBatch>,
    /// Target table for bulk ingest (`adbc.ingest.target_table`), if set.
    target_table: Option<String>,
    /// Named schema qualifying the ingest target table (`adbc.ingest.target_db_schema`), if set.
    /// `None` (or empty) targets Spanner's default, unnamed schema.
    target_db_schema: Option<String>,
    /// Ingest target catalog (`adbc.ingest.target_catalog`), if set. Spanner has a single, unnamed
    /// (`""`) catalog, so only the empty catalog is accepted; stored solely so the option
    /// round-trips through `get_option`.
    target_catalog: Option<String>,
    /// Ingest mode (`adbc.ingest.mode`), stored in canonical form once set so it round-trips
    /// through `get_option`. `append` (default), `create`, `create_append`, and `replace` are
    /// accepted; the create/replace modes build the table from the ingest data's Arrow schema.
    ingest_mode: Option<String>,
    /// Rows converted into each streamed Arrow batch by `execute` (`spanner.rows_per_batch`).
    rows_per_batch: usize,
    /// Enable Data Boost for partitioned execution (`spanner.data_boost_enabled`).
    data_boost: bool,
    /// Maximum number of partitions to request from `execute_partitions`
    /// (`spanner.max_partitions`); `None` lets Spanner choose.
    max_partitions: Option<i64>,
    /// Read staleness / timestamp bound for this statement's read-only queries
    /// (`spanner.read.staleness` / `spanner.read.timestamp`), inherited from the connection at
    /// creation time and overridable per statement. Default is a strong read.
    read_staleness: ReadStaleness,
    /// Cancellation signal for this statement's in-flight execution (see [`Statement::cancel`]).
    cancel: CancelSignal,
}

impl SpannerStatement {
    #[allow(clippy::too_many_arguments)] // constructor threads the connection's config verbatim
    pub(crate) fn new(
        runtime: SharedRuntime,
        client: DatabaseClient,
        spanner: Spanner,
        database: String,
        read_only: bool,
        isolation: IsolationLevel,
        read_staleness: ReadStaleness,
        txn: SharedTxn,
    ) -> Self {
        Self {
            runtime,
            client,
            spanner,
            database,
            read_only,
            isolation,
            txn,
            sql: None,
            bound: Vec::new(),
            target_table: None,
            target_db_schema: None,
            target_catalog: None,
            ingest_mode: None,
            rows_per_batch: DEFAULT_ROWS_PER_BATCH,
            data_boost: false,
            max_partitions: None,
            read_staleness,
            cancel: CancelSignal::new(),
        }
    }

    /// Build one Spanner statement per bound row, binding its columns as named parameters.
    fn build_bound_statements(&self, sql: &str) -> Result<Vec<SpannerSql>> {
        let mut statements = Vec::new();
        for batch in &self.bound {
            if batch.num_rows() == 0 {
                continue;
            }
            // Resolve the column→parameter mapping once per batch (it lexes `sql`), then reuse it
            // for every row instead of re-lexing the SQL per bound row.
            let names = bind::resolve_parameter_names(sql, batch)?;
            for row in 0..batch.num_rows() {
                statements
                    .push(bind::bind_params(SpannerSql::builder(sql), &names, batch, row)?.build());
            }
        }
        Ok(statements)
    }

    /// DDL to run before an ingest, for the create/replace ingest modes (`None` for append).
    ///
    /// `create` builds the table (erroring if it exists), `create_append` builds it if absent, and
    /// `replace` drops any existing table first. The schema comes from the bound ingest data.
    fn build_ingest_table_ddl(
        &self,
        table: &str,
        mode: Option<&str>,
    ) -> Result<Option<Vec<String>>> {
        let (if_not_exists, drop_first) = match mode {
            // Append (the default) into an existing table: no DDL.
            None | Some("adbc.ingest.mode.append") => return Ok(None),
            Some("adbc.ingest.mode.create") => (false, false),
            Some("adbc.ingest.mode.create_append") => (true, false),
            Some("adbc.ingest.mode.replace") => (false, true),
            Some(other) => return Err(not_implemented(&format!("ingest mode {other:?}"))),
        };
        let schema = self
            .bound
            .first()
            .ok_or_else(|| invalid_state("cannot create the ingest table: no data is bound"))?
            .schema();
        let db_schema = self.target_db_schema.as_deref();
        let mut statements = Vec::new();
        if drop_first {
            statements.push(format!(
                "DROP TABLE IF EXISTS {}",
                bind::qualified_table(db_schema, table)
            ));
        }
        statements.push(bind::create_table_sql(
            table,
            db_schema,
            &schema,
            if_not_exists,
        )?);
        Ok(Some(statements))
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
            let sql = bind::insert_sql(table, self.target_db_schema.as_deref(), &columns);
            for row in 0..batch.num_rows() {
                statements.push(bind::bind_row(SpannerSql::builder(&sql), batch, row)?.build());
            }
        }
        Ok(statements)
    }

    /// Remap a failed `append`-mode bulk ingest onto the statuses the ADBC bulk-ingest contract
    /// mandates.
    ///
    /// A successful (or, in manual-transaction mode, merely buffered) outcome is returned unchanged.
    /// On failure the target table is probed via the shared [`table_exists`](crate::connection::table_exists)
    /// query: a missing table becomes [`Status::NotFound`], and an existing table — so the insert
    /// must have failed because the bound data's schema is incompatible with the table's — becomes
    /// [`Status::AlreadyExists`]. Only these two cases are remapped; the original Spanner error's
    /// detail is folded into the message. If the probe itself fails (e.g. a transport error) that
    /// probe error is surfaced instead, so a genuine outage is not masked as a schema mismatch.
    fn remap_ingest_append_error(
        &self,
        table: &str,
        result: Result<Option<i64>>,
    ) -> Result<Option<i64>> {
        let error = match result {
            Ok(count) => return Ok(count),
            Err(error) => error,
        };
        // Probe the target table in its schema (`adbc.ingest.target_db_schema`, empty = Spanner's
        // default, unnamed schema).
        let db_schema = self.target_db_schema.as_deref().unwrap_or("");
        let exists = crate::connection::table_exists(
            &self.runtime,
            &self.client,
            &self.cancel,
            db_schema,
            table,
        )?;
        if exists {
            Err(err(
                format!(
                    "bulk ingest append into table {table:?} failed: the bound data is \
                     incompatible with the existing table's schema ({})",
                    error.message
                ),
                Status::AlreadyExists,
            ))
        } else {
            Err(err(
                format!(
                    "bulk ingest append target table {table:?} not found ({})",
                    error.message
                ),
                Status::NotFound,
            ))
        }
    }

    /// Build the DML statements to apply for `sql`: one per bound row for parameterized DML,
    /// otherwise a `;`-separated batch (e.g. dbt's `DELETE; INSERT`) split into individual
    /// statements so the whole batch is applied atomically. Shared by `execute` and `execute_update`.
    fn build_dml_statements(&self, sql: &str) -> Result<Vec<SpannerSql>> {
        if !self.bound.is_empty() {
            self.build_bound_statements(sql)
        } else {
            Ok(crate::ddl::split_statements(sql)
                .into_iter()
                .map(|s| SpannerSql::builder(s).build())
                .collect())
        }
    }

    /// An empty result reader (empty schema, no rows), for statements that yield no result set.
    fn empty_reader() -> Box<dyn RecordBatchReader + Send + 'static> {
        let schema = Arc::new(Schema::empty());
        let empty: Vec<std::result::Result<RecordBatch, ArrowError>> = Vec::new();
        Box::new(RecordBatchIterator::new(empty, schema))
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
        let count = crate::connection::run_batch_dml(
            &self.runtime,
            &self.client,
            &self.cancel,
            self.isolation.clone(),
            statements,
        )?;
        Ok(Some(count))
    }

    /// Run DML with a `THEN RETURN` clause: one read/write transaction executes every statement
    /// via `ExecuteSql` (not `ExecuteBatchDml`, which rejects `THEN RETURN`), draining each result
    /// set **before** commit, as Spanner requires for returned rows.
    ///
    /// Returns the concatenated result batches (with the schema from the first) and the total
    /// affected-row count from the result-set stats. The rows are drained *inside* the runner's
    /// closure keeping the client's own error type, so a transaction abort still retries — the
    /// (cloned) statement list is simply replayed, and only the last attempt's rows are returned.
    /// Conversion to Arrow happens after the transaction commits.
    fn execute_returning_dml(
        &self,
        statements: Vec<SpannerSql>,
    ) -> Result<(Vec<RecordBatch>, SchemaRef, i64)> {
        let client = self.client.clone();
        let isolation = self.isolation.clone();
        let results = block_on_cancellable(&self.runtime, &self.cancel, async move {
            let runner = apply_isolation(client.read_write_transaction(), isolation)
                .build()
                .await
                .map_err(from_spanner)?;
            let outcome = runner
                .run(move |transaction: ReadWriteTransaction| {
                    let statements = statements.clone();
                    async move {
                        let mut results = Vec::with_capacity(statements.len());
                        for statement in statements {
                            let mut result_set = transaction.execute_query(statement).await?;
                            let mut rows = Vec::new();
                            while let Some(row) = result_set.next().await {
                                rows.push(row?);
                            }
                            // Stats (including the affected-row count) arrive with the end of
                            // the stream. `THEN RETURN` yields one row per affected row, so the
                            // drained row count is the fallback.
                            let count = result_set.update_count().unwrap_or(rows.len() as i64);
                            results.push((result_set.metadata().cloned(), rows, count));
                        }
                        Ok(results)
                    }
                })
                .await
                .map_err(from_spanner)?;
            Ok::<_, Error>(outcome.result)
        })?;

        let mut schema = None;
        let mut batches = Vec::with_capacity(results.len());
        let mut affected = 0i64;
        for (metadata, rows, count) in &results {
            let (sch, batch) = crate::conversion::rows_to_batch(metadata.as_ref(), rows)?;
            schema.get_or_insert(sch);
            batches.push(batch);
            affected += count;
        }
        let schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        Ok((batches, schema, affected))
    }

    /// Route DML through the right executor: `THEN RETURN` statements run individually in a
    /// read/write transaction (returning their rows and count), everything else goes through
    /// [`Self::run_or_buffer`].
    ///
    /// `THEN RETURN` is incompatible with manual transaction mode: buffered DML only executes at
    /// `commit`, and `ExecuteBatchDml` — the commit path — rejects `THEN RETURN` outright, so the
    /// returned rows would be silently unobtainable. It is rejected up front instead.
    fn run_dml(&self, sql: &str) -> Result<DmlOutcome> {
        if !crate::ddl::is_dml_returning(sql) {
            let statements = self.build_dml_statements(sql)?;
            return Ok(DmlOutcome::Plain(self.run_or_buffer(statements)?));
        }
        if !self.txn.lock().unwrap().autocommit() {
            return Err(invalid_state(
                "DML with THEN RETURN cannot run in a manual transaction: buffered DML is applied \
                 via ExecuteBatchDml on commit, which does not support THEN RETURN. Re-enable \
                 autocommit to run it",
            ));
        }
        let statements = if self.bound.is_empty() {
            let parts = crate::ddl::split_statements(sql);
            if parts.len() > 1 {
                return Err(not_implemented(
                    "THEN RETURN in a multi-statement (`;`-separated) DML batch",
                ));
            }
            parts
                .into_iter()
                .map(|s| SpannerSql::builder(s).build())
                .collect()
        } else {
            self.build_bound_statements(sql)?
        };
        let (batches, schema, affected) = self.execute_returning_dml(statements)?;
        Ok(DmlOutcome::Returning {
            batches,
            schema,
            affected,
        })
    }

    /// Run a parameterized query once per bound row, concatenating the result batches.
    fn execute_bound_query(
        &self,
        sql: &str,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let statements = self.build_bound_statements(sql)?;
        let client = self.client.clone();
        let bound = self.read_staleness.timestamp_bound()?;
        let (schema, batches): (Option<SchemaRef>, Vec<RecordBatch>) =
            block_on_cancellable(&self.runtime, &self.cancel, async move {
                let mut schema = None;
                let mut batches = Vec::new();
                for statement in statements {
                    let transaction = crate::staleness::single_use(&client, bound.clone());
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
        block_on_cancellable(&self.runtime, &self.cancel, async move {
            let admin = spanner
                .database_admin_builder()
                .build()
                .await
                .map_err(from_builder)?;
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

    /// Build a Spanner query statement for `sql`, binding the first bound row (if any) as named
    /// parameters. With `plan = true` the statement is set to `QueryMode::Plan` so it returns column
    /// metadata without scanning data. Used by `execute_partitions` for both the schema probe and the
    /// partitioned query itself. Only the first bound row is used — partitioned execution has no
    /// per-row fan-out, so extra bound rows are ignored.
    fn build_query_statement(&self, sql: &str, plan: bool) -> Result<SpannerSql> {
        let mut builder = SpannerSql::builder(sql);
        if plan {
            builder = builder.set_query_mode(QueryMode::Plan);
        }
        if let Some(batch) = self.bound.first() {
            if batch.num_rows() > 0 {
                let names = bind::resolve_parameter_names(sql, batch)?;
                builder = bind::bind_params(builder, &names, batch, 0)?;
            }
        }
        Ok(builder.build())
    }
}

impl Optionable for SpannerStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionStatement::TargetTable => self.target_table = Some(string_option(value)?),
            OptionStatement::TargetDbSchema => {
                // Named schema for the ingest target table; qualifies the INSERT / CREATE TABLE via
                // `qualified_table` (empty selects Spanner's default, unnamed schema).
                self.target_db_schema = Some(string_option(value)?);
            }
            OptionStatement::TargetCatalog => {
                // Spanner exposes a single, unnamed catalog, so only the empty catalog is accepted.
                self.target_catalog = Some(check_target_catalog(string_option(value)?)?);
            }
            OptionStatement::IngestMode => {
                // Append into an existing table, or create it (from the ingest data's Arrow schema,
                // with a synthetic UUID primary key) in the create/replace modes.
                let canonical = match string_option(value)?.as_str() {
                    "adbc.ingest.mode.append" | "append" => "adbc.ingest.mode.append",
                    "adbc.ingest.mode.create" | "create" => "adbc.ingest.mode.create",
                    "adbc.ingest.mode.create_append" | "create_append" => {
                        "adbc.ingest.mode.create_append"
                    }
                    "adbc.ingest.mode.replace" | "replace" => "adbc.ingest.mode.replace",
                    other => return Err(not_implemented(&format!("ingest mode {other:?}"))),
                };
                self.ingest_mode = Some(canonical.to_string());
            }
            OptionStatement::Other(k) if k == crate::OPTION_ROWS_PER_BATCH => {
                self.rows_per_batch = rows_per_batch_option(value)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_DATA_BOOST => {
                self.data_boost = bool_option(value)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_MAX_PARTITIONS => {
                self.max_partitions = Some(max_partitions_option(value)?);
            }
            OptionStatement::Other(k) if k == crate::OPTION_READ_STALENESS => {
                self.read_staleness.set_staleness(value)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_READ_TIMESTAMP => {
                self.read_staleness.set_timestamp(value)?;
            }
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
            OptionStatement::TargetDbSchema => self.target_db_schema.clone(),
            OptionStatement::TargetCatalog => self.target_catalog.clone(),
            OptionStatement::IngestMode => self.ingest_mode.clone(),
            OptionStatement::Other(k) if k == crate::OPTION_ROWS_PER_BATCH => {
                Some(self.rows_per_batch.to_string())
            }
            OptionStatement::Other(k) if k == crate::OPTION_DATA_BOOST => {
                Some(self.data_boost.to_string())
            }
            OptionStatement::Other(k) if k == crate::OPTION_MAX_PARTITIONS => {
                self.max_partitions.map(|n| n.to_string())
            }
            OptionStatement::Other(k) if k == crate::OPTION_READ_STALENESS => {
                self.read_staleness.staleness_string().map(str::to_string)
            }
            OptionStatement::Other(k) if k == crate::OPTION_READ_TIMESTAMP => {
                self.read_staleness.timestamp_string().map(str::to_string)
            }
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
        if let OptionStatement::Other(k) = &key {
            if k == crate::OPTION_ROWS_PER_BATCH {
                return Ok(self.rows_per_batch as i64);
            }
            if k == crate::OPTION_MAX_PARTITIONS {
                if let Some(n) = self.max_partitions {
                    return Ok(n);
                }
            }
        }
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
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        let sql = self.sql()?;
        if crate::ddl::is_ddl(&sql) {
            self.run_ddl(crate::ddl::split_statements(&sql))?;
            // DDL has no result set — return an empty reader with an empty schema.
            return Ok(Self::empty_reader());
        }
        // DML arriving through the query entry point. Standard ADBC clients (the Python DBAPI, R,
        // etc.) issue every statement — including INSERT/UPDATE/DELETE — through `ExecuteQuery`, so
        // route DML onto the read/write path (or buffer it in manual mode) rather than the read-only
        // single-use transaction below, which Spanner rejects for DML. This mirrors `execute_update`.
        // DML with a `THEN RETURN` clause returns its rows; plain DML yields an empty result (the
        // query interface has nowhere to report the affected-row count, so it is discarded).
        if crate::ddl::is_dml(&sql) {
            if self.read_only {
                return Err(invalid_state(
                    "cannot execute DML: the connection is read-only",
                ));
            }
            let result = self.run_dml(&sql);
            self.bound.clear();
            return match result? {
                DmlOutcome::Returning {
                    batches, schema, ..
                } => {
                    let batches: Vec<std::result::Result<RecordBatch, ArrowError>> =
                        batches.into_iter().map(Ok).collect();
                    Ok(Box::new(RecordBatchIterator::new(batches, schema)))
                }
                DmlOutcome::Plain(_) => Ok(Self::empty_reader()),
            };
        }
        // Query path (SELECT / WITH / …). Strip any trailing statement terminator(s): Spanner's
        // single-use query API rejects a trailing `;` ("Expected end of input but got `;`"), yet
        // clients and conformance suites routinely append one (e.g. `SELECT current_date;;;`). The
        // DDL and DML paths above go through `split_statements`, which already drops empty trailing
        // segments, so this stripping is scoped to the query path and never splits a `;`-batch.
        let sql = crate::ddl::strip_trailing_terminators(&sql);
        // Parameterized query: run once per bound row.
        if !self.bound.is_empty() {
            let reader = self.execute_bound_query(&sql)?;
            self.bound.clear();
            return Ok(reader);
        }
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.clone();
        let batch_size = self.rows_per_batch;
        let bound = self.read_staleness.timestamp_bound()?;
        // Stream the result: `stream_query` fetches the first chunk (settling the schema) and the
        // returned reader converts the rest to Arrow one bounded chunk at a time as it is iterated.
        let reader = block_on_cancellable(&self.runtime, &self.cancel, async move {
            let transaction = crate::staleness::single_use(&client, bound);
            let statement = SpannerSql::builder(sql).build();
            let result_set = transaction
                .execute_query(statement)
                .await
                .map_err(from_spanner)?;
            stream_query(runtime, cancel, result_set, batch_size).await
        })?;
        Ok(Box::new(reader))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        // Bulk ingest: insert the bound rows into the target table (needs no SQL query).
        if let Some(table) = self.target_table.clone() {
            if self.read_only {
                return Err(invalid_state("cannot ingest: the connection is read-only"));
            }
            if self.bound.is_empty() {
                return Err(invalid_state("cannot ingest: no data has been bound"));
            }
            // In the create/replace modes, first build the table from the ingest data's Arrow schema
            // (with a synthetic UUID primary key) — Spanner DDL runs immediately, before the inserts.
            let mode = self.ingest_mode.as_deref();
            let ingest_ddl = self.build_ingest_table_ddl(&table, mode)?;
            // `append` (the default) is the only mode that inserts into a pre-existing table, so it
            // is the only one whose failure the ADBC spec wants remapped to NotFound / AlreadyExists.
            // `build_ingest_table_ddl` returns `None` for exactly that mode.
            let is_append = ingest_ddl.is_none();
            if let Some(ddl) = ingest_ddl {
                self.run_ddl(ddl)?;
            }
            let statements = self.build_ingest_statements(&table)?;
            self.bound.clear();
            let result = self.run_or_buffer(statements);
            return if is_append {
                self.remap_ingest_append_error(&table, result)
            } else {
                result
            };
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
        let result = self.run_dml(&sql);
        self.bound.clear();
        match result? {
            // THEN RETURN through the update entry point: the rows are discarded (this interface
            // only reports a count), taken from the result-set stats.
            DmlOutcome::Returning { affected, .. } => Ok(Some(affected)),
            DmlOutcome::Plain(count) => Ok(count),
        }
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        let sql = self.sql()?;
        if crate::ddl::is_ddl(&sql) {
            return Err(invalid_state("execute_schema is only valid for queries"));
        }
        let client = self.client.clone();
        let bound = self.bound.clone();
        let schema = block_on_cancellable(&self.runtime, &self.cancel, async move {
            let transaction = client.single_use().build();
            // QueryMode::Plan analyses the query and returns its column metadata without scanning
            // any data, so dbt can introspect a model's output columns without wrapping it in a
            // `SELECT ... WHERE false` subquery.
            let mut builder = SpannerSql::builder(sql.as_str()).set_query_mode(QueryMode::Plan);
            // Bind parameters if any were provided (values are irrelevant to the schema) so that
            // `@param` references resolve.
            if let Some(batch) = bound.first() {
                if batch.num_rows() > 0 {
                    let names = bind::resolve_parameter_names(&sql, batch)?;
                    builder = bind::bind_params(builder, &names, batch, 0)?;
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
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        let sql = self.sql()?;
        if crate::ddl::is_ddl(&sql) {
            return Err(invalid_state(
                "execute_partitions is only valid for queries",
            ));
        }
        // Probe the schema and create the partitions. The partition query runs inside a batch
        // read-only transaction; each returned partition carries its session, transaction id and
        // partition token and is independently serializable, so it maps directly onto ADBC's opaque
        // partition descriptor. The (Arc-shared, multiplexed) session lives as long as the
        // connection's `DatabaseClient`, so the descriptors stay valid after this statement is gone,
        // to be executed later by `Connection::read_partition`.
        let plan_stmt = self.build_query_statement(&sql, true)?;
        let query_stmt = self.build_query_statement(&sql, false)?;
        let client = self.client.clone();
        let data_boost = self.data_boost;
        let max_partitions = self.max_partitions;
        // The partitioned read honours the statement's read staleness: it is baked into the batch
        // read-only transaction, so every partition executes at that bound wherever it is read back.
        let bound = self.read_staleness.timestamp_bound()?;

        let (schema, partitions) = block_on_cancellable(&self.runtime, &self.cancel, async move {
            // Schema via a PLAN of the query: column metadata without scanning any data.
            let plan_rs = crate::staleness::single_use(&client, bound.clone())
                .execute_query(plan_stmt)
                .await
                .map_err(from_spanner)?;
            let (schema, _batch) = result_set_to_batch(plan_rs).await?;

            // Partition the query across a batch read-only transaction.
            let mut txn_builder = client.batch_read_only_transaction();
            if let Some(b) = bound {
                txn_builder = txn_builder.set_timestamp_bound(b);
            }
            let transaction = txn_builder.build().await.map_err(from_spanner)?;
            let mut options = PartitionOptions::default();
            if let Some(n) = max_partitions {
                options = options.set_max_partitions(n);
            }
            let partitions = transaction
                .partition_query(query_stmt, options)
                .await
                .map_err(from_spanner)?;

            // Serialize each partition into an opaque ADBC descriptor, baking in the Data Boost
            // choice so it travels with the token (honoured wherever the partition is executed).
            let mut tokens: Vec<Vec<u8>> = Vec::with_capacity(partitions.len());
            for partition in partitions {
                let partition = if data_boost {
                    partition.set_data_boost(true)
                } else {
                    partition
                };
                let token = serde_json::to_vec(&partition).map_err(|e| {
                    err(
                        format!("failed to serialize partition descriptor: {e}"),
                        Status::Internal,
                    )
                })?;
                tokens.push(token);
            }
            Ok::<_, Error>((schema, tokens))
        })?;

        Ok(PartitionedResult {
            partitions,
            schema: (*schema).clone(),
            // A read query has no affected-row count; ADBC uses -1 for "unknown".
            rows_affected: -1,
        })
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
        // ADBC requires InvalidState when there is nothing to prepare. Otherwise this is a no-op:
        // Spanner prepares/plans statements server-side on execution, so preparing a set query — or
        // a bulk-ingest target (which needs no SQL) — has nothing to do here.
        if self.sql.is_none() && self.target_table.is_none() {
            return Err(invalid_state(
                "cannot prepare: no SQL query set on statement; call set_sql_query first",
            ));
        }
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
        // Latch the (sticky) signal: an in-flight execution wakes and returns Cancelled, and a
        // cancel landing between two chunk fetches of a streamed result still cancels the next
        // fetch. The latch is cleared when the statement starts its next operation, so a cancel
        // with nothing running does not affect later executions.
        self.cancel.signal();
        Ok(())
    }
}

fn string_option(value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(invalid_argument("statement option requires a string value")),
    }
}

/// Validate the `adbc.ingest.target_catalog` option. Spanner has a single, unnamed (`""`) catalog,
/// so only the empty catalog is accepted; any other name is rejected as unsupported.
fn check_target_catalog(catalog: String) -> Result<String> {
    if catalog.is_empty() {
        Ok(catalog)
    } else {
        Err(not_implemented(&format!(
            "ingest target catalog {catalog:?}: Spanner has only the default (empty) catalog"
        )))
    }
}

/// Parse a boolean statement option, accepted as a bool-ish string (`true`/`false`/`1`/`0`/…) or an
/// integer (`0` = false, non-zero = true).
fn bool_option(value: OptionValue) -> Result<bool> {
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

/// Parse a positive `max_partitions` option, accepted as either an integer or a numeric string.
fn max_partitions_option(value: OptionValue) -> Result<i64> {
    let n = match value {
        OptionValue::Int(i) => i,
        OptionValue::String(s) => s
            .parse::<i64>()
            .map_err(|_| invalid_argument("max_partitions must be a positive integer"))?,
        _ => {
            return Err(invalid_argument(
                "max_partitions must be a positive integer",
            ))
        }
    };
    if n > 0 {
        Ok(n)
    } else {
        Err(invalid_argument(
            "max_partitions must be a positive integer",
        ))
    }
}

/// Parse a positive batch-size option, accepted as either an integer or a numeric string.
fn rows_per_batch_option(value: OptionValue) -> Result<usize> {
    let n = match value {
        OptionValue::Int(i) => i,
        OptionValue::String(s) => s
            .parse::<i64>()
            .map_err(|_| invalid_argument("rows_per_batch must be a positive integer"))?,
        _ => {
            return Err(invalid_argument(
                "rows_per_batch must be a positive integer",
            ))
        }
    };
    usize::try_from(n)
        .ok()
        .filter(|&n| n > 0)
        .ok_or_else(|| invalid_argument("rows_per_batch must be a positive integer"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    #[test]
    fn accepts_only_the_empty_ingest_catalog() {
        // Spanner's single, unnamed catalog is accepted and preserved for round-tripping.
        assert_eq!(check_target_catalog(String::new()).unwrap(), "");
        // Any named catalog is rejected as unsupported.
        let error = check_target_catalog("main".to_string()).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
    }
}

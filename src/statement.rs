//! The [`SpannerStatement`] — an ADBC statement that runs SQL against Spanner and returns Arrow.
//!
//! A statement holds a SQL string set via [`Statement::set_sql_query`].
//! [`Statement::execute`] runs it as a query in a single-use read-only transaction, returning a
//! streaming Arrow [`RecordBatchReader`] that converts rows in bounded chunks (see
//! [`OPTION_ROWS_PER_BATCH`](crate::OPTION_ROWS_PER_BATCH)) as the consumer iterates.
//! [`Statement::execute_update`] runs DML in a read/write transaction and returns the affected-row
//! count, and routes DDL to the admin API. SQL that is neither (a query — `adbc.h` sanctions
//! executing any statement without expecting a result set) runs through the same read-only
//! machinery as `execute`, rows drained and discarded and no count (`None`) reported.
//!
//! DML with a `THEN RETURN` clause returns rows: through [`Statement::execute`] as an Arrow result
//! (via `ExecuteSql` in a read/write transaction, since `ExecuteBatchDml` does not support
//! `THEN RETURN`); through [`Statement::execute_update`] the rows are discarded and the count is
//! reported from the result-set stats.

mod ingest;

use ingest::{check_target_catalog, ingest_batch_write_option, ingest_mode_option};

use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use adbc_core::{CancelHandle, Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use google_cloud_lro::Poller as _;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::PartitionOptions;
use google_cloud_spanner::model::execute_sql_request::QueryMode;
use google_cloud_spanner::statement::{Statement as SpannerSql, StatementBuilder};
use google_cloud_spanner::transaction::{MultiUseReadOnlyTransaction, ReadWriteTransaction};

use crate::bind;
use crate::connection::{SharedTxn, TxnKind, build_runner, lock_txn};
use crate::conversion::{
    BoundStatementSource, TimestampPrecision, result_set_to_batch, stream_bound_query, stream_query,
};
use crate::driver::SharedDatabaseAdmin;
use crate::error::{
    err, from_builder, from_spanner, invalid_argument, invalid_state, not_implemented,
    option_not_set, unknown_option, unsupported,
};
use crate::options::{
    SharedConfig, bool_option, impl_shared_option_dispatch, impl_typed_option_getters,
};
use crate::runtime::{CancelSlot, SharedRuntime, SlotCancelHandle, block_on_cancellable};
use crate::timeout::with_timeout;

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

/// The lazy [`BoundStatementSource`] backing [`SpannerStatement::execute_bound_query`]: it builds
/// each per-bound-row `SpannerSql` on demand, right before the reader executes it, so a large
/// `executemany` SELECT holds a single statement in memory instead of one per row.
///
/// Parameter names are resolved once per batch up front (paired into `groups`); this defers only
/// the per-row [`bind::bind_params`] + `read_sql_builder`-clone, producing the same statement
/// sequence, in the same order, the eager path would have.
struct LazyBoundStatements {
    /// A fully-configured read-only query builder for the SQL (directed reads + request tags +
    /// query optimizer options + retry already applied); cloned once per row before binding.
    base_builder: StatementBuilder,
    /// The resolved column→parameter names paired with each non-empty bound batch, in bind order.
    groups: Vec<(Vec<String>, RecordBatch)>,
    /// Cursor into `groups`.
    group: usize,
    /// Next row to bind within `groups[group].1`.
    row: usize,
}

impl BoundStatementSource for LazyBoundStatements {
    fn next_statement(&mut self) -> Option<Result<SpannerSql>> {
        loop {
            let (names, batch) = self.groups.get(self.group)?;
            if self.row >= batch.num_rows() {
                self.group += 1;
                self.row = 0;
                continue;
            }
            let row = self.row;
            self.row += 1;
            return Some(
                bind::bind_params(self.base_builder.clone(), names, batch, row).map(|b| b.build()),
            );
        }
    }
}

/// An ADBC statement bound to a Spanner [`DatabaseClient`].
#[derive(Debug)]
pub struct SpannerStatement {
    runtime: SharedRuntime,
    client: DatabaseClient,
    spanner: Spanner,
    database: String,
    /// The lazily-built Database Admin client for the DDL path (`run_ddl`, including the
    /// `CREATE TABLE` a create-mode ingest issues), shared (`Arc`) across every connection and
    /// statement minted from the database's cached client stack: the first DDL statement builds it
    /// and later ones clone it (see [`SharedDatabaseAdmin`]).
    admin: SharedDatabaseAdmin,
    /// Every option-settable value on this statement, inherited from the connection at creation
    /// time ([`SharedConfig::inherit`]) and overridable here for the fields the statement also
    /// exposes. See [`SharedConfig`] for the per-field detail — including which values are
    /// connection-set only (the readonly flag, live-shared; the isolation level) and which are
    /// per-object rather than inherited (the commit-stats cell).
    config: SharedConfig,
    txn: SharedTxn,
    sql: Option<String>,
    /// Parameter / bulk-ingest data bound via [`Statement::bind`] or [`Statement::bind_stream`].
    bound: Vec<RecordBatch>,
    /// The Arrow schema declared by a [`Statement::bind_stream`] that yielded **zero** batches (an
    /// empty bulk ingest). Kept *separate* from `bound` — rather than synthesised into it as a
    /// zero-row batch — so an empty stream neither diverts the parameter-binding DML/query paths
    /// (which key off `bound` being non-empty) nor is mistaken for a bound parameter row. Consumed
    /// only by the bulk-ingest paths; cleared whenever `bound` is (re)set.
    ingest_schema: Option<SchemaRef>,
    /// Target table for bulk ingest (`adbc.ingest.target_table`), if set.
    target_table: Option<String>,
    /// Named schema qualifying the ingest target table (`adbc.ingest.target_db_schema`), if set.
    /// `None` (or empty) targets Spanner's default, unnamed schema.
    target_db_schema: Option<String>,
    /// Ingest target catalog (`adbc.ingest.target_catalog`), if set. Spanner has a single, unnamed
    /// (`""`) catalog, so only the empty catalog is accepted; stored solely so the option
    /// round-trips through `get_option`.
    target_catalog: Option<String>,
    /// Ingest mode (`adbc.ingest.mode`), parsed once in `set_option` (which rejects unknown
    /// modes) so the ingest paths match it exhaustively; `get_option` reports the spec's
    /// canonical `adbc.ingest.mode.*` spelling. `create` (the ADBC spec default — unset `None`
    /// resolves to it), `append`, `create_append`, and `replace`; the create/replace modes build
    /// the table from the ingest data's Arrow schema.
    ingest_mode: Option<IngestMode>,
    /// Route an autocommit bulk ingest's per-chunk mutations through Spanner's **BatchWrite** RPC
    /// (`spanner.ingest.batch_write`) instead of a write-only transaction. Ignored in
    /// manual-transaction mode; see [`OPTION_INGEST_BATCH_WRITE`](crate::OPTION_INGEST_BATCH_WRITE).
    ingest_batch_write: bool,
    /// Run the statement's DML as **Partitioned DML** (`spanner.dml.partitioned`, boolean, default
    /// `false`) instead of in a read/write transaction — non-atomic, idempotence-requiring, and
    /// free of the per-commit mutation limit. See [`run_partitioned_dml`](Self::run_partitioned_dml)
    /// and [`OPTION_DML_PARTITIONED`](crate::OPTION_DML_PARTITIONED).
    dml_partitioned: bool,
    /// How bound columns pair with the query's `@name` parameters
    /// (`adbc.statement.bind_by_name`): `false` (the default) binds positionally, `true` forces
    /// strict by-name. See [`bind::resolve_parameter_names`].
    bind_by_name: bool,
    /// Rows converted into each streamed Arrow batch by `execute` (`spanner.rows_per_batch`).
    rows_per_batch: usize,
    /// Enable Data Boost for partitioned execution (`spanner.data_boost`).
    data_boost: bool,
    /// Per-operation cancellation for this statement (see [`Statement::get_cancel_handle`]): each
    /// execution entry point mints a fresh [`crate::runtime::CancelSignal`] here, and a cancel
    /// latches the current one forever, so a cancelled streamed reader stays cancelled. Shared
    /// through an [`Arc`] so the [`SlotCancelHandle`]s handed out by `get_cancel_handle` keep
    /// targeting the *current* operation.
    cancel: Arc<CancelSlot>,
}

impl SpannerStatement {
    // Shared `set_shared_option` / `shared_option_string` for the "staleness-pattern" options
    // (request priority/tag, directed read, max_commit_delay, commit_stats, query optimizer opts,
    // RPC timeouts, retry tuning, …) that the statement and connection dispatch identically.
    impl_shared_option_dispatch!();

    /// `config` is the connection's [`SharedConfig::inherit`]ed configuration; everything else is a
    /// handle cloned from the connection's client stack.
    pub(crate) fn new(
        runtime: SharedRuntime,
        client: DatabaseClient,
        spanner: Spanner,
        database: String,
        admin: SharedDatabaseAdmin,
        config: SharedConfig,
        txn: SharedTxn,
    ) -> Self {
        Self {
            runtime,
            client,
            spanner,
            database,
            admin,
            config,
            txn,
            sql: None,
            bound: Vec::new(),
            ingest_schema: None,
            target_table: None,
            target_db_schema: None,
            target_catalog: None,
            ingest_mode: None,
            ingest_batch_write: false,
            dml_partitioned: false,
            bind_by_name: false,
            rows_per_batch: DEFAULT_ROWS_PER_BATCH,
            data_boost: false,
            cancel: Arc::new(CancelSlot::new()),
        }
    }

    /// A Spanner statement builder for `sql` with this statement's request priority / request tag
    /// (`spanner.request.priority` / `spanner.request.tag`), query optimizer options
    /// (`spanner.query.optimizer_version` / `spanner.query.optimizer_statistics_package`) and retry
    /// policy (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`) applied. Every
    /// query/DML statement the driver builds goes through here so the options apply uniformly.
    fn sql_builder(&self, sql: &str) -> StatementBuilder {
        self.config.retry.apply_to_statement(
            self.config.query_options.apply_to_statement(
                self.config
                    .request
                    .apply_to_statement(SpannerSql::builder(sql)),
            ),
        )
    }

    /// A Spanner statement builder for a **read-only query** `sql`: [`sql_builder`](Self::sql_builder)
    /// plus this statement's directed-read replica selection (`spanner.directed_read`). Used only on
    /// the read-only query paths — Spanner rejects directed reads on a read/write transaction, so the
    /// DML paths keep using [`sql_builder`](Self::sql_builder) directly.
    fn read_sql_builder(&self, sql: &str) -> StatementBuilder {
        self.config
            .directed_read
            .apply_to_statement(self.sql_builder(sql))
    }

    /// Build one Spanner statement per bound row for the **DML** (`THEN RETURN` / `ExecuteBatchDml`)
    /// path, binding each row's columns as named parameters. The builders use
    /// [`sql_builder`](Self::sql_builder) (not [`read_sql_builder`](Self::read_sql_builder)) so the
    /// directed-read replica selection never reaches a read/write transaction, which Spanner
    /// rejects. The read-only bound-query path builds its statements lazily instead (see
    /// [`execute_bound_query`](Self::execute_bound_query)), one at a time, rather than materialising
    /// the whole `Vec` up front.
    fn build_bound_statements(&self, sql: &str) -> Result<Vec<SpannerSql>> {
        let mut statements = Vec::new();
        for batch in &self.bound {
            if batch.num_rows() == 0 {
                continue;
            }
            // Resolve the column→parameter mapping once per batch (it lexes `sql`), then reuse it
            // for every row instead of re-lexing the SQL per bound row.
            let names = bind::resolve_parameter_names(sql, batch, self.bind_by_name)?;
            for row in 0..batch.num_rows() {
                statements
                    .push(bind::bind_params(self.sql_builder(sql), &names, batch, row)?.build());
            }
        }
        Ok(statements)
    }

    /// The Arrow schema of the bound ingest data: the first bound batch's schema, or — when a bound
    /// stream yielded zero batches — the schema that stream declared ([`ingest_schema`](Self)). Used
    /// only by the bulk-ingest paths, so an empty ingest can still build its target table.
    fn bound_ingest_schema(&self) -> Option<SchemaRef> {
        self.bound
            .first()
            .map(RecordBatch::schema)
            .or_else(|| self.ingest_schema.clone())
    }

    /// Discard all bound data, resetting **both** `bound` and its companion
    /// [`ingest_schema`](Self) together so they can never desync. Every execution path that consumes
    /// bound data calls this — a reused statement handle must not silently re-apply stale bound rows
    /// or a stale empty-stream ingest schema. The `set_sql_query` / ingest-option setters
    /// deliberately leave bound data intact, since binding may precede setting the destination.
    fn clear_bound(&mut self) {
        self.bound.clear();
        self.ingest_schema = None;
    }

    /// Build the DML statements to apply for `sql`: one per bound row for parameterized DML,
    /// otherwise a `;`-separated batch (e.g. dbt's `DELETE; INSERT`) split into individual
    /// statements so the whole batch is applied atomically. Shared by `execute` and `execute_update`.
    fn build_dml_statements(&self, sql: &str) -> Result<Vec<SpannerSql>> {
        if !self.bound.is_empty() {
            return self.build_bound_statements(sql);
        }
        let statements = crate::sql::split_statements(sql);
        // The batch is applied via `ExecuteBatchDml`, which executes DML only — reject a batch
        // mixing in a query or DDL up front (see `check_all_dml_batch`), crucially *before* any
        // statement is buffered in a manual transaction.
        check_all_dml_batch(&statements)?;
        Ok(statements
            .into_iter()
            .map(|s| self.sql_builder(&s).build())
            .collect())
    }

    /// Run a `QueryMode::Plan` probe of `sql` and return its result schema without scanning any
    /// rows. Binds parameter values from the first bound batch when it has rows (the values are
    /// irrelevant to the schema, but they let `@param` references resolve). Shared by
    /// [`Statement::execute_schema`] and the zero-row arm of
    /// [`execute_bound_query`](Self::execute_bound_query), so both advertise the query's real schema.
    fn plan_query_schema(&self, sql: &str) -> Result<SchemaRef> {
        let client = self.client.clone();
        let bound = self.bound.clone();
        let bind_by_name = self.bind_by_name;
        let sql = sql.to_string();
        // The PLAN probe runs in a single-use read-only transaction, so honour the statement's read
        // staleness like `execute` and `execute_partitions`' probe do — schema is itself versioned,
        // so a stale bound must observe the structure as of that timestamp, matching the data read.
        let read_bound = self.config.read_staleness.timestamp_bound()?;
        // The PLAN probe's schema must carry the same timestamp unit as the data `execute` would
        // stream, so the advertised schema and the actual batches can never disagree.
        let precision = self.config.timestamp_precision;
        // QueryMode::Plan analyses the query and returns its column metadata without scanning
        // any data, so dbt can introspect a model's output columns without wrapping it in a
        // `SELECT ... WHERE false` subquery.
        let plan_builder = self.read_sql_builder(&sql).set_query_mode(QueryMode::Plan);
        // The schema probe is a query execution, so the query timeout bounds it.
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = crate::staleness::single_use(&client, read_bound);
                    let mut builder = plan_builder;
                    // Bind parameters if any were provided (values are irrelevant to the schema) so
                    // that `@param` references resolve.
                    if let Some(batch) = bound.first()
                        && batch.num_rows() > 0
                    {
                        let names = bind::resolve_parameter_names(&sql, batch, bind_by_name)?;
                        builder = bind::bind_params(builder, &names, batch, 0)?;
                    }
                    let result_set = transaction
                        .execute_query(builder.build())
                        .await
                        .map_err(from_spanner)?;
                    let (schema, _batch) = result_set_to_batch(result_set, precision).await?;
                    Ok::<SchemaRef, Error>(schema)
                },
            ),
        )
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
    /// `None` is returned (the count is unknown until commit). Bulk ingest goes through
    /// [`run_ingest_mutations`](Self::run_ingest_mutations) instead, which ships mutations and
    /// chunks the autocommit path under Spanner's commit limits; user statements are never
    /// chunked.
    fn run_or_buffer(&self, statements: Vec<SpannerSql>) -> Result<Option<i64>> {
        {
            let mut txn = lock_txn(&self.txn);
            if !txn.autocommit() {
                // Fixes the transaction's kind to DML — a transaction that began with a query
                // rejects the buffer (kinds cannot mix).
                txn.buffer_dml(statements)?;
                return Ok(None);
            }
        }
        let count = crate::connection::run_batch_dml(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            statements,
        )?;
        Ok(Some(count))
    }

    /// Run DML with a `THEN RETURN` clause: one read/write transaction executes every statement
    /// via `ExecuteSql` (not `ExecuteBatchDml`, which rejects `THEN RETURN`), draining each result
    /// set **before** commit, as Spanner requires for returned rows.
    ///
    /// Returns the concatenated result batches (schema from the first) and the total
    /// affected-row count from the result-set stats. The rows are drained *inside* the runner's
    /// closure keeping the client's own error type, so a transaction abort still retries — the
    /// (cloned) statement list is replayed and only the last attempt's rows are returned.
    fn execute_returning_dml(
        &self,
        statements: Vec<SpannerSql>,
    ) -> Result<(Vec<RecordBatch>, SchemaRef, i64)> {
        let client = self.client.clone();
        let isolation = self.config.isolation.clone();
        let request = self.config.request.clone();
        let retry = self.config.retry;
        // DML with THEN RETURN is a write path: the update timeout bounds the whole transaction.
        let update_timeout = self.config.timeouts.update_timeout();
        let transaction = async move {
            let runner = build_runner(&client, isolation, &request, retry).await?;
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
            // The commit stats (only when `spanner.commit_stats` requested them) ride on the commit
            // response of this THEN RETURN read/write transaction.
            let mutation_count = outcome
                .commit_response
                .commit_stats
                .as_ref()
                .map(|stats| stats.mutation_count);
            Ok::<_, Error>((outcome.result, mutation_count))
        };
        let (results, mutation_count) = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                update_timeout,
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                transaction,
            ),
        )?;
        self.config.commit_stats.record(mutation_count);

        let mut schema = None;
        let mut batches = Vec::with_capacity(results.len());
        let mut affected = 0i64;
        for (metadata, rows, count) in &results {
            let (sch, batch) = crate::conversion::rows_to_batch(
                metadata.as_ref(),
                rows,
                self.config.timestamp_precision,
            )?;
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
    ///
    /// The `adbc.connection.readonly` guard lives here rather than at the two entry points, so the
    /// one rejection covers every DML route — plain, `THEN RETURN` and partitioned alike.
    fn run_dml(&self, sql: &str) -> Result<DmlOutcome> {
        if self.config.is_read_only() {
            return Err(invalid_state(
                "cannot execute DML: the connection is read-only",
            ));
        }
        if self.dml_partitioned {
            return Ok(DmlOutcome::Plain(Some(self.run_partitioned_dml(sql)?)));
        }
        if !crate::sql::is_dml_returning(sql) {
            let statements = self.build_dml_statements(sql)?;
            return Ok(DmlOutcome::Plain(self.run_or_buffer(statements)?));
        }
        if !lock_txn(&self.txn).autocommit() {
            return Err(invalid_state(
                "DML with THEN RETURN cannot run in a manual transaction: buffered DML is applied \
                 via ExecuteBatchDml on commit, which does not support THEN RETURN. Re-enable \
                 autocommit to run it",
            ));
        }
        let statements = if self.bound.is_empty() {
            let parts = crate::sql::split_statements(sql);
            if parts.len() > 1 {
                return Err(not_implemented(
                    "THEN RETURN in a multi-statement (`;`-separated) DML batch",
                ));
            }
            parts
                .into_iter()
                .map(|s| self.sql_builder(&s).build())
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

    /// Run one DML statement as **Partitioned DML** (`spanner.dml.partitioned`), returning the
    /// lower-bound affected-row count Spanner reports.
    ///
    /// The guarantees it trades away are documented on
    /// [`OPTION_DML_PARTITIONED`](crate::OPTION_DML_PARTITIONED); the statement shapes it cannot
    /// express are rejected up front by [`check_partitioned_dml`] rather than left to a
    /// server-side error. The statement is built by [`sql_builder`](Self::sql_builder), so the
    /// request priority/tag, query optimizer options and retry/backoff policies ride along; the
    /// transaction-level knobs the builder has no setter for — the transaction tag, the commit
    /// options and the isolation level — do not apply.
    fn run_partitioned_dml(&self, sql: &str) -> Result<i64> {
        let bound_rows = self.bound.iter().map(RecordBatch::num_rows).sum();
        check_partitioned_dml(sql, lock_txn(&self.txn).autocommit(), bound_rows)?;
        let mut statements = self.build_dml_statements(sql)?;
        // A bound stream of zero rows has nothing to execute, exactly as on the batch-DML path.
        let Some(statement) = statements.pop() else {
            return Ok(0);
        };
        // `RequestConfig` exposes the flag only as its round-trip string.
        let exclude_from_change_streams =
            self.config.request.exclude_txn_from_change_streams_string() == "true";
        let client = self.client.clone();
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async move {
                    let transaction = client
                        .partitioned_dml_transaction()
                        .with_exclude_txn_from_change_streams(exclude_from_change_streams)
                        .build()
                        .await
                        .map_err(from_spanner)?;
                    transaction
                        .execute_update(statement)
                        .await
                        .map_err(from_spanner)
                },
            ),
        )
    }

    /// Run a parameterized query once per bound row, streaming the concatenated results.
    ///
    /// Every bound row executes in **one** read-only snapshot, so the per-row results are mutually
    /// consistent: a single bound row keeps the cheap single-use transaction, while several bound
    /// rows share one multi-use read-only transaction pinned at the statement's read bound via
    /// [`ReadStaleness::multi_use_timestamp_bound`](crate::staleness::ReadStaleness::multi_use_timestamp_bound).
    /// Results stream through the same bounded-chunk machinery as `execute`.
    fn execute_bound_query(
        &self,
        sql: &str,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // Resolve the column→parameter mapping once per non-empty bound batch (it lexes `sql`), so
        // a structural mismatch still fails before any statement runs. Only the per-row
        // `bind::bind_params` is deferred to the reader, so at most one `SpannerSql` resides in
        // memory at a time rather than one per bound row.
        let mut groups: Vec<(Vec<String>, RecordBatch)> = Vec::new();
        let mut total_rows = 0usize;
        for batch in &self.bound {
            if batch.num_rows() == 0 {
                continue;
            }
            let names = bind::resolve_parameter_names(sql, batch, self.bind_by_name)?;
            total_rows += batch.num_rows();
            groups.push((names, batch.clone()));
        }
        // One fully-configured read-only query builder, cloned per row before binding — the shared
        // config is applied once, not re-resolved per row.
        let base_builder = self.read_sql_builder(sql);
        // In a manual transaction every bound row runs on the transaction's shared snapshot
        // (opening it if this query is the transaction's first statement).
        let manual_txn = self.manual_read_transaction()?;
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.current();
        let batch_size = self.rows_per_batch;
        let precision = self.config.timestamp_precision;
        // The query timeout bounds the initial execution (through the first chunk); the fetch
        // timeout bounds each later chunk as the reader is iterated.
        let query_timeout = self.config.timeouts.query_timeout();
        let fetch_timeout = self.config.timeouts.fetch_timeout();
        if total_rows <= 1 {
            // Zero or one bound row. One statement is one snapshot already, and (in autocommit
            // mode) the single-use transaction keeps the exact semantics of the bounded-staleness
            // kinds.
            let Some((names, batch)) = groups.first() else {
                // Zero total bound rows (e.g. a DBAPI `executemany` with an empty parameter set):
                // there is nothing to run, but returning an empty schema would disagree with every
                // non-empty execution. Advertise the query's real schema via the PLAN probe and
                // return a zero-row reader.
                let schema = self.plan_query_schema(sql)?;
                let empty: Vec<std::result::Result<RecordBatch, ArrowError>> = Vec::new();
                return Ok(Box::new(RecordBatchIterator::new(empty, schema)));
            };
            let statement = bind::bind_params(base_builder, names, batch, 0)?.build();
            let bound = self.config.read_staleness.timestamp_bound()?;
            let reader = block_on_cancellable(
                &self.runtime,
                &self.cancel.current(),
                with_timeout(query_timeout, crate::OPTION_RPC_TIMEOUT_QUERY, async move {
                    let result_set = match manual_txn {
                        Some(txn) => txn.execute_query(statement).await.map_err(from_spanner)?,
                        None => crate::staleness::single_use(&client, bound)
                            .execute_query(statement)
                            .await
                            .map_err(from_spanner)?,
                    };
                    stream_query(
                        runtime,
                        cancel,
                        result_set,
                        batch_size,
                        precision,
                        fetch_timeout,
                    )
                    .await
                }),
            )?;
            return Ok(Box::new(reader));
        }
        let bound = self.config.read_staleness.multi_use_timestamp_bound()?;
        let statements: Box<dyn BoundStatementSource> = Box::new(LazyBoundStatements {
            base_builder,
            groups,
            group: 0,
            row: 0,
        });
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(query_timeout, crate::OPTION_RPC_TIMEOUT_QUERY, async move {
                let transaction = match manual_txn {
                    // Manual transaction: the shared snapshot, already pinned at the
                    // transaction's read bound.
                    Some(txn) => txn,
                    // Autocommit: a dedicated multi-use read-only transaction at this
                    // statement's (multi-use-pinned) bound, dropped when the reader is.
                    None => Arc::new(crate::staleness::multi_use(&client, bound).await?),
                };
                stream_bound_query(
                    runtime,
                    cancel,
                    transaction,
                    statements,
                    batch_size,
                    precision,
                    fetch_timeout,
                )
                .await
            }),
        )?;
        Ok(Box::new(reader))
    }

    /// Execute `sql` as a **read-only query** and return its streaming reader — the shared query
    /// tail of [`Statement::execute`] and the query-shaped arm of [`Statement::execute_update`],
    /// so both entry points get identical read semantics (staleness, directed reads, query
    /// optimizer options and timeouts included).
    ///
    /// Strips any trailing statement terminator(s) — Spanner's single-use query API rejects a
    /// trailing `;` ("Expected end of input but got `;`"), yet clients and conformance suites
    /// routinely append one; the stripping is scoped to the query path so it never splits a
    /// `;`-batch. Applies the manual-transaction kind guard
    /// ([`ensure_query_allowed`](Self::ensure_query_allowed)), and dispatches to the bound-query
    /// path (consuming the bound rows) when parameter rows are bound.
    ///
    /// In a manual transaction the query runs on the transaction's shared multi-use read-only
    /// transaction ([`manual_read_transaction`](Self::manual_read_transaction)), opening it if this
    /// is the transaction's first statement; in autocommit mode it runs in its own single-use
    /// transaction at this statement's read bound.
    fn execute_query_reader(
        &mut self,
        sql: &str,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let sql = crate::sql::strip_trailing_terminators(sql);
        // Reject a data-returning query (plain or the parameterized bound-query path below) issued
        // in a manual transaction that began with DML — the buffered work only executes at
        // commit, so the query would not observe it (no read-your-writes).
        self.ensure_query_allowed()?;
        // Parameterized query: run once per bound row. The attempt consumes the bound rows on
        // every exit path — following the DML/ingest/partition convention (see `clear_bound`), a
        // failed bound query must not leave stale rows for a later, unrelated `execute`.
        if !self.bound.is_empty() {
            let result = self.execute_bound_query(&sql);
            self.clear_bound();
            return result;
        }
        let manual_txn = self.manual_read_transaction()?;
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.current();
        let batch_size = self.rows_per_batch;
        let precision = self.config.timestamp_precision;
        let bound = self.config.read_staleness.timestamp_bound()?;
        let statement = self.read_sql_builder(&sql).build();
        let fetch_timeout = self.config.timeouts.fetch_timeout();
        // `stream_query` fetches the first chunk (settling the schema); the returned reader
        // converts the rest one bounded chunk at a time. The query timeout bounds the initial
        // execution through that first chunk, the fetch timeout each later one.
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let result_set = match manual_txn {
                        // Manual transaction: the shared snapshot (already pinned at the
                        // transaction's read bound; `bound` is not re-applied).
                        Some(txn) => txn.execute_query(statement).await.map_err(from_spanner)?,
                        // Autocommit: a fresh single-use transaction at this statement's bound.
                        None => crate::staleness::single_use(&client, bound)
                            .execute_query(statement)
                            .await
                            .map_err(from_spanner)?,
                    };
                    stream_query(
                        runtime,
                        cancel,
                        result_set,
                        batch_size,
                        precision,
                        fetch_timeout,
                    )
                    .await
                },
            ),
        )?;
        Ok(Box::new(reader))
    }

    /// Apply one or more DDL statements as a single Spanner `UpdateDatabaseDdl` schema change.
    ///
    /// Batching all statements into one call makes a multi-step change (for example dbt's
    /// intermediate-table build followed by a rename swap) near-atomic. DDL always runs
    /// **immediately, regardless of the connection's transaction mode** — Spanner DDL goes through
    /// the admin API and is never transactional, so it neither fixes a manual transaction's kind
    /// nor is rejected by it, and it cannot be rolled back.
    ///
    /// The update timeout bounds the whole change — the admin-client build, the
    /// `UpdateDatabaseDdl` call, **and** its long-running operation poll loop, which otherwise
    /// polls without any bound.
    fn run_ddl(&self, statements: Vec<String>) -> Result<()> {
        if self.config.is_read_only() {
            return Err(invalid_state(
                "cannot execute DDL: the connection is read-only",
            ));
        }
        let spanner = self.spanner.clone();
        let admin_cell = self.admin.clone();
        let database = self.database.clone();
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async move {
                    // Build the Database Admin client once per cached client stack and reuse it:
                    // like the data-plane client, it holds its connection pool behind an `Arc`, so
                    // rebuilding it per DDL statement would redo the endpoint/credential setup for
                    // nothing. A failed build stays uncached and the next DDL retries it.
                    let admin = admin_cell
                        .get_or_try_init(|| async {
                            spanner
                                .database_admin_builder()
                                .build()
                                .await
                                .map_err(from_builder)
                        })
                        .await?;
                    admin
                        .update_database_ddl()
                        .set_database(database)
                        .set_statements(statements)
                        .poller()
                        .until_done()
                        .await
                        .map_err(from_spanner)?;
                    Ok::<(), Error>(())
                },
            ),
        )
    }

    fn sql(&self) -> Result<String> {
        self.sql
            .clone()
            .ok_or_else(|| invalid_state("no SQL query set on statement; call set_sql_query first"))
    }

    /// Build a Spanner query statement for `sql`, binding the single bound parameter row (if any)
    /// as named parameters. With `plan = true` the statement is set to `QueryMode::Plan` so it
    /// returns column metadata without scanning data. Used by `execute_partitions` for both the
    /// schema probe and the partitioned query itself; its caller has already rejected more than one
    /// bound row ([`check_single_bound_row`](Self::check_single_bound_row) — partitioned execution
    /// has no per-row fan-out), so the row bound here is *the* bound row, wherever it sits in the
    /// bound batches.
    fn build_query_statement(&self, sql: &str, plan: bool) -> Result<SpannerSql> {
        let mut builder = self.read_sql_builder(sql);
        if plan {
            builder = builder.set_query_mode(QueryMode::Plan);
        }
        if let Some(batch) = self.bound.iter().find(|batch| batch.num_rows() > 0) {
            let names = bind::resolve_parameter_names(sql, batch, self.bind_by_name)?;
            builder = bind::bind_params(builder, &names, batch, 0)?;
        }
        Ok(builder.build())
    }

    /// Guard for `execute_partitions`: at most **one** bound parameter row. Partitioned execution
    /// has no per-row fan-out (each bound row would need its own partitioned query, and ADBC's
    /// partition surface cannot attribute descriptors back to rows), so the ambiguous case is
    /// rejected up front, before any RPC, rather than silently truncated to the first row.
    fn check_single_bound_row(&self) -> Result<()> {
        let rows: usize = self.bound.iter().map(RecordBatch::num_rows).sum();
        if rows > 1 {
            return Err(invalid_argument(format!(
                "execute_partitions supports at most one bound parameter row, but {rows} rows \
                 are bound: partitioned execution has no per-row fan-out; bind a single row per \
                 execute_partitions call"
            )));
        }
        Ok(())
    }

    /// The body of [`execute_partitions`](Statement::execute_partitions) from the bound-data
    /// guard onward, split out so its caller clears the bound data however the attempt ends (the
    /// [`run_ingest`](Self::run_ingest) / DML-path convention).
    fn run_partition_query(&self, sql: &str) -> Result<PartitionedResult> {
        // Several bound rows cannot be partitioned (no per-row fan-out) — reject before any RPC.
        self.check_single_bound_row()?;
        // Probe the schema and create the partitions in a batch read-only transaction. Each
        // partition carries its session, transaction id and partition token and is independently
        // serializable, so it maps directly onto ADBC's opaque descriptor. The (Arc-shared,
        // multiplexed) session lives as long as the connection's `DatabaseClient`, so descriptors
        // stay valid after this statement is gone.
        let plan_stmt = self.build_query_statement(sql, true)?;
        let query_stmt = self.build_query_statement(sql, false)?;
        let client = self.client.clone();
        let data_boost = self.data_boost;
        // The advertised schema carries this statement's timestamp precision. Note the partitions
        // themselves are decoded by `Connection::read_partition` under the **reading** connection's
        // `spanner.max_timestamp_precision`, so set the two to the same mode.
        let precision = self.config.timestamp_precision;
        // The partitioned read honours the statement's read staleness: it is baked into the batch
        // read-only transaction, so every partition executes at that bound wherever it is read back.
        let bound = self.config.read_staleness.timestamp_bound()?;

        // Partitioning is a query-side operation: the query timeout bounds the schema probe plus
        // the PartitionQuery call.
        let partition_op = async move {
            // Schema via a PLAN of the query: column metadata without scanning any data.
            let plan_rs = crate::staleness::single_use(&client, bound.clone())
                .execute_query(plan_stmt)
                .await
                .map_err(from_spanner)?;
            let (schema, _batch) = result_set_to_batch(plan_rs, precision).await?;

            // Partition the query across a batch read-only transaction.
            let mut txn_builder = client.batch_read_only_transaction();
            if let Some(b) = bound {
                txn_builder = txn_builder.set_timestamp_bound(b);
            }
            let transaction = txn_builder.build().await.map_err(from_spanner)?;
            let partitions = transaction
                .partition_query(query_stmt, PartitionOptions::default())
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
                tokens.push(crate::connection::encode_partition(&partition)?);
            }
            Ok::<_, Error>((schema, tokens))
        };
        let (schema, partitions) = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                partition_op,
            ),
        )?;

        Ok(PartitionedResult {
            partitions,
            schema: (*schema).clone(),
            // A read query has no affected-row count; ADBC uses -1 for "unknown".
            rows_affected: -1,
        })
    }

    /// Ask Spanner to type this statement's `@name` parameters: a `QueryMode::Plan` probe of the
    /// SQL returns the statement's *undeclared parameters* — every parameter the request itself
    /// did not declare, i.e. all of them here — with the type the surrounding SQL implies (e.g.
    /// `INT64` for a parameter compared against an `INT64` column). Returns the name → type map;
    /// a parameter whose type Spanner cannot pin down is simply absent from it.
    ///
    /// Queries plan in a single-use read-only transaction. DML can only be planned inside a
    /// read/write transaction (Spanner rejects it read-only), so it runs through the transaction
    /// runner: the plan executes nothing and the transaction commits empty. On a read-only
    /// connection the DML probe is skipped, and DDL is never planned (not plannable over
    /// `ExecuteSql`, and Spanner DDL takes no query parameters) — both return an empty map, typing
    /// every parameter `Null`. Either probe is bounded by the *query* timeout: introspection is a
    /// read-shaped operation regardless of the statement's verb.
    fn plan_parameter_types(
        &self,
        sql: &str,
    ) -> Result<BTreeMap<String, google_cloud_spanner::value::Type>> {
        // The probe runs through the same ExecuteSql surface as `execute`, which rejects a
        // trailing `;`, yet introspection callers routinely append one.
        let sql = crate::sql::strip_trailing_terminators(sql);
        if crate::sql::is_ddl(&sql) {
            return Ok(BTreeMap::new());
        }
        // Mint a fresh cancel signal for this operation (see `CancelSlot`).
        self.cancel.begin_operation();
        if crate::sql::is_dml(&sql) {
            if self.config.is_read_only() {
                return Ok(BTreeMap::new());
            }
            return self.plan_dml_parameter_types(&sql);
        }
        let plan_builder = self.read_sql_builder(&sql).set_query_mode(QueryMode::Plan);
        let client = self.client.clone();
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = client.single_use().build();
                    let result_set = transaction
                        .execute_query(plan_builder.build())
                        .await
                        .map_err(from_spanner)?;
                    Ok(undeclared_parameter_types(result_set.metadata()))
                },
            ),
        )
    }

    /// The DML arm of [`plan_parameter_types`](Self::plan_parameter_types): plan the statement in
    /// a read/write transaction runner (the same builder chain as `execute_returning_dml`, minus
    /// commit stats — an empty commit has none worth recording). The closure keeps the client's
    /// own error type so a transaction abort still retries the plan.
    fn plan_dml_parameter_types(
        &self,
        sql: &str,
    ) -> Result<BTreeMap<String, google_cloud_spanner::value::Type>> {
        let plan_stmt = self
            .sql_builder(sql)
            .set_query_mode(QueryMode::Plan)
            .build();
        let client = self.client.clone();
        let isolation = self.config.isolation.clone();
        let request = self.config.request.clone();
        let retry = self.config.retry;
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let runner = build_runner(&client, isolation, &request, retry).await?;
                    let outcome = runner
                        .run(move |transaction: ReadWriteTransaction| {
                            let statement = plan_stmt.clone();
                            async move {
                                let result_set = transaction.execute_query(statement).await?;
                                Ok(undeclared_parameter_types(result_set.metadata()))
                            }
                        })
                        .await
                        .map_err(from_spanner)?;
                    Ok(outcome.result)
                },
            ),
        )
    }

    /// Guard for [`execute`](Statement::execute) / [`execute_update`](Statement::execute_update)
    /// when data has been bound but there is nothing to apply it to — neither a SQL query nor a
    /// bulk-ingest target. Binding *before* setting `adbc.ingest.target_table` is legal, so this
    /// can only be diagnosed at execution time — and the message names both remedies, instead of
    /// the plain "no SQL query set" error that would hide the missing ingest option.
    fn check_bound_has_destination(&self) -> Result<()> {
        if self.sql.is_none()
            && self.target_table.is_none()
            && (!self.bound.is_empty() || self.ingest_schema.is_some())
        {
            return Err(invalid_state(
                "data has been bound but no SQL query or bulk-ingest target is set; call \
                 set_sql_query or set the adbc.ingest.target_table option before executing",
            ));
        }
        Ok(())
    }

    /// Guard the data-returning read paths against kind-mixing in a manual transaction.
    ///
    /// In manual mode the transaction's kind is fixed by its first statement: buffered DML (and
    /// bulk-ingest mutations) only executes at `commit`, so a query could never observe it. Rather
    /// than silently returning a pre-write result (an `INSERT` followed by `SELECT COUNT(*)`
    /// reporting the *old* count), reject the query up front. Queries in an unset or query-kind
    /// manual transaction, and every query in autocommit mode, pass. DDL is not transaction-aware
    /// (unguarded), and `execute_schema` (a `QueryMode::Plan` probe returning no data) has no
    /// data-visibility concern.
    fn ensure_query_allowed(&self) -> Result<()> {
        lock_txn(&self.txn).check_kind_allowed(TxnKind::Read)
    }

    /// The shared multi-use read-only transaction of a manual transaction that began (or begins
    /// now) with a query — `None` in autocommit mode, where each query runs in its own single-use
    /// transaction.
    ///
    /// The first data-returning query of a manual transaction builds the transaction — pinned at
    /// this statement's read bound via
    /// [`ReadStaleness::multi_use_timestamp_bound`](crate::staleness::ReadStaleness::multi_use_timestamp_bound)
    /// — and installs it in the shared [`TxnState`], fixing the transaction's kind to queries;
    /// every later query returns the installed handle, so all reads observe one consistent
    /// snapshot, and later statements' staleness settings are ignored. Building issues no RPC (the
    /// client's default inline begin folds `BeginTransaction` into the first query), so a
    /// transaction is never begun for a query that then fails.
    ///
    /// [`TxnState`]: crate::connection::TxnState
    fn manual_read_transaction(&self) -> Result<Option<Arc<MultiUseReadOnlyTransaction>>> {
        {
            let st = lock_txn(&self.txn);
            if st.autocommit() {
                return Ok(None);
            }
            if let Some(txn) = st.read_txn() {
                return Ok(Some(txn));
            }
        }
        let bound = self.config.read_staleness.multi_use_timestamp_bound()?;
        let client = self.client.clone();
        let built = block_on_cancellable(&self.runtime, &self.cancel.current(), async move {
            crate::staleness::multi_use(&client, bound).await
        })?;
        let mut st = lock_txn(&self.txn);
        if st.autocommit() {
            // The mode flipped to autocommit while the transaction was being built (which issued
            // no RPC): drop it and run the query as plain autocommit.
            return Ok(None);
        }
        // Install under the lock, re-checking the kind: a concurrent statement may have fixed the
        // transaction to DML/DDL (rejected here) or installed its own read transaction (returned
        // instead) in the unlocked window.
        st.start_read_txn(Arc::new(built)).map(Some)
    }
}

impl Optionable for SpannerStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionStatement::TargetTable => {
                self.target_table = Some(string_option(&key, value)?);
                // Mutually exclusive with a SQL query (see `set_sql_query`): setting an ingest
                // target clears any query left on a reused handle — e.g. the DBAPI `Cursor` reuses
                // one statement, so `cur.execute("CREATE TABLE …")` then `cur.adbc_ingest(…)` would
                // otherwise leave the stale CREATE set and skip the ingest.
                self.sql = None;
            }
            OptionStatement::TargetDbSchema => {
                // Named schema for the ingest target table; qualifies the INSERT / CREATE TABLE via
                // `qualified_table` (empty selects Spanner's default, unnamed schema).
                self.target_db_schema = Some(string_option(&key, value)?);
            }
            OptionStatement::TargetCatalog => {
                // Spanner exposes a single, unnamed catalog, so only the empty catalog is accepted.
                self.target_catalog = Some(check_target_catalog(string_option(&key, value)?)?);
            }
            OptionStatement::Temporary => {
                // Spanner has no temporary tables. The spec default (`false`) is accepted as a
                // no-op so generic clients that always set the option keep working; `true` is
                // rejected as unsupported.
                check_unsupported_true(
                    value,
                    "option adbc.ingest.temporary",
                    "setting adbc.ingest.temporary to true: Spanner has no temporary tables; \
                     leave it unset or false",
                )?;
            }
            OptionStatement::Incremental => {
                // Incremental `execute_partitions` is not implemented. The spec default
                // (`false`) is accepted as a no-op so generic clients that always set the option
                // keep working; `true` is rejected as unsupported (the `Temporary` pattern).
                check_unsupported_true(
                    value,
                    "option adbc.statement.exec.incremental",
                    "setting adbc.statement.exec.incremental to true: incremental \
                     execute_partitions is not implemented; leave it unset or false",
                )?;
            }
            OptionStatement::IngestMode => {
                // Append into an existing table, or create it (keyless, from the ingest data's
                // Arrow schema) in the create/replace modes.
                self.ingest_mode = Some(ingest_mode_option(&key, value)?);
            }
            OptionStatement::Other(k) if k == crate::OPTION_INGEST_BATCH_WRITE => {
                self.ingest_batch_write = ingest_batch_write_option(value)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_DML_PARTITIONED => {
                self.dml_partitioned = dml_partitioned_option(value)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_BIND_BY_NAME => {
                self.bind_by_name =
                    crate::options::bool_option(value, "option adbc.statement.bind_by_name")?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_ROWS_PER_BATCH => {
                self.rows_per_batch = rows_per_batch_option(value)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_DATA_BOOST => {
                self.data_boost = bool_option(value, "option spanner.data_boost")?;
            }
            // Every remaining `spanner.*` option the statement and connection dispatch identically
            // goes through the shared table. An unrecognised key returns `None`, mapped to
            // `NotImplemented` (so the connection-only `spanner.transaction.tag`, absent from the
            // shared table, stays unsupported here).
            OptionStatement::Other(k) => {
                if self.set_shared_option(k, value)?.is_none() {
                    return Err(unknown_option("statement", k));
                }
            }
            other => {
                return Err(unknown_option("statement", other.as_ref()));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let value = match &key {
            OptionStatement::TargetTable => self.target_table.clone(),
            OptionStatement::TargetDbSchema => self.target_db_schema.clone(),
            OptionStatement::TargetCatalog => self.target_catalog.clone(),
            // Only the spec default (`false`) is ever accepted (see `check_unsupported_true`), so
            // the driver's state is always `false` — report exactly that.
            OptionStatement::Temporary => Some(false.to_string()),
            // Same shape: only `false` is ever accepted (see `check_unsupported_true`).
            OptionStatement::Incremental => Some(false.to_string()),
            // Reported in the spec's canonical `adbc.ingest.mode.*` spelling; unset reports the
            // effective default, `create`.
            OptionStatement::IngestMode => {
                Some(String::from(self.ingest_mode.unwrap_or(IngestMode::Create)))
            }
            // A plain boolean; reports "true"/"false" (the default is "false", write-only txn).
            OptionStatement::Other(k) if k == crate::OPTION_INGEST_BATCH_WRITE => {
                Some(self.ingest_batch_write.to_string())
            }
            // A plain boolean; reports "true"/"false" (the default is "false", read/write txn).
            OptionStatement::Other(k) if k == crate::OPTION_DML_PARTITIONED => {
                Some(self.dml_partitioned.to_string())
            }
            // A plain boolean; reports "true"/"false" (the default is "false", positional).
            OptionStatement::Other(k) if k == crate::OPTION_BIND_BY_NAME => {
                Some(self.bind_by_name.to_string())
            }
            OptionStatement::Other(k) if k == crate::OPTION_ROWS_PER_BATCH => {
                Some(self.rows_per_batch.to_string())
            }
            OptionStatement::Other(k) if k == crate::OPTION_DATA_BOOST => {
                Some(self.data_boost.to_string())
            }
            // Every remaining `spanner.*` option the statement and connection report identically
            // goes through the shared table (each the effective value: the connection's, unless
            // overridden on this statement), which returns the same `NotFound` for an unset (or
            // unknown) key that the fall-through below would.
            OptionStatement::Other(k) => return self.shared_option_string(k),
            _ => None,
        };
        value.ok_or_else(|| option_not_set(key.as_ref()))
    }

    impl_typed_option_getters!();
}

impl Statement for SpannerStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound = vec![batch];
        // Real bound data supersedes any empty-stream ingest schema from a prior bind_stream.
        self.ingest_schema = None;
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        // Capture the stream's schema up front: a stream may yield *zero* batches yet still declare a
        // schema (an empty bulk ingest — `AdbcStatementBindStream` of an empty array stream). Without
        // it a zero-batch ingest would lose the schema entirely and be rejected as "no data has been
        // bound", when it should create the table from the schema and commit zero rows.
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|e| {
                err(
                    format!("failed to read bound stream: {e}"),
                    Status::InvalidData,
                )
            })?);
        }
        // Kept *separately*, not as a synthetic zero-row batch in `bound` — that would make the
        // parameter-binding DML/query paths see bound rows and silently no-op (see
        // `ingest_schema`). A non-empty stream clears it.
        self.ingest_schema = batches.is_empty().then_some(schema);
        self.bound = batches;
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // Mint a fresh cancel signal for this operation (see `CancelSlot`).
        self.cancel.begin_operation();
        // Bulk ingest arriving through the query entry point (needs no SQL query): a standard ADBC
        // FFI caller may drive an ingest via `execute` with a non-null stream out-pointer. Run it
        // the same way `execute_update` does and return an empty stream — the query interface has
        // nowhere to report the affected-row count, so it is discarded. Gate on there being no SQL:
        // a query and an ingest target are mutually exclusive (each setter clears the other), so a
        // reused handle whose most recent config was a query runs that query, not a data-less ingest.
        if self.sql.is_none()
            && let Some(table) = self.target_table.clone()
        {
            self.run_ingest(&table)?;
            return Ok(Self::empty_reader());
        }
        self.check_bound_has_destination()?;
        let sql = self.sql()?;
        if crate::sql::is_ddl(&sql) {
            self.run_ddl(crate::sql::split_statements(&sql))?;
            // A reused statement handle must not silently re-bind stale rows past a DDL statement.
            self.clear_bound();
            // DDL has no result set — return an empty reader with an empty schema.
            return Ok(Self::empty_reader());
        }
        // DML arriving through the query entry point. Standard ADBC clients (the Python DBAPI, R,
        // etc.) issue every statement — including INSERT/UPDATE/DELETE — through `ExecuteQuery`, so
        // route DML onto the read/write path (or buffer it in manual mode) rather than the read-only
        // single-use transaction below, which Spanner rejects for DML. This mirrors `execute_update`.
        // DML with a `THEN RETURN` clause returns its rows; plain DML yields an empty result (the
        // query interface has nowhere to report the affected-row count, so it is discarded).
        if crate::sql::is_dml(&sql) {
            let result = self.run_dml(&sql);
            self.clear_bound();
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
        // Query path (SELECT / WITH / …): the shared read-only query machinery (which also backs
        // `execute_update`'s query-shaped arm).
        self.execute_query_reader(&sql)
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        // Mint a fresh cancel signal for this operation (see `CancelSlot`).
        self.cancel.begin_operation();
        // Bulk ingest: insert the bound rows into the target table (needs no SQL query). Gate on
        // there being no SQL for the same reason as `execute` — a query and an ingest target are
        // mutually exclusive (each setter clears the other), so a reused handle runs whichever was
        // configured most recently rather than the stale other one.
        if self.sql.is_none()
            && let Some(table) = self.target_table.clone()
        {
            return self.run_ingest(&table);
        }
        self.check_bound_has_destination()?;

        let sql = self.sql()?;
        if crate::sql::is_ddl(&sql) {
            self.run_ddl(crate::sql::split_statements(&sql))?;
            // A reused statement handle must not silently re-bind stale rows past a DDL statement.
            self.clear_bound();
            // DDL does not report an affected-row count (and is never transactional in Spanner,
            // so it always runs immediately rather than buffering).
            return Ok(None);
        }
        // Neither DDL nor DML (SELECT / WITH / GRAPH / …): a query. `adbc.h` sanctions executing
        // any statement without expecting a result set ("Pass NULL if the client does not expect a
        // result set"), and such a call lands here, so run it through the same read-only machinery
        // as `execute`, then drain and discard the rows. Do NOT route it into the DML pipeline:
        // that surfaces a raw `ExecuteBatchDml` error in autocommit mode and buffers the query as
        // pending "DML" in manual mode, poisoning commit.
        if !crate::sql::is_dml(&sql) {
            // A multi-statement `;`-batch whose first statement is not DML is neither a query nor
            // an all-DML batch — reject it up front with a clear message (the DML arm below gets
            // the same check via `build_dml_statements`).
            check_all_dml_batch(&crate::sql::split_statements(&sql))?;
            let reader = self.execute_query_reader(&sql)?;
            drain_discarding_rows(reader)?;
            return Ok(None);
        }
        let result = self.run_dml(&sql);
        self.clear_bound();
        match result? {
            // THEN RETURN through the update entry point: the rows are discarded (this interface
            // only reports a count), taken from the result-set stats.
            DmlOutcome::Returning { affected, .. } => Ok(Some(affected)),
            DmlOutcome::Plain(count) => Ok(count),
        }
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        // Mint a fresh cancel signal for this operation (see `CancelSlot`).
        self.cancel.begin_operation();
        let sql = self.sql()?;
        check_schema_query(&sql)?;
        // Query path only (`check_schema_query` rejected DDL/DML): strip any trailing statement
        // terminator(s), exactly as `execute` does — the PLAN probe runs through the same single-use
        // ExecuteSql surface, which rejects a trailing `;` ("Expected end of input but got `;`"),
        // yet introspection callers routinely append one (e.g. `SELECT current_date;`).
        let sql = crate::sql::strip_trailing_terminators(&sql);
        Ok((*self.plan_query_schema(&sql)?).clone())
    }

    /// Partition this query and return one opaque descriptor per partition, to be executed later by
    /// `Connection::read_partition`.
    ///
    /// # Bound parameter rows
    ///
    /// At most **one** bound parameter row is supported: partitioned execution has no per-row
    /// fan-out, so several bound rows are rejected with `InvalidArguments` up front (before any
    /// RPC) rather than silently truncated to the first. The bound data is consumed by the call
    /// either way — success or failure — matching the DML paths, so a reused statement handle
    /// never silently re-applies stale rows.
    ///
    /// # Security
    ///
    /// Each returned descriptor is **opaque but executable**: a versioned JSON envelope
    /// (`{"v":1,"partition":…}`) around the serde form of the client's `Partition`, carrying the
    /// SQL text plus the session and transaction identity. Anyone who can hand a descriptor to
    /// `Connection::read_partition` can run arbitrary SQL with that connection's credentials — the
    /// version envelope guards against format drift, it does **not** authenticate the blob.
    /// Transport descriptors only over trusted channels and never accept one from an untrusted
    /// source.
    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        // Mint a fresh cancel signal for this operation (see `CancelSlot`).
        self.cancel.begin_operation();
        let sql = self.sql()?;
        check_partition_query(&sql)?;
        // Query path only (`check_partition_query` rejected DDL/DML): strip any trailing statement
        // terminator(s), exactly as `execute` does — both the PLAN probe and `partition_query` run
        // through the same ExecuteSql surface, which rejects a trailing `;`, yet callers routinely
        // append one.
        let sql = crate::sql::strip_trailing_terminators(&sql);
        // Partitioning a read has the same read-your-writes hazard as `execute`: reject it in a
        // manual transaction that began with DML (the partitions would read a pre-write
        // snapshot). Note a partitioned read never joins a query transaction's shared snapshot —
        // it always runs in its own batch read-only transaction below.
        self.ensure_query_allowed()?;
        let result = self.run_partition_query(&sql);
        // Consumed by the attempt either way, including a failed one (see `clear_bound`).
        self.clear_bound();
        result
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        // If parameter (or bulk-ingest) data has already been bound, each column *is* a parameter,
        // so its schema is the parameter schema — carrying real, known types.
        if let Some(batch) = self.bound.first() {
            return Ok((*batch.schema()).clone());
        }
        // Otherwise derive the parameter *names* from the statement's `@name` references and ask
        // Spanner for their types via a PLAN probe (see `plan_parameter_types`). A parameter the
        // probe cannot type — or a failed probe, this being best-effort introspection — is typed
        // `Null`, ADBC's convention for "type cannot be determined"
        // (`AdbcStatementGetParameterSchema` in adbc.h).
        let sql = self.sql()?;
        let names = crate::sql::named_parameters(&sql);
        if names.is_empty() {
            return Ok(Schema::new(Vec::<Field>::new()));
        }
        let types = self.plan_parameter_types(&sql).unwrap_or_default();
        let fields: Vec<Field> = names
            .into_iter()
            .map(|name| {
                // GoogleSQL parameter names are case-insensitive, and the planner reports each
                // parameter under the spelling the SQL used *first* — which may differ from this
                // occurrence's. Match exactly, then case-insensitively.
                let ty = types.get(&name).or_else(|| {
                    types
                        .iter()
                        .find_map(|(k, v)| k.eq_ignore_ascii_case(&name).then_some(v))
                });
                match ty {
                    // The field is built by the same mapping as result columns, so a `JSON`-typed
                    // parameter carries the `arrow.json` extension tag the bind path understands.
                    Some(ty) => crate::conversion::arrow_field(
                        &name,
                        ty,
                        true,
                        self.config.timestamp_precision,
                    ),
                    None => Ok(Field::new(name, DataType::Null, true)),
                }
            })
            .collect::<Result<_>>()?;
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
        // A SQL query and a bulk-ingest target are mutually exclusive: setting a query clears any
        // ingest target left on a reused handle, so `execute`/`execute_update` run this query rather
        // than re-entering the (now data-less) ingest branch. See the matching clear in `set_option`.
        self.target_table = None;
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        // Spanner has no Substrait support (it executes GoogleSQL / PostgreSQL text), so there is
        // nothing to execute a Substrait plan against.
        Err(unsupported(
            "setting a Substrait plan: Spanner executes GoogleSQL or PostgreSQL text, not \
             Substrait plans; use set_sql_query instead",
        ))
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        // The handle latches the current operation's (sticky) signal: an in-flight execution wakes
        // and returns Cancelled, and a cancel landing between two chunk fetches of a streamed
        // result still cancels the next fetch — permanently, since the latch is never cleared. The
        // statement's next operation mints a fresh signal instead, so a cancel with nothing running
        // does not affect later executions, and later executions cannot revive a cancelled reader.
        Box::new(SlotCancelHandle::new(self.cancel.clone()))
    }
}

/// Parse a plain string statement option, naming `key` in the error (the `driver.rs`
/// `string_value` pattern: the label is the option's own key, so it can never drift).
fn string_option(key: &OptionStatement, value: OptionValue) -> Result<String> {
    crate::options::string_option(value, &format!("option {}", key.as_ref()))
}

/// Extract the undeclared-parameter name → type map from a PLAN probe's result-set metadata.
/// Metadata is delivered with the first partial result set and retained by the `ResultSet`, so it
/// is available as soon as `execute_query` returns — a PLAN returns no rows to drain.
fn undeclared_parameter_types(
    metadata: Option<&google_cloud_spanner::result::ResultSetMetadata>,
) -> BTreeMap<String, google_cloud_spanner::value::Type> {
    metadata
        .map(|m| m.undeclared_parameters().clone())
        .unwrap_or_default()
}

/// Shared guard for the query-only entry points (`execute_schema`, `execute_partitions`): both run
/// through read-only transactions, and letting DML reach them surfaces Spanner's raw "DML
/// statements can only be performed in a read-write transaction" error, which misleads the caller
/// into thinking the transaction mode is the problem. Catch DDL and DML up front instead (this
/// also covers `THEN RETURN` DML — it produces rows, but Spanner cannot run it read-only).
/// `dml_rationale` completes "DML (INSERT/UPDATE/DELETE) cannot be …" with the entry point's
/// read-only operation.
fn check_query_only(sql: &str, entry_point: &str, dml_rationale: &str) -> Result<()> {
    if crate::sql::is_ddl(sql) {
        return Err(invalid_argument(format!(
            "{entry_point} is only valid for queries"
        )));
    }
    if crate::sql::is_dml(sql) {
        return Err(invalid_argument(format!(
            "{entry_point} only supports queries: DML (INSERT/UPDATE/DELETE) cannot be \
             {dml_rationale}; run it via execute or execute_update instead"
        )));
    }
    Ok(())
}

/// Guard for `execute_schema`: only queries can be planned (the PLAN probe runs in a single-use
/// read-only transaction).
fn check_schema_query(sql: &str) -> Result<()> {
    check_query_only(sql, "execute_schema", "planned in a read-only schema probe")
}

/// Guard for `execute_partitions`: only queries can be partitioned (`partition_query` runs in a
/// batch read-only transaction).
fn check_partition_query(sql: &str) -> Result<()> {
    check_query_only(
        sql,
        "execute_partitions",
        "partitioned in a batch read-only transaction",
    )
}

/// Guard for `;`-separated **multi-statement** batches on the DML paths: `ExecuteBatchDml`
/// executes DML only, so a batch mixing DML with queries or DDL can neither run atomically nor be
/// split across Spanner's execution surfaces. Reject it up front, naming the offending statement —
/// crucially *before* anything is buffered in a manual transaction, where a poisoned buffer would
/// otherwise fail the eventual commit of the whole batch (recoverable only by `rollback`). A
/// single statement (or empty text) always passes. All-DDL batches never reach this — the leading
/// keyword routes them to `run_ddl` first.
fn check_all_dml_batch(statements: &[String]) -> Result<()> {
    if statements.len() > 1
        && let Some(other) = statements.iter().find(|s| !crate::sql::is_dml(s))
    {
        return Err(invalid_argument(format!(
            "a `;`-separated statement batch must be all-DML (INSERT/UPDATE/DELETE), but it \
             contains {other:?}: run queries and DDL as individual statements"
        )));
    }
    Ok(())
}

/// Guard for the **Partitioned DML** path (`spanner.dml.partitioned`): reject up front the
/// statement shapes Spanner's partitioned-DML mode cannot express, naming the option and the reason
/// rather than letting a confusing server-side error surface.
///
/// A partitioned-DML transaction runs exactly one statement, returns no rows, and is its own
/// transaction type — so a `;`-separated batch, a `THEN RETURN` clause, and a manual (buffer-and-
/// commit) transaction are all refused. Several bound parameter rows are refused for the same
/// one-statement reason: there is no per-row fan-out to run them on.
fn check_partitioned_dml(sql: &str, autocommit: bool, bound_rows: usize) -> Result<()> {
    if !autocommit {
        return Err(invalid_state(
            "option spanner.dml.partitioned is set, but the connection is in a manual transaction: \
             partitioned DML is its own transaction type and cannot join one. Commit (or re-enable \
             adbc.connection.autocommit) first, or unset the option",
        ));
    }
    if crate::sql::is_dml_returning(sql) {
        return Err(invalid_argument(
            "option spanner.dml.partitioned is set, but the statement has a THEN RETURN clause: \
             partitioned DML returns no rows. Unset the option to run it in a read/write \
             transaction",
        ));
    }
    let statements = crate::sql::split_statements(sql).len();
    if statements > 1 {
        return Err(invalid_argument(format!(
            "option spanner.dml.partitioned is set, but the SQL is a `;`-separated batch of \
             {statements} statements: partitioned DML runs exactly one statement. Execute each \
             statement separately, or unset the option"
        )));
    }
    if bound_rows > 1 {
        return Err(invalid_argument(format!(
            "option spanner.dml.partitioned is set, but {bound_rows} parameter rows are bound: \
             partitioned DML runs exactly one statement. Bind a single row, or unset the option"
        )));
    }
    Ok(())
}

/// Fully drain a query's streaming reader, discarding the rows. Backs `execute_update`'s
/// query-shaped arm: the statement still executes (and any mid-stream failure still surfaces),
/// but that entry point has no result stream to hand back. A failure is unwrapped back to the
/// ADBC error the streaming layer wrapped into `ArrowError::ExternalError` (see `to_arrow_error`
/// in `src/conversion.rs`), so the caller sees the same status/message `execute` would surface.
fn drain_discarding_rows(reader: Box<dyn RecordBatchReader + Send + 'static>) -> Result<()> {
    for batch in reader {
        batch.map_err(|e| {
            if let ArrowError::ExternalError(inner) = e {
                match inner.downcast::<Error>() {
                    Ok(adbc_error) => *adbc_error,
                    Err(other) => err(
                        format!("query failed while its discarded result set was drained: {other}"),
                        Status::Internal,
                    ),
                }
            } else {
                err(
                    format!("query failed while its discarded result set was drained: {e}"),
                    Status::Internal,
                )
            }
        })?;
    }
    Ok(())
}

/// Validate an option whose only supported value is the spec default `false` (in any of the shared
/// boolean spellings), accepted as a no-op; `true` is rejected as unsupported. `what` names the
/// option for the boolean coercion error, `rejection` is the whole message `true` is refused with.
///
/// Shared by `adbc.ingest.temporary` (Spanner has no temporary tables) and
/// `adbc.statement.exec.incremental` (incremental `execute_partitions` is not implemented), so the
/// two validators cannot drift apart.
fn check_unsupported_true(value: OptionValue, what: &str, rejection: &str) -> Result<()> {
    if bool_option(value, what)? {
        Err(unsupported(rejection))
    } else {
        Ok(())
    }
}

/// Parse the `spanner.dml.partitioned` statement option, the same shape as
/// [`ingest_batch_write_option`]: an empty/whitespace string unsets it (back to `false`, the
/// ordinary read/write path); otherwise a boolean string (exactly `true`/`false`).
fn dml_partitioned_option(value: OptionValue) -> Result<bool> {
    crate::options::bool_option_unsettable(
        value,
        &format!("option {}", crate::OPTION_DML_PARTITIONED),
    )
}

/// Parse the positive `spanner.rows_per_batch` option, accepted as either an integer or a numeric
/// string.
fn rows_per_batch_option(value: OptionValue) -> Result<usize> {
    crate::options::positive_usize(value, "option spanner.rows_per_batch")
}

#[cfg(test)]
mod tests;

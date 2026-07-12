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
use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use google_cloud_lro::Poller as _;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::PartitionOptions;
use google_cloud_spanner::model::execute_sql_request::QueryMode;
use google_cloud_spanner::model::transaction_options::IsolationLevel;
use google_cloud_spanner::mutation::{Mutation, MutationGroup};
use google_cloud_spanner::statement::{Statement as SpannerSql, StatementBuilder};
use google_cloud_spanner::transaction::ReadWriteTransaction;

use crate::bind;
use crate::connection::{SharedTxn, apply_isolation};
use crate::conversion::{
    BoundStatementSource, TimestampPrecision, result_set_to_batch, stream_bound_query, stream_query,
};
use crate::directed_read::DirectedRead;
use crate::error::{
    err, from_builder, from_spanner, from_status_parts, invalid_argument, invalid_state,
    not_implemented,
};
use crate::options::impl_shared_option_dispatch;
use crate::query_options::QueryOptionsConfig;
use crate::request::{CommitStats, RequestConfig};
use crate::retry::RetryConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::staleness::ReadStaleness;
use crate::timeout::{RpcTimeouts, with_timeout};

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
/// the per-row [`bind::bind_params`] + `read_sql_builder`-clone, producing the exact same statement
/// sequence, in the same order, the eager path would have — one `(names, batch)` group at a time,
/// row by row, skipping past a drained batch.
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
    /// The connection's `adbc.connection.readonly` flag, shared live (`Arc`) rather than
    /// snapshotted: each write path loads it at execution time, so toggling the option on the
    /// connection immediately affects this statement in both directions.
    read_only: Arc<AtomicBool>,
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
    /// Ingest mode (`adbc.ingest.mode`), parsed once in `set_option` (which rejects unknown
    /// modes) so the ingest paths match it exhaustively; `get_option` reports the spec's
    /// canonical `adbc.ingest.mode.*` spelling. `create` (the ADBC spec default — unset `None`
    /// resolves to it), `append`, `create_append`, and `replace`; the create/replace modes build
    /// the table from the ingest data's Arrow schema.
    ingest_mode: Option<IngestMode>,
    /// Primary-key columns for a create-mode bulk ingest (`spanner.ingest.primary_key`). `None`
    /// (the default) makes the create modes append a synthetic `adbc_ingest_key` UUID key; `Some`
    /// keys on these existing ingest columns instead (in order), adding no synthetic column. Parsed
    /// once in `set_option` (comma-separated, trimmed, empties dropped); `""` unsets. See
    /// [`bind::create_table_sql`].
    ingest_primary_key: Option<Vec<String>>,
    /// Route an autocommit bulk ingest's per-chunk mutations through Spanner's **BatchWrite** RPC
    /// (`spanner.ingest.batch_write`, boolean, default `false`) instead of a write-only
    /// transaction — a non-atomic, higher-throughput "firehose" transport. See
    /// [`run_ingest_mutations`](Self::run_ingest_mutations). Ignored in manual-transaction mode,
    /// where ingests buffer and commit atomically with the surrounding transaction.
    ingest_batch_write: bool,
    /// How bound columns pair with the query's `@name` parameters
    /// (`adbc.statement.bind_by_name`): `false` (the default) binds positionally, `true` forces
    /// strict by-name. See [`bind::resolve_parameter_names`].
    bind_by_name: bool,
    /// Rows converted into each streamed Arrow batch by `execute` (`spanner.rows_per_batch`).
    rows_per_batch: usize,
    /// Enable Data Boost for partitioned execution (`spanner.data_boost`).
    data_boost: bool,
    /// Maximum number of partitions to request from `execute_partitions`
    /// (`spanner.partition.max_count`); `None` lets Spanner choose.
    max_partitions: Option<i64>,
    /// Read bound for this statement's read-only queries (`spanner.read.staleness`), inherited from
    /// the connection at creation time and overridable per statement. Default is a strong read.
    read_staleness: ReadStaleness,
    /// Request priority and request/transaction tags (`spanner.request.priority` /
    /// `spanner.request.tag`), inherited from the connection at creation time; the priority and
    /// request tag are overridable per statement. The transaction tag (connection-level only)
    /// rides along for the read/write transaction runners this statement builds.
    request: RequestConfig,
    /// Directed-read replica selection (`spanner.directed_read`), inherited from the connection at
    /// creation time and overridable per statement. Applied to this statement's read-only query
    /// paths only (Spanner rejects it on writes). Unset by default (Spanner's own routing).
    directed_read: DirectedRead,
    /// Query optimizer options (`spanner.query.optimizer_version` /
    /// `spanner.query.optimizer_statistics_package`), inherited from the connection at creation time
    /// and overridable per statement. Applied to every query statement builder this statement
    /// produces (via [`Self::sql_builder`]).
    query_options: QueryOptionsConfig,
    /// How `TIMESTAMP` columns map to Arrow (`spanner.max_timestamp_precision`), inherited from
    /// the connection at creation time and overridable per statement. Applied uniformly to every
    /// result path of this statement: `execute` (plain and bound queries), DML `THEN RETURN`
    /// rows, `execute_schema`, and the `execute_partitions` schema probe.
    timestamp_precision: TimestampPrecision,
    /// RPC timeouts (`spanner.rpc.timeout_seconds.{query,update,fetch}`), inherited from the
    /// connection at creation time and overridable per statement. Unset (the default) means no
    /// deadline; an expired deadline fails with [`Status::Timeout`].
    timeouts: RpcTimeouts,
    /// Retry-policy tuning (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`),
    /// inherited from the connection at creation time and overridable per statement. Unset (the
    /// default) leaves the client's own retry policy; when set it bounds the client's retrying on
    /// every statement/DML/transaction builder this statement produces.
    retry: RetryConfig,
    /// Mutation count captured from this statement's most recent commit that requested commit
    /// statistics (`spanner.commit_stats`) — autocommit DML or a bulk ingest — read back via
    /// `spanner.commit_stats.mutation_count`. Statement-owned and **not** inherited from the
    /// connection (the connection owns the manual-mode commit's stats); fresh (unset) per statement.
    commit_stats: CommitStats,
    /// Cancellation signal for this statement's in-flight execution (see [`Statement::cancel`]).
    cancel: CancelSignal,
}

impl SpannerStatement {
    // Shared `set_shared_option` / `shared_option_string` for the "staleness-pattern" options
    // (request priority/tag, directed read, max_commit_delay, commit_stats, query optimizer opts,
    // RPC timeouts, retry tuning, …) that the statement and connection dispatch identically.
    impl_shared_option_dispatch!();

    #[allow(clippy::too_many_arguments)] // constructor threads the connection's config verbatim
    pub(crate) fn new(
        runtime: SharedRuntime,
        client: DatabaseClient,
        spanner: Spanner,
        database: String,
        read_only: Arc<AtomicBool>,
        isolation: IsolationLevel,
        read_staleness: ReadStaleness,
        request: RequestConfig,
        directed_read: DirectedRead,
        query_options: QueryOptionsConfig,
        timestamp_precision: TimestampPrecision,
        timeouts: RpcTimeouts,
        retry: RetryConfig,
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
            ingest_primary_key: None,
            ingest_batch_write: false,
            bind_by_name: false,
            rows_per_batch: DEFAULT_ROWS_PER_BATCH,
            data_boost: false,
            max_partitions: None,
            read_staleness,
            request,
            directed_read,
            query_options,
            timestamp_precision,
            timeouts,
            retry,
            commit_stats: CommitStats::default(),
            cancel: CancelSignal::new(),
        }
    }

    /// The *live* value of the connection's `adbc.connection.readonly` flag. Loaded at each write
    /// attempt (never cached), so a toggle on the connection applies to this statement immediately.
    fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Acquire)
    }

    /// A Spanner statement builder for `sql` with this statement's request priority / request tag
    /// (`spanner.request.priority` / `spanner.request.tag`), query optimizer options
    /// (`spanner.query.optimizer_version` / `spanner.query.optimizer_statistics_package`) and retry
    /// policy (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`) applied. Every
    /// query/DML statement the driver builds goes through here so the options apply uniformly.
    fn sql_builder(&self, sql: &str) -> StatementBuilder {
        self.retry.apply_to_statement(
            self.query_options
                .apply_to_statement(self.request.apply_to_statement(SpannerSql::builder(sql))),
        )
    }

    /// A Spanner statement builder for a **read-only query** `sql`: [`sql_builder`](Self::sql_builder)
    /// plus this statement's directed-read replica selection (`spanner.directed_read`). Used only on
    /// the read-only query paths — Spanner rejects directed reads on a read/write transaction, so the
    /// DML paths keep using [`sql_builder`](Self::sql_builder) directly.
    fn read_sql_builder(&self, sql: &str) -> StatementBuilder {
        self.directed_read.apply_to_statement(self.sql_builder(sql))
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

    /// DDL to run before an ingest, for the create/replace ingest modes (`None` for append).
    ///
    /// `create` (the default — see [`ingest_mode`](Self::ingest_mode)) builds the table (erroring
    /// if it exists), `create_append` builds it if absent, and `replace` drops any existing table
    /// first. The schema comes from the bound ingest data.
    fn build_ingest_table_ddl(
        &self,
        table: &str,
        mode: Option<IngestMode>,
    ) -> Result<Option<Vec<String>>> {
        // Exhaustive: unknown modes were already rejected by `set_option` (`ingest_mode_option`).
        // Unset (`None`) is `create`, the ADBC spec default.
        let (if_not_exists, drop_first) = match mode {
            // Append into an existing table: no DDL.
            Some(IngestMode::Append) => return Ok(None),
            None | Some(IngestMode::Create) => (false, false),
            Some(IngestMode::CreateAppend) => (true, false),
            Some(IngestMode::Replace) => (false, true),
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
                crate::sql::qualified_table(db_schema, table)
            ));
        }
        statements.push(bind::create_table_sql(
            table,
            db_schema,
            &schema,
            if_not_exists,
            self.ingest_primary_key.as_deref(),
        )?);
        Ok(Some(statements))
    }

    /// Remap a failed `append`- or `create_append`-mode bulk ingest onto the statuses the ADBC
    /// bulk-ingest contract mandates.
    ///
    /// A successful (or, in manual-transaction mode, merely buffered) outcome is returned unchanged.
    /// A failure that already carries [`Status::AlreadyExists`] — a bound row duplicating a primary
    /// key already in the table, since insert mutations keep `INSERT` semantics — keeps that status
    /// and just gets the target table's name folded into the message. Any other failure probes the
    /// target table via the shared [`table_exists`](crate::connection::table_exists) query: a
    /// missing table becomes [`Status::NotFound`], and an existing table — so the insert must have
    /// failed because the bound data's schema is incompatible with the table's — becomes
    /// [`Status::AlreadyExists`]. Only these cases are remapped; the original Spanner error's
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
        // Already `AlreadyExists`: a duplicate primary key. The status is the one the contract
        // wants — name the target table (consumers key off it) instead of running the exists
        // probe, whose "incompatible schema" wording would misreport a duplicate key.
        if error.status == Status::AlreadyExists {
            let mut named = err(
                format!(
                    "bulk ingest append into table {table:?} failed: {}",
                    error.message
                ),
                Status::AlreadyExists,
            );
            named.vendor_code = error.vendor_code;
            return Err(named);
        }
        // Probe the target table in its schema (`adbc.ingest.target_db_schema`, empty = Spanner's
        // default, unnamed schema).
        let db_schema = self.target_db_schema.as_deref().unwrap_or("");
        let exists = crate::connection::table_exists(
            &self.runtime,
            &self.client,
            &self.cancel,
            self.timeouts.query_timeout(),
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
            Ok(crate::sql::split_statements(sql)
                .into_iter()
                .map(|s| self.sql_builder(&s).build())
                .collect())
        }
    }

    /// Run a bulk ingest of the bound rows into `table`, honouring the configured ingest mode.
    ///
    /// Shared by `execute` and `execute_update` so both entry points ingest identically: an ingest
    /// needs no SQL query, so an FFI caller reaches it through either the query out-pointer
    /// (`execute`) or the affected-rows path (`execute_update`). In the create/replace modes the
    /// table is first built from the ingest data's Arrow schema (with a synthetic UUID primary key)
    /// via DDL, which Spanner runs immediately before the inserts. Returns the ingested-row count
    /// (summed across chunk transactions — see [`run_ingest_mutations`](Self::run_ingest_mutations)),
    /// or `None` when the rows were buffered for a manual-transaction commit.
    ///
    /// An ingest small enough for one chunk (the common case) applies atomically; one large enough
    /// to need several chunks does **not** — each chunk commits in its own transaction, so a
    /// mid-ingest failure leaves the earlier chunks' rows committed (the error reports their exact
    /// count — see [`note_rows_already_committed`]).
    fn run_ingest(&mut self, table: &str) -> Result<Option<i64>> {
        if self.is_read_only() {
            return Err(invalid_state("cannot ingest: the connection is read-only"));
        }
        if self.bound.is_empty() {
            return Err(invalid_state("cannot ingest: no data has been bound"));
        }
        let result = self.run_bound_ingest(table);
        // The bound data is consumed by the ingest attempt either way — including a failed
        // create-mode DDL: a reused statement handle must not silently re-ingest stale rows after
        // a failure.
        self.bound.clear();
        result
    }

    /// The body of [`run_ingest`](Self::run_ingest), split out so its caller clears the bound data
    /// on every exit path (success, failed DDL, failed insert) in one place.
    fn run_bound_ingest(&self, table: &str) -> Result<Option<i64>> {
        let ingest_ddl = self.build_ingest_table_ddl(table, self.ingest_mode)?;
        if let Some(ddl) = ingest_ddl {
            self.run_ddl(ddl)
                .map_err(|error| self.remap_ingest_create_error(table, error))?;
        }
        let result = self.run_ingest_mutations(table);
        // `append` and `create_append` both insert into a table that may already exist, so the
        // ADBC spec wants their insert failure remapped to NotFound / AlreadyExists. For
        // `append` a missing table is NotFound and a present one is a schema mismatch
        // (AlreadyExists); for `create_append` the `CREATE TABLE IF NOT EXISTS` above guarantees
        // the table is present, so only the schema-mismatch AlreadyExists side can surface (its
        // spec contract: "error if the table exists, but the schema does not match"). `create`
        // and `replace` keep the raw insert error — their DDL step already owns the
        // table-existence contract (`remap_ingest_create_error`).
        if matches!(
            self.ingest_mode,
            Some(IngestMode::Append) | Some(IngestMode::CreateAppend)
        ) {
            self.remap_ingest_append_error(table, result)
        } else {
            result
        }
    }

    /// Remap a failed `create`-mode ingest DDL onto [`Status::AlreadyExists`] when the target
    /// table already exists.
    ///
    /// `create` mode promises to build the table, so hitting an existing one is the
    /// ADBC-contractual `AlreadyExists` — consumers branch on that status (e.g. to fall back to
    /// append). Spanner reports it as a generic schema-change failure ("Duplicate name in
    /// schema"), so the existence is confirmed via the shared
    /// [`table_exists`](crate::connection::table_exists) probe and the remapped message names the
    /// table. Only `create` is remapped: `create_append` guards with `IF NOT EXISTS` and `replace`
    /// drops first, so their DDL failures are never about the table already existing. If the table
    /// is absent — or the probe itself fails — the original DDL error surfaces unchanged.
    fn remap_ingest_create_error(&self, table: &str, error: Error) -> Error {
        // Unset (`None`) is `create`, the default, so remap its DDL failure too.
        if !matches!(self.ingest_mode, None | Some(IngestMode::Create)) {
            return error;
        }
        let db_schema = self.target_db_schema.as_deref().unwrap_or("");
        match crate::connection::table_exists(
            &self.runtime,
            &self.client,
            &self.cancel,
            self.timeouts.query_timeout(),
            db_schema,
            table,
        ) {
            Ok(true) => err(
                format!(
                    "bulk ingest create target table {table:?} already exists ({})",
                    error.message
                ),
                Status::AlreadyExists,
            ),
            _ => error,
        }
    }

    /// Ship the bound rows as Spanner **insert mutations**, honouring the connection's transaction
    /// mode and Spanner's per-commit limits.
    ///
    /// Mutations are the `Commit` RPC's native write format: unlike the per-row parameterized
    /// `INSERT` DML this driver used to build, they carry no SQL for Spanner to parse and plan per
    /// row, which makes them the fast path for bulk loads. Each cell converts through the same
    /// Arrow→Spanner value mapping as parameter binding (see [`bind::insert_mutation`]). Insert
    /// mutations keep `INSERT` semantics — ingesting a duplicate primary key fails with
    /// `ALREADY_EXISTS`, as the DML path's `INSERT` did. (Mutations take no isolation level:
    /// Spanner commits blind writes serializably.)
    ///
    /// **Manual mode** buffers every row's mutation for the next `commit`, which applies them
    /// atomically in the *same* read/write transaction as any buffered DML — Spanner applies
    /// buffered mutations at commit time, after the transaction's DML has executed. Never chunked:
    /// the commit applies the user's whole transaction atomically, so an over-limit manual-mode
    /// ingest fails at commit, as any over-limit user transaction would.
    ///
    /// **Autocommit mode** builds and ships the mutations chunk by chunk, each chunk in its own
    /// write-only transaction (with the client's retry/replay protection), returning the ingested
    /// row count summed across chunks. Why chunk: Spanner caps a single commit at ~80,000 mutations
    /// — counted roughly as rows × columns, plus secondary-index entries — and ~100 MB, so one
    /// unchunked commit fails outright once the ingest crosses those cliffs (10k rows × 10 columns
    /// is already there). An ingest that fits [`IngestChunkBudget`]'s conservative budgets still
    /// commits as a single atomic transaction; only an ingest big enough to need several chunks —
    /// one that could not have committed at all as one transaction — loses whole-ingest atomicity.
    /// When a later chunk's commit fails, the error reports exactly how many rows the earlier
    /// chunks already committed (see [`note_rows_already_committed`]), so the caller knows the
    /// table's state. Building per chunk also bounds memory: only one chunk of mutations is
    /// materialised at a time, instead of all N rows up front.
    ///
    /// The `rows × columns` budget cannot see the **secondary-index** entries that also count
    /// toward the per-commit cap, so a heavily-indexed table can overshoot it even inside a
    /// driver-"safe" chunk. As a reactive backstop, a write-only chunk whose commit is rejected for
    /// *too many mutations* is split in half and its halves retried, down to a single row — see
    /// [`write_mutation_range`](Self::write_mutation_range). Like the multi-chunk case, a bisected
    /// chunk is not atomic as a whole; the row count and the already-committed accounting stay
    /// exact. (The BatchWrite path — `spanner.ingest.batch_write` — is not bisected: it ships one
    /// group per row, so the mutation cap does not bind it the same way.)
    fn run_ingest_mutations(&self, table: &str) -> Result<Option<i64>> {
        // Mutations name their target table directly (no SQL quoting; a named schema joins with a
        // plain dot).
        let target = bind::mutation_table(self.target_db_schema.as_deref(), table);
        {
            let mut txn = self.txn.lock().unwrap();
            if !txn.autocommit() {
                for batch in &self.bound {
                    for row in 0..batch.num_rows() {
                        txn.buffer_mutation(bind::insert_mutation(&target, batch, row)?);
                    }
                }
                return Ok(None);
            }
        }
        // Autocommit: walk the flattened row sequence (all bound batches concatenated), cutting it
        // into commit chunks by the same [`IngestChunkBudget`]. A chunk is a contiguous
        // `[start, end)` **range** over that sequence rather than a materialised `Vec<Mutation>`;
        // its mutations are (re)built cheaply from the batches on demand
        // ([`commit_ingest_range`](Self::commit_ingest_range)), so nothing is cloned up front just
        // to enable the reactive bisect-and-retry the write-only path performs when a chunk
        // overshoots Spanner's per-commit mutation cap.
        let mut total = 0_i64;
        let mut budget = IngestChunkBudget::default();
        let mut chunk_start = 0_usize;
        let mut row_index = 0_usize;
        for batch in &self.bound {
            let columns = batch.num_columns();
            // A cheap per-row size estimate: the batch's Arrow buffer footprint averaged over its
            // rows. Capacity-based, so it slightly over-estimates the wire size — the conservative
            // direction for a budget.
            let row_bytes = batch.get_array_memory_size() / batch.num_rows().max(1);
            for _ in 0..batch.num_rows() {
                if !budget.fits(columns, row_bytes) {
                    total += self.commit_ingest_range(&target, chunk_start, row_index, total)?;
                    budget = IngestChunkBudget::default();
                    chunk_start = row_index;
                }
                budget.add(columns, row_bytes);
                row_index += 1;
            }
        }
        total += self.commit_ingest_range(&target, chunk_start, row_index, total)?;
        Ok(Some(total))
    }

    /// Build the insert mutations for the flattened row range `[start, end)` across the bound
    /// batches, mapping each global row index back to its `(batch, row)`.
    ///
    /// The same cheap Arrow→Spanner build the forward path uses ([`bind::insert_mutation`]), so a
    /// bisected retry rebuilds a half's mutations straight from the batches — no `Vec<Mutation>` is
    /// cloned on the happy path solely to keep a copy around for a retry that usually never happens.
    fn build_range_mutations(
        &self,
        target: &str,
        start: usize,
        end: usize,
    ) -> Result<Vec<Mutation>> {
        let mut mutations = Vec::with_capacity(end.saturating_sub(start));
        let mut base = 0_usize;
        for batch in &self.bound {
            let rows = batch.num_rows();
            // Intersect the requested global range with this batch's slice of the flattened
            // sequence (`[base, base + rows)`), then translate to batch-local row offsets.
            let lo = start.max(base).saturating_sub(base);
            let hi = end.min(base + rows).saturating_sub(base);
            for row in lo..hi {
                mutations.push(bind::insert_mutation(target, batch, row)?);
            }
            base += rows;
            if base >= end {
                break;
            }
        }
        Ok(mutations)
    }

    /// Commit the flattened row range `[start, end)` as one autocommit ingest chunk, dispatching on
    /// the `spanner.ingest.batch_write` option: the default write-only transaction (with the
    /// reactive mutation-limit bisect — [`write_mutation_range`](Self::write_mutation_range)), or
    /// Spanner's BatchWrite RPC ([`batch_write_chunk`](Self::batch_write_chunk)) for a non-atomic
    /// firehose load. Both return the range's ingested-row count (`0` for an empty range).
    ///
    /// `prior_total` is how many rows this ingest's earlier chunks have already committed; it is
    /// woven into a mid-ingest failure via [`note_rows_already_committed`] so the caller learns the
    /// exact table state.
    fn commit_ingest_range(
        &self,
        target: &str,
        start: usize,
        end: usize,
        prior_total: i64,
    ) -> Result<i64> {
        if start >= end {
            return Ok(0);
        }
        if self.ingest_batch_write {
            // BatchWrite ships one MutationGroup per row and applies groups independently, so the
            // per-commit mutation cap does not bind it the way a write-only `Commit` is bound — it
            // is deliberately left out of the mutation-limit bisect. (Its own per-request size limit
            // could warrant a follow-up, but is not this backstop's concern.)
            let mutations = self.build_range_mutations(target, start, end)?;
            self.batch_write_chunk(mutations)
                .map_err(|e| note_rows_already_committed(e, prior_total))
        } else {
            self.write_mutation_range(target, start, end, prior_total)
        }
    }

    /// Commit the flattened row range `[start, end)` in one write-only transaction, **splitting it
    /// in half and retrying the two halves** if — and only if — Spanner rejects the commit for
    /// exceeding its per-commit mutation limit.
    ///
    /// The forward path sizes chunks by `rows × columns` mutations ([`IngestChunkBudget`]), but the
    /// *true* commit-time mutation count also includes secondary-index entries the driver cannot
    /// see, so a heavily-indexed table can overshoot Spanner's ~80,000-mutation cap even inside a
    /// driver-"safe" chunk. This is the reactive backstop: on that specific error
    /// ([`is_mutation_limit_exceeded`]) the range is bisected and each half retried, recursing down
    /// to a single row. Every **other** error — a duplicate key (`AlreadyExists`), a bad value, a
    /// timeout, a cancel, an `ABORTED` — propagates unchanged, so the append/create remaps and the
    /// [`note_rows_already_committed`] annotation still fire. A single row that *still* overshoots
    /// is genuinely un-splittable, so its error propagates too (no infinite recursion, no empty
    /// commit). Like the multi-chunk ingest, a bisected chunk is **not atomic as a whole**; the
    /// summed row count and the "rows already committed" accounting stay exact — `prior_total` is
    /// threaded through the recursion so a mid-bisect failure reports every row committed before it.
    fn write_mutation_range(
        &self,
        target: &str,
        start: usize,
        end: usize,
        prior_total: i64,
    ) -> Result<i64> {
        let mutations = self.build_range_mutations(target, start, end)?;
        match self.write_mutation_chunk(mutations) {
            Ok(count) => Ok(count),
            Err(error) if end - start > 1 && is_mutation_limit_exceeded(&error) => {
                let mid = start + (end - start) / 2;
                let left = self.write_mutation_range(target, start, mid, prior_total)?;
                let right = self.write_mutation_range(target, mid, end, prior_total + left)?;
                Ok(left + right)
            }
            Err(error) => Err(note_rows_already_committed(error, prior_total)),
        }
    }

    /// Commit one ingest chunk in its own write-only transaction, returning its row count (`0` for
    /// an empty chunk, which sends nothing).
    ///
    /// `WriteOnlyTransaction::write` carries the client's replay protection — on success the
    /// mutations were applied exactly once, retrying internally on `ABORTED`. A commit reports no
    /// affected-row count, but each insert mutation is exactly one row, so the chunk length is the
    /// count.
    fn write_mutation_chunk(&self, mutations: Vec<Mutation>) -> Result<i64> {
        if mutations.is_empty() {
            return Ok(0);
        }
        let count = mutations.len() as i64;
        let client = self.client.clone();
        let request = self.request.clone();
        let retry = self.retry;
        let mutation_count = block_on_cancellable(
            &self.runtime,
            &self.cancel,
            with_timeout(
                self.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async move {
                    let response = retry
                        .apply_to_write_only(
                            request.apply_to_write_only(client.write_only_transaction()),
                        )
                        .build()
                        .write(mutations)
                        .await
                        .map_err(from_spanner)?;
                    // The commit stats (only when `spanner.commit_stats` requested them) ride on the
                    // write-only commit response.
                    Ok::<Option<i64>, Error>(
                        response
                            .commit_stats
                            .as_ref()
                            .map(|stats| stats.mutation_count),
                    )
                },
            ),
        )?;
        self.commit_stats.record(mutation_count);
        Ok(count)
    }

    /// Apply one ingest chunk through Spanner's **BatchWrite** RPC (the
    /// `spanner.ingest.batch_write` autocommit path), returning the number of rows applied.
    ///
    /// Each row's insert mutation is sent as its own [`MutationGroup`]. BatchWrite applies groups
    /// **independently and non-atomically** — the same "not atomic as a whole" guarantee the
    /// multi-chunk write-only path already carries, but now within a chunk too — which is what makes
    /// it the cheaper firehose transport for large loads. Each streamed [`BatchWriteResponse`]
    /// reports, per group index, whether it applied: an `OK`/absent status counts those rows as
    /// applied, and the first non-`OK` group status becomes the returned error via
    /// [`from_status_parts`] (so a duplicate primary key still surfaces as `AlreadyExists` and the
    /// append/create remaps fire, exactly as on the write-only path). Because a non-atomic batch may
    /// have applied some groups before the failing one, an error here is combined with the
    /// already-committed-rows annotation by the caller ([`run_ingest_mutations`](Self::run_ingest_mutations)).
    ///
    /// BatchWrite carries no per-request commit options and its response has no commit statistics,
    /// so `spanner.request.priority` / `spanner.request.tag` / `spanner.commit.max_delay` /
    /// `spanner.commit_stats` do not apply on this path (documented on
    /// [`OPTION_INGEST_BATCH_WRITE`](crate::OPTION_INGEST_BATCH_WRITE)).
    fn batch_write_chunk(&self, mutations: Vec<Mutation>) -> Result<i64> {
        if mutations.is_empty() {
            return Ok(0);
        }
        let client = self.client.clone();
        // One mutation group per row: groups are applied independently, so per-row insert failures
        // (e.g. a duplicate key) do not roll back the rest of the chunk.
        let groups: Vec<MutationGroup> = mutations
            .into_iter()
            .map(|m| MutationGroup::new(vec![m]))
            .collect();
        block_on_cancellable(
            &self.runtime,
            &self.cancel,
            with_timeout(
                self.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async move {
                    let mut stream = client
                        .batch_write_transaction()
                        .build()
                        .execute_streaming(groups)
                        .await
                        .map_err(from_spanner)?;
                    let mut applied = 0_i64;
                    let mut first_error: Option<Error> = None;
                    while let Some(response) = stream.next().await {
                        let response = response.map_err(from_spanner)?;
                        // An `OK` (or absent) status means the referenced groups applied; any other
                        // status marks them failed — capture the first such failure to return.
                        match response.status.as_ref().filter(|s| s.code != 0) {
                            None => applied += response.indexes.len() as i64,
                            Some(status) if first_error.is_none() => {
                                first_error = Some(from_status_parts(status.code, &status.message));
                            }
                            Some(_) => {}
                        }
                    }
                    match first_error {
                        Some(error) => Err(error),
                        None => Ok(applied),
                    }
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
    /// `None` is returned (the count is unknown until commit). Routing user-authored DML — plain
    /// `;`-batches and parameterized DML — through here keeps it consistent with the
    /// buffer-and-commit model. (Bulk ingest goes through
    /// [`run_ingest_mutations`](Self::run_ingest_mutations) instead, which ships mutations and
    /// chunks the autocommit path under Spanner's commit limits; user statements are never
    /// chunked.)
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
            self.request.clone(),
            self.retry,
            self.timeouts.update_timeout(),
            &self.commit_stats,
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
        let request = self.request.clone();
        let retry = self.retry;
        // DML with THEN RETURN is a write path: the update timeout bounds the whole transaction.
        let update_timeout = self.timeouts.update_timeout();
        let transaction = async move {
            let runner =
                retry
                    .apply_to_runner(request.apply_to_runner(apply_isolation(
                        client.read_write_transaction(),
                        isolation,
                    )))
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
            &self.cancel,
            with_timeout(
                update_timeout,
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                transaction,
            ),
        )?;
        self.commit_stats.record(mutation_count);

        let mut schema = None;
        let mut batches = Vec::with_capacity(results.len());
        let mut affected = 0i64;
        for (metadata, rows, count) in &results {
            let (sch, batch) = crate::conversion::rows_to_batch(
                metadata.as_ref(),
                rows,
                self.timestamp_precision,
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
    fn run_dml(&self, sql: &str) -> Result<DmlOutcome> {
        if !crate::sql::is_dml_returning(sql) {
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

    /// Run a parameterized query once per bound row, streaming the concatenated results.
    ///
    /// Every bound row executes in **one** read-only snapshot, so the per-row results are mutually
    /// consistent: a single bound row keeps the cheap single-use transaction, while several bound
    /// rows share one multi-use read-only transaction pinned at the statement's read bound. The
    /// bounded-staleness kinds (`max:` / `min:`), which Spanner only accepts on single-use
    /// transactions, are pinned to the most stale timestamp their window allows for the multi-row
    /// case (see [`ReadStaleness::multi_use_timestamp_bound`]).
    ///
    /// Results stream through the same bounded-chunk machinery as `execute`: rows are converted to
    /// Arrow in chunks of `spanner.rows_per_batch` (plus the byte budget) as the reader is
    /// iterated, never materialised whole.
    fn execute_bound_query(
        &self,
        sql: &str,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // Resolve the column→parameter mapping once per non-empty bound batch — the eager path's
        // up-front count/name validation (it lexes `sql`), kept here so a structural mismatch still
        // fails before any statement runs — pairing each mapping with its batch. Only the per-row
        // `bind::bind_params` is deferred to the reader, so at most one `SpannerSql` resides in
        // memory at a time (O(1)) rather than one per bound row (O(rows)).
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
        // One fully-configured read-only query builder (directed reads + request tags + query
        // optimizer options + retry applied, exactly as the eager path's `read_sql_builder`), cloned
        // per row before binding — the shared config is applied once, not re-resolved per row.
        let base_builder = self.read_sql_builder(sql);
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.clone();
        let batch_size = self.rows_per_batch;
        let precision = self.timestamp_precision;
        // The query timeout bounds the initial execution (through the first chunk); the fetch
        // timeout bounds each later chunk as the reader is iterated.
        let query_timeout = self.timeouts.query_timeout();
        let fetch_timeout = self.timeouts.fetch_timeout();
        if total_rows <= 1 {
            // Zero or one bound row. One statement is one snapshot already, and the single-use
            // transaction keeps the exact semantics of the bounded-staleness kinds.
            let Some((names, batch)) = groups.first() else {
                return Ok(Self::empty_reader());
            };
            let statement = bind::bind_params(base_builder, names, batch, 0)?.build();
            let bound = self.read_staleness.timestamp_bound()?;
            let reader = block_on_cancellable(
                &self.runtime,
                &self.cancel,
                with_timeout(query_timeout, crate::OPTION_RPC_TIMEOUT_QUERY, async move {
                    let transaction = crate::staleness::single_use(&client, bound);
                    let result_set = transaction
                        .execute_query(statement)
                        .await
                        .map_err(from_spanner)?;
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
        let bound = self.read_staleness.multi_use_timestamp_bound()?;
        let statements: Box<dyn BoundStatementSource> = Box::new(LazyBoundStatements {
            base_builder,
            groups,
            group: 0,
            row: 0,
        });
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel,
            with_timeout(query_timeout, crate::OPTION_RPC_TIMEOUT_QUERY, async move {
                let mut builder = client.read_only_transaction();
                if let Some(b) = bound {
                    builder = builder.set_timestamp_bound(b);
                }
                let transaction = builder.build().await.map_err(from_spanner)?;
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

    /// Apply one or more DDL statements as a single Spanner `UpdateDatabaseDdl` schema change.
    ///
    /// Batching all statements into one call makes a multi-step change (for example dbt's
    /// intermediate-table build followed by a rename swap) near-atomic.
    fn run_ddl(&self, statements: Vec<String>) -> Result<()> {
        if self.is_read_only() {
            return Err(invalid_state(
                "cannot execute DDL: the connection is read-only",
            ));
        }
        let spanner = self.spanner.clone();
        let database = self.database.clone();
        // DDL is issued through the write/update path, so the update timeout bounds the whole
        // change — the admin-client build, the `UpdateDatabaseDdl` call, **and** its long-running
        // operation poll loop (which otherwise polls without any bound). An expired deadline fails
        // with `Status::Timeout`; unset (the default) leaves the poll unbounded.
        block_on_cancellable(
            &self.runtime,
            &self.cancel,
            with_timeout(
                self.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async move {
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
                },
            ),
        )
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
        let mut builder = self.read_sql_builder(sql);
        if plan {
            builder = builder.set_query_mode(QueryMode::Plan);
        }
        if let Some(batch) = self.bound.first()
            && batch.num_rows() > 0
        {
            let names = bind::resolve_parameter_names(sql, batch, self.bind_by_name)?;
            builder = bind::bind_params(builder, &names, batch, 0)?;
        }
        Ok(builder.build())
    }

    /// Guard for [`execute`](Statement::execute) / [`execute_update`](Statement::execute_update)
    /// when data has been bound but there is nothing to apply it to — neither a SQL query nor a
    /// bulk-ingest target. Binding *before* setting `adbc.ingest.target_table` is legal (the bind
    /// and the ingest options may arrive in either order), so this can only be diagnosed at
    /// execution time — and the message names both remedies, instead of the plain "no SQL query
    /// set" error that would hide the missing ingest option.
    fn check_bound_has_destination(&self) -> Result<()> {
        if self.sql.is_none() && self.target_table.is_none() && !self.bound.is_empty() {
            return Err(invalid_state(
                "data has been bound but no SQL query or bulk-ingest target is set; call \
                 set_sql_query or set the adbc.ingest.target_table option before executing",
            ));
        }
        Ok(())
    }
}

impl Optionable for SpannerStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionStatement::TargetTable => {
                self.target_table = Some(string_option(value)?);
                // Mutually exclusive with a SQL query (see `set_sql_query`): setting an ingest
                // target clears any query left on a reused handle — e.g. the DBAPI `Cursor` reuses
                // one statement, so `cur.execute("CREATE TABLE …")` then `cur.adbc_ingest(…)` would
                // otherwise leave the stale CREATE set and skip the ingest.
                self.sql = None;
            }
            OptionStatement::TargetDbSchema => {
                // Named schema for the ingest target table; qualifies the INSERT / CREATE TABLE via
                // `qualified_table` (empty selects Spanner's default, unnamed schema).
                self.target_db_schema = Some(string_option(value)?);
            }
            OptionStatement::TargetCatalog => {
                // Spanner exposes a single, unnamed catalog, so only the empty catalog is accepted.
                self.target_catalog = Some(check_target_catalog(string_option(value)?)?);
            }
            OptionStatement::Temporary => {
                // Spanner has no temporary tables. The spec default (`false`) is accepted as a
                // no-op so generic clients that always set the option keep working; `true` is
                // rejected as unsupported.
                check_ingest_temporary(value)?;
            }
            OptionStatement::IngestMode => {
                // Append into an existing table, or create it (from the ingest data's Arrow schema,
                // with a synthetic UUID primary key) in the create/replace modes.
                self.ingest_mode = Some(ingest_mode_option(value)?);
            }
            OptionStatement::Other(k) if k == crate::OPTION_INGEST_PRIMARY_KEY => {
                // Comma-separated existing column names; `""` (or all-whitespace) unsets, back to the
                // synthetic key. Column existence and Spanner key-type validity are checked when the
                // CREATE TABLE is built (`bind::create_table_sql`) / by Spanner at DDL time.
                let cols: Vec<String> = string_option(value)?
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
                self.ingest_primary_key = (!cols.is_empty()).then_some(cols);
            }
            OptionStatement::Other(k) if k == crate::OPTION_INGEST_BATCH_WRITE => {
                self.ingest_batch_write = ingest_batch_write_option(value)?;
            }
            OptionStatement::Other(k) if k == crate::OPTION_BIND_BY_NAME => {
                self.bind_by_name =
                    crate::options::bool_option(value, "option adbc.statement.bind_by_name")?;
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
            // Every remaining `spanner.*` option the statement and connection dispatch identically —
            // staleness, request priority/tag, directed read, max_commit_delay, commit_stats, query
            // optimizer opts, `spanner.max_timestamp_precision` (`""` resets to the driver default),
            // RPC timeouts, retry tuning — goes through the shared table. An unrecognised key returns
            // `None`, mapped to the same `NotImplemented` as before (so the connection-only
            // `spanner.transaction.tag`, absent from the shared table, stays unsupported here).
            OptionStatement::Other(k) => {
                if self.set_shared_option(k, value)?.is_none() {
                    return Err(not_implemented(&format!("statement option {k}")));
                }
            }
            other => {
                return Err(not_implemented(&format!(
                    "statement option {}",
                    other.as_ref()
                )));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let value = match &key {
            OptionStatement::TargetTable => self.target_table.clone(),
            OptionStatement::TargetDbSchema => self.target_db_schema.clone(),
            OptionStatement::TargetCatalog => self.target_catalog.clone(),
            // Only the spec default (`false`) is ever accepted (see `check_ingest_temporary`), so
            // the driver's state is always `false` — report exactly that.
            OptionStatement::Temporary => Some(false.to_string()),
            // Reported in the spec's canonical `adbc.ingest.mode.*` spelling; unset reports the
            // effective default, `create`.
            OptionStatement::IngestMode => {
                Some(String::from(self.ingest_mode.unwrap_or(IngestMode::Create)))
            }
            // The comma-joined key columns when set; unset (the synthetic key) reports NotFound.
            OptionStatement::Other(k) if k == crate::OPTION_INGEST_PRIMARY_KEY => {
                self.ingest_primary_key.as_ref().map(|cols| cols.join(","))
            }
            // A plain boolean; reports "true"/"false" (the default is "false", write-only txn).
            OptionStatement::Other(k) if k == crate::OPTION_INGEST_BATCH_WRITE => {
                Some(self.ingest_batch_write.to_string())
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
            OptionStatement::Other(k) if k == crate::OPTION_MAX_PARTITIONS => {
                self.max_partitions.map(|n| n.to_string())
            }
            // Every remaining `spanner.*` option the statement and connection report identically —
            // staleness, request priority/tag, directed read, max_commit_delay,
            // commit_stats(.mutation_count), query optimizer opts, max_timestamp_precision, RPC
            // timeouts, retry tuning (each the effective value: the connection's, unless overridden
            // on this statement) — goes through the shared table, which returns the same `NotFound`
            // for an unset (or unknown) key that the fall-through below would.
            OptionStatement::Other(k) => return self.shared_option_string(k),
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
        let what = format!("option {}", key.as_ref());
        crate::options::int_from_stored_string(self.get_option_string(key), &what)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let what = format!("option {}", key.as_ref());
        crate::options::double_from_stored_string(self.get_option_string(key), &what)
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
            self.bound.clear();
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
            if self.is_read_only() {
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
        let sql = crate::sql::strip_trailing_terminators(&sql);
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
        let precision = self.timestamp_precision;
        let bound = self.read_staleness.timestamp_bound()?;
        let statement = self.read_sql_builder(&sql).build();
        let fetch_timeout = self.timeouts.fetch_timeout();
        // Stream the result: `stream_query` fetches the first chunk (settling the schema) and the
        // returned reader converts the rest to Arrow one bounded chunk at a time as it is
        // iterated, with a background task prefetching the next chunk ahead of the consumer.
        // The query timeout bounds the initial execution through that first chunk; the fetch
        // timeout bounds each later chunk inside the prefetch task.
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel,
            with_timeout(
                self.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = crate::staleness::single_use(&client, bound);
                    let result_set = transaction
                        .execute_query(statement)
                        .await
                        .map_err(from_spanner)?;
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

    fn execute_update(&mut self) -> Result<Option<i64>> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
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
            self.bound.clear();
            // DDL does not report an affected-row count (and is never transactional in Spanner, so
            // it always runs immediately rather than buffering).
            return Ok(None);
        }
        if self.is_read_only() {
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
        check_schema_query(&sql)?;
        // Query path only (`check_schema_query` rejected DDL/DML): strip any trailing statement
        // terminator(s), exactly as `execute` does — the PLAN probe runs through the same single-use
        // ExecuteSql surface, which rejects a trailing `;` ("Expected end of input but got `;`"),
        // yet introspection callers routinely append one (e.g. `SELECT current_date;`).
        let sql = crate::sql::strip_trailing_terminators(&sql);
        let client = self.client.clone();
        let bound = self.bound.clone();
        let bind_by_name = self.bind_by_name;
        // The PLAN probe's schema must carry the same timestamp unit as the data `execute` would
        // stream, so the advertised schema and the actual batches can never disagree.
        let precision = self.timestamp_precision;
        // QueryMode::Plan analyses the query and returns its column metadata without scanning
        // any data, so dbt can introspect a model's output columns without wrapping it in a
        // `SELECT ... WHERE false` subquery.
        let plan_builder = self.read_sql_builder(&sql).set_query_mode(QueryMode::Plan);
        // The schema probe is a query execution, so the query timeout bounds it.
        let schema = block_on_cancellable(
            &self.runtime,
            &self.cancel,
            with_timeout(
                self.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = client.single_use().build();
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
        )?;
        Ok((*schema).clone())
    }

    /// Partition this query and return one opaque descriptor per partition, to be executed later by
    /// `Connection::read_partition`.
    ///
    /// # Security
    ///
    /// Each returned descriptor is **opaque but executable**: a versioned JSON envelope
    /// (`{"v":1,"partition":…}`) around the serde form of the client's `Partition`, carrying the
    /// SQL text (inside its `ExecuteSqlRequest`) plus the session and transaction identity. Anyone
    /// who can hand a descriptor to `Connection::read_partition` can run arbitrary SQL with that
    /// connection's credentials — the version envelope guards against format drift between driver
    /// versions, it does **not** authenticate the blob. Treat descriptors as executable request
    /// blobs, not opaque data:
    /// transport them only over trusted channels and never accept one from an untrusted source.
    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        let sql = self.sql()?;
        if crate::sql::is_ddl(&sql) {
            return Err(invalid_state(
                "execute_partitions is only valid for queries",
            ));
        }
        // Query path (not DDL): strip any trailing statement terminator(s), exactly as `execute`
        // does — both the PLAN probe and `partition_query` run through the same ExecuteSql surface,
        // which rejects a trailing `;`, yet callers routinely append one.
        let sql = crate::sql::strip_trailing_terminators(&sql);
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
        // The advertised schema carries this statement's timestamp precision. Note the partitions
        // themselves are decoded by `Connection::read_partition` under the **reading** connection's
        // `spanner.max_timestamp_precision`, so set the two to the same mode.
        let precision = self.timestamp_precision;
        // The partitioned read honours the statement's read staleness: it is baked into the batch
        // read-only transaction, so every partition executes at that bound wherever it is read back.
        let bound = self.read_staleness.timestamp_bound()?;

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
                tokens.push(crate::connection::encode_partition(&partition)?);
            }
            Ok::<_, Error>((schema, tokens))
        };
        let (schema, partitions) = block_on_cancellable(
            &self.runtime,
            &self.cancel,
            with_timeout(
                self.timeouts.query_timeout(),
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
        let fields: Vec<Field> = crate::sql::named_parameters(&sql)
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
        // A SQL query and a bulk-ingest target are mutually exclusive: setting a query clears any
        // ingest target left on a reused handle, so `execute`/`execute_update` run this query rather
        // than re-entering the (now data-less) ingest branch. See the matching clear in `set_option`.
        self.target_table = None;
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
    crate::options::string_option(value, "statement option")
}

/// Guard for `execute_schema`: only queries can be planned. The PLAN probe runs in a single-use
/// read-only transaction, and letting DML reach it surfaces Spanner's raw "DML statements can only
/// be performed in a read-write transaction" error, which misleads the caller into thinking the
/// transaction mode is the problem. Catch DDL and DML up front with a clear message instead. (This
/// also covers `THEN RETURN` DML — it does produce rows, but Spanner cannot plan it read-only.)
fn check_schema_query(sql: &str) -> Result<()> {
    if crate::sql::is_ddl(sql) {
        return Err(invalid_state("execute_schema is only valid for queries"));
    }
    if crate::sql::is_dml(sql) {
        return Err(invalid_argument(
            "execute_schema only supports queries: DML (INSERT/UPDATE/DELETE) cannot be planned \
             in a read-only schema probe; run it via execute or execute_update instead",
        ));
    }
    Ok(())
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

/// Parse the `adbc.ingest.mode` option into an [`IngestMode`], accepting both the spec's canonical
/// `adbc.ingest.mode.*` spellings and the bare short forms (`append`, `create`, …). Unknown modes
/// are rejected here — at `set_option` time — which is what lets the ingest paths
/// ([`SpannerStatement::build_ingest_table_ddl`]) match the enum exhaustively, with no fallback arm
/// to drift.
fn ingest_mode_option(value: OptionValue) -> Result<IngestMode> {
    use adbc_core::constants::{
        ADBC_INGEST_OPTION_MODE_APPEND, ADBC_INGEST_OPTION_MODE_CREATE,
        ADBC_INGEST_OPTION_MODE_CREATE_APPEND, ADBC_INGEST_OPTION_MODE_REPLACE,
    };
    match string_option(value)?.as_str() {
        ADBC_INGEST_OPTION_MODE_APPEND | "append" => Ok(IngestMode::Append),
        ADBC_INGEST_OPTION_MODE_CREATE | "create" => Ok(IngestMode::Create),
        ADBC_INGEST_OPTION_MODE_CREATE_APPEND | "create_append" => Ok(IngestMode::CreateAppend),
        ADBC_INGEST_OPTION_MODE_REPLACE | "replace" => Ok(IngestMode::Replace),
        other => Err(not_implemented(&format!("ingest mode {other:?}"))),
    }
}

/// Validate the `adbc.ingest.temporary` option. Spanner has no temporary tables, so only the spec
/// default (`false`, in any of the shared boolean spellings) is accepted — as a no-op; `true` is
/// rejected as unsupported.
fn check_ingest_temporary(value: OptionValue) -> Result<()> {
    if bool_option(value)? {
        Err(not_implemented(
            "temporary ingest target tables: Spanner has no temporary tables",
        ))
    } else {
        Ok(())
    }
}

/// Parse a boolean statement option, accepted as a bool-ish string (`true`/`false`/`1`/`0`/…) or an
/// integer (`0` = false, non-zero = true).
fn bool_option(value: OptionValue) -> Result<bool> {
    crate::options::bool_option(value, "option")
}

/// Parse the `spanner.ingest.batch_write` statement option. Like the driver's other unset-able
/// booleans (`spanner.commit_stats`), an empty/whitespace string unsets it (back to `false`, the
/// write-only-transaction path); otherwise it is a bool-ish value.
fn ingest_batch_write_option(value: OptionValue) -> Result<bool> {
    match &value {
        OptionValue::String(s) if s.trim().is_empty() => Ok(false),
        _ => crate::options::bool_option(value, "option spanner.ingest.batch_write"),
    }
}

/// Parse a positive `max_partitions` option, accepted as either an integer or a numeric string.
fn max_partitions_option(value: OptionValue) -> Result<i64> {
    crate::options::positive_i64(value, "max_partitions")
}

/// Parse a positive batch-size option, accepted as either an integer or a numeric string.
fn rows_per_batch_option(value: OptionValue) -> Result<usize> {
    crate::options::positive_usize(value, "rows_per_batch")
}

/// Annotate a failed chunk commit of a multi-chunk autocommit ingest with the number of rows the
/// earlier chunks have already committed.
///
/// Each chunk commits in its own transaction (see
/// [`SpannerStatement::run_ingest_mutations`]), so a mid-ingest failure leaves the earlier chunks'
/// rows in the table. The count is known exactly — it is the sum of the committed chunk sizes —
/// and reporting it tells the caller what state the table was left in instead of making them
/// guess. A failure in the first (or only) chunk committed nothing and passes through unchanged.
/// The status and `vendor_code` are preserved, so callers still branch on the underlying failure
/// (e.g. `AlreadyExists` for a duplicate primary key).
fn note_rows_already_committed(error: Error, committed: i64) -> Error {
    if committed == 0 {
        return error;
    }
    let mut annotated = err(
        format!(
            "{} ({committed} row(s) from this bulk ingest's earlier chunks were already \
             committed and remain in the table)",
            error.message
        ),
        error.status,
    );
    annotated.vendor_code = error.vendor_code;
    annotated
}

/// Whether `error` is Spanner's specific "this commit has too many mutations" rejection — the one
/// error the autocommit bulk-ingest write-only path treats as recoverable by splitting the failing
/// chunk and retrying its halves (see [`SpannerStatement::write_mutation_range`]).
///
/// Deliberately narrow. Spanner reports the per-commit mutation-count limit as an `INVALID_ARGUMENT`
/// whose message reads "The transaction contains too many mutations. …Please reduce the number of
/// writes, or use fewer indexes. (Maximum number: N)" — a phrasing that has stayed stable across the
/// successive 20k→40k→80k limit bumps and names the exact cause (index entries) this backstop
/// targets. We match on the ADBC [`Status::InvalidArguments`] **and** that anchor phrase so that no
/// *other* `INVALID_ARGUMENT` — a malformed value, an unparseable literal, a schema mismatch — is
/// ever mistaken for it and silently bisected; those must keep propagating so the ingest
/// append/create remaps and [`note_rows_already_committed`] still fire. The numeric gRPC code stays
/// available in the error's `vendor_code` (INVALID_ARGUMENT = 3). The companion commit-size limit
/// (~100 MB / the gRPC request-size cap) is intentionally **not** matched here: the byte budget
/// ([`INGEST_CHUNK_BYTE_BUDGET`]) already keeps chunks well under it, and its "request too large"
/// wording is far less stable than the mutation-count phrasing.
fn is_mutation_limit_exceeded(error: &Error) -> bool {
    error.status == Status::InvalidArguments
        && error
            .message
            .to_ascii_lowercase()
            .contains("too many mutations")
}

/// Per-chunk mutation budget for bulk ingest. Spanner caps a single commit at ~80,000 mutations,
/// and an insert mutation counts roughly its column count **plus** secondary-index entries the
/// driver cannot see, so the budget stays at a quarter of the cap to leave headroom for indexed
/// tables.
const INGEST_CHUNK_MUTATION_LIMIT: u64 = 20_000;

/// Per-chunk approximate byte budget for bulk ingest: well under both Spanner's ~100 MB commit cap
/// and typical gRPC request-size limits (~10 MB), with headroom because the per-row estimate
/// ([`IngestChunkBudget`]) is approximate.
const INGEST_CHUNK_BYTE_BUDGET: u64 = 4 * 1024 * 1024;

/// Budgets the rows of one bulk-ingest commit chunk against Spanner's per-commit limits.
///
/// Each row costs its column count in mutations and an approximate byte size; a chunk is cut when
/// the next row no longer [`fits`](Self::fits) under [`INGEST_CHUNK_MUTATION_LIMIT`] and
/// [`INGEST_CHUNK_BYTE_BUDGET`]. Pure arithmetic — unit-tested offline below.
#[derive(Default)]
struct IngestChunkBudget {
    rows: u64,
    mutations: u64,
    bytes: u64,
}

impl IngestChunkBudget {
    /// Whether a `columns`-wide row of approximately `row_bytes` bytes still fits in the current
    /// chunk. The first row always fits, so a single row larger than the whole budget still forms
    /// its own one-row chunk (never an empty chunk or an infinite loop).
    fn fits(&self, columns: usize, row_bytes: usize) -> bool {
        self.rows == 0
            || (self.mutations.saturating_add(columns as u64) <= INGEST_CHUNK_MUTATION_LIMIT
                && self.bytes.saturating_add(row_bytes as u64) <= INGEST_CHUNK_BYTE_BUDGET)
    }

    /// Record a row as added to the current chunk.
    fn add(&mut self, columns: usize, row_bytes: usize) {
        self.rows += 1;
        self.mutations = self.mutations.saturating_add(columns as u64);
        self.bytes = self.bytes.saturating_add(row_bytes as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    /// Simulate the [`SpannerStatement::run_ingest_mutations`] chunk loop for `rows` uniform rows
    /// of `columns` columns and ~`row_bytes` bytes each, returning the chunk boundaries as
    /// per-chunk row counts.
    fn chunk_lengths(rows: usize, columns: usize, row_bytes: usize) -> Vec<usize> {
        let mut lengths = Vec::new();
        let mut current = 0_usize;
        let mut budget = IngestChunkBudget::default();
        for _ in 0..rows {
            if !budget.fits(columns, row_bytes) {
                lengths.push(current);
                current = 0;
                budget = IngestChunkBudget::default();
            }
            current += 1;
            budget.add(columns, row_bytes);
        }
        if current > 0 {
            lengths.push(current);
        }
        lengths
    }

    #[test]
    fn ingest_chunks_cut_at_the_mutation_limit() {
        // 10-column rows of negligible byte size: the mutation budget binds, so the boundary falls
        // at LIMIT / columns rows per chunk (2,000 with the current 20,000 budget).
        let per_chunk = (INGEST_CHUNK_MUTATION_LIMIT / 10) as usize;
        assert_eq!(chunk_lengths(per_chunk, 10, 1), vec![per_chunk]);
        assert_eq!(chunk_lengths(per_chunk + 1, 10, 1), vec![per_chunk, 1]);
        assert_eq!(
            chunk_lengths(3 * per_chunk - 1, 10, 1),
            vec![per_chunk, per_chunk, per_chunk - 1]
        );
        // A column count that doesn't divide the limit still stays under it: 20,000 / 3 = 6,666.
        assert_eq!(chunk_lengths(6_667, 3, 1), vec![6_666, 1]);
    }

    #[test]
    fn ingest_chunks_cut_at_the_byte_budget() {
        // 1 MiB rows: the byte budget binds long before the mutation budget does.
        let mib = 1024 * 1024;
        let per_chunk = (INGEST_CHUNK_BYTE_BUDGET / mib as u64) as usize;
        assert_eq!(chunk_lengths(per_chunk, 2, mib), vec![per_chunk]);
        assert_eq!(
            chunk_lengths(2 * per_chunk + 1, 2, mib),
            vec![per_chunk, per_chunk, 1]
        );
    }

    #[test]
    fn ingest_chunks_never_starve_on_an_oversized_row() {
        // A single row larger than the whole budget (in both dimensions, with saturating cost
        // arithmetic) still forms its own one-row chunk instead of an empty chunk / infinite loop.
        assert_eq!(chunk_lengths(3, usize::MAX, usize::MAX), vec![1, 1, 1]);
        // Zero-cost rows never cut: everything fits in one chunk.
        assert_eq!(chunk_lengths(100_000, 0, 0), vec![100_000]);
    }

    #[test]
    fn ingest_of_zero_rows_emits_no_chunks() {
        // Bound batches holding no rows at all (e.g. a stream of zero-row batches) must produce no
        // commit chunk — the trailing `write_mutation_chunk` guards the empty case, so nothing is
        // sent to Spanner.
        assert_eq!(chunk_lengths(0, 10, 100), Vec::<usize>::new());
    }

    #[test]
    fn mid_ingest_failure_notes_committed_rows() {
        let source = || {
            let mut e = err("Spanner error: row already exists", Status::AlreadyExists);
            e.vendor_code = 6; // gRPC ALREADY_EXISTS
            e
        };
        // First-chunk failure: nothing was committed, the error passes through untouched.
        let untouched = note_rows_already_committed(source(), 0);
        assert_eq!(untouched.message, "Spanner error: row already exists");
        assert_eq!(untouched.status, Status::AlreadyExists);
        // Later-chunk failure: the exact committed row count is reported, and the status and
        // vendor_code survive so callers still branch on the underlying failure.
        let annotated = note_rows_already_committed(source(), 4_000);
        assert!(
            annotated
                .message
                .contains("4000 row(s) from this bulk ingest's earlier chunks were already"),
            "{}",
            annotated.message
        );
        assert!(
            annotated.message.contains("row already exists"),
            "the original failure must stay in the message: {}",
            annotated.message
        );
        assert_eq!(annotated.status, Status::AlreadyExists);
        assert_eq!(annotated.vendor_code, 6);
    }

    #[test]
    fn mutation_limit_predicate_matches_only_the_too_many_mutations_error() {
        // The real Spanner rejection: INVALID_ARGUMENT with the stable "too many mutations" phrase.
        // `from_spanner` prefixes "Spanner error: ", which the substring match sees through.
        let mut over_limit = err(
            "Spanner error: The transaction contains too many mutations. Insert and update \
             operations count with the multiplicity of the number of columns they affect. …Please \
             reduce the number of writes, or use fewer indexes. (Maximum number: 80000)",
            Status::InvalidArguments,
        );
        over_limit.vendor_code = 3; // INVALID_ARGUMENT
        assert!(is_mutation_limit_exceeded(&over_limit));

        // Right phrase, wrong status: only an INVALID_ARGUMENT is the mutation-limit rejection.
        let mut wrong_status = over_limit.clone();
        wrong_status.status = Status::Internal;
        assert!(!is_mutation_limit_exceeded(&wrong_status));

        // Other INVALID_ARGUMENTs must NOT bisect — they have to propagate so the append/create
        // remaps and the committed-rows annotation still fire.
        for message in [
            "Spanner error: Invalid value for column Foo: expected INT64",
            "Spanner error: Syntax error: Unexpected token",
            "Spanner error: The commit request is too large",
            "Spanner error: Table not found: Nope",
        ] {
            let other = err(message, Status::InvalidArguments);
            assert!(
                !is_mutation_limit_exceeded(&other),
                "must not match: {message}"
            );
        }

        // A duplicate primary key (AlreadyExists) — the most important non-match — never bisects.
        let mut dup = err("Spanner error: row already exists", Status::AlreadyExists);
        dup.vendor_code = 6;
        assert!(!is_mutation_limit_exceeded(&dup));
    }

    #[test]
    fn accepts_only_the_empty_ingest_catalog() {
        // Spanner's single, unnamed catalog is accepted and preserved for round-tripping.
        assert_eq!(check_target_catalog(String::new()).unwrap(), "");
        // Any named catalog is rejected as unsupported.
        let error = check_target_catalog("main".to_string()).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
    }

    #[test]
    fn ingest_mode_parses_both_spellings_and_rejects_unknown() {
        // Both the spec's canonical `adbc.ingest.mode.*` spelling and the bare short form parse to
        // the same mode, and the mode reports back (`get_option`) in canonical form.
        for (canonical, short, mode) in [
            ("adbc.ingest.mode.append", "append", IngestMode::Append),
            ("adbc.ingest.mode.create", "create", IngestMode::Create),
            (
                "adbc.ingest.mode.create_append",
                "create_append",
                IngestMode::CreateAppend,
            ),
            ("adbc.ingest.mode.replace", "replace", IngestMode::Replace),
        ] {
            for spelling in [canonical, short] {
                assert_eq!(
                    ingest_mode_option(OptionValue::String(spelling.into())).unwrap(),
                    mode,
                    "spelling {spelling:?}"
                );
            }
            assert_eq!(String::from(mode), canonical);
        }
        // Unknown modes are rejected at set_option time, as unimplemented.
        let error = ingest_mode_option(OptionValue::String("upsert".into())).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        assert!(error.message.contains("ingest mode \"upsert\""), "{error}");
        // Non-string values fail string coercion.
        let error = ingest_mode_option(OptionValue::Int(1)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn ingest_temporary_accepts_false_and_rejects_true() {
        // The spec default (`false`, in any accepted boolean spelling) is a no-op.
        for falsy in ["false", "FALSE", "0", "no"] {
            check_ingest_temporary(OptionValue::String(falsy.into())).unwrap();
        }
        check_ingest_temporary(OptionValue::Int(0)).unwrap();
        // Spanner has no temporary tables: any truthy value is rejected as unimplemented.
        for truthy in ["true", "TRUE", "1", "yes"] {
            let error = check_ingest_temporary(OptionValue::String(truthy.into())).unwrap_err();
            assert_eq!(error.status, Status::NotImplemented);
        }
        let error = check_ingest_temporary(OptionValue::Int(1)).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        // Malformed values fail boolean coercion, not the temporary-table check.
        let error = check_ingest_temporary(OptionValue::String("maybe".into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn ingest_batch_write_option_coerces_and_unsets_on_empty() {
        // Every accepted truthy / falsy spelling coerces, as a string or an int.
        for truthy in ["true", "TRUE", "1", "yes"] {
            assert!(ingest_batch_write_option(OptionValue::String(truthy.into())).unwrap());
        }
        for falsy in ["false", "FALSE", "0", "no"] {
            assert!(!ingest_batch_write_option(OptionValue::String(falsy.into())).unwrap());
        }
        assert!(ingest_batch_write_option(OptionValue::Int(1)).unwrap());
        assert!(!ingest_batch_write_option(OptionValue::Int(0)).unwrap());
        // Empty / whitespace unsets it, back to the default (false) — never an error.
        for empty in ["", "   "] {
            assert!(!ingest_batch_write_option(OptionValue::String(empty.into())).unwrap());
        }
        // A non-bool string is rejected with InvalidArguments (the shared boolean coercion).
        let error = ingest_batch_write_option(OptionValue::String("maybe".into())).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn execute_schema_guard_rejects_ddl_and_dml() {
        // Queries — plain, CTE, parenthesised, statement-hinted — pass through to the PLAN probe.
        for sql in [
            "SELECT 1",
            "WITH cte AS (SELECT 1 AS a) SELECT a FROM cte",
            "(SELECT 1)",
            "@{USE_ADDITIONAL_PARALLELISM=true} SELECT 1",
            "GRAPH g MATCH (n) RETURN n.id",
        ] {
            check_schema_query(sql).unwrap_or_else(|e| panic!("query should pass: {sql}: {e}"));
        }
        // DDL is rejected up front (unchanged behaviour).
        let error = check_schema_query("CREATE TABLE t (id INT64) PRIMARY KEY (id)").unwrap_err();
        assert_eq!(error.status, Status::InvalidState);
        // DML — in any spelling, hinted, or with THEN RETURN — gets a clear `InvalidArguments`
        // instead of Spanner's raw read-only-transaction error from the PLAN probe.
        for sql in [
            "INSERT INTO t (id) VALUES (1)",
            "update t set c = 1 where true",
            "Delete From t Where true",
            "/* comment */ INSERT INTO t (id) VALUES (1)",
            "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true",
            "INSERT INTO t (id) VALUES (1) THEN RETURN id",
        ] {
            let error = check_schema_query(sql).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{sql}");
            assert!(
                error.message.contains("only supports queries"),
                "unexpected message for {sql}: {}",
                error.message
            );
        }
    }

    #[test]
    fn string_option_requires_a_string_value() {
        assert_eq!(
            string_option(OptionValue::String("hi".into())).unwrap(),
            "hi"
        );
        // A non-string value kind is rejected as an invalid argument.
        for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
            let error = string_option(value).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
    }

    #[test]
    fn bool_option_parses_bool_ish_strings_and_ints() {
        // Accepted truthy / falsy spellings, case-insensitive.
        for truthy in ["true", "TRUE", "True", "1", "yes", "YES"] {
            assert!(bool_option(OptionValue::String(truthy.into())).unwrap());
        }
        for falsy in ["false", "FALSE", "False", "0", "no", "NO"] {
            assert!(!bool_option(OptionValue::String(falsy.into())).unwrap());
        }
        // Integers: zero is false, any non-zero is true.
        assert!(!bool_option(OptionValue::Int(0)).unwrap());
        assert!(bool_option(OptionValue::Int(1)).unwrap());
        assert!(bool_option(OptionValue::Int(-1)).unwrap());
    }

    #[test]
    fn bool_option_rejects_non_bool_values() {
        // A string that is not a recognised boolean spelling.
        for bad in ["maybe", "", "2", "t", "on"] {
            let error = bool_option(OptionValue::String(bad.into())).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
        // A non-string, non-int value kind is rejected outright.
        let error = bool_option(OptionValue::Double(1.0)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn bind_by_name_option_parses_as_a_boolean_naming_the_option() {
        // The option is a plain boolean parsed by the shared `bool_option`; an invalid value is
        // rejected with `InvalidArguments`, and the error names the option.
        let what = "option adbc.statement.bind_by_name";
        assert!(crate::options::bool_option(OptionValue::String("true".into()), what).unwrap());
        assert!(!crate::options::bool_option(OptionValue::String("false".into()), what).unwrap());
        assert!(crate::options::bool_option(OptionValue::Int(1), what).unwrap());
        let error =
            crate::options::bool_option(OptionValue::String("maybe".into()), what).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("adbc.statement.bind_by_name"),
            "{}",
            error.message
        );
    }

    #[test]
    fn max_partitions_option_accepts_positive_ints_and_strings() {
        assert_eq!(max_partitions_option(OptionValue::Int(4)).unwrap(), 4);
        assert_eq!(
            max_partitions_option(OptionValue::String("16".into())).unwrap(),
            16
        );
    }

    #[test]
    fn max_partitions_option_rejects_non_positive_and_malformed() {
        // Zero and negatives are not positive partition counts.
        for bad in [OptionValue::Int(0), OptionValue::Int(-1)] {
            let error = max_partitions_option(bad).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
        // Strings must parse to a positive integer.
        for bad in ["0", "-3", "abc", "1.5", ""] {
            let error = max_partitions_option(OptionValue::String(bad.into())).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
        // A non-int, non-string value kind is rejected.
        let error = max_partitions_option(OptionValue::Double(2.0)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }

    #[test]
    fn rows_per_batch_option_accepts_positive_ints_and_strings() {
        assert_eq!(rows_per_batch_option(OptionValue::Int(1)).unwrap(), 1);
        assert_eq!(
            rows_per_batch_option(OptionValue::String("8192".into())).unwrap(),
            8192
        );
    }

    #[test]
    fn rows_per_batch_option_rejects_zero_negative_and_malformed() {
        // Zero is explicitly invalid (a batch must hold at least one row).
        let error = rows_per_batch_option(OptionValue::Int(0)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        // Negatives fail the `usize::try_from` / positivity filter.
        let error = rows_per_batch_option(OptionValue::Int(-8192)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        // Strings must parse to a positive integer.
        for bad in ["0", "-1", "abc", "1.5", ""] {
            let error = rows_per_batch_option(OptionValue::String(bad.into())).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
        // A non-int, non-string value kind is rejected.
        let error = rows_per_batch_option(OptionValue::Double(3.0)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

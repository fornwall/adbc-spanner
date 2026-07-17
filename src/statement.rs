//! The [`SpannerStatement`] — an ADBC statement that runs SQL against Spanner and returns Arrow.
//!
//! A statement holds a SQL string set via [`Statement::set_sql_query`]. Calling
//! [`Statement::execute`] runs it as a query in a single-use read-only transaction and returns a
//! streaming Arrow [`RecordBatchReader`]: rows are pulled from Spanner and converted to Arrow in
//! bounded chunks (see [`OPTION_ROWS_PER_BATCH`](crate::OPTION_ROWS_PER_BATCH)) as the consumer
//! iterates, so a large result set is not fully materialised in memory. Calling
//! [`Statement::execute_update`] runs DML inside a read/write transaction and returns the number
//! of affected rows, and routes DDL to the admin API. SQL that is neither (a query — `adbc.h`
//! sanctions executing any statement without expecting a result set) runs through the same
//! read-only query machinery as `execute`, with the rows drained and discarded and no count
//! (`None`) reported.
//!
//! DML with a `THEN RETURN` clause returns rows: through [`Statement::execute`] they come back as
//! an Arrow result (running via `ExecuteSql` in a read/write transaction, since `ExecuteBatchDml`
//! does not support `THEN RETURN`); through [`Statement::execute_update`] the rows are discarded
//! and the affected-row count is reported from the result-set stats.

use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use google_cloud_lro::Poller as _;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::PartitionOptions;
use google_cloud_spanner::model::execute_sql_request::QueryMode;
use google_cloud_spanner::mutation::{Mutation, MutationGroup};
use google_cloud_spanner::statement::{Statement as SpannerSql, StatementBuilder};
use google_cloud_spanner::transaction::{MultiUseReadOnlyTransaction, ReadWriteTransaction};

use crate::bind;
use crate::connection::{SharedTxn, TxnKind, apply_isolation, lock_txn, write_mutations_txn};
use crate::conversion::{
    BoundStatementSource, TimestampPrecision, result_set_to_batch, stream_bound_query, stream_query,
};
use crate::driver::SharedDatabaseAdmin;
use crate::error::{
    err, from_builder, from_spanner, from_status_parts, invalid_argument, invalid_state,
    not_implemented,
};
use crate::options::{
    SharedConfig, bool_option, impl_shared_option_dispatch, impl_typed_option_getters,
};
use crate::runtime::{CancelSlot, SharedRuntime, block_on_cancellable};
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
    /// (which key off `bound` being non-empty) nor is mistaken for a bound parameter row. It is
    /// consumed **only** by the bulk-ingest paths: to build the target table from the schema in the
    /// create/replace modes, and to permit a zero-row ingest. Cleared whenever `bound` is (re)set.
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
    /// Per-operation cancellation for this statement (see [`Statement::cancel`]): each execution
    /// entry point mints a fresh [`crate::runtime::CancelSignal`] here, and `cancel()` latches the
    /// current one — forever, so a cancelled streamed reader stays cancelled even after this
    /// statement starts a new operation.
    cancel: CancelSlot,
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
            ingest_primary_key: None,
            ingest_batch_write: false,
            bind_by_name: false,
            rows_per_batch: DEFAULT_ROWS_PER_BATCH,
            data_boost: false,
            cancel: CancelSlot::new(),
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
    /// *or* a stale empty-stream ingest schema to a later, unrelated execution. (The `bind` /
    /// `bind_stream` setters overwrite both directly; the `set_sql_query` / ingest-option setters
    /// deliberately leave bound data intact, since binding may precede setting the destination.)
    fn clear_bound(&mut self) {
        self.bound.clear();
        self.ingest_schema = None;
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
            .bound_ingest_schema()
            .ok_or_else(|| invalid_state("cannot create the ingest table: no data is bound"))?;
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

    /// Whether `table` exists in the ingest target schema (`adbc.ingest.target_db_schema`; empty =
    /// Spanner's default, unnamed schema), via the shared
    /// [`table_exists`](crate::connection::table_exists) probe. Shared by the two ingest error
    /// remaps below.
    fn ingest_table_exists(&self, table: &str) -> Result<bool> {
        crate::connection::table_exists(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            self.config.timeouts.query_timeout(),
            self.target_db_schema.as_deref().unwrap_or(""),
            table,
        )
    }

    /// Remap a failed `append`- or `create_append`-mode bulk ingest onto the statuses the ADBC
    /// bulk-ingest contract mandates.
    ///
    /// Both modes insert into a table that may already exist, so the spec wants their insert
    /// failure remapped: for `append` a missing table is [`Status::NotFound`] and a present one is a
    /// schema mismatch ([`Status::AlreadyExists`]); for `create_append` the `CREATE TABLE IF NOT
    /// EXISTS` step guarantees the table is present, so only the schema-mismatch side can surface
    /// (its spec contract: "error if the table exists, but the schema does not match"). `create` and
    /// `replace` keep the raw insert error — their DDL step already owns the table-existence
    /// contract ([`remap_ingest_create_error`](Self::remap_ingest_create_error)).
    ///
    /// A failure that already carries [`Status::AlreadyExists`] — a bound row duplicating a primary
    /// key already in the table, since insert mutations keep `INSERT` semantics — keeps that status
    /// and just gets the target table's name folded into the message. Any other failure is
    /// reinterpreted from the [`ingest_table_exists`](Self::ingest_table_exists) probe; the original
    /// Spanner error's detail is folded into the message.
    fn remap_ingest_append_error(&self, table: &str, error: Error) -> Error {
        if !matches!(
            self.ingest_mode,
            Some(IngestMode::Append) | Some(IngestMode::CreateAppend)
        ) {
            return error;
        }
        // A driver-side transaction-state rejection (ingesting in a manual transaction that began
        // with a query) is not an insert failure, so the spec's NotFound/AlreadyExists
        // append contract does not apply — it propagates unchanged.
        if error.status == Status::InvalidState {
            return error;
        }
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
            // Pure annotation: this branch only names the table, so the vendor code and forwarded
            // `google.rpc.Status` details carry through. (The probe branches below *reinterpret*
            // the error, deriving a new status from the table's existence, so they keep neither.)
            named.vendor_code = error.vendor_code;
            named.details = error.details;
            return named;
        }
        match self.ingest_table_exists(table) {
            Ok(true) => err(
                format!(
                    "bulk ingest append into table {table:?} failed: the bound data is \
                     incompatible with the existing table's schema ({})",
                    error.message
                ),
                Status::AlreadyExists,
            ),
            Ok(false) => err(
                format!(
                    "bulk ingest append target table {table:?} not found ({})",
                    error.message
                ),
                Status::NotFound,
            ),
            // A failed probe teaches us nothing about the table, so the insert error stands (see
            // `table_exists`).
            Err(_) => error,
        }
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
        if self.config.is_read_only() {
            return Err(invalid_state("cannot ingest: the connection is read-only"));
        }
        // An empty (zero-batch) bound stream still declares a schema (`ingest_schema`), which is
        // enough to create the table and commit zero rows — so only a statement with *neither*
        // bound rows nor an ingest schema has genuinely bound nothing.
        if self.bound.is_empty() && self.ingest_schema.is_none() {
            return Err(invalid_state("cannot ingest: no data has been bound"));
        }
        let result = self.run_bound_ingest(table);
        // Consumed by the attempt either way, including a failed create-mode DDL (see
        // `clear_bound`).
        self.clear_bound();
        result
    }

    /// The body of [`run_ingest`](Self::run_ingest), split out so its caller clears the bound data
    /// on every exit path (success, failed DDL, failed insert) in one place.
    fn run_bound_ingest(&self, table: &str) -> Result<Option<i64>> {
        // Reject a DML-kind ingest inside a manual *query* transaction BEFORE any DDL side effect:
        // DDL is not transaction-aware and runs immediately, so a create/replace-mode ingest would
        // otherwise create (or drop) the table before `run_ingest_mutations`'s kind check rejects
        // it. This guard changes no state; the authoritative check still runs under the txn lock at
        // buffer time.
        //
        // It only closes the race for the single-threaded case: a concurrent statement could fix
        // the transaction to query-kind between this check and the DDL, orphaning the table. Fully
        // closing it would mean holding the connection-wide txn lock across a multi-second admin
        // `UpdateDatabaseDdl` RPC — not worth stalling every other statement. The residual window
        // is exactly the documented "DDL is not transaction-aware / DML–DDL reorder" caveat (see
        // CLAUDE.md, `run_ddl`).
        {
            let txn = lock_txn(&self.txn);
            if !txn.autocommit() {
                txn.check_kind_allowed(TxnKind::Dml)?;
            }
        }
        let ingest_ddl = self.build_ingest_table_ddl(table, self.ingest_mode)?;
        if let Some(ddl) = ingest_ddl {
            self.run_ddl(ddl)
                .map_err(|error| self.remap_ingest_create_error(table, error))?;
        }
        // Each remap gates on the ingest mode itself: the DDL failure above is `create`'s to
        // reinterpret, the insert failure below `append`/`create_append`'s.
        self.run_ingest_mutations(table)
            .map_err(|error| self.remap_ingest_append_error(table, error))
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
        match self.ingest_table_exists(table) {
            Ok(true) => err(
                format!(
                    "bulk ingest create target table {table:?} already exists ({})",
                    error.message
                ),
                Status::AlreadyExists,
            ),
            // An absent table means the DDL failed for some other reason; a failed probe teaches us
            // nothing. Either way the original DDL error stands (see `table_exists`).
            Ok(false) | Err(_) => error,
        }
    }

    /// Ship the bound rows as Spanner **insert mutations**, honouring the connection's transaction
    /// mode and Spanner's per-commit limits.
    ///
    /// Mutations are the `Commit` RPC's native write format: no SQL for Spanner to parse and plan
    /// per row (why they beat per-row `INSERT` DML for bulk loads). Each cell converts through the
    /// same Arrow→Spanner value mapping as parameter binding (see [`bind::insert_mutation`]).
    /// Insert mutations keep `INSERT` semantics — a duplicate primary key fails with
    /// `ALREADY_EXISTS`. (Mutations take no isolation level: Spanner commits blind writes
    /// serializably.)
    ///
    /// **Manual mode** buffers every row's mutation for the next `commit`, which applies them
    /// atomically in the *same* read/write transaction as any buffered DML — Spanner applies
    /// buffered mutations at commit time, after the transaction's DML has executed. Never chunked:
    /// the commit applies the user's whole transaction atomically, so an over-limit manual-mode
    /// ingest fails at commit, as any over-limit user transaction would. Buffering is
    /// **all-or-nothing**: the whole batch is built (outside the transaction lock) before any of
    /// it is buffered, so a row that fails Arrow→Spanner conversion leaves the pending buffer
    /// exactly as it was — a later `commit` never silently applies a partial batch.
    ///
    /// **Autocommit mode** builds and ships the mutations chunk by chunk, each chunk in its own
    /// write-only transaction (with the client's retry/replay protection), returning the ingested
    /// row count summed across chunks. Why chunk: Spanner caps a single commit at ~80,000 mutations
    /// — counted roughly as rows × columns, plus secondary-index entries — and ~100 MB, so one
    /// unchunked commit fails outright once the ingest crosses those cliffs (10k rows × 10 columns
    /// is already there). An ingest that fits [`IngestChunkBudget`]'s conservative budgets still
    /// commits as a single atomic transaction; only one big enough to need several chunks — which
    /// could not have committed as one transaction anyway — loses whole-ingest atomicity, and a
    /// later chunk's failure reports exactly how many rows the earlier chunks committed (see
    /// [`note_rows_already_committed`]). Building per chunk also bounds memory to one chunk of
    /// mutations at a time.
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
        let manual = {
            let txn = lock_txn(&self.txn);
            if txn.autocommit() {
                false
            } else {
                // An ingest is DML-kind work: a transaction that began with a query rejects it
                // up front, before any mutation-building work is done.
                txn.check_kind_allowed(TxnKind::Dml)?;
                true
            }
        };
        if manual {
            // Manual mode: build *every* row's mutation before touching the buffer, and build
            // outside the txn lock. All-or-nothing buffering keeps the commit contract honest — a
            // mid-row conversion failure (e.g. an out-of-range date) must not strand the rows
            // before it for a later `commit` to apply silently. Keeping the O(rows) build out of
            // the connection-wide mutex also avoids stalling concurrent txn-state users and cannot
            // poison the mutex on a panic.
            let rows = self.bound.iter().map(RecordBatch::num_rows).sum();
            let mutations = self.build_range_mutations(&target, 0, rows)?;
            // An empty append buffers nothing and would commit clean, so a missing target table
            // would never surface — probe existence now (as the autocommit path does, and outside
            // the txn lock) so a manual-mode empty append to an absent table still fails NotFound,
            // consistent with what a non-empty manual append surfaces at commit.
            self.check_empty_append_target(table, rows as i64)?;
            let mut txn = lock_txn(&self.txn);
            if !txn.autocommit() {
                // `buffer_mutation` re-checks the DML kind under this lock (a concurrent
                // statement may have fixed the transaction to queries in the unlocked window);
                // a rejection fails the *first* call, before anything is buffered, so
                // all-or-nothing still holds. Its first success fixes the transaction's kind.
                for mutation in mutations {
                    txn.buffer_mutation(mutation)?;
                }
                return Ok(None);
            }
            // The mode flipped to autocommit while the batch was being built (enabling
            // autocommit commits the manual transaction): fall through to the autocommit path
            // below, exactly where a fresh mode check would have routed this ingest.
        }
        // Autocommit: walk the flattened row sequence (all bound batches concatenated), cutting it
        // into commit chunks by `IngestChunkBudget`. A chunk is a contiguous `[start, end)` range
        // rather than a materialised `Vec<Mutation>`; its mutations are (re)built cheaply from the
        // batches on demand (`commit_ingest_range`), so nothing is cloned up front just to enable
        // the write-only path's bisect-and-retry when a chunk overshoots the per-commit cap.
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
        self.check_empty_append_target(table, total)?;
        Ok(Some(total))
    }

    /// Surface [`Status::NotFound`] for an **empty** `append` ingest whose target table is absent.
    ///
    /// A zero-row ingest ships nothing, so the insert error that normally drives
    /// [`remap_ingest_append_error`](Self::remap_ingest_append_error)'s NotFound never fires — yet
    /// the ADBC append contract is NotFound for an absent table regardless of row count. Both ingest
    /// paths call this after a zero-row ingest (`ingested == 0`): the autocommit path after its
    /// (no-op) commit, the manual path before buffering nothing. Only `append` needs it —
    /// `create_append`'s `CREATE TABLE IF NOT EXISTS` guarantees the table exists, and
    /// `create`/`replace` own existence via their own DDL. A probe that itself fails teaches nothing
    /// about the table, so — as everywhere else — the empty ingest just succeeds (see
    /// [`table_exists`](crate::connection::table_exists)).
    fn check_empty_append_target(&self, table: &str, ingested: i64) -> Result<()> {
        if ingested == 0
            && matches!(self.ingest_mode, Some(IngestMode::Append))
            && matches!(self.ingest_table_exists(table), Ok(false))
        {
            return Err(err(
                format!("bulk ingest append target table {table:?} not found"),
                Status::NotFound,
            ));
        }
        Ok(())
    }

    /// Build the insert mutations for the flattened row range `[start, end)` across the bound
    /// batches, mapping each global row index back to its `(batch, row)`.
    ///
    /// The same cheap Arrow→Spanner build the forward path uses ([`bind::insert_mutation`]), so a
    /// bisected retry rebuilds a half's mutations straight from the batches — no `Vec<Mutation>` is
    /// cloned on the happy path solely to keep a copy around for a retry that usually never happens.
    ///
    /// A conversion failure here (e.g. an out-of-range date) on a *later* chunk is annotated by the
    /// autocommit callers with the earlier chunks' committed-row count (COR-6), like a commit
    /// failure, so the `run_ingest` "reports their exact count" contract holds for build errors too.
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
            let mutations = self
                .build_range_mutations(target, start, end)
                .map_err(|e| note_rows_already_committed(e, prior_total))?;
            self.batch_write_chunk(mutations, prior_total)
        } else {
            self.write_mutation_range(target, start, end, prior_total)
        }
    }

    /// Commit the flattened row range `[start, end)` in one write-only transaction, **splitting it
    /// in half and retrying the two halves** if — and only if — Spanner rejects the commit for
    /// exceeding its per-commit mutation limit.
    ///
    /// The forward path sizes chunks by `rows × columns` mutations ([`IngestChunkBudget`]), but the
    /// *true* commit-time count also includes secondary-index entries the driver cannot see, so a
    /// heavily-indexed table can overshoot Spanner's ~80,000-mutation cap even inside a
    /// driver-"safe" chunk. This is the reactive backstop: on that specific error
    /// ([`is_mutation_limit_exceeded`]) the range is bisected and each half retried, down to a
    /// single row. Every **other** error — a duplicate key, a bad value, a timeout, a cancel, an
    /// `ABORTED` — propagates unchanged, so the append/create remaps and
    /// [`note_rows_already_committed`] still fire. A single row that *still* overshoots is
    /// un-splittable, so its error propagates too (no infinite recursion, no empty commit). Like the
    /// multi-chunk ingest, a bisected chunk is **not atomic as a whole**; `prior_total` is threaded
    /// through the recursion so a mid-bisect failure reports every row committed before it.
    fn write_mutation_range(
        &self,
        target: &str,
        start: usize,
        end: usize,
        prior_total: i64,
    ) -> Result<i64> {
        let mutations = self
            .build_range_mutations(target, start, end)
            .map_err(|e| note_rows_already_committed(e, prior_total))?;
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
    /// Delegates to the shared [`write_mutations_txn`] commit (also the mutations-only
    /// manual-commit path): `WriteOnlyTransaction::write` carries the client's replay protection —
    /// on success the mutations were applied exactly once, retrying internally on `ABORTED`. A
    /// commit reports no affected-row count, but each insert mutation is exactly one row, so the
    /// chunk length is the count.
    fn write_mutation_chunk(&self, mutations: Vec<Mutation>) -> Result<i64> {
        let count = mutations.len() as i64;
        write_mutations_txn(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            mutations,
        )?;
        Ok(count)
    }

    /// Apply one ingest chunk through Spanner's **BatchWrite** RPC (the
    /// `spanner.ingest.batch_write` autocommit path), returning the number of rows applied.
    ///
    /// Each row's insert mutation is sent as its own [`MutationGroup`]. BatchWrite applies groups
    /// **independently and non-atomically** — the same "not atomic as a whole" guarantee the
    /// multi-chunk write-only path carries, but now within a chunk too — which is what makes it the
    /// cheaper firehose transport. Each streamed [`BatchWriteResponse`] reports, per group index,
    /// whether it applied: an `OK`/absent status counts those rows as applied, and the first
    /// non-`OK` group status — code, message *and* its `google.rpc.Status` details — becomes the
    /// returned error via [`from_status_parts`] (so a duplicate primary key still surfaces as
    /// `AlreadyExists`, its details reach `Error::details`, and the append/create remaps fire,
    /// exactly as on the write-only path). Because a non-atomic batch may have applied some groups
    /// before the failing one — or before a mid-stream transport error — any error is annotated via
    /// [`note_rows_already_committed`] (COR-5), folding this chunk's `applied` groups (one row each)
    /// into `prior_total` so the count covers earlier chunks *and* this chunk's committed rows.
    ///
    /// The request does carry `spanner.request.priority` and `spanner.transaction.tag`
    /// ([`RequestConfig::apply_to_batch_write`](crate::request::RequestConfig::apply_to_batch_write)).
    /// It has no per-request *commit* options and its response has no commit statistics, so
    /// `spanner.commit.max_delay` and `spanner.commit_stats` do not apply on this path; nor does
    /// `spanner.request.tag`, which Spanner ignores for BatchWrite (documented on
    /// [`OPTION_INGEST_BATCH_WRITE`](crate::OPTION_INGEST_BATCH_WRITE)).
    fn batch_write_chunk(&self, mutations: Vec<Mutation>, prior_total: i64) -> Result<i64> {
        if mutations.is_empty() {
            return Ok(0);
        }
        // One mutation group per row: groups are applied independently, so per-row insert failures
        // (e.g. a duplicate key) do not roll back the rest of the chunk.
        let groups: Vec<MutationGroup> = mutations
            .into_iter()
            .map(|m| MutationGroup::new(vec![m]))
            .collect();
        let transaction = self
            .config
            .request
            .apply_to_batch_write(self.client.batch_write_transaction())
            .build();
        block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.update_timeout(),
                crate::OPTION_RPC_TIMEOUT_UPDATE,
                async move {
                    let mut stream = match transaction.execute_streaming(groups).await {
                        Ok(stream) => stream,
                        // Nothing streamed yet, so only earlier chunks are committed.
                        Err(e) => {
                            return Err(note_rows_already_committed(from_spanner(e), prior_total));
                        }
                    };
                    let mut applied = 0_i64;
                    let mut first_error: Option<Error> = None;
                    while let Some(response) = stream.next().await {
                        // A mid-stream transport error: the groups reported OK so far stay
                        // committed (BatchWrite is non-atomic), so fold `applied` into the count.
                        let response = match response {
                            Ok(response) => response,
                            Err(e) => {
                                return Err(note_rows_already_committed(
                                    from_spanner(e),
                                    prior_total + applied,
                                ));
                            }
                        };
                        // An `OK` (or absent) status means the referenced groups applied; any other
                        // status marks them failed — capture the first such failure to return.
                        match response.status.as_ref().filter(|s| s.code != 0) {
                            None => applied += response.indexes.len() as i64,
                            Some(status) if first_error.is_none() => {
                                first_error = Some(from_status_parts(
                                    status.code,
                                    &status.message,
                                    &status.details,
                                ));
                            }
                            Some(_) => {}
                        }
                    }
                    match first_error {
                        // A failing group is non-atomic with the rest, so the groups that did apply
                        // (this chunk's `applied`, plus earlier chunks) are folded into the count.
                        Some(error) => {
                            Err(note_rows_already_committed(error, prior_total + applied))
                        }
                        None => Ok(applied),
                    }
                },
            ),
        )
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
        let isolation = self.config.isolation.clone();
        let request = self.config.request.clone();
        let retry = self.config.retry;
        // DML with THEN RETURN is a write path: the update timeout bounds the whole transaction.
        let update_timeout = self.config.timeouts.update_timeout();
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
    fn run_dml(&self, sql: &str) -> Result<DmlOutcome> {
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

    /// Run a parameterized query once per bound row, streaming the concatenated results.
    ///
    /// Every bound row executes in **one** read-only snapshot, so the per-row results are mutually
    /// consistent: a single bound row keeps the cheap single-use transaction, while several bound
    /// rows share one multi-use read-only transaction pinned at the statement's read bound. The
    /// bounded-staleness kinds (`max:` / `min:`), which Spanner only accepts on single-use
    /// transactions, are pinned to the most stale timestamp their window allows for the multi-row
    /// case (see [`ReadStaleness::multi_use_timestamp_bound`](crate::staleness::ReadStaleness::multi_use_timestamp_bound)).
    ///
    /// Results stream through the same bounded-chunk machinery as `execute`: rows are converted to
    /// Arrow in chunks of `spanner.rows_per_batch` (plus the byte budget) as the reader is
    /// iterated, never materialised whole.
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
                // return a zero-row reader (COR-9).
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
                    None => {
                        let mut builder = client.read_only_transaction();
                        if let Some(b) = bound {
                            builder = builder.set_timestamp_bound(b);
                        }
                        Arc::new(builder.build().await.map_err(from_spanner)?)
                    }
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
    /// routinely append one (e.g. `SELECT current_date;;;`); the DDL and DML paths go through
    /// `split_statements`, which already drops empty trailing segments, so the stripping is scoped
    /// to the query path and never splits a `;`-batch. Applies the manual-transaction kind guard
    /// ([`ensure_query_allowed`](Self::ensure_query_allowed)), and dispatches to the bound-query
    /// path (consuming the bound rows) when parameter rows are bound.
    ///
    /// In a manual transaction the query runs on the transaction's shared multi-use read-only
    /// transaction ([`manual_read_transaction`](Self::manual_read_transaction)) — opening it if
    /// this is the transaction's first statement — so every query in the transaction observes one
    /// consistent snapshot; in autocommit mode it runs in its own single-use transaction at this
    /// statement's read bound.
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
        // failed bound query must not leave stale rows for a later, unrelated `execute` (COR-12).
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
        // Stream the result: `stream_query` fetches the first chunk (settling the schema) and the
        // returned reader converts the rest to Arrow one bounded chunk at a time as it is
        // iterated, with a background task prefetching the next chunk ahead of the consumer.
        // The query timeout bounds the initial execution through that first chunk; the fetch
        // timeout bounds each later chunk inside the prefetch task.
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
    /// **immediately, regardless of the connection's transaction mode** — like the ADBC BigQuery
    /// driver, which classifies nothing, the driver never gates DDL on the transaction: Spanner
    /// DDL goes through the admin API and is never transactional, so it neither fixes a manual
    /// transaction's kind nor is rejected by it (and it cannot be rolled back).
    ///
    /// DDL is issued through the write/update path, so the update timeout bounds the whole
    /// change — the admin-client build (first DDL on the database's client stack only; the built
    /// client is cached in the shared [`SharedDatabaseAdmin`] cell and cloned thereafter), the
    /// `UpdateDatabaseDdl` call, **and** its long-running operation poll loop (which otherwise
    /// polls without any bound). An expired deadline fails with `Status::Timeout`; unset (the
    /// default) leaves the poll unbounded.
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
        // stay valid after this statement is gone, for `Connection::read_partition`.
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
    /// Queries plan in a single-use read-only transaction, the same probe surface as
    /// `execute_schema`. DML can only be planned inside a read/write transaction (Spanner rejects
    /// it read-only — see `check_schema_query`), so it runs through the transaction runner: the
    /// plan executes nothing, and the transaction commits empty. On a read-only connection the
    /// DML probe is skipped (no read/write transaction just to introspect) and DDL is never
    /// planned (not plannable over ExecuteSql; Spanner DDL takes no query parameters anyway) —
    /// both return an empty map, typing every parameter `Null`. Either probe is bounded by the
    /// query timeout: introspection is a read-shaped operation regardless of the statement's verb.
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
                    let runner = retry
                        .apply_to_runner(request.apply_to_runner(apply_isolation(
                            client.read_write_transaction(),
                            isolation,
                        )))
                        .build()
                        .await
                        .map_err(from_spanner)?;
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
    /// bulk-ingest target. Binding *before* setting `adbc.ingest.target_table` is legal (the bind
    /// and the ingest options may arrive in either order), so this can only be diagnosed at
    /// execution time — and the message names both remedies, instead of the plain "no SQL query
    /// set" error that would hide the missing ingest option.
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
    /// than silently returning a pre-write result (e.g. an `INSERT` followed by `SELECT COUNT(*)`
    /// reporting the *old* count), reject the query up front. Queries in an unset or query-kind
    /// manual transaction, and every query in autocommit mode, pass. A *query* routed through
    /// `execute_update` is guarded identically (both go through
    /// [`execute_query_reader`](Self::execute_query_reader)); the DML path enforces its own kind
    /// when buffering; DDL is not transaction-aware (unguarded); and `execute_schema` (a
    /// `QueryMode::Plan` probe returning no data) has no data-visibility concern.
    fn ensure_query_allowed(&self) -> Result<()> {
        lock_txn(&self.txn).check_kind_allowed(TxnKind::Read)
    }

    /// The shared multi-use read-only transaction of a manual transaction that began (or begins
    /// now) with a query — `None` in autocommit mode, where each query runs in its own single-use
    /// transaction.
    ///
    /// The first data-returning query of a manual transaction builds the transaction — pinned at
    /// this statement's read bound, with the bounded-staleness kinds pinned to their most-stale
    /// legal equivalent, since Spanner accepts those only on single-use transactions (see
    /// [`ReadStaleness::multi_use_timestamp_bound`](crate::staleness::ReadStaleness::multi_use_timestamp_bound)) — and installs it in the shared [`TxnState`]
    /// (fixing the transaction's kind to queries); every later query returns the installed
    /// handle, so all reads in the transaction observe one consistent snapshot. Later statements'
    /// staleness settings are ignored — the transaction is already pinned. Building issues no RPC
    /// (the client's default inline begin folds `BeginTransaction` into the first query), so a
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
            let mut builder = client.read_only_transaction();
            if let Some(b) = bound {
                builder = builder.set_timestamp_bound(b);
            }
            builder.build().await.map_err(from_spanner)
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
                check_ingest_temporary(value)?;
            }
            OptionStatement::Incremental => {
                // Incremental `execute_partitions` is not implemented. The spec default
                // (`false`) is accepted as a no-op so generic clients that always set the option
                // keep working; `true` is rejected as unsupported (the `Temporary` pattern).
                check_exec_incremental(value)?;
            }
            OptionStatement::IngestMode => {
                // Append into an existing table, or create it (from the ingest data's Arrow schema,
                // with a synthetic UUID primary key) in the create/replace modes.
                self.ingest_mode = Some(ingest_mode_option(&key, value)?);
            }
            OptionStatement::Other(k) if k == crate::OPTION_INGEST_PRIMARY_KEY => {
                // Comma-separated existing column names; `""` (or all-whitespace) unsets, back to the
                // synthetic key. Column existence and Spanner key-type validity are checked when the
                // CREATE TABLE is built (`bind::create_table_sql`) / by Spanner at DDL time.
                let cols: Vec<String> = string_option(&key, value)?
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
                self.data_boost = bool_option(value, "option spanner.data_boost")?;
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
            // Same shape: only `false` is ever accepted (see `check_exec_incremental`).
            OptionStatement::Incremental => Some(false.to_string()),
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
            if self.config.is_read_only() {
                return Err(invalid_state(
                    "cannot execute DML: the connection is read-only",
                ));
            }
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
        // any statement without expecting a result set (`ExecuteQuery`'s out-stream may be NULL —
        // "Pass NULL if the client does not expect a result set"), and such a call lands here, so
        // run the query through the same read-only machinery as `execute` — including the
        // manual-transaction read-your-writes guard and every read-side option — then drain and
        // discard the rows: this entry point only reports a count, and a read query has none.
        // Do NOT route it into the DML pipeline: that surfaces a raw `ExecuteBatchDml` error in
        // autocommit mode and buffers the query as pending "DML" in manual mode, poisoning commit.
        if !crate::sql::is_dml(&sql) {
            // A multi-statement `;`-batch whose first statement is not DML is neither a query nor
            // an all-DML batch — reject it up front with a clear message (the DML arm below gets
            // the same check via `build_dml_statements`).
            check_all_dml_batch(&crate::sql::split_statements(&sql))?;
            let reader = self.execute_query_reader(&sql)?;
            drain_discarding_rows(reader)?;
            return Ok(None);
        }
        if self.config.is_read_only() {
            return Err(invalid_state(
                "cannot execute DML: the connection is read-only",
            ));
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
    /// SQL text (inside its `ExecuteSqlRequest`) plus the session and transaction identity. Anyone
    /// who can hand a descriptor to `Connection::read_partition` can run arbitrary SQL with that
    /// connection's credentials — the version envelope guards against format drift between driver
    /// versions, it does **not** authenticate the blob. Treat descriptors as executable request
    /// blobs, not opaque data:
    /// transport them only over trusted channels and never accept one from an untrusted source.
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
        // probe cannot type — DDL (not plannable over ExecuteSql), DML on a read-only connection
        // (planning DML needs a read/write transaction), a parameter whose type the SQL context
        // doesn't pin down, or a failed probe (this is best-effort introspection; the execute
        // paths surface real errors with full context) — is typed `Null`, ADBC's convention for
        // "type cannot be determined" (`AdbcStatementGetParameterSchema` in adbc.h).
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
        Err(not_implemented(
            "Substrait: Spanner does not support Substrait plans",
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        // Latch the current operation's (sticky) signal: an in-flight execution wakes and returns
        // Cancelled, and a cancel landing between two chunk fetches of a streamed result still
        // cancels the next fetch — permanently, since the latch is never cleared. The statement's
        // next operation mints a fresh signal instead, so a cancel with nothing running does not
        // affect later executions, and later executions cannot revive a cancelled reader.
        self.cancel.signal();
        Ok(())
    }
}

/// Parse a plain string statement option, naming `key` in the error (the `driver.rs`
/// `string_value` pattern: the label is the option's own key, so it can never drift — IDIO-7).
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
/// into thinking the transaction mode is the problem. Catch DDL and DML up front with a clear
/// message instead. (This also covers `THEN RETURN` DML — it does produce rows, but Spanner cannot
/// run it read-only.) `dml_rationale` completes "DML (INSERT/UPDATE/DELETE) cannot be …" with the
/// entry point's read-only operation. Both DDL and DML are the same "not a query" class — the
/// caller passed the wrong kind of statement — so both reject with `InvalidArguments` (SPEC-6).
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
/// split across Spanner's different execution surfaces. Reject it up front, naming the offending
/// statement — crucially *before* anything is buffered in a manual transaction, where a poisoned
/// buffer would otherwise fail the eventual commit of the whole batch (recoverable only by
/// `rollback`). A single statement (or empty text) always passes: classifying a lone statement is
/// the caller's concern. (All-DDL batches never reach this — the leading keyword routes them to
/// `run_ddl` first.)
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
fn ingest_mode_option(key: &OptionStatement, value: OptionValue) -> Result<IngestMode> {
    use adbc_core::constants::{
        ADBC_INGEST_OPTION_MODE_APPEND, ADBC_INGEST_OPTION_MODE_CREATE,
        ADBC_INGEST_OPTION_MODE_CREATE_APPEND, ADBC_INGEST_OPTION_MODE_REPLACE,
    };
    match string_option(key, value)?.as_str() {
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
    if bool_option(value, "option adbc.ingest.temporary")? {
        Err(not_implemented(
            "temporary ingest target tables: Spanner has no temporary tables",
        ))
    } else {
        Ok(())
    }
}

/// Validate the `adbc.statement.exec.incremental` option. Incremental `execute_partitions` is not
/// implemented, so only the spec default (`false`, DISABLED, in any of the shared boolean
/// spellings) is accepted — as a no-op; `true` is rejected as unsupported.
fn check_exec_incremental(value: OptionValue) -> Result<()> {
    if bool_option(value, "option adbc.statement.exec.incremental")? {
        Err(not_implemented(
            "incremental statement execution (adbc.statement.exec.incremental)",
        ))
    } else {
        Ok(())
    }
}

/// Parse the `spanner.ingest.batch_write` statement option. Like the driver's other unset-able
/// booleans (`spanner.commit_stats`), an empty/whitespace string unsets it (back to `false`, the
/// write-only-transaction path); otherwise it is a boolean string (exactly `true`/`false`).
fn ingest_batch_write_option(value: OptionValue) -> Result<bool> {
    match &value {
        OptionValue::String(s) if s.trim().is_empty() => Ok(false),
        _ => crate::options::bool_option(value, "option spanner.ingest.batch_write"),
    }
}

/// Parse the positive `spanner.rows_per_batch` option, accepted as either an integer or a numeric
/// string.
fn rows_per_batch_option(value: OptionValue) -> Result<usize> {
    crate::options::positive_usize(value, "option spanner.rows_per_batch")
}

/// Annotate a failed autocommit ingest commit with the number of rows already committed and left in
/// the table.
///
/// Each chunk commits in its own transaction (see
/// [`SpannerStatement::run_ingest_mutations`]), so a mid-ingest failure leaves the earlier chunks'
/// rows in the table. On the write-only path a chunk is atomic, so `committed` is just the earlier
/// chunks; on the non-atomic BatchWrite path it also includes the failing chunk's groups that did
/// apply (COR-5). Either way that count is known exactly, and reporting it tells the caller what
/// state the table was left in instead of making them guess.
///
/// A [`Status::Timeout`]/[`Status::Cancelled`] failure is the exception (CON-5): cancel/timeout
/// *drops* the in-flight `Commit` future, which may still land server-side, so the **failing
/// chunk's own** outcome is unknown — a caller-driven retry could duplicate its rows. There the
/// exact count still covers the earlier work, but the annotation also flags the ambiguity rather
/// than implying the failing chunk committed nothing. Other statuses keep the plain accounting; a
/// first-chunk failure with a known outcome (nothing committed) passes through unchanged.
/// The status and `vendor_code` are preserved, so callers still branch on the underlying failure
/// (e.g. `AlreadyExists` for a duplicate primary key).
fn note_rows_already_committed(error: Error, committed: i64) -> Error {
    let outcome_unknown = matches!(error.status, Status::Timeout | Status::Cancelled);
    if committed == 0 && !outcome_unknown {
        return error;
    }
    let mut note = String::new();
    if committed > 0 {
        note.push_str(&format!(
            "{committed} row(s) from this bulk ingest were already committed and remain in the \
             table"
        ));
    }
    if outcome_unknown {
        if !note.is_empty() {
            note.push_str("; ");
        }
        note.push_str(
            "this chunk's own commit outcome is unknown — it may still have landed server-side, so \
             retrying could duplicate rows",
        );
    }
    let mut annotated = err(format!("{} ({note})", error.message), error.status);
    // Pure annotation, like the append remap's `AlreadyExists` branch: vendor code and forwarded
    // `google.rpc.Status` details survive the rebuilt message.
    annotated.vendor_code = error.vendor_code;
    annotated.details = error.details;
    annotated
}

/// Whether `error` is Spanner's specific "this commit has too many mutations" rejection — the one
/// error the autocommit bulk-ingest write-only path treats as recoverable by splitting the failing
/// chunk and retrying its halves (see [`SpannerStatement::write_mutation_range`]).
///
/// Deliberately narrow. Spanner reports the per-commit mutation-count limit as an `INVALID_ARGUMENT`
/// reading "The transaction contains too many mutations. …Please reduce the number of writes, or use
/// fewer indexes. (Maximum number: N)" — a phrasing stable across the successive 20k→40k→80k limit
/// bumps that names the exact cause (index entries) this backstop targets. Matching on
/// [`Status::InvalidArguments`] **and** that anchor phrase keeps any *other* `INVALID_ARGUMENT` — a
/// malformed value, a schema mismatch — from being silently bisected; those must keep propagating so
/// the ingest append/create remaps and [`note_rows_already_committed`] still fire. The companion
/// commit-size limit (~100 MB / the gRPC request-size cap) is intentionally **not** matched: the byte
/// budget ([`INGEST_CHUNK_BYTE_BUDGET`]) already keeps chunks well under it, and its "request too
/// large" wording is far less stable.
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
            e.details = Some(vec![(
                "google.rpc.errorinfo".to_string(),
                br#"{"reason":"DUPLICATE_KEY"}"#.to_vec(),
            )]);
            e
        };
        // First-chunk failure: nothing was committed, the error passes through untouched.
        let untouched = note_rows_already_committed(source(), 0);
        assert_eq!(untouched.message, "Spanner error: row already exists");
        assert_eq!(untouched.status, Status::AlreadyExists);
        // Later-chunk failure: the exact committed row count is reported, and the status,
        // vendor_code and forwarded details survive so callers still branch on — and diagnose —
        // the underlying failure.
        let annotated = note_rows_already_committed(source(), 4_000);
        assert!(
            annotated
                .message
                .contains("4000 row(s) from this bulk ingest were already committed"),
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
        assert_eq!(annotated.details, source().details);
    }

    #[test]
    fn timed_out_or_cancelled_chunk_reports_unknown_outcome() {
        // CON-5: a cancel/timeout drops the in-flight `Commit` future, which may still land
        // server-side, so the failing chunk's own outcome is unknown — the annotation must flag
        // the ambiguity (and the duplicate-row risk) rather than implying exact accounting.
        for status in [Status::Timeout, Status::Cancelled] {
            // Even a first-chunk failure (nothing counted as committed) must warn, because the
            // failing chunk itself may have landed.
            let first = note_rows_already_committed(err("commit interrupted", status), 0);
            assert_eq!(first.status, status);
            assert!(
                first.message.contains("outcome is unknown")
                    && first.message.contains("duplicate rows"),
                "an interrupted first chunk must flag the ambiguous outcome: {}",
                first.message
            );

            // A later-chunk failure keeps the exact earlier-chunk count *and* flags the failing
            // chunk's unknown outcome.
            let later = note_rows_already_committed(err("commit interrupted", status), 4_000);
            assert!(
                later
                    .message
                    .contains("4000 row(s) from this bulk ingest were already committed"),
                "the exact earlier-chunk count must survive: {}",
                later.message
            );
            assert!(
                later.message.contains("outcome is unknown"),
                "the failing chunk's ambiguity must still be flagged: {}",
                later.message
            );
        }
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
        let key = OptionStatement::IngestMode;
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
                    ingest_mode_option(&key, OptionValue::String(spelling.into())).unwrap(),
                    mode,
                    "spelling {spelling:?}"
                );
            }
            assert_eq!(String::from(mode), canonical);
        }
        // Unknown modes are rejected at set_option time, as unimplemented.
        let error = ingest_mode_option(&key, OptionValue::String("upsert".into())).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        assert!(error.message.contains("ingest mode \"upsert\""), "{error}");
        // Non-string values fail string coercion, naming the option's full key (IDIO-7).
        let error = ingest_mode_option(&key, OptionValue::Int(1)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("adbc.ingest.mode"), "{error}");
    }

    #[test]
    fn ingest_temporary_accepts_false_and_rejects_true() {
        // The spec default (`false`, as the exact string) is a no-op.
        check_ingest_temporary(OptionValue::String("false".into())).unwrap();
        // Spanner has no temporary tables: a truthy value is rejected as unimplemented.
        let error = check_ingest_temporary(OptionValue::String("true".into())).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        // Malformed values fail boolean coercion, not the temporary-table check — the
        // formerly-accepted lenient spellings (COR-7) and int-typed sets (COR-4) alike.
        for bad in ["maybe", "FALSE", "0", "no", "TRUE", "1", "yes"] {
            let error = check_ingest_temporary(OptionValue::String(bad.into())).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{bad}");
        }
        for bad in [OptionValue::Int(0), OptionValue::Int(1)] {
            let error = check_ingest_temporary(bad).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
    }

    #[test]
    fn exec_incremental_accepts_false_and_rejects_true() {
        // The spec default (`false`, as the exact string) is a no-op.
        check_exec_incremental(OptionValue::String("false".into())).unwrap();
        // Incremental execution is not implemented: a truthy value is rejected as such.
        let error = check_exec_incremental(OptionValue::String("true".into())).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        // Malformed values fail boolean coercion, not the incremental check — the
        // formerly-accepted lenient spellings (COR-7) and int-typed sets (COR-4) alike.
        for bad in ["maybe", "FALSE", "0", "no", "TRUE", "1", "yes"] {
            let error = check_exec_incremental(OptionValue::String(bad.into())).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{bad}");
        }
        for bad in [OptionValue::Int(0), OptionValue::Int(1)] {
            let error = check_exec_incremental(bad).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
    }

    #[test]
    fn ingest_batch_write_option_coerces_and_unsets_on_empty() {
        // The accepted boolean spellings coerce: exactly the strings "true"/"false".
        assert!(ingest_batch_write_option(OptionValue::String("true".into())).unwrap());
        assert!(!ingest_batch_write_option(OptionValue::String("false".into())).unwrap());
        // Empty / whitespace unsets it, back to the default (false) — never an error.
        for empty in ["", "   "] {
            assert!(!ingest_batch_write_option(OptionValue::String(empty.into())).unwrap());
        }
        // A non-bool string — including the formerly-accepted lenient spellings (COR-7) — and an
        // int-typed set (COR-4) are rejected with InvalidArguments (the shared boolean coercion).
        for bad in ["maybe", "TRUE", "1", "yes", "FALSE", "0", "no"] {
            let error = ingest_batch_write_option(OptionValue::String(bad.into())).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{bad}");
        }
        let error = ingest_batch_write_option(OptionValue::Int(1)).unwrap_err();
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
        // DDL is rejected up front with the same `InvalidArguments` as DML — both are the "not a
        // query" class (SPEC-6).
        let error = check_schema_query("CREATE TABLE t (id INT64) PRIMARY KEY (id)").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
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
    fn execute_partitions_guard_rejects_ddl_and_dml() {
        // Queries — plain, CTE, parenthesised, statement-hinted — pass through to partitioning.
        for sql in [
            "SELECT 1",
            "WITH cte AS (SELECT 1 AS a) SELECT a FROM cte",
            "(SELECT 1)",
            "@{USE_ADDITIONAL_PARALLELISM=true} SELECT 1",
            "GRAPH g MATCH (n) RETURN n.id",
        ] {
            check_partition_query(sql).unwrap_or_else(|e| panic!("query should pass: {sql}: {e}"));
        }
        // DDL is rejected up front with the same `InvalidArguments` as DML — both are the "not a
        // query" class (SPEC-6).
        let error =
            check_partition_query("CREATE TABLE t (id INT64) PRIMARY KEY (id)").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("execute_partitions"),
            "unexpected message: {}",
            error.message
        );
        // DML — in any spelling, hinted, or with THEN RETURN — gets a clear `InvalidArguments`
        // instead of Spanner's raw read-only-transaction error from `partition_query` (COR-11).
        for sql in [
            "INSERT INTO t (id) VALUES (1)",
            "update t set c = 1 where true",
            "Delete From t Where true",
            "/* comment */ INSERT INTO t (id) VALUES (1)",
            "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true",
            "INSERT INTO t (id) VALUES (1) THEN RETURN id",
        ] {
            let error = check_partition_query(sql).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{sql}");
            assert!(
                error
                    .message
                    .contains("execute_partitions only supports queries"),
                "unexpected message for {sql}: {}",
                error.message
            );
        }
    }

    #[test]
    fn all_dml_batch_guard_rejects_mixed_batches_and_passes_single_statements() {
        let split = |sql: &str| crate::sql::split_statements(sql);
        // Single statements always pass — classification of a lone statement is the caller's
        // concern — as do genuine all-DML batches and empty text.
        for sql in [
            "SELECT 1",
            "INSERT INTO t (id) VALUES (1)",
            "DELETE FROM t WHERE true; INSERT INTO t (id) VALUES (1)",
            "@{PDML_MAX_PARALLELISM=1} DELETE FROM t WHERE true; update t set c = 1 where true",
            "",
        ] {
            check_all_dml_batch(&split(sql)).unwrap_or_else(|e| panic!("should pass: {sql}: {e}"));
        }
        // A multi-statement batch mixing DML with a query or DDL is rejected with
        // InvalidArguments, naming the offending statement — whichever side comes first.
        for (sql, offending) in [
            ("DELETE FROM t WHERE true; SELECT 1", "SELECT 1"),
            ("SELECT 1; DELETE FROM t WHERE true", "SELECT 1"),
            ("DELETE FROM t WHERE true; DROP TABLE t", "DROP TABLE t"),
            ("SELECT 1; SELECT 2", "SELECT 1"),
        ] {
            let error = check_all_dml_batch(&split(sql)).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{sql}");
            assert!(
                error.message.contains("all-DML") && error.message.contains(offending),
                "unexpected message for {sql}: {}",
                error.message
            );
        }
        // A `;` inside a literal is not a separator, so this is a single statement and passes.
        check_all_dml_batch(&split("SELECT 'a;b'")).unwrap();
    }

    #[test]
    fn string_option_requires_a_string_value() {
        let key = OptionStatement::TargetTable;
        assert_eq!(
            string_option(&key, OptionValue::String("hi".into())).unwrap(),
            "hi"
        );
        // A non-string value kind is rejected as an invalid argument, and the error names the
        // offending option's full key rather than a generic "statement option" (IDIO-7).
        for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
            let error = string_option(&key, value).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(
                error.message.contains("adbc.ingest.target_table"),
                "{}",
                error.message
            );
        }
    }

    #[test]
    fn bool_option_parses_exact_true_false() {
        // Only the exact lowercase ADBC canonical spellings are accepted (matching adbc_core's
        // own `TryFrom<OptionValue> for bool` and the reference C++ drivers).
        assert!(bool_option(OptionValue::String("true".into()), "option o").unwrap());
        assert!(!bool_option(OptionValue::String("false".into()), "option o").unwrap());
    }

    #[test]
    fn bool_option_rejects_non_bool_values() {
        // A string that is not exactly "true"/"false" — including the formerly-accepted lenient
        // spellings (case variants, 1/0, yes/no), dropped for ADBC-ecosystem parity (COR-7).
        for bad in [
            "maybe", "", "2", "t", "on", "TRUE", "True", "FALSE", "False", "1", "0", "yes", "no",
            "YES", "NO",
        ] {
            let error = bool_option(OptionValue::String(bad.into()), "option o").unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
        // Non-string value kinds — including int-typed sets (COR-4) — are rejected outright.
        for bad in [
            OptionValue::Int(0),
            OptionValue::Int(1),
            OptionValue::Double(1.0),
        ] {
            let error = bool_option(bad, "option o").unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
    }

    #[test]
    fn bind_by_name_option_parses_as_a_boolean_naming_the_option() {
        // The option is a plain boolean parsed by the shared `bool_option`; an invalid value is
        // rejected with `InvalidArguments`, and the error names the option.
        let what = "option adbc.statement.bind_by_name";
        assert!(crate::options::bool_option(OptionValue::String("true".into()), what).unwrap());
        assert!(!crate::options::bool_option(OptionValue::String("false".into()), what).unwrap());
        for bad in [
            OptionValue::String("maybe".into()),
            // An int-typed set is rejected like any other non-string value (COR-4).
            OptionValue::Int(1),
        ] {
            let error = crate::options::bool_option(bad, what).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(
                error.message.contains("adbc.statement.bind_by_name"),
                "{}",
                error.message
            );
        }
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

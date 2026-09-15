//! The [`SpannerConnection`] — an ADBC connection backed by a Spanner [`DatabaseClient`].
//!
//! ## Transactions
//!
//! By default the connection is in **autocommit** mode: every statement runs in its own Spanner
//! transaction (a single-use read-only transaction for queries, a read/write transaction for DML).
//!
//! Setting `adbc.connection.autocommit` to `false` begins **manual** transaction mode, where a
//! transaction is exactly one of two kinds — **queries** or **DML** — fixed by its *first*
//! statement; a statement of the other kind is rejected with [`Status::InvalidState`] until
//! [`Connection::commit`] or [`Connection::rollback`] ends it:
//!
//! - **Queries**: one **multi-use read-only transaction** carries every query of the transaction,
//!   so all reads observe a single consistent snapshot (pinned at the first query's
//!   `spanner.read.staleness` bound). Commit and rollback are local — a Spanner read-only
//!   transaction needs no RPC, so the snapshot is simply dropped.
//! - **DML**: Spanner's client exposes read/write transactions only through a closure-based runner
//!   (no public begin/commit handle), so the driver *buffers* DML statements — and the insert
//!   **mutations** of any bulk ingest — and applies the whole batch atomically in one read/write
//!   transaction on commit. That also makes retry-on-abort safe: the buffer is simply replayed.
//!
//! **DDL is not transaction-aware**: it always executes immediately through the admin
//! `UpdateDatabaseDdl` API (Spanner DDL is never transactional) and leaves the transaction state
//! untouched, so DDL issued after buffered DML runs before it.
//!
//! The user-facing consequences — no read-your-writes, `None` DML counts before commit, the
//! commit-failure replay semantics and the read-only-connection commit guard — are documented on
//! [`SpannerConnection`] and in
//! [docs/transactions.md](https://github.com/fornwall/adbc-spanner/blob/main/docs/transactions.md).

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use adbc_core::error::{Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{CancelHandle, Connection, Optionable};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use google_cloud_spanner::batch::Partition;
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement as SpannerSql;

use crate::conversion::{TimestampPrecision, result_set_to_batch, stream_query};
use crate::driver::{Connected, SharedDatabaseAdmin};
use crate::error::{
    err, from_spanner, invalid_argument, invalid_state, option_not_set, unknown_option, unsupported,
};
use crate::options::{SharedConfig, impl_shared_option_dispatch, impl_typed_option_getters};
use crate::runtime::{CancelSlot, SharedRuntime, SlotCancelHandle, block_on_cancellable};
use crate::sql::qualified_table;
use crate::statement::{DEFAULT_ROWS_PER_BATCH, SpannerStatement};
use crate::timeout::with_timeout;

mod exec;
mod txn;

use crate::metadata::{check_lookup_catalog, like_match, metadata_sql_builder, table_exists};
pub(crate) use exec::{build_runner, run_batch_dml, run_batch_txn, write_mutations_txn};
use exec::{isolation_to_adbc_string, parse_isolation_level};
use txn::{ManualTxn, check_commit_writable};
pub(crate) use txn::{SharedTxn, TxnKind, TxnState, lock_txn};

/// An ADBC connection to a Spanner database.
///
/// # Transactions
///
/// The connection is in **autocommit** mode by default. Setting `adbc.connection.autocommit` to
/// `false` enters **manual** transaction mode. A manual transaction is exactly one of two kinds
/// — **queries** or **DML** — fixed by its *first* statement; mixing kinds is rejected with
/// [`Status::InvalidState`] until [`Connection::commit`] or [`Connection::rollback`] ends the
/// transaction:
///
/// - **Queries** all run on one shared multi-use read-only transaction (a single consistent
///   snapshot, pinned at the first query's `spanner.read.staleness` bound); commit/rollback
///   simply drop it (Spanner read-only transactions need no commit RPC).
/// - **DML** — and the insert mutations of any bulk ingest — is *buffered* and applied atomically
///   in one read/write transaction on commit, because the Spanner client exposes read/write
///   transactions only through a closure-based runner (no begin/commit handle). A transaction
///   that buffered **only mutations** (bulk ingests, no DML) commits through the client's
///   replay-protected write-only transaction instead, so an ambiguous transport failure cannot
///   double-apply it.
///
/// **DDL is not transaction-aware** (matching the ADBC BigQuery driver): it always executes
/// immediately via the admin API — Spanner DDL is never transactional — and leaves the
/// transaction state untouched, so DDL issued after buffered DML executes *before* it.
///
/// A connection set `adbc.connection.readonly` rejects the *commit* of buffered DML/ingest work
/// too — not just the statements that buffer it — with [`Status::InvalidState`], keeping the
/// transaction replayable; committing a query transaction and [`Connection::rollback`] stay
/// available (neither writes).
///
/// See [docs/transactions.md](https://github.com/fornwall/adbc-spanner/blob/main/docs/transactions.md)
/// for the full model: no read-your-writes, `None` DML counts before commit, and the
/// commit-failure replay semantics.
#[derive(Debug)]
pub struct SpannerConnection {
    runtime: SharedRuntime,
    client: DatabaseClient,
    spanner: Spanner,
    database: String,
    /// The lazily-built Database Admin client for the DDL path, shared (`Arc`) with every statement
    /// the connection creates — and, via the cached [`Connected`] stack, with every other connection
    /// on the same database — so the first DDL statement builds it and later ones clone it (see
    /// [`SharedDatabaseAdmin`]).
    admin: SharedDatabaseAdmin,
    /// Every option-settable value on this connection — the readonly flag, the isolation level, the
    /// read staleness, request/retry/timeout/optimizer config and this connection's commit-stats
    /// cell. Each statement the connection creates starts from a [`SharedConfig::inherit`]ed copy
    /// and may override the fields it exposes; see [`SharedConfig`] for the per-field detail.
    config: SharedConfig,
    txn: SharedTxn,
    /// Per-operation cancellation for this connection's metadata/commit operations (see
    /// [`Connection::get_cancel_handle`]): each entry point mints a fresh [`CancelSignal`] here,
    /// and a cancel latches the current one — forever, so a cancelled `read_partition` stream stays
    /// cancelled even after this connection starts a new operation. Shared through an [`Arc`] so
    /// the [`SlotCancelHandle`]s handed out by `get_cancel_handle` keep targeting the *current*
    /// operation for this connection's whole life.
    cancel: Arc<CancelSlot>,
}

impl SpannerConnection {
    // Shared `set_shared_option` / `shared_option_string` for the "staleness-pattern" options
    // (request priority/tag, directed read, max_commit_delay, commit_stats, query optimizer opts,
    // RPC timeouts, retry tuning, …) that the connection and statement dispatch identically.
    impl_shared_option_dispatch!();

    pub(crate) fn new(runtime: SharedRuntime, connected: Connected) -> Self {
        Self {
            runtime,
            client: connected.client,
            spanner: connected.spanner,
            database: connected.database,
            admin: connected.admin,
            config: SharedConfig::default(),
            txn: Arc::new(Mutex::new(TxnState::new())),
            cancel: Arc::new(CancelSlot::new()),
        }
    }

    /// Apply the buffered work of a manual transaction: DML statements and ingest mutations
    /// atomically in one transaction. A read-only (or empty) transaction has nothing to apply —
    /// its snapshot ends by being dropped when the caller clears the state.
    ///
    /// Rejected outright when the connection is `adbc.connection.readonly` and there *is*
    /// buffered work to apply (see [`check_commit_writable`]); the caller keeps the buffer, so
    /// the transaction stays replayable exactly as after any other failed commit.
    fn apply_manual_txn(&self, work: &ManualTxn) -> Result<()> {
        check_commit_writable(self.config.is_read_only(), work)?;
        match work {
            ManualTxn::Unset | ManualTxn::Read(_) => Ok(()),
            ManualTxn::Dml {
                statements,
                mutations,
            } => self.apply_transaction(statements.clone(), mutations.clone()),
        }
    }

    /// Apply the buffered DML statements and mutations atomically in one transaction, discarding
    /// the affected-row count (a commit reports no count).
    ///
    /// A transaction with DML runs through the read/write runner ([`run_batch_txn`]); a
    /// **mutations-only** transaction (bulk ingests that buffered no DML) commits through the
    /// write-only path ([`write_mutations_txn`]) instead, whose commit is replay-protected —
    /// applied exactly once even across ambiguous transport failures, where a replayed
    /// read/write commit could double-apply (the module-doc caveat).
    fn apply_transaction(
        &self,
        statements: Vec<SpannerSql>,
        mutations: Vec<Mutation>,
    ) -> Result<()> {
        // Mint a fresh cancel signal: a stale cancel cannot leak in, and no later operation can
        // un-cancel this one's streamed reader (see `CancelSlot`).
        self.cancel.begin_operation();
        if statements.is_empty() {
            return write_mutations_txn(
                &self.runtime,
                &self.client,
                &self.cancel.current(),
                &self.config,
                mutations,
            );
        }
        run_batch_txn(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            statements,
            mutations,
            // Manual commit buffers mutations that Spanner applies at commit, so this batch is not
            // the transaction's last request — no `last_statement` optimization here.
            false,
        )?;
        Ok(())
    }

    /// Whether a table exists, via a parameterized `INFORMATION_SCHEMA.TABLES` lookup. The default
    /// (unnamed) schema is the empty string in Spanner. Delegates to the shared [`table_exists`]
    /// probe so the same query serves the connection's introspection and the statement's ingest
    /// error path.
    fn table_exists(&self, db_schema: &str, table_name: &str) -> Result<bool> {
        table_exists(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            self.config.timeouts.query_timeout(),
            db_schema,
            table_name,
        )
    }
}

impl Optionable for SpannerConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::AutoCommit => {
                let enable =
                    crate::options::bool_option(value, "option adbc.connection.autocommit")?;
                // Enabling autocommit commits any active manual transaction. The mode flip and the
                // state take are one lock acquisition (`enter_autocommit`), so nothing a
                // concurrent statement buffers is stranded behind the flip. Like `commit`, a
                // failed apply must not lose the work: `restore_manual` re-enters manual mode with
                // the state restored, so apply from a borrow — the taken state must still be
                // around to restore. (A taken read-only transaction has nothing to apply; dropping
                // it ends the snapshot.)
                let pending = {
                    let mut st = lock_txn(&self.txn);
                    if enable && !st.autocommit {
                        Some(st.enter_autocommit())
                    } else {
                        st.autocommit = enable;
                        None
                    }
                };
                if let Some(work) = pending
                    && let Err(e) = self.apply_manual_txn(&work)
                {
                    lock_txn(&self.txn).restore_manual(work);
                    return Err(e);
                }
            }
            OptionConnection::ReadOnly => self.config.read_only.store(
                crate::options::bool_option(value, "option adbc.connection.readonly")?,
                Ordering::Release,
            ),
            OptionConnection::IsolationLevel => {
                self.config.isolation = parse_isolation_level(value)?
            }
            // Connection-only: the transaction tag applies to the whole read/write transaction, so
            // it is not a per-statement option (not in the shared dispatch below).
            OptionConnection::Other(k) if k == crate::OPTION_TRANSACTION_TAG => {
                self.config.request.set_transaction_tag(value)?;
            }
            // Every other `spanner.*` option the connection and statement dispatch identically —
            // request priority/tag, directed read, staleness, max_commit_delay, commit_stats, query
            // optimizer opts, RPC timeouts, retry tuning — goes through the shared table. An
            // unrecognised key returns `None`, mapped to the same `NotImplemented` as before.
            OptionConnection::Other(k) => {
                if self.set_shared_option(k, value)?.is_none() {
                    return Err(unknown_option("connection", &connection_option_name(&key)));
                }
            }
            // Spanner has no settable current catalog/schema (named schemas are addressed by
            // qualified name and enumerated by `get_objects`, but none can be made "current"). Both
            // are fixed at `""`: setting `""` is a conformant no-op, a non-empty value is
            // unsupported → `NotImplemented` (see `check_unnamed_catalog_or_schema`).
            OptionConnection::CurrentCatalog => {
                check_unnamed_catalog_or_schema(value, "current catalog")?;
            }
            OptionConnection::CurrentSchema => {
                check_unnamed_catalog_or_schema(value, "current schema")?;
            }
            other => {
                return Err(unknown_option("connection", &connection_option_name(other)));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionConnection::AutoCommit => Ok(lock_txn(&self.txn).autocommit.to_string()),
            OptionConnection::ReadOnly => Ok(self.config.is_read_only().to_string()),
            OptionConnection::IsolationLevel => {
                Ok(isolation_to_adbc_string(&self.config.isolation).to_string())
            }
            // Connection-only (see the setter): reports the transaction tag, or NotFound when unset.
            OptionConnection::Other(k) if k == crate::OPTION_TRANSACTION_TAG => self
                .config
                .request
                .transaction_tag_string()
                .map(str::to_string)
                .ok_or_else(|| option_not_set(crate::OPTION_TRANSACTION_TAG)),
            // Every other `spanner.*` option the connection and statement report identically —
            // including `spanner.commit_stats.mutation_count` — goes through the shared table, which
            // returns the same `NotFound` for an unset (or unknown) key.
            OptionConnection::Other(k) => self.shared_option_string(k),
            // A Spanner database has a single, unnamed catalog and (default) schema — both the empty
            // string in INFORMATION_SCHEMA, which is what `get_objects` reports — so the "current"
            // catalog/schema are reported as "". (They can't be switched; setting them is unsupported.)
            OptionConnection::CurrentCatalog | OptionConnection::CurrentSchema => Ok(String::new()),
            other => Err(option_not_set(&connection_option_name(other))),
        }
    }

    impl_typed_option_getters!();
}

impl Connection for SpannerConnection {
    type StatementType = SpannerStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(SpannerStatement::new(
            self.runtime.clone(),
            self.client.clone(),
            self.spanner.clone(),
            self.database.clone(),
            self.admin.clone(),
            self.config.inherit(),
            self.txn.clone(),
        ))
    }

    fn get_cancel_handle(&self) -> Box<dyn CancelHandle> {
        // The handle latches the current operation's (sticky) signal, so an in-flight operation
        // wakes and returns Cancelled and a cancel between two chunk fetches still cancels the next
        // one. Statements have their own signal, so this does not affect a query running on a
        // statement from this connection.
        Box::new(SlotCancelHandle::new(self.cancel.clone()))
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
        // Mint a fresh cancel signal: a stale cancel cannot leak in, and no later operation can
        // un-cancel this one's streamed reader (see `CancelSlot`).
        self.cancel.begin_operation();
        let out_schema = adbc_core::schemas::GET_OBJECTS_SCHEMA.clone();
        // Spanner has a single catalog (""); a catalog filter that excludes it yields no rows.
        if catalog.is_some_and(|c| !like_match(c, "")) {
            return Ok(Box::new(RecordBatchIterator::new(Vec::new(), out_schema)));
        }
        let schemas = crate::objects::collect_objects(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            depth,
            &crate::objects::ObjectFilters {
                db_schema,
                table_name,
                table_type: table_type.as_deref(),
                column_name,
            },
        )?;
        let batch = crate::objects::build(depth, schemas)?;
        Ok(Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            out_schema,
        )))
    }

    /// Return the Arrow schema of a table.
    ///
    /// Implemented by running a zero-row `SELECT * FROM <table> LIMIT 0` and mapping the result-set
    /// column metadata to Arrow (the same mapping used for query results). Spanner has a single,
    /// unnamed (`""`) catalog, so `catalog` must be `None` or `Some("")`; any other catalog fails
    /// with [`Status::NotFound`].
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        // Mint a fresh cancel signal: a stale cancel cannot leak in, and no later operation can
        // un-cancel this one's streamed reader (see `CancelSlot`).
        self.cancel.begin_operation();
        check_lookup_catalog(catalog)?;
        let table = qualified_table(db_schema, table_name);
        let sql = format!("SELECT * FROM {table} LIMIT 0");
        let client = self.client.clone();
        let bound = self.config.read_staleness.timestamp_bound()?;
        // The reported schema honours the connection's timestamp precision, so it matches what a
        // query on this connection would actually stream.
        let precision = self.config.timestamp_precision;
        let statement = metadata_sql_builder(&self.config, sql).build();
        // A metadata read, so the connection's query timeout bounds it.
        let result = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = crate::staleness::single_use(&client, bound);
                    let result_set = transaction
                        .execute_query(statement)
                        .await
                        .map_err(from_spanner)?;
                    result_set_to_batch(result_set, precision).await
                },
            ),
        );
        match result {
            Ok((schema, _batch)) => Ok((*schema).clone()),
            // A missing table surfaces from the query analyzer as `INVALID_ARGUMENT` ("Table not
            // found"), but ADBC wants `NotFound`. Only touch `INFORMATION_SCHEMA` on the error path
            // so the common (table exists) case stays a single query.
            Err(error) => Err(
                match self.table_exists(db_schema.unwrap_or(""), table_name) {
                    Ok(false) => err(format!("table {table_name:?} not found"), Status::NotFound),
                    // The table is there (so the query failed for some other reason), or the probe
                    // itself failed and teaches us nothing. Either way the original error stands (see
                    // `table_exists`).
                    Ok(true) | Err(_) => error,
                },
            ),
        }
    }

    /// Return the table types supported by Spanner as a single-column (`table_type: utf8`) batch,
    /// per the ADBC specification. The values are Spanner's own
    /// `INFORMATION_SCHEMA.TABLES.TABLE_TYPE` vocabulary (`BASE TABLE` / `VIEW`), which is what
    /// `get_objects` reports per table — so every value returned here round-trips as a
    /// `get_objects` `table_type` filter.
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let array = Arc::new(StringArray::from(vec!["BASE TABLE", "VIEW"])) as ArrayRef;
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

    /// Table/column statistics, computed exactly from aggregate scans (`ROW_COUNT`, and per column
    /// `NULL_COUNT` and `DISTINCT_COUNT`). Name arguments are ADBC `LIKE` patterns.
    ///
    /// `approximate` makes no difference: Spanner keeps no cheap/pre-computed statistics, so both
    /// modes run the same exact aggregate scans. That is spec-conformant — `approximate = true`
    /// merely *allows* approximate values, and exact values always satisfy it (each returned row
    /// reports `statistic_is_approximate = false`).
    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // Mint a fresh cancel signal: a stale cancel cannot leak in, and no later operation can
        // un-cancel this one's streamed reader (see `CancelSlot`).
        self.cancel.begin_operation();
        let out_schema = adbc_core::schemas::GET_STATISTICS_SCHEMA.clone();
        // Spanner is a single unnamed catalog (""); a catalog filter that excludes it yields nothing.
        if catalog.is_some_and(|c| !like_match(c, "")) {
            return Ok(Box::new(RecordBatchIterator::new(Vec::new(), out_schema)));
        }
        // `approximate` is deliberately ignored: Spanner has no cheaper source of statistics, and
        // exact values are always a conformant answer to an approximate request.
        let _ = approximate;
        let schemas = crate::statistics::collect_statistics(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
            &self.config,
            db_schema,
            table_name,
        )?;
        let batch = crate::statistics::build(schemas, out_schema.clone())?;
        Ok(Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            out_schema,
        )))
    }

    fn commit(&mut self) -> Result<()> {
        // Apply from a *clone* of the buffered state and clear it only after success. Taking the
        // state up front would lose the work on a failed apply (e.g. ABORTED once the runner's
        // retries are exhausted — the very code `error.rs` preserves in `vendor_code` so callers
        // can retry) and, worse, a retried `commit()` would then see an empty transaction and
        // report success with nothing written. Keeping the buffer makes retry a genuine replay
        // and leaves `rollback()` available to discard instead (see the module doc for the
        // replay caveats).
        //
        // Committing a **read-only** transaction applies nothing: the snapshot is simply dropped
        // (Spanner read-only transactions need no commit RPC).
        let work = {
            let st = lock_txn(&self.txn);
            if st.autocommit {
                return Err(invalid_state(
                    "commit invoked with autocommit enabled; no active transaction",
                ));
            }
            st.txn.clone()
        };
        self.apply_manual_txn(&work)?;
        lock_txn(&self.txn).finish_commit(&work);
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let mut st = lock_txn(&self.txn);
        if st.autocommit {
            return Err(invalid_state(
                "rollback invoked with autocommit enabled; no active transaction",
            ));
        }
        // Discards any buffered DML and drops a read-only transaction's snapshot (Spanner
        // read-only transactions need no rollback RPC).
        st.txn = ManualTxn::Unset;
        Ok(())
    }

    /// Execute a partition descriptor produced by `Statement::execute_partitions` and stream its
    /// rows as Arrow.
    ///
    /// # Security
    ///
    /// A partition descriptor is **opaque but executable**: a versioned JSON envelope
    /// (`{"v":1,"partition":…}`) around the serde form of the client's `Partition`, whose inner
    /// `ExecuteSqlRequest` carries the SQL text along with the session and transaction identity.
    /// `read_partition` runs whatever that blob contains with **this connection's credentials**, so
    /// a crafted descriptor executes arbitrary SQL as the connection's principal. The version
    /// envelope only guards against format drift between driver versions (an unsupported version is
    /// rejected as `InvalidArguments`); there is no in-band authentication. Transport a descriptor
    /// only over trusted channels and **never accept one from an untrusted source**.
    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // Mint a fresh cancel signal: a stale cancel cannot leak in, and no later operation can
        // un-cancel this one's streamed reader (see `CancelSlot`).
        self.cancel.begin_operation();
        // Decode the opaque descriptor produced by `Statement::execute_partitions`. It carries the
        // session, transaction id, partition token and Data Boost flag, so it executes on this
        // connection's client (which shares the same multiplexed session) with no further setup.
        let partition = decode_partition(partition.as_ref())?;
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.current();
        // Stream the partition's rows to Arrow exactly like `Statement::execute`. The connection has
        // no per-statement batch-size option, so the default chunk size is used; the timestamp
        // precision is the **reading** connection's `spanner.max_timestamp_precision` (set it to the
        // same mode as the producing statement so the descriptor's advertised schema matches). The
        // connection's query timeout bounds the initial execute + first chunk; its fetch timeout
        // bounds each later chunk inside the prefetch task.
        let precision = self.config.timestamp_precision;
        let fetch_timeout = self.config.timeouts.fetch_timeout();
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
            with_timeout(
                self.config.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let result_set = partition.execute(&client).await.map_err(from_spanner)?;
                    stream_query(
                        runtime,
                        cancel,
                        result_set,
                        DEFAULT_ROWS_PER_BATCH,
                        precision,
                        fetch_timeout,
                    )
                    .await
                },
            ),
        )?;
        Ok(Box::new(reader))
    }
}

/// The partition-descriptor envelope version written by [`encode_partition`].
///
/// The payload is the client's [`Partition`] serde form — a compatibility surface this driver does
/// not control — while descriptors travel between processes and driver versions. Bump this when the
/// payload format changes incompatibly, so an older driver rejects a newer descriptor with a clear
/// error instead of a confusing shape mismatch.
pub(crate) const PARTITION_DESCRIPTOR_VERSION: u64 = 1;

/// Encode a [`Partition`] into an opaque ADBC partition descriptor: the versioned JSON envelope
/// `{"v":1,"partition":<serde form of the client's Partition>}`. The inverse of
/// [`decode_partition`].
pub(crate) fn encode_partition(partition: &Partition) -> Result<Vec<u8>> {
    let internal = |e: serde_json::Error| {
        err(
            format!("failed to serialize partition descriptor: {e}"),
            Status::Internal,
        )
    };
    let payload = serde_json::to_value(partition).map_err(internal)?;
    let envelope = serde_json::json!({ "v": PARTITION_DESCRIPTOR_VERSION, "partition": payload });
    serde_json::to_vec(&envelope).map_err(internal)
}

/// Decode an opaque partition descriptor produced by `Statement::execute_partitions` — the
/// versioned JSON envelope written by [`encode_partition`] (`{"v":1,"partition":…}`). A missing
/// or unsupported version, and anything that does not decode (empty input, non-JSON bytes, or
/// valid JSON of the wrong shape) are [`Status::InvalidArguments`] errors, never a panic. A pure
/// function so the rejection paths are unit-testable without a connection.
pub(crate) fn decode_partition(descriptor: &[u8]) -> Result<Partition> {
    let invalid =
        |e: serde_json::Error| invalid_argument(format!("invalid partition descriptor: {e}"));
    let value: serde_json::Value = serde_json::from_slice(descriptor).map_err(invalid)?;
    // Check the version before touching the payload, so a future-format descriptor fails on the
    // version — not on its (unknown) payload shape.
    let v = value.get("v").ok_or_else(|| {
        invalid_argument("invalid partition descriptor: missing \"v\" version field")
    })?;
    let v = v.as_u64().ok_or_else(|| {
        invalid_argument(format!(
            "invalid partition descriptor: version {v} is not an unsigned integer"
        ))
    })?;
    if v != PARTITION_DESCRIPTOR_VERSION {
        return Err(invalid_argument(format!(
            "partition descriptor version {v} not supported by this driver"
        )));
    }
    let payload = value.get("partition").cloned().ok_or_else(|| {
        invalid_argument("invalid partition descriptor: missing \"partition\" field")
    })?;
    serde_json::from_value(payload).map_err(invalid)
}

/// Validate a `current_catalog` / `current_schema` set request. Spanner has a single, unnamed (`""`)
/// catalog, and — although it supports named schemas — no settable session/current schema to select
/// one. Both "current" values are therefore fixed at `""` (as `get_option` reports), so the only
/// conformant value is the empty string, accepted as a no-op; any other value is rejected with
/// `NotImplemented` (matching the C++ PostgreSQL driver), and a non-string value with
/// `InvalidArguments`. `what` names the option in the error.
fn check_unnamed_catalog_or_schema(value: OptionValue, what: &str) -> Result<()> {
    let OptionValue::String(s) = value else {
        return Err(invalid_argument(format!("expected a string {what} value")));
    };
    if s.is_empty() {
        Ok(())
    } else {
        Err(unsupported(format!(
            "setting the {what} to {s:?}: Spanner has no settable {what}; only \"\" is valid"
        )))
    }
}

fn connection_option_name(key: &OptionConnection) -> String {
    key.as_ref().to_string()
}

#[cfg(test)]
mod tests;

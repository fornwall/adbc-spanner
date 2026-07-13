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
//! *buffering* DML statements — and the insert **mutations** of any bulk ingest — and applying the
//! whole batch atomically in a single read/write transaction on [`Connection::commit`] — which
//! also makes the retry-on-abort safe, since the buffer is simply replayed.
//! [`Connection::rollback`] discards the buffer.
//!
//! Consequences of this model, which callers should be aware of:
//! - In manual mode, `execute_update` on DML returns `None` (the affected-row count is not known
//!   until commit).
//! - DML with a `THEN RETURN` clause is rejected in manual mode: it must run via `ExecuteSql` to
//!   produce its rows, but buffered DML is applied through `ExecuteBatchDml` (which does not
//!   support `THEN RETURN`) — and the rows would be unobtainable at commit time anyway.
//! - **No read-your-writes (guarded):** queries always run immediately in a fresh single-use
//!   read-only snapshot, so a query cannot observe DML buffered earlier in the same manual
//!   transaction. Rather than silently returning a *pre-insert* result, a data-returning query
//!   (`execute`, the bound-query path, and `execute_partitions`) issued while any write — a
//!   buffered DML statement *or* an ingest mutation — is pending is rejected with
//!   [`Status::InvalidState`]. Commit or roll back first if a statement needs to see earlier
//!   writes. (`execute_schema`, a schema-only PLAN probe, is not guarded.)
//! - **DML and DDL reorder:** DDL also runs immediately (DDL is never transactional in Spanner),
//!   so DDL issued after buffered DML executes before it.
//! - **Ingest mutations apply at commit time:** a buffered bulk ingest's insert mutations are
//!   applied by Spanner as part of the commit itself — after every buffered DML statement in the
//!   transaction has executed, regardless of issue order — so DML in the same transaction cannot
//!   observe the ingested rows.
//! - A **failed** commit keeps the buffer and the transaction open: the caller can retry
//!   [`Connection::commit`] (replaying the batch) or [`Connection::rollback`] to discard it. The
//!   same holds when re-enabling autocommit fails to commit the buffer: the connection stays in
//!   manual mode. On `ABORTED` (the retriable code preserved in `vendor_code`) the failed attempt
//!   is guaranteed not to have committed, so the replay is exact; after an *ambiguous* transport
//!   failure the usual Spanner caveat applies — the commit may have landed, so a replay can apply
//!   the batch twice unless the DML is idempotent.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::{
    Array, ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use google_cloud_spanner::batch::Partition;
use google_cloud_spanner::builder::{BatchDmlBuilder, TransactionRunnerBuilder};
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::transaction_options::IsolationLevel;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::ReadWriteTransaction;

use crate::conversion::{TimestampPrecision, result_set_to_batch, stream_query};
use crate::directed_read::DirectedRead;
use crate::driver::Connected;
use crate::error::{err, from_spanner, invalid_argument, invalid_state, not_implemented};
use crate::options::impl_shared_option_dispatch;
use crate::query_options::QueryOptionsConfig;
use crate::request::{CommitStats, RequestConfig};
use crate::retry::RetryConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::sql::qualified_table;
use crate::staleness::ReadStaleness;
use crate::statement::{DEFAULT_ROWS_PER_BATCH, SpannerStatement};
use crate::timeout::{RpcTimeouts, with_timeout};

/// Transaction state shared between a connection and the statements it creates.
#[derive(Debug)]
pub(crate) struct TxnState {
    /// When false, the connection is in manual transaction mode and DML is buffered.
    autocommit: bool,
    /// DML statements buffered while in manual mode, applied atomically on commit. Built
    /// statements (not raw SQL) so that parameterized DML — which carries bound values — buffers
    /// just like a plain `;`-batch does.
    pending: Vec<SpannerSql>,
    /// Insert mutations buffered while in manual mode (a bulk ingest issued inside the manual
    /// transaction), committed atomically with [`Self::pending`] in the same read/write transaction. Spanner
    /// applies buffered mutations at commit time, so they land *after* every buffered DML
    /// statement executes, regardless of the order they were issued in.
    pending_mutations: Vec<Mutation>,
}

impl TxnState {
    fn new() -> Self {
        Self {
            autocommit: true,
            pending: Vec::new(),
            pending_mutations: Vec::new(),
        }
    }

    /// Whether the connection is currently in autocommit mode.
    pub(crate) fn autocommit(&self) -> bool {
        self.autocommit
    }

    /// Whether a data-returning query would silently miss buffered writes: the connection is in
    /// **manual** mode *and* at least one write is buffered (a DML statement *or* an ingest
    /// mutation). Manual-mode queries run against a fresh read-only snapshot that does not observe
    /// buffered writes, so this is the condition under which the driver rejects a query up front
    /// (see [`SpannerStatement::ensure_no_buffered_writes_for_query`]) rather than returning a
    /// pre-write result (no read-your-writes).
    pub(crate) fn query_would_miss_buffered_writes(&self) -> bool {
        !self.autocommit && (!self.pending.is_empty() || !self.pending_mutations.is_empty())
    }

    /// Buffer a DML statement to be applied on the next commit.
    pub(crate) fn buffer(&mut self, statement: SpannerSql) {
        self.pending.push(statement);
    }

    /// Buffer a mutation to be applied on the next commit (alongside any buffered DML).
    pub(crate) fn buffer_mutation(&mut self, mutation: Mutation) {
        self.pending_mutations.push(mutation);
    }

    /// Atomically flip into autocommit mode and take the buffered DML and mutations that must be
    /// committed first.
    ///
    /// Doing both in one step — under the caller's single lock acquisition — is what closes the
    /// enable-autocommit race: `run_or_buffer` checks the mode and buffers under this same mutex,
    /// so once the mode reads autocommit no statement can add to the buffer, and the batch taken
    /// here is the complete transaction. Flipping only *after* the apply (in a later acquisition)
    /// would strand any DML buffered while the commit RPC was in flight.
    fn enter_autocommit(&mut self) -> (Vec<SpannerSql>, Vec<Mutation>) {
        self.autocommit = true;
        (
            std::mem::take(&mut self.pending),
            std::mem::take(&mut self.pending_mutations),
        )
    }

    /// Re-enter manual mode with `pending` / `mutations` re-buffered — the failure path of
    /// [`Self::enter_autocommit`], so a failed apply keeps the transaction open and replayable
    /// (retry the toggle or `commit`, or `rollback` to discard). Nothing can have buffered while
    /// autocommit was on (see `enter_autocommit`), but splice at the front anyway so replay order
    /// would survive even if that invariant ever changed.
    fn restore_manual(&mut self, pending: Vec<SpannerSql>, mutations: Vec<Mutation>) {
        self.autocommit = false;
        self.pending.splice(0..0, pending);
        self.pending_mutations.splice(0..0, mutations);
    }
}

#[cfg(test)]
mod txn_state_tests {
    use super::{Mutation, SpannerSql, TxnState};

    fn sql(s: &str) -> SpannerSql {
        SpannerSql::builder(s).build()
    }

    fn mutation(id: i64) -> Mutation {
        Mutation::new_insert_builder("t").set("Id").to(&id).build()
    }

    fn pending_sqls(st: &TxnState) -> Vec<&str> {
        st.pending.iter().map(|s| s.sql()).collect()
    }

    /// The mode flip and the buffer take must be one atomic step: after `enter_autocommit` the
    /// state already reads as autocommit (so `run_or_buffer`, which checks under the same mutex,
    /// routes new DML to immediate execution) and the taken batch is the complete buffer — both
    /// the DML statements and the mutations.
    #[test]
    fn enter_autocommit_flips_and_takes_in_one_step() {
        let mut st = TxnState::new();
        st.autocommit = false;
        st.buffer(sql("UPDATE a"));
        st.buffer(sql("UPDATE b"));
        st.buffer_mutation(mutation(1));
        let (taken, taken_mutations) = st.enter_autocommit();
        assert!(st.autocommit());
        assert!(st.pending.is_empty());
        assert!(st.pending_mutations.is_empty());
        assert_eq!(
            taken.iter().map(|s| s.sql()).collect::<Vec<_>>(),
            ["UPDATE a", "UPDATE b"]
        );
        assert_eq!(taken_mutations, [mutation(1)]);
    }

    /// The failure path must re-enter manual mode with the batch re-buffered — replaying the
    /// toggle (or `commit`) then applies exactly the original transaction, and any DML that
    /// somehow got buffered in between stays *behind* the restored batch.
    #[test]
    fn restore_manual_rebuffers_in_front() {
        let mut st = TxnState::new();
        st.autocommit = false;
        st.buffer(sql("UPDATE a"));
        st.buffer_mutation(mutation(1));
        let (taken, taken_mutations) = st.enter_autocommit();
        // Defensively simulate a buffer written during the window (run_or_buffer cannot actually
        // do this while autocommit is on): restore must keep it, ordered after the original batch.
        st.buffer(sql("UPDATE late"));
        st.buffer_mutation(mutation(2));
        st.restore_manual(taken, taken_mutations);
        assert!(!st.autocommit());
        assert_eq!(pending_sqls(&st), ["UPDATE a", "UPDATE late"]);
        assert_eq!(st.pending_mutations, [mutation(1), mutation(2)]);
    }
}

/// A handle to a connection's transaction state, shared with its statements.
pub(crate) type SharedTxn = Arc<Mutex<TxnState>>;

/// An ADBC connection to a Spanner database.
///
/// # Transactions
///
/// The connection is in **autocommit** mode by default. Setting `adbc.connection.autocommit` to
/// `false` enters **manual** transaction mode, in which DML — and the insert mutations of any bulk
/// ingest — is *buffered* and applied atomically in one read/write transaction on
/// [`Connection::commit`] ([`Connection::rollback`] discards the buffer), because the Spanner
/// client exposes read/write transactions only through a closure-based runner (no begin/commit
/// handle). Two consequences callers must be aware of:
///
/// - **No read-your-writes (guarded):** queries always run immediately in a fresh read-only
///   snapshot, so a query cannot observe DML buffered earlier in the same manual transaction.
///   Rather than silently returning a *pre-insert* result, a data-returning query issued while any
///   write (a buffered DML statement or an ingest mutation) is pending is rejected with
///   [`Status::InvalidState`]. Commit or roll back first if a statement needs to see earlier writes.
/// - **DML and DDL reorder:** DDL also runs immediately (DDL is never transactional in Spanner),
///   so DDL issued after buffered DML executes before it.
#[derive(Debug)]
pub struct SpannerConnection {
    runtime: SharedRuntime,
    client: DatabaseClient,
    spanner: Spanner,
    database: String,
    /// The standard `adbc.connection.readonly` flag. Shared (`Arc`) with every statement the
    /// connection creates, and read by statements at *execution* time, so toggling the option
    /// immediately affects existing statements in both directions.
    read_only: Arc<AtomicBool>,
    /// Isolation level applied to read/write transactions (autocommit DML and manual-mode commit),
    /// set via the standard ADBC `adbc.connection.transaction.isolation_level` option.
    /// [`IsolationLevel::Unspecified`] (the default) leaves the client/database default in place.
    isolation: IsolationLevel,
    /// Read bound for read-only queries (`spanner.read.staleness`). The default is a strong read;
    /// this becomes the default for statements created on the connection, which may override it.
    read_staleness: ReadStaleness,
    /// Request priority and request/transaction tags (`spanner.request.priority` /
    /// `spanner.request.tag` / `spanner.transaction.tag`). Unset by default; becomes the default
    /// for statements created on the connection, which may override the priority and request tag
    /// (the transaction tag is connection-level only).
    request: RequestConfig,
    /// Directed-read replica selection for read-only queries (`spanner.directed_read`). Unset by
    /// default (Spanner's own routing); becomes the default for statements created on the
    /// connection, which may override it.
    directed_read: DirectedRead,
    /// Query optimizer options (`spanner.query.optimizer_version` /
    /// `spanner.query.optimizer_statistics_package`). Unset by default; becomes the default for
    /// statements created on the connection, which may override either field.
    query_options: QueryOptionsConfig,
    /// How `TIMESTAMP` columns map to Arrow (`spanner.max_timestamp_precision`): nanoseconds that
    /// error on out-of-range instants (the default) or microseconds covering Spanner's full range.
    /// Becomes the default for statements created on the connection, which may override it; also
    /// applied to `get_table_schema` and `read_partition` (which have no statement).
    timestamp_precision: TimestampPrecision,
    /// RPC timeouts (`spanner.rpc.timeout_seconds.{query,update,fetch}`). Unset by default (no
    /// deadline); becomes the default for statements created on the connection, which may override
    /// each value. The connection itself applies the update timeout to its commit paths and the
    /// query/fetch timeouts to `read_partition`.
    timeouts: RpcTimeouts,
    /// Retry-policy tuning (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`).
    /// Unset by default (the client's own policy); becomes the default for statements created on the
    /// connection, which may override each knob. The connection itself applies it to its commit
    /// paths (autocommit DML, the manual-mode commit, ingest commits).
    retry: RetryConfig,
    /// Mutation count captured from the connection's most recent manual-mode commit that requested
    /// commit statistics (`spanner.commit_stats`), read back via
    /// `spanner.commit_stats.mutation_count`. Connection-owned (not shared with statements): the
    /// manual-mode commit runs on the connection, so its stats belong here.
    commit_stats: CommitStats,
    txn: SharedTxn,
    /// Cancellation signal for this connection's in-flight metadata/commit operations
    /// (see [`Connection::cancel`]).
    cancel: CancelSignal,
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
            read_only: Arc::new(AtomicBool::new(false)),
            isolation: IsolationLevel::Unspecified,
            read_staleness: ReadStaleness::default(),
            request: RequestConfig::default(),
            directed_read: DirectedRead::default(),
            query_options: QueryOptionsConfig::default(),
            timestamp_precision: TimestampPrecision::default(),
            timeouts: RpcTimeouts::default(),
            retry: RetryConfig::default(),
            commit_stats: CommitStats::default(),
            txn: Arc::new(Mutex::new(TxnState::new())),
            cancel: CancelSignal::new(),
        }
    }

    /// Apply the buffered DML statements and mutations atomically in one read/write transaction,
    /// discarding the affected-row count (a commit reports no count).
    fn apply_transaction(
        &self,
        statements: Vec<SpannerSql>,
        mutations: Vec<Mutation>,
    ) -> Result<()> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        run_batch_txn(
            &self.runtime,
            &self.client,
            &self.cancel,
            self.isolation.clone(),
            self.request.clone(),
            self.retry,
            self.timeouts.update_timeout(),
            &self.commit_stats,
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
            &self.cancel,
            self.timeouts.query_timeout(),
            db_schema,
            table_name,
        )
    }
}

/// Apply the connection's isolation level to a read/write transaction runner builder.
///
/// [`IsolationLevel::Unspecified`] leaves the builder untouched so the client/database default
/// (`SERIALIZABLE`) stands; a specific level is forwarded to
/// [`TransactionRunnerBuilder::set_isolation_level`].
pub(crate) fn apply_isolation(
    builder: TransactionRunnerBuilder,
    isolation: IsolationLevel,
) -> TransactionRunnerBuilder {
    match isolation {
        IsolationLevel::Unspecified => builder,
        level => builder.set_isolation_level(level),
    }
}

/// Map the standard ADBC `adbc.connection.transaction.isolation_level` value to the Spanner client's
/// [`IsolationLevel`]. Spanner supports `SERIALIZABLE` (the default) and `REPEATABLE_READ`; the
/// `default` value leaves the database default in place.
///
/// The four spec levels Spanner does not natively expose are **promoted upward** to the weakest
/// supported level that still satisfies their guarantees, rather than being rejected. A stronger
/// isolation level always satisfies a weaker one's guarantees, so promotion is semantically safe,
/// and it is spec-permitted: the ADBC spec says a driver *should* (not *must*) error on an
/// unsupported level, and JDBC explicitly sanctions substituting a higher/more-restrictive level.
/// The promotion table:
///
/// | requested          | promoted to    | rationale                                                     |
/// |--------------------|----------------|---------------------------------------------------------------|
/// | `read_uncommitted` | `REPEATABLE_READ` | weakest supported level that satisfies it                  |
/// | `read_committed`   | `REPEATABLE_READ` | weakest supported level that satisfies it                  |
/// | `snapshot`         | `SERIALIZABLE`    | snapshot/RR are incomparable, so map to the top to be safe |
/// | `linearizable`     | `SERIALIZABLE`    | Spanner R/W txns are externally consistent (strict serializable = linearizable) |
///
/// The stored (promoted) level is what `get_option` reports back, so callers see the level that
/// will actually run, never an unsupported input echoed. A truly unknown/unparseable level string
/// is still rejected with `InvalidArguments`.
fn parse_isolation_level(value: OptionValue) -> Result<IsolationLevel> {
    use adbc_core::constants::*;
    let OptionValue::String(s) = value else {
        return Err(invalid_argument(
            "expected a string isolation-level option value",
        ));
    };
    match s.as_str() {
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT => Ok(IsolationLevel::Unspecified),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE => Ok(IsolationLevel::Serializable),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ => Ok(IsolationLevel::RepeatableRead),
        // Promote levels Spanner does not natively expose to the weakest supported level that
        // still satisfies their guarantees (see the table in this function's rustdoc).
        ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED
        | ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED => Ok(IsolationLevel::RepeatableRead),
        ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT | ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE => {
            Ok(IsolationLevel::Serializable)
        }
        other => Err(invalid_argument(format!(
            "unknown isolation level {other:?}"
        ))),
    }
}

/// The ADBC value string for the stored isolation level, so `get_option` round-trips what was set.
fn isolation_to_adbc_string(isolation: &IsolationLevel) -> &'static str {
    use adbc_core::constants::*;
    match isolation {
        IsolationLevel::Serializable => ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
        IsolationLevel::RepeatableRead => ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ,
        // Unspecified and any future variant report as the driver/database default.
        _ => ADBC_OPTION_ISOLATION_LEVEL_DEFAULT,
    }
}

/// Apply DML `statements` atomically in one read/write transaction via Spanner's `ExecuteBatchDml`
/// (a single RPC), returning the total affected-row count.
///
/// The runner may retry the closure on abort, so the (cloned) statement list is replayed on each
/// attempt. Shared by autocommit `execute_update`, the manual-mode commit path, and each chunk of
/// an autocommit DML bulk ingest (which calls this once per chunk so a retry only ever clones one
/// chunk, not the whole ingest).
///
/// `last_statement` optimization: a single-statement autocommit batch is the entire transaction —
/// the runner runs this one-statement `ExecuteBatchDml` and immediately commits, with no further
/// statement, read, or query in the transaction. Flagging the batch as the transaction's last
/// request (`ExecuteBatchDmlRequest.last_statements`) lets Spanner release the transaction as part
/// of the same round-trip, saving the separate `Commit` RPC for the common single-DML case. We
/// only set it for `statements.len() == 1`: a multi-statement `;`-batch is deliberately left
/// unflagged (conservative — the flag is a whole-batch assertion, and we reserve it for the
/// unambiguous single-statement case), and mutation-carrying / manual-commit batches go through
/// [`run_batch_txn`] with the flag off (their commit still applies buffered mutations, so the
/// batch is *not* the transaction's last request).
#[allow(clippy::too_many_arguments)] // threads one connection/statement config item per argument
pub(crate) fn run_batch_dml(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    isolation: IsolationLevel,
    request: RequestConfig,
    retry: RetryConfig,
    timeout: Option<Duration>,
    commit_stats: &CommitStats,
    statements: Vec<SpannerSql>,
) -> Result<i64> {
    // Free RPC saving only for the single-statement autocommit case (see the doc comment above).
    let last_statements = statements.len() == 1;
    run_batch_txn(
        runtime,
        client,
        cancel,
        isolation,
        request,
        retry,
        timeout,
        commit_stats,
        statements,
        Vec::new(),
        last_statements,
    )
}

/// Apply DML `statements` and buffered `mutations` atomically in **one** read/write transaction,
/// returning the DML statements' total affected-row count (mutations report no count).
///
/// The statements run via `ExecuteBatchDml`; the mutations are buffered on the transaction and
/// applied by Spanner as part of its commit — i.e. *after* every statement has executed, whatever
/// order they were issued in. The runner may retry the closure on abort, so both (cloned) lists
/// are replayed on each attempt. This is the manual-transaction commit path; the DML-only wrapper
/// is [`run_batch_dml`].
///
/// `timeout` — the caller's `spanner.rpc.timeout_seconds.update` value — is an overall deadline on
/// the whole transaction (including the runner's abort retries); expiry fails with
/// [`Status::Timeout`]. Note a commit whose confirmation the driver stopped waiting for may still
/// have landed server-side, the usual ambiguity of any timed-out commit.
///
/// `last_statements` marks this batch as the transaction's final request (see the
/// [`run_batch_dml`] doc for the `last_statement` optimization). Callers must pass `false` unless
/// the batch is genuinely the whole transaction: the manual-commit path buffers `mutations` that
/// Spanner applies *at* commit, so the batch is never the last request there.
#[allow(clippy::too_many_arguments)] // threads one connection/statement config item per argument
pub(crate) fn run_batch_txn(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    isolation: IsolationLevel,
    request: RequestConfig,
    retry: RetryConfig,
    timeout: Option<Duration>,
    commit_stats: &CommitStats,
    statements: Vec<SpannerSql>,
    mutations: Vec<Mutation>,
    last_statements: bool,
) -> Result<i64> {
    if statements.is_empty() && mutations.is_empty() {
        return Ok(0);
    }
    let client = client.clone();
    let transaction = async move {
        // The commit priority and transaction tag ride on the runner; the request tag rides on the
        // ExecuteBatchDml batch inside the (retryable) closure.
        let runner = retry
            .apply_to_runner(
                request
                    .apply_to_runner(apply_isolation(client.read_write_transaction(), isolation)),
            )
            .build()
            .await
            .map_err(from_spanner)?;
        let outcome = runner
            .run(move |transaction: ReadWriteTransaction| {
                let statements = statements.clone();
                let mutations = mutations.clone();
                let request = request.clone();
                async move {
                    transaction.buffer(mutations)?;
                    if statements.is_empty() {
                        return Ok(0);
                    }
                    let mut batch = retry
                        .apply_to_batch_dml(request.apply_to_batch_dml(BatchDmlBuilder::new()))
                        .set_last_statements(last_statements);
                    for statement in statements {
                        batch = batch.add_statement(statement);
                    }
                    let counts = transaction.execute_batch_update(batch.build()).await?;
                    Ok(counts.into_iter().sum::<i64>())
                }
            })
            .await
            .map_err(from_spanner)?;
        // The commit stats (if any — only when `spanner.commit_stats` requested them) ride on the
        // commit response; capture the mutation count so the caller can record it into its cell.
        let mutation_count = outcome
            .commit_response
            .commit_stats
            .as_ref()
            .map(|stats| stats.mutation_count);
        Ok::<(i64, Option<i64>), Error>((outcome.result, mutation_count))
    };
    let (count, mutation_count) = block_on_cancellable(
        runtime,
        cancel,
        with_timeout(timeout, crate::OPTION_RPC_TIMEOUT_UPDATE, transaction),
    )?;
    commit_stats.record(mutation_count);
    Ok(count)
}

/// Validate a lookup's `catalog` argument. Spanner has a single, unnamed (`""`) catalog, so `None`
/// and `Some("")` are accepted; any other catalog does not exist — nothing can be found in it — so
/// the lookup fails with [`Status::NotFound`] (matching how a missing table is reported).
fn check_lookup_catalog(catalog: Option<&str>) -> Result<()> {
    match catalog {
        None | Some("") => Ok(()),
        Some(other) => Err(err(
            format!("catalog {other:?} not found: Spanner has only the default (empty) catalog"),
            Status::NotFound,
        )),
    }
}

/// Whether a table exists, via a parameterized `INFORMATION_SCHEMA.TABLES` lookup. The default
/// (unnamed) schema is the empty string in Spanner.
///
/// A free function (rather than only a [`SpannerConnection`] method) so the statement's bulk-ingest
/// error path can reuse the exact same probe to remap a failed `append` to the spec-mandated status
/// (a missing table → `NotFound`, an existing-but-incompatible table → `AlreadyExists`).
pub(crate) fn table_exists(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    timeout: Option<Duration>,
    db_schema: &str,
    table_name: &str,
) -> Result<bool> {
    let client = client.clone();
    let (schema, table) = (db_schema.to_string(), table_name.to_string());
    // A metadata read, so the caller's query timeout (`spanner.rpc.timeout_seconds.query`) bounds
    // it; unset (the default) leaves it unbounded.
    block_on_cancellable(
        runtime,
        cancel,
        with_timeout(timeout, crate::OPTION_RPC_TIMEOUT_QUERY, async move {
            let statement = SpannerSql::builder(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
                 WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table",
            )
            .add_param("schema", &schema)
            .add_param("table", &table)
            .build();
            let transaction = client.single_use().build();
            let result_set = transaction
                .execute_query(statement)
                .await
                .map_err(from_spanner)?;
            // A TABLE_NAME probe returns only strings, so the timestamp precision is irrelevant.
            let (_schema, batch) =
                result_set_to_batch(result_set, TimestampPrecision::default()).await?;
            Ok::<bool, Error>(batch.num_rows() > 0)
        }),
    )
}

/// Run a query and return its single materialised record batch. Shared with the `INFORMATION_SCHEMA`
/// collector in [`crate::statistics`] ([`crate::objects`] runs its queries concurrently on one
/// multi-use read-only transaction instead).
pub(crate) async fn query_batch(client: &DatabaseClient, sql: &str) -> Result<RecordBatch> {
    let transaction = client.single_use().build();
    let result_set = transaction
        .execute_query(SpannerSql::builder(sql).build())
        .await
        .map_err(from_spanner)?;
    // The INFORMATION_SCHEMA collectors read only string/int columns, never TIMESTAMPs, so the
    // default timestamp precision is fine here.
    let (_schema, batch) = result_set_to_batch(result_set, TimestampPrecision::default()).await?;
    Ok(batch)
}

/// Extract column `index` of an `INFORMATION_SCHEMA` batch as a [`StringArray`]. Shared with the
/// collectors in [`crate::objects`] and [`crate::statistics`].
pub(crate) fn str_col(batch: &RecordBatch, index: usize) -> Result<&StringArray> {
    // `RecordBatch::column` panics on an out-of-range index; a malformed / unexpectedly-shaped
    // (e.g. zero-column) metadata batch must surface as an error, not a panic.
    if index >= batch.num_columns() {
        return Err(err(
            format!(
                "INFORMATION_SCHEMA batch has {} column(s); column {index} is out of range",
                batch.num_columns()
            ),
            Status::Internal,
        ));
    }
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

/// A compiled ADBC `LIKE` pattern (`%` = any run, `_` = one char), matched case-sensitively.
///
/// The pattern chars are collected once so a collector can reuse one matcher across every candidate
/// row (the pattern is loop-invariant) instead of re-collecting it on each call; the free
/// [`like_match`] helper wraps it for one-off matches.
///
/// [`matches`](LikeMatcher::matches) is iterative with backtrack pointers (O(pattern × value), no
/// recursion) so adversarial patterns like `%a%a%a…` cannot cause exponential blowup or stack
/// overflow.
pub(crate) struct LikeMatcher {
    pattern: Vec<char>,
}

impl LikeMatcher {
    pub(crate) fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.chars().collect(),
        }
    }

    pub(crate) fn matches(&self, value: &str) -> bool {
        let p = &self.pattern;
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
}

/// Match an ADBC `LIKE` pattern (`%` = any run, `_` = one char) against a value, case-sensitively.
///
/// A one-off wrapper over [`LikeMatcher`]; use [`LikeMatcher`] directly to match one pattern against
/// many values without re-compiling it each time.
pub(crate) fn like_match(pattern: &str, value: &str) -> bool {
    LikeMatcher::new(pattern).matches(value)
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
                // Enabling autocommit commits any active manual transaction. The mode flip and the
                // buffer take happen in ONE lock acquisition (`enter_autocommit`): once the mode is
                // autocommit, `run_or_buffer` — which checks-and-buffers under this same mutex —
                // can no longer add DML, so nothing a concurrent statement buffers can be stranded
                // behind the flip (the old read/apply/flip-in-separate-acquisitions shape had
                // exactly that race). Like `commit`, a failed apply must not lose the writes:
                // `restore_manual` re-enters manual mode with the batch re-buffered so the caller
                // can retry the toggle (a genuine replay) or roll back. Apply from a clone so the
                // taken batch is still around to restore.
                let pending = {
                    let mut st = self.txn.lock().unwrap();
                    if enable && !st.autocommit {
                        Some(st.enter_autocommit())
                    } else {
                        st.autocommit = enable;
                        None
                    }
                };
                if let Some((pending, mutations)) = pending
                    && let Err(e) = self.apply_transaction(pending.clone(), mutations.clone())
                {
                    self.txn.lock().unwrap().restore_manual(pending, mutations);
                    return Err(e);
                }
            }
            OptionConnection::ReadOnly => {
                self.read_only.store(parse_bool(value)?, Ordering::Release)
            }
            OptionConnection::IsolationLevel => self.isolation = parse_isolation_level(value)?,
            // Connection-only: the transaction tag applies to the whole read/write transaction, so
            // it is not a per-statement option (not in the shared dispatch below).
            OptionConnection::Other(k) if k == crate::OPTION_TRANSACTION_TAG => {
                self.request.set_transaction_tag(value)?;
            }
            // Every other `spanner.*` option the connection and statement dispatch identically —
            // request priority/tag, directed read, staleness, max_commit_delay, commit_stats, query
            // optimizer opts, RPC timeouts, retry tuning — goes through the shared table. An
            // unrecognised key returns `None`, mapped to the same `NotImplemented` as before.
            OptionConnection::Other(k) => {
                if self.set_shared_option(k, value)?.is_none() {
                    return Err(not_implemented(&format!(
                        "unsupported Spanner connection option: {}",
                        connection_option_name(&key)
                    )));
                }
            }
            // A Spanner database has a single, unnamed catalog, and — although Spanner supports named
            // schemas (addressed by qualified name, e.g. `sales.Orders`, and enumerated by
            // `get_objects`) — it exposes no settable session/current schema to point at one. So the
            // "current" catalog and schema are both fixed at `""`, which is what the `get_option` side
            // always reports; setting either to `""` is a conformant no-op, and any other value is an
            // `InvalidArguments` (there is no such switchable current catalog/schema), not
            // `NotImplemented`.
            OptionConnection::CurrentCatalog => {
                check_unnamed_catalog_or_schema(value, "current catalog")?;
            }
            OptionConnection::CurrentSchema => {
                check_unnamed_catalog_or_schema(value, "current schema")?;
            }
            other => {
                return Err(not_implemented(&format!(
                    "unsupported Spanner connection option: {}",
                    connection_option_name(other)
                )));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionConnection::AutoCommit => Ok(self.txn.lock().unwrap().autocommit.to_string()),
            OptionConnection::ReadOnly => Ok(self.read_only.load(Ordering::Acquire).to_string()),
            OptionConnection::IsolationLevel => {
                Ok(isolation_to_adbc_string(&self.isolation).to_string())
            }
            // Connection-only (see the setter): reports the transaction tag, or NotFound when unset.
            OptionConnection::Other(k) if k == crate::OPTION_TRANSACTION_TAG => self
                .request
                .transaction_tag_string()
                .map(str::to_string)
                .ok_or_else(|| {
                    err(
                        format!("option {} is not set", crate::OPTION_TRANSACTION_TAG),
                        Status::NotFound,
                    )
                }),
            // Every other `spanner.*` option the connection and statement report identically —
            // including `spanner.commit_stats.mutation_count` — goes through the shared table, which
            // returns the same `NotFound` for an unset (or unknown) key.
            OptionConnection::Other(k) => self.shared_option_string(k),
            // A Spanner database has a single, unnamed catalog and (default) schema — both the empty
            // string in INFORMATION_SCHEMA, which is what `get_objects` reports — so the "current"
            // catalog/schema are reported as "". (They can't be switched; setting them is unsupported.)
            OptionConnection::CurrentCatalog | OptionConnection::CurrentSchema => Ok(String::new()),
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
        let what = format!("option {}", connection_option_name(&key));
        crate::options::int_from_stored_string(self.get_option_string(key), &what)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let what = format!("option {}", connection_option_name(&key));
        crate::options::double_from_stored_string(self.get_option_string(key), &what)
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
            self.read_only.clone(),
            self.isolation.clone(),
            self.read_staleness.clone(),
            self.request.clone(),
            self.directed_read.clone(),
            self.query_options.clone(),
            self.timestamp_precision,
            self.timeouts,
            self.retry,
            self.txn.clone(),
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        // Latch the (sticky) signal: an in-flight metadata/commit operation wakes and returns
        // Cancelled, and a cancel landing between two chunk fetches of a `read_partition` stream
        // still cancels the next fetch. The latch is cleared when the connection starts its next
        // operation. Statements have their own signal, so this does not affect a query running on
        // a statement from this connection.
        self.cancel.signal();
        Ok(())
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
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        let out_schema = adbc_core::schemas::GET_OBJECTS_SCHEMA.clone();
        // Spanner has a single catalog (""); a catalog filter that excludes it yields no rows.
        if catalog.is_some_and(|c| !like_match(c, "")) {
            return Ok(Box::new(RecordBatchIterator::new(Vec::new(), out_schema)));
        }
        let schemas = crate::objects::collect_objects(
            &self.runtime,
            &self.client,
            &self.cancel,
            self.timeouts.query_timeout(),
            depth,
            db_schema,
            table_name,
            &table_type,
            column_name,
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
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        check_lookup_catalog(catalog)?;
        let table = qualified_table(db_schema, table_name);
        let sql = format!("SELECT * FROM {table} LIMIT 0");
        let client = self.client.clone();
        let bound = self.read_staleness.timestamp_bound()?;
        // The reported schema honours the connection's timestamp precision, so it matches what a
        // query on this connection would actually stream.
        let precision = self.timestamp_precision;
        // A metadata read, so the connection's query timeout bounds it.
        let result = block_on_cancellable(
            &self.runtime,
            &self.cancel,
            with_timeout(
                self.timeouts.query_timeout(),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                async move {
                    let transaction = crate::staleness::single_use(&client, bound);
                    let result_set = transaction
                        .execute_query(SpannerSql::builder(sql).build())
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
            Err(error) => {
                if self.table_exists(db_schema.unwrap_or(""), table_name)? {
                    Err(error)
                } else {
                    Err(err(
                        format!("table {table_name:?} not found"),
                        Status::NotFound,
                    ))
                }
            }
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
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
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
            &self.cancel,
            self.timeouts.query_timeout(),
            &self.read_staleness,
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
        // Apply from a *clone* of the buffer and drain it only after success. Taking the buffer
        // up front would lose the DML on a failed apply (e.g. ABORTED once the runner's retries
        // are exhausted — the very code `error.rs` preserves in `vendor_code` so callers can
        // retry) and, worse, a retried `commit()` would then see an empty list and report
        // success with nothing written. Keeping the buffer makes retry a genuine replay and
        // leaves `rollback()` available to discard instead (see the module doc for the replay
        // caveats).
        let (pending, mutations) = {
            let st = self.txn.lock().unwrap();
            if st.autocommit {
                return Err(invalid_state(
                    "commit invoked with autocommit enabled; no active transaction",
                ));
            }
            (st.pending.clone(), st.pending_mutations.clone())
        };
        let applied = pending.len();
        let applied_mutations = mutations.len();
        self.apply_transaction(pending, mutations)?;
        // Drain exactly the statements/mutations that were applied; anything buffered concurrently
        // while the commit RPC ran stays pending for the next commit.
        let mut st = self.txn.lock().unwrap();
        st.pending.drain(..applied);
        st.pending_mutations.drain(..applied_mutations);
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let mut st = self.txn.lock().unwrap();
        if st.autocommit {
            return Err(invalid_state(
                "rollback invoked with autocommit enabled; no active transaction",
            ));
        }
        st.pending.clear();
        st.pending_mutations.clear();
        Ok(())
    }

    /// Execute a partition descriptor produced by `Statement::execute_partitions` and stream its
    /// rows as Arrow.
    ///
    /// # Security
    ///
    /// A partition descriptor is **opaque but executable**: a versioned JSON envelope
    /// (`{"v":1,"partition":…}`) around the serde form of the client's `Partition`, whose inner
    /// `ExecuteSqlRequest` carries the SQL text itself along with the session and transaction
    /// identity. `read_partition` runs whatever that blob contains against this connection's
    /// `DatabaseClient`, with **this connection's credentials** — so a crafted descriptor executes
    /// arbitrary SQL as the connection's principal. This is inherent to ADBC's portable-descriptor
    /// design and the upstream serde format. The version envelope only guards against format drift
    /// between driver versions (an unsupported version is rejected as `InvalidArguments`); there
    /// is no in-band authentication of the blob. Treat a descriptor as an executable request, not
    /// as opaque data:
    /// transport it only over trusted channels and **never accept one from an untrusted source**.
    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        // Decode the opaque descriptor produced by `Statement::execute_partitions`. It carries the
        // session, transaction id, partition token and Data Boost flag, so it executes on this
        // connection's client (which shares the same multiplexed session) with no further setup.
        let partition = decode_partition(partition.as_ref())?;
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.clone();
        // Stream the partition's rows to Arrow exactly like `Statement::execute`. The connection has
        // no per-statement batch-size option, so the default chunk size is used; the timestamp
        // precision is the **reading** connection's `spanner.max_timestamp_precision` (set it to the
        // same mode as the producing statement so the descriptor's advertised schema matches). The
        // connection's query timeout bounds the initial execute + first chunk; its fetch timeout
        // bounds each later chunk inside the prefetch task.
        let precision = self.timestamp_precision;
        let fetch_timeout = self.timeouts.fetch_timeout();
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel,
            with_timeout(
                self.timeouts.query_timeout(),
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
/// The descriptor's payload is the client's [`Partition`] serde form — a compatibility surface
/// this driver does not control (a client-crate bump can silently change it), while descriptors
/// travel between processes and driver versions. The version envelope makes that drift
/// detectable: bump this when the payload format changes incompatibly, so an older driver rejects
/// a newer descriptor with a clear error instead of a confusing shape mismatch.
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

fn parse_bool(value: OptionValue) -> Result<bool> {
    crate::options::bool_option(value, "option")
}

/// Validate a `current_catalog` / `current_schema` set request. Spanner has a single, unnamed (`""`)
/// catalog, and — although it supports named schemas (addressed by qualified name and enumerated by
/// `get_objects`) — no settable session/current schema to select one. Both "current" values are
/// therefore fixed at `""` (mirrored by the `get_option` side, which always reports `""`), so the
/// only conformant value is the empty string, accepted as a no-op; any other value is rejected with
/// `InvalidArguments` (there is no such switchable current catalog/schema), not `NotImplemented`.
/// `what` names the option in the error.
fn check_unnamed_catalog_or_schema(value: OptionValue, what: &str) -> Result<()> {
    let OptionValue::String(s) = value else {
        return Err(invalid_argument(format!("expected a string {what} value")));
    };
    if s.is_empty() {
        Ok(())
    } else {
        Err(invalid_argument(format!(
            "Spanner has no settable {what}; only \"\" is valid, got {s:?}"
        )))
    }
}

fn connection_option_name(key: &OptionConnection) -> String {
    key.as_ref().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_isolation_levels() {
        use adbc_core::constants::*;
        let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE).unwrap(),
            IsolationLevel::Serializable
        );
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ).unwrap(),
            IsolationLevel::RepeatableRead
        );
        // `default` maps to the client's unspecified level, leaving the database default in place.
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_DEFAULT).unwrap(),
            IsolationLevel::Unspecified
        );
    }

    #[test]
    fn promotes_unsupported_isolation_levels() {
        use adbc_core::constants::*;
        let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
        // Spec levels Spanner does not natively expose are promoted upward to the weakest
        // supported level that still satisfies their guarantees (never rejected).
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED).unwrap(),
            IsolationLevel::RepeatableRead
        );
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED).unwrap(),
            IsolationLevel::RepeatableRead
        );
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT).unwrap(),
            IsolationLevel::Serializable
        );
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE).unwrap(),
            IsolationLevel::Serializable
        );
        // A completely unknown value is still an invalid argument.
        assert_eq!(
            parse("not-a-level").unwrap_err().status,
            Status::InvalidArguments
        );
        // A non-string option value is rejected.
        assert_eq!(
            parse_isolation_level(OptionValue::Int(1))
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
    }

    #[test]
    fn isolation_level_round_trips_to_adbc_string() {
        use adbc_core::constants::*;
        assert_eq!(
            isolation_to_adbc_string(&IsolationLevel::Serializable),
            ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
        );
        assert_eq!(
            isolation_to_adbc_string(&IsolationLevel::RepeatableRead),
            ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
        );
        assert_eq!(
            isolation_to_adbc_string(&IsolationLevel::Unspecified),
            ADBC_OPTION_ISOLATION_LEVEL_DEFAULT
        );
    }

    #[test]
    fn promoted_isolation_level_round_trips_to_effective_level() {
        use adbc_core::constants::*;
        // `get_option` reports the effective (promoted) level that will actually run, not the
        // unsupported input that was set: parse then render must land on a supported level.
        let effective = |s: &str| {
            let level = parse_isolation_level(OptionValue::String(s.to_string())).expect("parses");
            isolation_to_adbc_string(&level)
        };
        assert_eq!(
            effective(ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED),
            ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
        );
        assert_eq!(
            effective(ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED),
            ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
        );
        assert_eq!(
            effective(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT),
            ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
        );
        assert_eq!(
            effective(ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE),
            ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
        );
    }

    #[test]
    fn lookup_catalog_accepts_only_the_default_empty_catalog() {
        // Spanner's single catalog is the empty string; `None` means "don't filter".
        assert!(check_lookup_catalog(None).is_ok());
        assert!(check_lookup_catalog(Some("")).is_ok());
        // Any named catalog does not exist, so a lookup in it is NotFound.
        let err = check_lookup_catalog(Some("main")).unwrap_err();
        assert_eq!(err.status, Status::NotFound);
        assert!(err.message.contains("\"main\""), "{}", err.message);
    }

    #[test]
    fn setting_current_catalog_or_schema_accepts_only_the_empty_string() {
        let set = |s: &str| {
            check_unnamed_catalog_or_schema(OptionValue::String(s.to_string()), "current catalog")
        };
        // The "current" catalog/schema is fixed at `""` (no settable session catalog/schema), so
        // setting it to `""` is a no-op success.
        assert!(set("").is_ok());
        // Any other value has no switchable current catalog/schema to select → InvalidArguments (not
        // NotImplemented, which is what the blanket fall-through arm would have produced).
        let err = set("foo").unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("\"foo\""), "{}", err.message);
        // A non-string option value is likewise rejected as an invalid argument.
        assert_eq!(
            check_unnamed_catalog_or_schema(OptionValue::Int(1), "current schema")
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
    }

    /// A garbage partition descriptor — `read_partition`'s input is caller-supplied opaque bytes —
    /// must be rejected as `InvalidArguments` by the decode step (before anything executes), never
    /// panic. Covers empty input, non-JSON bytes, truncated JSON, and well-formed JSON that is not
    /// a partition descriptor.
    #[test]
    fn garbage_partition_descriptors_error_cleanly() {
        let cases: [&[u8]; 6] = [
            b"",                      // empty
            b"\xff\xfe\x00 not json", // non-UTF-8, non-JSON bytes
            b"{",                     // truncated JSON
            b"{}",                    // valid JSON object missing every descriptor field
            br#"{"hello": "world"}"#, // valid JSON object of the wrong shape
            b"[1, 2, 3]",             // valid JSON that is not even an object
        ];
        for descriptor in cases {
            let error = decode_partition(descriptor).unwrap_err();
            assert_eq!(
                error.status,
                Status::InvalidArguments,
                "descriptor {descriptor:?}"
            );
            assert!(
                error.message.contains("invalid partition descriptor"),
                "unexpected message for {descriptor:?}: {}",
                error.message
            );
        }
    }

    /// `encode_partition` writes the versioned envelope, and decode → encode is a byte-for-byte
    /// fixed point.
    #[test]
    fn partition_descriptor_envelope_round_trips() {
        let descriptor: &[u8] = br#"{"v":1,"partition":{"inner":{"Query":{"sql":"SELECT 1"}}}}"#;
        let partition = decode_partition(descriptor).expect("enveloped descriptor decodes");

        let encoded = encode_partition(&partition).expect("encode");
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(value["v"], PARTITION_DESCRIPTOR_VERSION);
        assert!(
            value.get("partition").is_some(),
            "envelope carries the partition payload: {value}"
        );

        // The enveloped form is canonical: decode → encode reproduces it exactly.
        let again = decode_partition(&encoded).expect("enveloped descriptor decodes");
        assert_eq!(encode_partition(&again).expect("re-encode"), encoded);
    }

    /// A pre-envelope bare descriptor (no `"v"` key) is now rejected — the driver has never had
    /// users, so there are no legacy descriptors to accept, and a descriptor carries a live
    /// session/transaction identity that could not outlive a driver upgrade anyway.
    #[test]
    fn bare_partition_descriptor_is_rejected() {
        let bare: &[u8] = br#"{"inner":{"Query":{"sql":"SELECT 1"}}}"#;
        let error = decode_partition(bare).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("missing \"v\" version field"),
            "unexpected message: {}",
            error.message
        );
    }

    /// An envelope with an unknown version must be rejected up front with a clean
    /// `InvalidArguments` naming the version — not fail on its (unknown-format) payload shape.
    #[test]
    fn unsupported_partition_descriptor_version_errors_cleanly() {
        for descriptor in [
            br#"{"v":2,"partition":{"future":"format"}}"#.as_slice(),
            br#"{"v":0}"#.as_slice(),
        ] {
            let error = decode_partition(descriptor).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(
                error.message.contains("partition descriptor version")
                    && error.message.contains("not supported by this driver"),
                "unexpected message for {descriptor:?}: {}",
                error.message
            );
        }
        // The version 2 rejection names the version.
        let error = decode_partition(br#"{"v":2,"partition":{}}"#).unwrap_err();
        assert!(
            error
                .message
                .contains("partition descriptor version 2 not supported by this driver"),
            "{}",
            error.message
        );
    }

    /// Malformed envelopes — non-integer version, or a supported version with a missing/wrong
    /// payload — are `InvalidArguments`, never a panic.
    #[test]
    fn malformed_partition_descriptor_envelopes_error_cleanly() {
        let cases: [&[u8]; 4] = [
            br#"{"v":"one"}"#,            // non-integer version
            br#"{"v":-1}"#,               // negative version
            br#"{"v":1}"#,                // missing "partition" payload
            br#"{"v":1,"partition":{}}"#, // payload of the wrong shape
        ];
        for descriptor in cases {
            let error = decode_partition(descriptor).unwrap_err();
            assert_eq!(
                error.status,
                Status::InvalidArguments,
                "descriptor {descriptor:?}"
            );
            assert!(
                error.message.contains("invalid partition descriptor"),
                "unexpected message for {descriptor:?}: {}",
                error.message
            );
        }
    }

    #[test]
    fn str_col_errors_on_out_of_range_index() {
        // A zero-column batch: any column index is out of range and must error, not panic.
        let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let err = str_col(&empty, 0).unwrap_err();
        assert_eq!(err.status, Status::Internal);

        // A one-column batch: index 0 is fine, index 1 is out of range.
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, true)]));
        let col: ArrayRef = Arc::new(StringArray::from(vec![Some("x")]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
        assert!(str_col(&batch, 0).is_ok());
        assert_eq!(str_col(&batch, 1).unwrap_err().status, Status::Internal);
    }

    #[test]
    fn partition_descriptor_round_trips_large_floats() {
        // Regression for a nightly fuzz find (adbc-spanner#188): a descriptor whose payload
        // carries an integer literal too large for i64/u64 is parsed to f64, so re-encoding emits
        // a ryu float. serde_json's default float parser is fast-but-imprecise (up to one ULP
        // off), so `parse(ryu(x)) != x` and each decode → encode pass drifted to an adjacent ULP —
        // the fixed point `read_partition` relies on never settled. The `float_roundtrip` feature
        // makes the parser exact, so a single re-encode is already the fixed point.
        //
        // The unknown keys land in the generated request's `_unknown_fields` as `serde_json::Value`
        // numbers, exercising exactly that float path.
        let descriptor = br#"{"v": 1, "partition": {"inner": {"Query": {"con[": 44444424444444444249, "/+%n":4444440000000000000000074074764, "/s%n": "prns/s"}}}}"#;

        let partition = decode_partition(descriptor).expect("descriptor decodes");
        let first = encode_partition(&partition).expect("a decoded partition re-encodes");
        // `encode_partition`'s own output must be a byte-stable fixed point under decode → encode.
        let normalized = decode_partition(&first).expect("re-encoded descriptor decodes");
        let again = encode_partition(&normalized).expect("a decoded partition re-encodes");
        assert_eq!(
            first, again,
            "decode → encode of an encoder's output must be byte-stable"
        );
    }
}

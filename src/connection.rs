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
//! - DML with a `THEN RETURN` clause is rejected in manual mode: it must run via `ExecuteSql` to
//!   produce its rows, but buffered DML is applied through `ExecuteBatchDml` (which does not
//!   support `THEN RETURN`) — and the rows would be unobtainable at commit time anyway.
//! - **No read-your-writes:** queries (`execute`) always run immediately in a fresh single-use
//!   read-only snapshot, so a query does not observe DML buffered earlier in the same manual
//!   transaction — an `INSERT` followed by a `SELECT COUNT(*)` returns the *pre-insert* count.
//!   Commit first if a statement needs to see earlier writes.
//! - **DML and DDL reorder:** DDL also runs immediately (DDL is never transactional in Spanner),
//!   so DDL issued after buffered DML executes before it.
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
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::ReadWriteTransaction;

use crate::bind::qualified_table;
use crate::conversion::{result_set_to_batch, stream_query};
use crate::driver::Connected;
use crate::error::{err, from_spanner, invalid_argument, invalid_state, not_implemented};
use crate::runtime::{block_on_cancellable, CancelSignal, SharedRuntime};
use crate::staleness::ReadStaleness;
use crate::statement::{SpannerStatement, DEFAULT_ROWS_PER_BATCH};

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

    /// Atomically flip into autocommit mode and take the buffered DML that must be committed
    /// first.
    ///
    /// Doing both in one step — under the caller's single lock acquisition — is what closes the
    /// enable-autocommit race: `run_or_buffer` checks the mode and buffers under this same mutex,
    /// so once the mode reads autocommit no statement can add to the buffer, and the batch taken
    /// here is the complete transaction. Flipping only *after* the apply (in a later acquisition)
    /// would strand any DML buffered while the commit RPC was in flight.
    fn enter_autocommit(&mut self) -> Vec<SpannerSql> {
        self.autocommit = true;
        std::mem::take(&mut self.pending)
    }

    /// Re-enter manual mode with `pending` re-buffered — the failure path of
    /// [`Self::enter_autocommit`], so a failed apply keeps the transaction open and replayable
    /// (retry the toggle or `commit`, or `rollback` to discard). Nothing can have buffered while
    /// autocommit was on (see `enter_autocommit`), but splice at the front anyway so replay order
    /// would survive even if that invariant ever changed.
    fn restore_manual(&mut self, pending: Vec<SpannerSql>) {
        self.autocommit = false;
        self.pending.splice(0..0, pending);
    }
}

#[cfg(test)]
mod txn_state_tests {
    use super::{SpannerSql, TxnState};

    fn sql(s: &str) -> SpannerSql {
        SpannerSql::builder(s).build()
    }

    fn pending_sqls(st: &TxnState) -> Vec<&str> {
        st.pending.iter().map(|s| s.sql()).collect()
    }

    /// The mode flip and the buffer take must be one atomic step: after `enter_autocommit` the
    /// state already reads as autocommit (so `run_or_buffer`, which checks under the same mutex,
    /// routes new DML to immediate execution) and the taken batch is the complete buffer.
    #[test]
    fn enter_autocommit_flips_and_takes_in_one_step() {
        let mut st = TxnState::new();
        st.autocommit = false;
        st.buffer(sql("UPDATE a"));
        st.buffer(sql("UPDATE b"));
        let taken = st.enter_autocommit();
        assert!(st.autocommit());
        assert!(st.pending.is_empty());
        assert_eq!(
            taken.iter().map(|s| s.sql()).collect::<Vec<_>>(),
            ["UPDATE a", "UPDATE b"]
        );
    }

    /// The failure path must re-enter manual mode with the batch re-buffered — replaying the
    /// toggle (or `commit`) then applies exactly the original transaction, and any DML that
    /// somehow got buffered in between stays *behind* the restored batch.
    #[test]
    fn restore_manual_rebuffers_in_front() {
        let mut st = TxnState::new();
        st.autocommit = false;
        st.buffer(sql("UPDATE a"));
        let taken = st.enter_autocommit();
        // Defensively simulate a buffer written during the window (run_or_buffer cannot actually
        // do this while autocommit is on): restore must keep it, ordered after the original batch.
        st.buffer(sql("UPDATE late"));
        st.restore_manual(taken);
        assert!(!st.autocommit());
        assert_eq!(pending_sqls(&st), ["UPDATE a", "UPDATE late"]);
    }
}

/// A handle to a connection's transaction state, shared with its statements.
pub(crate) type SharedTxn = Arc<Mutex<TxnState>>;

/// An ADBC connection to a Spanner database.
///
/// # Transactions
///
/// The connection is in **autocommit** mode by default. Setting `adbc.connection.autocommit` to
/// `false` enters **manual** transaction mode, in which DML is *buffered* and applied atomically
/// in one read/write transaction on [`Connection::commit`] ([`Connection::rollback`] discards the
/// buffer), because the Spanner client exposes read/write transactions only through a
/// closure-based runner (no begin/commit handle). Two consequences callers must be aware of:
///
/// - **No read-your-writes:** queries always run immediately in a fresh read-only snapshot, so a
///   query does not observe DML buffered earlier in the same manual transaction — an `INSERT`
///   followed by a `SELECT COUNT(*)` returns the *pre-insert* count. Commit first if a statement
///   needs to see earlier writes.
/// - **DML and DDL reorder:** DDL also runs immediately (DDL is never transactional in Spanner),
///   so DDL issued after buffered DML executes before it.
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
    /// Read staleness / timestamp bound for read-only queries (`spanner.read.staleness` /
    /// `spanner.read.timestamp`). The default is a strong read; this becomes the default for
    /// statements created on the connection, which may override it.
    read_staleness: ReadStaleness,
    txn: SharedTxn,
    /// Cancellation signal for this connection's in-flight metadata/commit operations
    /// (see [`Connection::cancel`]).
    cancel: CancelSignal,
}

impl SpannerConnection {
    pub(crate) fn new(runtime: SharedRuntime, connected: Connected) -> Self {
        Self {
            runtime,
            client: connected.client,
            spanner: connected.spanner,
            database: connected.database,
            read_only: Arc::new(AtomicBool::new(false)),
            isolation: IsolationLevel::Unspecified,
            read_staleness: ReadStaleness::default(),
            txn: Arc::new(Mutex::new(TxnState::new())),
            cancel: CancelSignal::new(),
        }
    }

    /// Apply the buffered DML statements atomically in one read/write transaction, discarding the
    /// affected-row count (a commit reports no count).
    fn apply_transaction(&self, statements: Vec<SpannerSql>) -> Result<()> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        run_batch_dml(
            &self.runtime,
            &self.client,
            &self.cancel,
            self.isolation.clone(),
            statements,
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
/// `default` value leaves the database default in place. Any other spec level (read uncommitted /
/// read committed / snapshot / linearizable) is rejected with `NotImplemented` rather than silently
/// ignored, so callers are not misled into thinking an unsupported guarantee is in effect.
fn parse_isolation_level(value: OptionValue) -> Result<IsolationLevel> {
    use adbc_core::constants::*;
    let s = match value {
        OptionValue::String(s) => s,
        _ => {
            return Err(invalid_argument(
                "expected a string isolation-level option value",
            ))
        }
    };
    match s.as_str() {
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT => Ok(IsolationLevel::Unspecified),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE => Ok(IsolationLevel::Serializable),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ => Ok(IsolationLevel::RepeatableRead),
        ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED
        | ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED
        | ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT
        | ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE => Err(not_implemented(&format!(
            "Spanner does not support isolation level {s:?}; supported: {ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE:?}, {ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ:?}, {ADBC_OPTION_ISOLATION_LEVEL_DEFAULT:?}"
        ))),
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
/// an autocommit bulk ingest (which calls this once per chunk so a retry only ever clones one
/// chunk, not the whole ingest).
pub(crate) fn run_batch_dml(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    isolation: IsolationLevel,
    statements: Vec<SpannerSql>,
) -> Result<i64> {
    if statements.is_empty() {
        return Ok(0);
    }
    let client = client.clone();
    block_on_cancellable(runtime, cancel, async move {
        let runner = apply_isolation(client.read_write_transaction(), isolation)
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
    db_schema: &str,
    table_name: &str,
) -> Result<bool> {
    let client = client.clone();
    let (schema, table) = (db_schema.to_string(), table_name.to_string());
    block_on_cancellable(runtime, cancel, async move {
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
        let (_schema, batch) = result_set_to_batch(result_set).await?;
        Ok::<bool, Error>(batch.num_rows() > 0)
    })
}

/// Run a query and return its single materialised record batch. Shared with the `INFORMATION_SCHEMA`
/// collectors in [`crate::objects`] and [`crate::statistics`].
pub(crate) async fn query_batch(client: &DatabaseClient, sql: &str) -> Result<RecordBatch> {
    let transaction = client.single_use().build();
    let result_set = transaction
        .execute_query(SpannerSql::builder(sql).build())
        .await
        .map_err(from_spanner)?;
    let (_schema, batch) = result_set_to_batch(result_set).await?;
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
                if let Some(pending) = pending {
                    if let Err(e) = self.apply_transaction(pending.clone()) {
                        self.txn.lock().unwrap().restore_manual(pending);
                        return Err(e);
                    }
                }
            }
            OptionConnection::ReadOnly => {
                self.read_only.store(parse_bool(value)?, Ordering::Release)
            }
            OptionConnection::IsolationLevel => self.isolation = parse_isolation_level(value)?,
            OptionConnection::Other(k) if k == crate::OPTION_READ_STALENESS => {
                self.read_staleness.set_staleness(value)?;
            }
            OptionConnection::Other(k) if k == crate::OPTION_READ_TIMESTAMP => {
                self.read_staleness.set_timestamp(value)?;
            }
            other => {
                return Err(not_implemented(&format!(
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
            OptionConnection::ReadOnly => Ok(self.read_only.load(Ordering::Acquire).to_string()),
            OptionConnection::IsolationLevel => {
                Ok(isolation_to_adbc_string(&self.isolation).to_string())
            }
            OptionConnection::Other(k) if k == crate::OPTION_READ_STALENESS => self
                .read_staleness
                .staleness_string()
                .map(str::to_string)
                .ok_or_else(|| {
                    err(
                        format!("option {} is not set", crate::OPTION_READ_STALENESS),
                        Status::NotFound,
                    )
                }),
            OptionConnection::Other(k) if k == crate::OPTION_READ_TIMESTAMP => self
                .read_staleness
                .timestamp_string()
                .map(str::to_string)
                .ok_or_else(|| {
                    err(
                        format!("option {} is not set", crate::OPTION_READ_TIMESTAMP),
                        Status::NotFound,
                    )
                }),
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
            self.read_only.clone(),
            self.isolation.clone(),
            self.read_staleness.clone(),
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
        let result = block_on_cancellable(&self.runtime, &self.cancel, async move {
            let transaction = crate::staleness::single_use(&client, bound);
            let result_set = transaction
                .execute_query(SpannerSql::builder(sql).build())
                .await
                .map_err(from_spanner)?;
            result_set_to_batch(result_set).await
        });
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
    /// Spanner keeps no cheap/pre-computed statistics, so an `approximate` request returns nothing
    /// rather than triggering the expensive exact scans; pass `approximate = false` to compute them.
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
        let schemas = if approximate {
            Vec::new()
        } else {
            crate::statistics::collect_statistics(
                &self.runtime,
                &self.client,
                &self.cancel,
                &self.read_staleness,
                db_schema,
                table_name,
            )?
        };
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
        let pending = {
            let st = self.txn.lock().unwrap();
            if st.autocommit {
                return Err(invalid_state(
                    "commit invoked with autocommit enabled; no active transaction",
                ));
            }
            st.pending.clone()
        };
        let applied = pending.len();
        self.apply_transaction(pending)?;
        // Drain exactly the statements that were applied; anything buffered concurrently while
        // the commit RPC ran stays pending for the next commit.
        self.txn.lock().unwrap().pending.drain(..applied);
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
        Ok(())
    }

    /// Execute a partition descriptor produced by `Statement::execute_partitions` and stream its
    /// rows as Arrow.
    ///
    /// # Security
    ///
    /// A partition descriptor is **opaque but executable**: it is serde-JSON of the client's
    /// `Partition`, whose inner `ExecuteSqlRequest` carries the SQL text itself along with the
    /// session and transaction identity. `read_partition` runs whatever that blob contains against
    /// this connection's `DatabaseClient`, with **this connection's credentials** — so a crafted
    /// descriptor executes arbitrary SQL as the connection's principal. This is inherent to ADBC's
    /// portable-descriptor design and the upstream serde format, and there is no in-band
    /// authentication of the blob. Treat a descriptor as an executable request, not as opaque data:
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
        // no per-statement batch-size option, so the default chunk size is used.
        let reader = block_on_cancellable(&self.runtime, &self.cancel, async move {
            let result_set = partition.execute(&client).await.map_err(from_spanner)?;
            stream_query(runtime, cancel, result_set, DEFAULT_ROWS_PER_BATCH).await
        })?;
        Ok(Box::new(reader))
    }
}

/// Decode an opaque partition descriptor produced by `Statement::execute_partitions` — the
/// serde-JSON of the client's [`Partition`]. Anything that does not decode (empty input, non-JSON
/// bytes, or valid JSON of the wrong shape) is an [`Status::InvalidArguments`] error, never a
/// panic. A pure function so the rejection path is unit-testable without a connection.
fn decode_partition(descriptor: &[u8]) -> Result<Partition> {
    serde_json::from_slice(descriptor)
        .map_err(|e| invalid_argument(format!("invalid partition descriptor: {e}")))
}

fn parse_bool(value: OptionValue) -> Result<bool> {
    crate::options::bool_option(value, "option")
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
    fn rejects_unsupported_isolation_levels() {
        use adbc_core::constants::*;
        let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
        // Spec levels Spanner cannot honour are rejected (NotImplemented), not silently ignored.
        for level in [
            ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED,
            ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED,
            ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
            ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE,
        ] {
            let err = parse(level).unwrap_err();
            assert_eq!(err.status, Status::NotImplemented, "level {level}");
        }
        // A completely unknown value is an invalid argument.
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
    fn lookup_catalog_accepts_only_the_default_empty_catalog() {
        // Spanner's single catalog is the empty string; `None` means "don't filter".
        assert!(check_lookup_catalog(None).is_ok());
        assert!(check_lookup_catalog(Some("")).is_ok());
        // Any named catalog does not exist, so a lookup in it is NotFound.
        let err = check_lookup_catalog(Some("main")).unwrap_err();
        assert_eq!(err.status, Status::NotFound);
        assert!(err.message.contains("\"main\""), "{}", err.message);
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
}

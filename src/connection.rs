//! The [`SpannerConnection`] — an ADBC connection backed by a Spanner [`DatabaseClient`].
//!
//! ## Transactions
//!
//! By default the connection is in **autocommit** mode: every statement runs in its own Spanner
//! transaction (a single-use read-only transaction for queries, a read/write transaction for
//! DML).
//!
//! Setting the `adbc.connection.autocommit` option to `false` begins **manual** transaction mode.
//! A manual transaction is exactly one of two kinds — **queries** or **DML** — fixed by its
//! *first* statement; a statement of the other kind is rejected with [`Status::InvalidState`]
//! until [`Connection::commit`] or [`Connection::rollback`] ends the transaction:
//!
//! - **Queries** (first statement is a data-returning read): the driver opens one **multi-use
//!   read-only transaction** and runs every query of the transaction on it, so all reads observe
//!   a single consistent snapshot (pinned at the first query's `spanner.read.staleness` bound —
//!   the bounded-staleness kinds are pinned to their most-stale legal equivalent, as on any
//!   multi-use transaction). Commit and rollback are local: a Spanner read-only transaction needs
//!   no commit/rollback RPC, so the snapshot is simply dropped.
//! - **DML** (first statement is DML or a bulk ingest): because Spanner's client only exposes
//!   read/write transactions through a closure-based runner (there is no public begin/commit
//!   handle), the driver *buffers* DML statements — and the insert **mutations** of any bulk
//!   ingest — and applies the whole batch atomically in a single read/write transaction on
//!   commit, which also makes the retry-on-abort safe, since the buffer is simply replayed.
//!
//! **DDL is not transaction-aware.** Matching the ADBC BigQuery driver — which classifies
//! nothing and sends every statement down its one execution path — DDL always executes
//! immediately (through the admin `UpdateDatabaseDdl` API; Spanner DDL is never transactional)
//! and leaves the transaction state untouched: it neither fixes the transaction's kind nor is
//! rejected by it, and `commit`/`rollback` never affect it.
//!
//! [`Connection::rollback`] discards the buffered work (or drops the read-only snapshot).
//!
//! Consequences of this model, which callers should be aware of:
//! - In manual mode, `execute_update` on DML returns `None` (the affected-row count is not known
//!   until commit).
//! - DML with a `THEN RETURN` clause is rejected in manual mode: it must run via `ExecuteSql` to
//!   produce its rows, but buffered DML is applied through `ExecuteBatchDml` (which does not
//!   support `THEN RETURN`) — and the rows would be unobtainable at commit time anyway.
//! - **No read-your-writes (guarded):** buffered DML only executes at commit, so a query could
//!   never observe it. Rather than silently returning a *pre-write* result, a data-returning
//!   query (`execute`, the bound-query path, `execute_partitions`, and a query routed through
//!   `execute_update` — which executes it read-only and discards the rows) issued in a manual
//!   transaction that began with DML is rejected with [`Status::InvalidState`] — the kind-mixing
//!   rule above. (`execute_schema`, a schema-only PLAN probe returning no data, is not guarded;
//!   partitioned reads run in their own batch read-only transaction and do not join a query
//!   transaction's snapshot.)
//! - **DML and DDL reorder:** DDL executes immediately, so DDL issued after buffered DML runs
//!   before it. (Inside a query transaction, immediate DDL is invisible to the pinned snapshot —
//!   ordinary snapshot semantics.)
//! - **Ingest mutations apply at commit time:** a buffered bulk ingest's insert mutations are
//!   applied by Spanner as part of the commit itself — after every buffered DML statement in the
//!   transaction has executed, regardless of issue order — so DML in the same transaction cannot
//!   observe the ingested rows.
//! - **A read-only connection cannot commit buffered writes.** `adbc.connection.readonly` rejects
//!   *all* writes, and the commit that applies buffered DML / ingest mutations is one: with the
//!   flag set, [`Connection::commit`] — and enabling `adbc.connection.autocommit`, which commits
//!   any pending work as a side effect — fails with [`Status::InvalidState`] and leaves the
//!   transaction open and replayable (clear the flag and commit again to apply it). Ending a
//!   transaction that writes nothing is never gated: a query transaction commits (its snapshot is
//!   just dropped) and [`Connection::rollback`] always works, since discarding buffered work
//!   writes nothing.
//! - A **failed** commit keeps the buffer and the transaction open: the caller can retry
//!   [`Connection::commit`] (replaying the batch) or [`Connection::rollback`] to discard it. The
//!   same holds when re-enabling autocommit fails to commit the buffer: the connection stays in
//!   manual mode. On `ABORTED` (the retriable code preserved in `vendor_code`) the failed attempt
//!   is guaranteed not to have committed, so the replay is exact; after an *ambiguous* transport
//!   failure the usual Spanner caveat applies — the commit may have landed, so a replay can apply
//!   the batch twice unless the DML is idempotent. A **mutations-only** transaction (bulk ingests
//!   that buffered no DML) is exempt: it commits through the client's replay-protected write-only
//!   transaction ([`write_mutations_txn`]), which applies the mutations exactly once even across
//!   ambiguous transport failures.

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
use google_cloud_spanner::transaction::{MultiUseReadOnlyTransaction, ReadWriteTransaction};

use crate::conversion::{TimestampPrecision, result_set_to_batch, stream_query};
use crate::directed_read::DirectedRead;
use crate::driver::{Connected, SharedDatabaseAdmin};
use crate::error::{err, from_spanner, invalid_argument, invalid_state, not_implemented};
use crate::options::impl_shared_option_dispatch;
use crate::query_options::QueryOptionsConfig;
use crate::request::{CommitStats, RequestConfig};
use crate::retry::RetryConfig;
use crate::runtime::{CancelSignal, CancelSlot, SharedRuntime, block_on_cancellable};
use crate::sql::qualified_table;
use crate::staleness::ReadStaleness;
use crate::statement::{DEFAULT_ROWS_PER_BATCH, SpannerStatement};
use crate::timeout::{RpcTimeouts, with_timeout};

/// What a manual transaction has become — fixed by its **first** statement, after which work of
/// the other kind is rejected with [`Status::InvalidState`] until `commit` or `rollback` (see
/// [`TxnState::check_kind_allowed`]).
///
/// DDL is deliberately **not** a kind: like the ADBC BigQuery driver — which classifies nothing
/// and lets every statement run down the one execution path — this driver never gates DDL on the
/// transaction. DDL executes immediately through the admin API (Spanner DDL is never
/// transactional) and leaves the transaction state untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnKind {
    /// Data-returning queries, all running on one shared multi-use read-only transaction (a
    /// single consistent snapshot).
    Read,
    /// DML statements and bulk-ingest mutations, buffered and applied atomically in one
    /// read/write transaction on commit.
    Dml,
}

impl TxnKind {
    /// How the kind reads in an error message, as the thing being run / that began the txn.
    fn what(self) -> &'static str {
        match self {
            TxnKind::Read => "a query",
            TxnKind::Dml => "DML",
        }
    }

    /// Why work of the other kind cannot join a transaction of this (active) kind.
    fn rationale(self) -> &'static str {
        match self {
            TxnKind::Read => "the transaction is pinned to a multi-use read-only snapshot",
            TxnKind::Dml => {
                "the buffered DML is applied at commit, so a query would not observe the \
                 pending writes (no read-your-writes)"
            }
        }
    }
}

/// The state — kind *and* payload — of a manual transaction. The kind is fixed by the
/// transaction's **first** statement; holding the payload inside the variant makes the kinds
/// mutually exclusive by construction (a transaction cannot simultaneously carry a read-only
/// snapshot and buffered DML).
#[derive(Debug, Default, Clone)]
enum ManualTxn {
    /// No statement has fixed the transaction's kind yet.
    #[default]
    Unset,
    /// Began with a data-returning query: every query in the transaction runs on this shared
    /// multi-use read-only transaction, so all reads observe one consistent snapshot.
    /// `Arc`-shared so a statement can execute on it without holding the [`SharedTxn`] lock;
    /// dropped (no commit/rollback RPC needed) when the transaction ends.
    Read(Arc<MultiUseReadOnlyTransaction>),
    /// Began with DML or a bulk ingest: statements and insert mutations buffered here are
    /// applied atomically in one read/write transaction on commit. Built statements (not raw
    /// SQL) so that parameterized DML — which carries bound values — buffers just like a plain
    /// `;`-batch does. Spanner applies buffered mutations at commit time, so they land *after*
    /// every buffered DML statement executes, regardless of the order they were issued in.
    Dml {
        statements: Vec<SpannerSql>,
        mutations: Vec<Mutation>,
    },
}

impl ManualTxn {
    fn kind(&self) -> Option<TxnKind> {
        match self {
            ManualTxn::Unset => None,
            ManualTxn::Read(_) => Some(TxnKind::Read),
            ManualTxn::Dml { .. } => Some(TxnKind::Dml),
        }
    }

    /// Whether there is buffered work a commit must apply (a read-only transaction has none —
    /// it ends by being dropped).
    fn has_pending_work(&self) -> bool {
        match self {
            ManualTxn::Unset | ManualTxn::Read(_) => false,
            ManualTxn::Dml {
                statements,
                mutations,
            } => !statements.is_empty() || !mutations.is_empty(),
        }
    }
}

/// Transaction state shared between a connection and the statements it creates.
#[derive(Debug)]
pub(crate) struct TxnState {
    /// When false, the connection is in manual transaction mode: DML and DDL buffer into
    /// [`Self::txn`] and queries run on its shared read-only transaction.
    autocommit: bool,
    /// The manual transaction's state. Always [`ManualTxn::Unset`] in autocommit mode (ending a
    /// manual transaction resets it, and nothing buffers while autocommit is on).
    txn: ManualTxn,
}

impl TxnState {
    fn new() -> Self {
        Self {
            autocommit: true,
            txn: ManualTxn::Unset,
        }
    }

    /// Whether the connection is currently in autocommit mode.
    pub(crate) fn autocommit(&self) -> bool {
        self.autocommit
    }

    /// Check that work of `attempted` kind may run in the current transaction. Always allowed in
    /// autocommit mode; in manual mode the transaction's kind is fixed by its **first** statement
    /// (queries or DML) and the other kind is rejected with [`Status::InvalidState`] — naming
    /// both kinds and the reason — until `commit` or `rollback` ends the transaction. (DDL has no
    /// kind and is never checked: it executes immediately, outside the transaction.)
    ///
    /// The statement paths call this under the [`SharedTxn`] lock as part of buffering (the
    /// `buffer_*` methods) or when adopting the shared read-only transaction
    /// ([`Self::start_read_txn`]), which is what keeps the kinds mutually exclusive under
    /// concurrent statements; the read paths additionally call it up front so a mixed-kind query
    /// fails before any work is done.
    pub(crate) fn check_kind_allowed(&self, attempted: TxnKind) -> Result<()> {
        if self.autocommit {
            return Ok(());
        }
        let Some(active) = self.txn.kind() else {
            return Ok(());
        };
        if active == attempted {
            return Ok(());
        }
        Err(invalid_state(format!(
            "cannot run {} in a manual transaction that began with {}: {}. A manual transaction \
             is either queries or DML — its kind is fixed by its first statement. Commit or \
             roll back the transaction first.",
            attempted.what(),
            active.what(),
            active.rationale(),
        )))
    }

    /// The manual transaction's shared read-only transaction, if one is active.
    pub(crate) fn read_txn(&self) -> Option<Arc<MultiUseReadOnlyTransaction>> {
        match &self.txn {
            ManualTxn::Read(txn) => Some(txn.clone()),
            _ => None,
        }
    }

    /// Install `txn` as the manual transaction's shared read-only transaction and return the
    /// effective one — the existing transaction if a concurrent statement won the install race
    /// (`txn` is dropped; it has issued no RPC yet under the default inline begin), otherwise
    /// `txn` itself.
    ///
    /// Re-checks the transaction kind first: the caller built `txn` outside the lock (the build
    /// is async), so a concurrent statement may have fixed the transaction to DML/DDL in the
    /// window — re-checking here, under the same lock the buffer paths write under, closes that
    /// race with the same rejection the caller's up-front guard produces.
    pub(crate) fn start_read_txn(
        &mut self,
        txn: Arc<MultiUseReadOnlyTransaction>,
    ) -> Result<Arc<MultiUseReadOnlyTransaction>> {
        self.check_kind_allowed(TxnKind::Read)?;
        match &self.txn {
            ManualTxn::Read(existing) => Ok(existing.clone()),
            _ => {
                self.txn = ManualTxn::Read(txn.clone());
                Ok(txn)
            }
        }
    }

    /// Buffer DML statements to be applied on the next commit, fixing the transaction's kind to
    /// [`TxnKind::Dml`] (rejecting the buffer if a query fixed it to read-only).
    pub(crate) fn buffer_dml(&mut self, new: Vec<SpannerSql>) -> Result<()> {
        self.check_kind_allowed(TxnKind::Dml)?;
        if new.is_empty() {
            return Ok(());
        }
        match &mut self.txn {
            ManualTxn::Dml { statements, .. } => statements.extend(new),
            txn => {
                *txn = ManualTxn::Dml {
                    statements: new,
                    mutations: Vec::new(),
                }
            }
        }
        Ok(())
    }

    /// Buffer a bulk-ingest insert mutation to be applied on the next commit (alongside any
    /// buffered DML — an ingest counts as DML for the transaction's kind).
    pub(crate) fn buffer_mutation(&mut self, mutation: Mutation) -> Result<()> {
        self.check_kind_allowed(TxnKind::Dml)?;
        match &mut self.txn {
            ManualTxn::Dml { mutations, .. } => mutations.push(mutation),
            txn => {
                *txn = ManualTxn::Dml {
                    statements: Vec::new(),
                    mutations: vec![mutation],
                }
            }
        }
        Ok(())
    }

    /// Atomically flip into autocommit mode and take the manual transaction's state — whose
    /// buffered DML work, if any, must be committed first (a taken read-only transaction
    /// needs no commit; taking it out ends it by drop).
    ///
    /// Doing both in one step — under the caller's single lock acquisition — is what closes the
    /// enable-autocommit race: the buffer paths check the mode under this same mutex, so once the
    /// mode reads autocommit no statement can add to the buffer, and the state taken here is the
    /// complete transaction. Flipping only *after* the apply (in a later acquisition) would
    /// strand any DML buffered while the commit RPC was in flight.
    fn enter_autocommit(&mut self) -> ManualTxn {
        self.autocommit = true;
        std::mem::take(&mut self.txn)
    }

    /// Re-enter manual mode with the taken state restored — the failure path of
    /// [`Self::enter_autocommit`], so a failed apply keeps the transaction open and replayable
    /// (retry the toggle or `commit`, or `rollback` to discard). Nothing can have buffered while
    /// autocommit was on (`enter_autocommit` flips the mode and takes the state under one lock
    /// acquisition, and every buffer path checks the mode under the same mutex), so the current
    /// state is still `Unset` and the taken state simply moves back in.
    fn restore_manual(&mut self, work: ManualTxn) {
        self.autocommit = false;
        debug_assert!(matches!(self.txn, ManualTxn::Unset));
        self.txn = work;
    }

    /// End a committed transaction: remove exactly the `applied` work — anything buffered
    /// *concurrently* while the commit RPC ran (appended behind the applied prefix under this
    /// mutex) stays pending, and keeps the kind, for the next commit — and otherwise reset the
    /// state, which also ends a read-only transaction by dropping its snapshot, so the next
    /// statement fixes a fresh kind.
    fn finish_commit(&mut self, applied: &ManualTxn) {
        if let (
            ManualTxn::Dml {
                statements,
                mutations,
            },
            ManualTxn::Dml {
                statements: applied_statements,
                mutations: applied_mutations,
            },
        ) = (&mut self.txn, applied)
        {
            statements.drain(..applied_statements.len());
            mutations.drain(..applied_mutations.len());
        }
        if !self.txn.has_pending_work() {
            self.txn = ManualTxn::Unset;
        }
    }
}

/// Enforce `adbc.connection.readonly` on the commit paths: a read-only connection rejects **all**
/// writes, and a commit of buffered DML / ingest mutations is a write like any other — the flag
/// would otherwise be a statement-path-only guard that `commit()` (or the autocommit toggle, which
/// commits pending work as a side effect) silently walks around.
///
/// `read_only` is the flag's live value; `work` is the state the caller is about to apply. Only
/// work that would actually *write* is rejected, so ending a transaction that writes nothing still
/// succeeds on a read-only connection: an `Unset` transaction and a query transaction
/// ([`ManualTxn::Read`]) apply nothing — their commit only drops the snapshot.
///
/// The rejection leaves the buffer with the caller (`commit` never reaches `finish_commit`; the
/// autocommit toggle restores the taken state via [`TxnState::restore_manual`]), so the
/// transaction stays open and replayable — the same shape as any other failed commit. Clearing
/// `adbc.connection.readonly` and committing again applies exactly the buffered work; `rollback`
/// is never gated by the flag, since discarding buffered work writes nothing.
fn check_commit_writable(read_only: bool, work: &ManualTxn) -> Result<()> {
    if read_only && work.has_pending_work() {
        return Err(invalid_state(
            "cannot commit buffered DML: the connection is read-only. The buffered work is kept \
             and stays replayable: clear adbc.connection.readonly and commit again to apply it, \
             or roll back to discard it.",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod txn_state_tests {
    use adbc_core::error::Status;

    use super::{ManualTxn, Mutation, SpannerSql, TxnKind, TxnState, check_commit_writable};

    fn sql(s: &str) -> SpannerSql {
        SpannerSql::builder(s).build()
    }

    fn mutation(id: i64) -> Mutation {
        Mutation::new_insert_builder("t").set("Id").to(&id).build()
    }

    fn manual() -> TxnState {
        let mut st = TxnState::new();
        st.autocommit = false;
        st
    }

    /// In autocommit mode every kind passes the check — each statement is its own transaction,
    /// so there is no kind to mix with.
    #[test]
    fn autocommit_allows_every_kind() {
        let st = TxnState::new();
        for kind in [TxnKind::Read, TxnKind::Dml] {
            st.check_kind_allowed(kind)
                .expect("autocommit allows every kind");
        }
    }

    /// An unset manual transaction allows either first kind; the first buffered DML fixes the
    /// kind, after which queries are rejected with `InvalidState` (keeping the read-your-writes
    /// rationale in the message, and naming the active kind) until the transaction ends.
    #[test]
    fn first_statement_fixes_the_transaction_kind() {
        let mut st = manual();
        for kind in [TxnKind::Read, TxnKind::Dml] {
            st.check_kind_allowed(kind)
                .expect("an unset transaction allows either first kind");
        }
        st.buffer_dml(vec![sql("UPDATE a")])
            .expect("first DML fixes the kind");
        st.check_kind_allowed(TxnKind::Dml)
            .expect("more DML joins the transaction");
        let error = st.check_kind_allowed(TxnKind::Read).unwrap_err();
        assert_eq!(error.status, Status::InvalidState);
        assert!(
            error.message.contains("read-your-writes"),
            "the query rejection should explain the read-your-writes hazard: {}",
            error.message
        );
        assert!(
            error.message.contains("began with DML"),
            "the rejection should name the active kind: {}",
            error.message
        );
    }

    /// An ingest mutation counts as DML for the transaction's kind: it mixes freely with DML,
    /// and queries are rejected against it exactly as against buffered DML.
    #[test]
    fn mutations_share_the_dml_kind() {
        let mut st = manual();
        st.buffer_mutation(mutation(1))
            .expect("first ingest fixes the kind to DML");
        st.buffer_dml(vec![sql("UPDATE a")])
            .expect("DML joins an ingest-started transaction");
        let error = st.check_kind_allowed(TxnKind::Read).unwrap_err();
        assert_eq!(error.status, Status::InvalidState);
    }

    /// The mode flip and the state take must be one atomic step: after `enter_autocommit` the
    /// state already reads as autocommit (so the buffer paths, which check under the same mutex,
    /// route new DML to immediate execution) and the taken state is the complete transaction —
    /// both the DML statements and the mutations.
    #[test]
    fn enter_autocommit_flips_and_takes_in_one_step() {
        let mut st = manual();
        st.buffer_dml(vec![sql("UPDATE a"), sql("UPDATE b")])
            .unwrap();
        st.buffer_mutation(mutation(1)).unwrap();
        let taken = st.enter_autocommit();
        assert!(st.autocommit());
        assert!(matches!(st.txn, ManualTxn::Unset));
        let ManualTxn::Dml {
            statements,
            mutations,
        } = taken
        else {
            panic!("the taken state must be the DML transaction");
        };
        assert_eq!(
            statements.iter().map(|s| s.sql()).collect::<Vec<_>>(),
            ["UPDATE a", "UPDATE b"]
        );
        assert_eq!(mutations, [mutation(1)]);
    }

    /// The failure path must re-enter manual mode with the taken state restored — replaying the
    /// toggle (or `commit`) then applies exactly the original transaction.
    #[test]
    fn restore_manual_restores_the_taken_state() {
        let mut st = manual();
        st.buffer_dml(vec![sql("UPDATE a")]).unwrap();
        let taken = st.enter_autocommit();
        st.restore_manual(taken);
        assert!(!st.autocommit());
        assert!(matches!(&st.txn, ManualTxn::Dml { statements, .. } if statements.len() == 1));
    }

    /// `finish_commit` removes exactly the applied prefix: work buffered concurrently while the
    /// commit RPC ran stays pending (keeping the kind) for the next commit, and a fully-drained
    /// transaction resets to `Unset` so the next statement fixes a fresh kind.
    #[test]
    fn finish_commit_keeps_concurrently_buffered_work() {
        let mut st = manual();
        st.buffer_dml(vec![sql("UPDATE a")]).unwrap();
        let applied = st.txn.clone();
        // A statement buffers more DML while the commit RPC is in flight.
        st.buffer_dml(vec![sql("UPDATE late")]).unwrap();
        st.finish_commit(&applied);
        let ManualTxn::Dml { statements, .. } = &st.txn else {
            panic!("the late DML must stay pending, keeping the kind");
        };
        assert_eq!(
            statements.iter().map(|s| s.sql()).collect::<Vec<_>>(),
            ["UPDATE late"]
        );
        // Committing the remainder drains the state fully, resetting the kind.
        let applied = st.txn.clone();
        st.finish_commit(&applied);
        assert!(matches!(st.txn, ManualTxn::Unset));
    }

    /// `adbc.connection.readonly` rejects the commit paths too, not just the statement write
    /// paths: buffered DML — and buffered ingest mutations — are writes, so applying them on a
    /// read-only connection must fail with `InvalidState` rather than sneak a write through
    /// `commit()` / the autocommit toggle.
    #[test]
    fn read_only_rejects_a_commit_that_would_write() {
        for work in [
            ManualTxn::Dml {
                statements: vec![sql("UPDATE a SET x = 1 WHERE y = 2")],
                mutations: Vec::new(),
            },
            ManualTxn::Dml {
                statements: Vec::new(),
                mutations: vec![mutation(1)],
            },
        ] {
            let error = check_commit_writable(true, &work)
                .expect_err("a read-only connection must not commit buffered writes");
            assert_eq!(error.status, Status::InvalidState);
            assert!(
                error.message.contains("read-only"),
                "the rejection should name the read-only flag: {}",
                error.message
            );
            // The same work commits fine once the flag is clear.
            check_commit_writable(false, &work)
                .expect("a writable connection commits the buffered work");
        }
    }

    /// The guard gates *writes*, not the act of ending a transaction: a transaction with nothing
    /// to apply — never started, or fully drained by a previous commit — still commits cleanly on
    /// a read-only connection. (A query transaction, `ManualTxn::Read`, is likewise pending-work
    /// free; its client-owned snapshot cannot be built offline, so the wire-level proof that a
    /// read-only connection can still commit one lives in `tests/mock_spanner.rs`.)
    #[test]
    fn read_only_allows_a_commit_with_nothing_to_write() {
        for work in [
            ManualTxn::Unset,
            ManualTxn::Dml {
                statements: Vec::new(),
                mutations: Vec::new(),
            },
        ] {
            check_commit_writable(true, &work)
                .expect("committing nothing writes nothing, so read-only must allow it");
        }
    }
}

/// A handle to a connection's transaction state, shared with its statements.
pub(crate) type SharedTxn = Arc<Mutex<TxnState>>;

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
/// See the [crate documentation](crate) — and the fuller module-level notes in `connection.rs` —
/// for the list of consequences (no read-your-writes, `None` DML counts before commit,
/// commit-failure replay semantics).
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
    /// Per-operation cancellation for this connection's metadata/commit operations (see
    /// [`Connection::cancel`]): each entry point mints a fresh [`CancelSignal`] here, and
    /// `cancel()` latches the current one — forever, so a cancelled `read_partition` stream stays
    /// cancelled even after this connection starts a new operation.
    cancel: CancelSlot,
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
            cancel: CancelSlot::new(),
        }
    }

    /// The *live* value of this connection's `adbc.connection.readonly` flag — the same shared
    /// flag the statement write paths load at execution time.
    fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Acquire)
    }

    /// Apply the buffered work of a manual transaction: DML statements and ingest mutations
    /// atomically in one transaction. A read-only (or empty) transaction has nothing to apply —
    /// its snapshot ends by being dropped when the caller clears the state.
    ///
    /// Rejected outright when the connection is `adbc.connection.readonly` and there *is*
    /// buffered work to apply (see [`check_commit_writable`]); the caller keeps the buffer, so
    /// the transaction stays replayable exactly as after any other failed commit.
    fn apply_manual_txn(&self, work: &ManualTxn) -> Result<()> {
        check_commit_writable(self.is_read_only(), work)?;
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
        // A new operation begins: mint a fresh cancel signal for it, so a stale cancel aimed at a
        // previous operation does not leak in — and so no later operation can un-cancel this one's
        // streamed reader (see `CancelSlot`).
        self.cancel.begin_operation();
        if statements.is_empty() {
            return write_mutations_txn(
                &self.runtime,
                &self.client,
                &self.cancel.current(),
                self.request.clone(),
                self.retry,
                self.timeouts.update_timeout(),
                &self.commit_stats,
                mutations,
            );
        }
        run_batch_txn(
            &self.runtime,
            &self.client,
            &self.cancel.current(),
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
            &self.cancel.current(),
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
#[must_use]
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
/// attempt. This is the autocommit DML path: the batch is a complete transaction of its own,
/// applied immediately. Batches that belong to a manual transaction (and may carry buffered
/// mutations) go through [`run_batch_txn`] instead.
///
/// `last_statement` optimization: an autocommit batch — whether a single statement or a
/// multi-statement `;`-batch — is by construction the transaction's *entire* content: the runner
/// runs this one `ExecuteBatchDml` and immediately commits, with no further statement, read, or
/// query in the transaction. Flagging the batch as the transaction's last request
/// (`ExecuteBatchDmlRequest.last_statements`) lets Spanner release the transaction as part of the
/// same round-trip, so the trailing `Commit` needs no extra server work — covering the common
/// single-DML case and dbt-style `DELETE …; INSERT …` batches alike. Mutation-carrying /
/// manual-commit batches instead go through [`run_batch_txn`] with the flag off (their commit
/// still applies buffered mutations, so the batch is *not* the transaction's last request).
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
    // Every autocommit batch is the whole transaction — nothing follows it before the commit —
    // so it is always the transaction's last request (see the doc comment above).
    let last_statements = true;
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

/// Commit `mutations` alone — no DML — in one **write-only** transaction
/// (`WriteOnlyTransaction::write`): the mutations-only manual-commit path, and (via the
/// statement's `write_mutation_chunk`) each chunk of an autocommit bulk ingest.
///
/// Unlike the read/write runner — whose commit, replayed after an *ambiguous* transport failure,
/// can apply the batch twice — `write` is replay-protected: it begins the transaction with a
/// mutation key and retries internally on `ABORTED`, so on success the mutations were applied
/// **exactly once** whatever the underlying network did. The same commit configuration as the
/// runner path is applied via [`RequestConfig::apply_to_write_only`] /
/// [`RetryConfig::apply_to_write_only`]: commit priority, transaction tag,
/// `spanner.commit.max_delay`, `spanner.commit_stats` (the returned mutation count is recorded
/// into `commit_stats`), and the retry/backoff tuning on the Begin/Commit RPCs. The isolation
/// level does not apply here — the write-only builder exposes no isolation setter, and a
/// transaction that performs no reads has no reads for a level to constrain.
#[allow(clippy::too_many_arguments)] // threads one connection/statement config item per argument
pub(crate) fn write_mutations_txn(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    request: RequestConfig,
    retry: RetryConfig,
    timeout: Option<Duration>,
    commit_stats: &CommitStats,
    mutations: Vec<Mutation>,
) -> Result<()> {
    if mutations.is_empty() {
        return Ok(());
    }
    let client = client.clone();
    let transaction = async move {
        let response = retry
            .apply_to_write_only(request.apply_to_write_only(client.write_only_transaction()))
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
    };
    let mutation_count = block_on_cancellable(
        runtime,
        cancel,
        with_timeout(timeout, crate::OPTION_RPC_TIMEOUT_UPDATE, transaction),
    )?;
    commit_stats.record(mutation_count);
    Ok(())
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
        // Walk the value by byte offset, decoding one `char` at a time, so matching a candidate
        // allocates nothing (the old code collected a `Vec<char>` per value). `_` still consumes
        // exactly one *character*: every advance steps by the decoded char's UTF-8 width.
        let (mut pi, mut vi) = (0usize, 0usize);
        // Pattern index / value byte offset to backtrack to after the most recent `%`.
        let mut star: Option<(usize, usize)> = None;
        while vi < value.len() {
            // The char starting at byte offset `vi`; `vi` only ever lands on char boundaries.
            let ch = value[vi..].chars().next().expect("vi is a char boundary");
            // `%` must be tested before the literal/`_` branch: otherwise a `%` in the pattern that
            // happens to equal the current value char (e.g. both are `%`) would be consumed as a
            // literal instead of acting as a wildcard.
            if pi < p.len() && p[pi] == '%' {
                star = Some((pi, vi));
                pi += 1;
            } else if pi < p.len() && (p[pi] == '_' || p[pi] == ch) {
                pi += 1;
                vi += ch.len_utf8();
            } else if let Some((sp, sv)) = star {
                // Let the last `%` consume one more character and retry.
                let skipped = value[sv..].chars().next().expect("sv is a char boundary");
                pi = sp + 1;
                vi = sv + skipped.len_utf8();
                star = Some((sp, vi));
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

    #[test]
    fn like_matching_multibyte_utf8() {
        // The matcher walks values by byte offset; `_` must still consume one *character* (of any
        // UTF-8 width), never one byte, and `%` backtracking must skip whole characters.
        assert!(like_match("_", "é")); // 2-byte char is one `_`
        assert!(!like_match("__", "é")); // ... not two
        assert!(like_match("caf_", "café"));
        assert!(like_match("_本_", "日本語")); // 3-byte chars
        assert!(!like_match("____", "日本語"));
        assert!(like_match("_", "🦀")); // 4-byte char
        assert!(like_match("%語", "日本語")); // `%` backtracks over multi-byte chars
        assert!(like_match("%本%", "日本語"));
        assert!(!like_match("%x%", "日本語"));
        assert!(like_match("日%語", "日本語"));
        assert!(like_match("é%é", "été"));
        assert!(!like_match("é_é", "été été")); // literal tail must land on the right char
    }
}

impl Optionable for SpannerConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::AutoCommit => {
                let enable = parse_bool(value, "option adbc.connection.autocommit")?;
                // Enabling autocommit commits any active manual transaction. The mode flip and the
                // state take happen in ONE lock acquisition (`enter_autocommit`): once the mode is
                // autocommit, the buffer paths — which check-and-buffer under this same mutex —
                // can no longer add work, so nothing a concurrent statement buffers can be
                // stranded behind the flip (the old read/apply/flip-in-separate-acquisitions shape
                // had exactly that race). Like `commit`, a failed apply must not lose the work:
                // `restore_manual` re-enters manual mode with the state restored so the caller can
                // retry the toggle (a genuine replay) or roll back. Apply from a borrow so the
                // taken state is still around to restore. (A taken read-only transaction has
                // nothing to apply; dropping it ends the snapshot.)
                let pending = {
                    let mut st = self.txn.lock().unwrap();
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
                    self.txn.lock().unwrap().restore_manual(work);
                    return Err(e);
                }
            }
            OptionConnection::ReadOnly => self.read_only.store(
                parse_bool(value, "option adbc.connection.readonly")?,
                Ordering::Release,
            ),
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
            self.admin.clone(),
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
        // Latch the current operation's (sticky) signal: an in-flight metadata/commit operation
        // wakes and returns Cancelled, and a cancel landing between two chunk fetches of a
        // `read_partition` stream still cancels the next fetch — permanently, since the latch is
        // never cleared. The connection's next operation mints a fresh signal instead, so a cancel
        // with nothing running does not affect later operations, and later operations cannot
        // revive a cancelled reader. Statements have their own signal, so this does not affect a
        // query running on a statement from this connection.
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
        // A new operation begins: mint a fresh cancel signal for it, so a stale cancel aimed at a
        // previous operation does not leak in — and so no later operation can un-cancel this one's
        // streamed reader (see `CancelSlot`).
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
        // A new operation begins: mint a fresh cancel signal for it, so a stale cancel aimed at a
        // previous operation does not leak in — and so no later operation can un-cancel this one's
        // streamed reader (see `CancelSlot`).
        self.cancel.begin_operation();
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
            &self.cancel.current(),
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
        // A new operation begins: mint a fresh cancel signal for it, so a stale cancel aimed at a
        // previous operation does not leak in — and so no later operation can un-cancel this one's
        // streamed reader (see `CancelSlot`).
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
            let st = self.txn.lock().unwrap();
            if st.autocommit {
                return Err(invalid_state(
                    "commit invoked with autocommit enabled; no active transaction",
                ));
            }
            st.txn.clone()
        };
        self.apply_manual_txn(&work)?;
        self.txn.lock().unwrap().finish_commit(&work);
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let mut st = self.txn.lock().unwrap();
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
        // A new operation begins: mint a fresh cancel signal for it, so a stale cancel aimed at a
        // previous operation does not leak in — and so no later operation can un-cancel this one's
        // streamed reader (see `CancelSlot`).
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
        let precision = self.timestamp_precision;
        let fetch_timeout = self.timeouts.fetch_timeout();
        let reader = block_on_cancellable(
            &self.runtime,
            &self.cancel.current(),
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

fn parse_bool(value: OptionValue, what: &str) -> Result<bool> {
    crate::options::bool_option(value, what)
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

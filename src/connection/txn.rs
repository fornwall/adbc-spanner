//! The manual-transaction state machine: the kind a transaction has been fixed to ([`TxnKind`]),
//! the payload it buffers ([`ManualTxn`]) and the [`TxnState`] a connection shares with every
//! statement it creates.

use std::sync::{Arc, Mutex};

use adbc_core::error::Result;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::MultiUseReadOnlyTransaction;

use crate::error::invalid_state;

/// What a manual transaction has become — fixed by its **first** statement, after which work of
/// the other kind is rejected with [`Status::InvalidState`](adbc_core::error::Status::InvalidState) until `commit` or `rollback` (see
/// [`TxnState::check_kind_allowed`]).
///
/// DDL is deliberately **not** a kind: it executes immediately through the admin API (Spanner DDL
/// is never transactional) and leaves the transaction state untouched.
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
pub(super) enum ManualTxn {
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
    pub(super) autocommit: bool,
    /// The manual transaction's state. Always [`ManualTxn::Unset`] in autocommit mode (ending a
    /// manual transaction resets it, and nothing buffers while autocommit is on).
    pub(super) txn: ManualTxn,
}

impl TxnState {
    pub(super) fn new() -> Self {
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
    /// (queries or DML) and the other kind is rejected with [`Status::InvalidState`](adbc_core::error::Status::InvalidState) — naming
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
    /// Both must happen in one lock acquisition: the buffer paths check the mode under this same
    /// mutex, so once the mode reads autocommit no statement can add to the buffer and the state
    /// taken here is the complete transaction. Flipping only *after* the apply (in a later
    /// acquisition) would strand any DML buffered while the commit RPC was in flight.
    pub(super) fn enter_autocommit(&mut self) -> ManualTxn {
        self.autocommit = true;
        std::mem::take(&mut self.txn)
    }

    /// Re-enter manual mode with the taken state restored — the failure path of
    /// [`Self::enter_autocommit`], so a failed apply keeps the transaction open and replayable
    /// (retry the toggle or `commit`, or `rollback` to discard). Nothing can have buffered while
    /// autocommit was on (see [`Self::enter_autocommit`]), so the current state is still `Unset`
    /// and the taken state simply moves back in.
    pub(super) fn restore_manual(&mut self, work: ManualTxn) {
        self.autocommit = false;
        debug_assert!(matches!(self.txn, ManualTxn::Unset));
        self.txn = work;
    }

    /// End a committed transaction: remove exactly the `applied` work — anything buffered
    /// *concurrently* while the commit RPC ran (appended behind the applied prefix under this
    /// mutex) stays pending, and keeps the kind, for the next commit — and otherwise reset the
    /// state, which also ends a read-only transaction by dropping its snapshot, so the next
    /// statement fixes a fresh kind.
    pub(super) fn finish_commit(&mut self, applied: &ManualTxn) {
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

/// Enforce `adbc.connection.readonly` on the commit paths: committing buffered DML / ingest
/// mutations is a write like any other, so without this the flag would be a statement-path-only
/// guard that `commit()` — or the autocommit toggle, which commits pending work as a side effect —
/// silently walks around.
///
/// `read_only` is the flag's live value; `work` is the state the caller is about to apply. Only
/// work that would actually *write* is rejected, so ending a transaction that writes nothing still
/// succeeds on a read-only connection: an `Unset` transaction and a query transaction
/// ([`ManualTxn::Read`]) apply nothing — their commit only drops the snapshot.
///
/// The rejection leaves the buffer with the caller (`commit` never reaches `finish_commit`; the
/// autocommit toggle restores the taken state via [`TxnState::restore_manual`]), so the
/// transaction stays open and replayable, like any other failed commit. Clearing the flag and
/// committing again applies exactly the buffered work; `rollback` is never gated, since discarding
/// buffered work writes nothing.
pub(super) fn check_commit_writable(read_only: bool, work: &ManualTxn) -> Result<()> {
    if read_only && work.has_pending_work() {
        return Err(invalid_state(
            "cannot commit buffered DML: the connection is read-only. The buffered work is kept \
             and stays replayable: clear adbc.connection.readonly and commit again to apply it, \
             or roll back to discard it.",
        ));
    }
    Ok(())
}

/// A handle to a connection's transaction state, shared with its statements.
pub(crate) type SharedTxn = Arc<Mutex<TxnState>>;

/// Lock the shared [`TxnState`], recovering the guard even if the mutex was poisoned.
///
/// `TxnState` holds no invariant a poisoned-but-consistent state could violate: every method
/// leaves it well-formed, so a panic while the guard is held (e.g. in a callee) leaves the buffered
/// work either wholly applied or wholly not, never half-updated. Propagating the poison via
/// `.lock().unwrap()` would instead brick the whole connection — one panic latches the mutex and
/// every later txn-state op panics across the C ABI — so we take the inner guard on poison.
pub(crate) fn lock_txn(txn: &SharedTxn) -> std::sync::MutexGuard<'_, TxnState> {
    txn.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests;

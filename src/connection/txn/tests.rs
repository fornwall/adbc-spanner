//! Offline tests for the manual-transaction state machine: kind fixing, the atomic
//! autocommit flip, commit draining and the read-only commit guard.

use adbc_core::error::Status;

use super::{ManualTxn, Mutation, SpannerSql, TxnKind, TxnState, check_commit_writable};

fn sql(s: &str) -> SpannerSql {
    SpannerSql::builder(s).build()
}

fn mutation(id: i64) -> Mutation {
    Mutation::new_insert_builder("t").set("Id").to(id).build()
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

/// A panic while the txn mutex is held poisons it, but `TxnState` has no invariant a
/// poisoned-but-consistent state violates, so `lock_txn` must keep handing out a usable guard
/// rather than propagate the poison (which `.lock().unwrap()` would, bricking the connection).
#[test]
fn lock_txn_recovers_a_poisoned_mutex() {
    use std::sync::{Arc, Mutex};

    use super::lock_txn;

    let shared: Arc<Mutex<TxnState>> = Arc::new(Mutex::new(manual()));

    // Poison the mutex: panic while the guard is held.
    let poisoner = Arc::clone(&shared);
    std::thread::spawn(move || {
        let mut guard = poisoner.lock().unwrap();
        guard.buffer_dml(vec![sql("UPDATE a")]).unwrap();
        panic!("poison the txn mutex");
    })
    .join()
    .unwrap_err();
    assert!(
        shared.is_poisoned(),
        "the panic should have poisoned the mutex"
    );

    // The recovered guard still sees the state the poisoner left behind and stays usable.
    let guard = lock_txn(&shared);
    guard
        .check_kind_allowed(TxnKind::Dml)
        .expect("the recovered DML transaction is still consistent and usable");
    assert_eq!(
        guard.check_kind_allowed(TxnKind::Read).unwrap_err().status,
        Status::InvalidState,
    );
}

//! The read/write-transaction execution paths: the isolation-level plumbing plus the three
//! runners every DML/commit site shares — batch DML, the manual-commit batch, and the
//! mutations-only write-only commit.

use adbc_core::error::{Error, Result};
use adbc_core::options::OptionValue;
use google_cloud_spanner::builder::{BatchDmlBuilder, TransactionRunnerBuilder};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::transaction_options::IsolationLevel;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::{ReadWriteTransaction, TransactionRunner};

use crate::error::{from_spanner, invalid_argument};
use crate::options::SharedConfig;
use crate::request::RequestConfig;
use crate::retry::RetryConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::timeout::with_timeout;

/// Apply the connection's isolation level to a read/write transaction runner builder.
///
/// [`IsolationLevel::Unspecified`] leaves the builder untouched, so no level rides the
/// `TransactionOptions` and Spanner applies its own default, `SERIALIZABLE` (there is no
/// database-level or client-level isolation default to inherit — the option is per-transaction
/// only). A specific level is forwarded to [`TransactionRunnerBuilder::set_isolation_level`].
///
/// This is the only place an isolation level enters the driver, and it is reached only from the
/// read/write (DML) paths. Queries take a [timestamp bound](crate::staleness) instead — Spanner
/// does not accept an isolation level on a read-only or partitioned-DML transaction — and the
/// mutations-only ingest commit uses the write-only builder, which has no isolation setter.
#[must_use]
pub(super) fn apply_isolation(
    builder: TransactionRunnerBuilder,
    isolation: IsolationLevel,
) -> TransactionRunnerBuilder {
    match isolation {
        IsolationLevel::Unspecified => builder,
        level => builder.set_isolation_level(level),
    }
}

/// Build the read/write transaction runner every DML path shares: the connection's isolation
/// level, its commit priority and transaction tag, and its retry/backoff bounds, applied in that
/// order. The one place a read/write transaction is opened from driver-owned code.
pub(crate) async fn build_runner(
    client: &DatabaseClient,
    isolation: IsolationLevel,
    request: &RequestConfig,
    retry: RetryConfig,
) -> Result<TransactionRunner> {
    retry
        .apply_to_runner(
            request.apply_to_runner(apply_isolation(client.read_write_transaction(), isolation)),
        )
        .build()
        .await
        .map_err(from_spanner)
}

/// Map the standard ADBC `adbc.connection.transaction.isolation_level` value to the Spanner client's
/// [`IsolationLevel`]. Spanner exposes two levels, `SERIALIZABLE` and `REPEATABLE_READ`; the
/// `default` value sends none, which Spanner reads as `SERIALIZABLE`.
///
/// **Three spec levels map natively.** Spanner implements `REPEATABLE_READ` as *snapshot
/// isolation* — its proto definition matches ADBC's [`snapshot`] almost verbatim ("all reads
/// performed during the transaction observe a consistent snapshot of the database, and the
/// transaction is only successfully committed in the absence of conflicts between its updates and
/// any concurrent updates that have occurred since that snapshot") — so `snapshot` is an exact
/// match for `REPEATABLE_READ`, not a promotion. That also makes Spanner's `REPEATABLE_READ`
/// *stronger* than the ANSI level of the same name, so it satisfies a `repeatable_read` request too.
///
/// The remaining two levels are **promoted upward** to the weakest supported level that still
/// satisfies their guarantees, rather than being rejected. Isolation levels are
/// minimum-guarantee contracts — each names the *maximum* anomalies it permits — so a stronger
/// level always satisfies a weaker one's request, and promoting upward delivers *at least* what
/// was asked. The ADBC spec's "if the desired isolation level is not supported … return an
/// appropriate error" targets the opposite case, a driver that can only offer something *weaker*;
/// this driver never downgrades. The SQL standard and JDBC's `setTransactionIsolation` likewise
/// sanction substituting a higher level. (An unknown level string is still rejected below.)
///
/// | requested          | mapped to         | rationale                                                  |
/// |--------------------|-------------------|------------------------------------------------------------|
/// | `serializable`     | `SERIALIZABLE`    | native                                                      |
/// | `repeatable_read`  | `REPEATABLE_READ` | native (Spanner's RR is snapshot isolation, stronger than ANSI RR, so it satisfies the request) |
/// | `snapshot`         | `REPEATABLE_READ` | native — Spanner's `REPEATABLE_READ` *is* snapshot isolation |
/// | `read_uncommitted` | `REPEATABLE_READ` | promoted: weakest supported level that satisfies it          |
/// | `read_committed`   | `REPEATABLE_READ` | promoted: weakest supported level that satisfies it          |
/// | `linearizable`     | `SERIALIZABLE`    | promoted: Spanner R/W txns are externally consistent (strict serializable = linearizable) |
///
/// The stored (effective) level is what `get_option` reports back, so callers see the level that
/// will actually run, never an unsupported input echoed. A truly unknown/unparseable level string
/// is still rejected with `InvalidArguments`.
///
/// Note that under `REPEATABLE_READ` Spanner detects **write-write conflicts only**, so a DML
/// statement that reads rows it does not write (a subquery guard, a join, `INSERT … SELECT`) can
/// commit against a stale snapshot and produce write skew — including for a single autocommit
/// statement, not just a multi-statement transaction.
///
/// [`snapshot`]: adbc_core::constants::ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT
pub(super) fn parse_isolation_level(value: OptionValue) -> Result<IsolationLevel> {
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
        // Spanner implements `REPEATABLE_READ` as snapshot isolation, so `snapshot` is an exact
        // native match rather than a promotion (see this function's rustdoc).
        ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT => Ok(IsolationLevel::RepeatableRead),
        // Promote levels Spanner does not natively expose to the weakest supported level that
        // still satisfies their guarantees (see the table in this function's rustdoc).
        ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED
        | ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED => Ok(IsolationLevel::RepeatableRead),
        ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE => Ok(IsolationLevel::Serializable),
        other => Err(invalid_argument(format!(
            "unknown isolation level {other:?}"
        ))),
    }
}

/// The ADBC value string for the stored isolation level, so `get_option` round-trips what was set.
pub(super) fn isolation_to_adbc_string(isolation: &IsolationLevel) -> &'static str {
    use adbc_core::constants::*;
    match isolation {
        IsolationLevel::Serializable => ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
        IsolationLevel::RepeatableRead => ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ,
        // Unspecified and any future variant report as `default` (no level sent → SERIALIZABLE).
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
/// `last_statement` optimization: an autocommit batch — single statement or `;`-batch — is by
/// construction the transaction's *entire* content (the runner runs this one `ExecuteBatchDml`
/// and commits, with nothing else in the transaction). Flagging it as the transaction's last
/// request (`ExecuteBatchDmlRequest.last_statements`) lets Spanner release the transaction in the
/// same round-trip, so the trailing `Commit` needs no extra server work. Mutation-carrying /
/// manual-commit batches go through [`run_batch_txn`] with the flag off (their commit still
/// applies buffered mutations, so the batch is *not* the transaction's last request).
pub(crate) fn run_batch_dml(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    statements: Vec<SpannerSql>,
) -> Result<i64> {
    // Every autocommit batch is the whole transaction — nothing follows it before the commit —
    // so it is always the transaction's last request (see the doc comment above).
    let last_statements = true;
    run_batch_txn(
        runtime,
        client,
        cancel,
        config,
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
/// The caller's `spanner.rpc.timeout_seconds.update` value (`config.timeouts`) is an overall
/// deadline on the whole transaction (including the runner's abort retries); expiry fails with
/// [`Status::Timeout`](adbc_core::error::Status::Timeout). Note a commit whose confirmation the driver stopped waiting for may still
/// have landed server-side, the usual ambiguity of any timed-out commit.
///
/// `last_statements` marks this batch as the transaction's final request (see the
/// [`run_batch_dml`] doc for the `last_statement` optimization). Callers must pass `false` unless
/// the batch is genuinely the whole transaction: the manual-commit path buffers `mutations` that
/// Spanner applies *at* commit, so the batch is never the last request there.
pub(crate) fn run_batch_txn(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    statements: Vec<SpannerSql>,
    mutations: Vec<Mutation>,
    last_statements: bool,
) -> Result<i64> {
    if statements.is_empty() && mutations.is_empty() {
        return Ok(0);
    }
    // Owned copies for the `async move` below (the runner replays its closure on abort).
    let isolation = config.isolation.clone();
    let request = config.request.clone();
    let retry = config.retry;
    let timeout = config.timeouts.update_timeout();
    let client = client.clone();
    let transaction = async move {
        // The commit priority and transaction tag ride on the runner; the request tag rides on the
        // ExecuteBatchDml batch inside the (retryable) closure.
        let runner = build_runner(&client, isolation, &request, retry).await?;
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
    config.commit_stats.record(mutation_count);
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
/// runner path is applied via [`RequestConfig::apply_to_write_only`](crate::request::RequestConfig::apply_to_write_only) /
/// [`RetryConfig::apply_to_write_only`](crate::retry::RetryConfig::apply_to_write_only): commit priority, transaction tag,
/// `spanner.commit.max_delay`, `spanner.commit_stats` (the returned mutation count is recorded
/// into `config.commit_stats`), and the retry/backoff tuning on the Begin/Commit RPCs.
/// `config.isolation` is deliberately ignored here — the write-only builder exposes no isolation
/// setter, and a transaction that performs no reads has no reads for a level to constrain.
pub(crate) fn write_mutations_txn(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    config: &SharedConfig,
    mutations: Vec<Mutation>,
) -> Result<()> {
    if mutations.is_empty() {
        return Ok(());
    }
    // Owned copies for the `async move` below.
    let request = config.request.clone();
    let retry = config.retry;
    let timeout = config.timeouts.update_timeout();
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
    config.commit_stats.record(mutation_count);
    Ok(())
}

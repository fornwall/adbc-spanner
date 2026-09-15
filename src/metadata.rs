//! The driver's `INFORMATION_SCHEMA` plumbing: the statement builder every driver-internal
//! metadata read goes through, the shared `table_exists` probe and catalog check, the
//! `StringArray` column accessor its consumers decode with, and the ADBC `LIKE` matcher.

use std::time::Duration;

use adbc_core::error::{Error, Result, Status};
use arrow_array::{RecordBatch, StringArray};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::{Statement as SpannerSql, StatementBuilder};
use google_cloud_spanner::transaction::MultiUseReadOnlyTransaction;

use crate::conversion::{TimestampPrecision, result_set_to_batch};
use crate::error::{err, from_spanner};
use crate::options::SharedConfig;
use crate::runtime::{CancelSignal, SharedRuntime, block_on_cancellable};
use crate::timeout::with_timeout;

/// Validate a lookup's `catalog` argument. Spanner has a single, unnamed (`""`) catalog, so `None`
/// and `Some("")` are accepted; any other catalog does not exist — nothing can be found in it — so
/// the lookup fails with [`Status::NotFound`] (matching how a missing table is reported).
pub(crate) fn check_lookup_catalog(catalog: Option<&str>) -> Result<()> {
    match catalog {
        None | Some("") => Ok(()),
        Some(other) => Err(err(
            format!("catalog {other:?} not found: Spanner has only the default, unnamed catalog"),
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
///
/// **Probe-failure policy, shared by every caller that probes on an error path**
/// ([`SpannerConnection::get_table_schema`], [`SpannerStatement`](crate::statement::SpannerStatement)'s
/// two ingest remaps): the probe only *refines* an error the user's own operation already produced,
/// so when the probe itself fails — a transport blip, a cancel, or simply no `INFORMATION_SCHEMA`
/// read permission on a connection that may still write — that original error is returned unchanged.
/// It is the error from the operation the caller actually asked for, and on a genuine outage it
/// already reports the outage; replacing it with a failure from an internal metadata query the
/// caller never issued would only hide the cause. So each site matches
/// `Ok(true)`/`Ok(false)`/`Err(_) => original`, and none propagates the probe error with `?`.
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

/// A Spanner statement builder for a **driver-internal metadata read**: the `INFORMATION_SCHEMA`
/// queries and full-table aggregate scans of [`get_objects`](crate::objects::collect_objects),
/// [`get_statistics`](crate::statistics::collect_statistics) and
/// [`SpannerConnection::get_table_schema`].
///
/// These are the heaviest queries the driver issues on its own, so they honour the connection's
/// retry bounds (an unbounded retry of a `COUNT(*)` over every table is a real hazard), its
/// directed-read replica selection (legal here because they all run read-only) and its request
/// **priority**. They stay untagged — see
/// [`apply_priority_to_statement`](crate::request::RequestConfig::apply_priority_to_statement).
#[must_use]
pub(crate) fn metadata_sql_builder(
    config: &SharedConfig,
    sql: impl Into<String>,
) -> StatementBuilder {
    config.retry.apply_to_statement(
        config.directed_read.apply_to_statement(
            config
                .request
                .apply_priority_to_statement(SpannerSql::builder(sql)),
        ),
    )
}

/// Run one metadata statement on a shared multi-use read-only transaction and materialise its
/// result batch.
///
/// Every driver-internal read of `get_objects` and `get_statistics` — `INFORMATION_SCHEMA`
/// discovery and per-table aggregate scan alike — goes through one transaction, so they all observe
/// a single consistent snapshot. The results are string metadata or INT64 counts, never a
/// TIMESTAMP, so the default timestamp precision is fine.
pub(crate) async fn query_txn(
    txn: &MultiUseReadOnlyTransaction,
    statement: impl Into<SpannerSql>,
) -> Result<RecordBatch> {
    let result_set = txn
        .execute_query(statement.into())
        .await
        .map_err(from_spanner)?;
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
/// row; the free [`like_match`] helper wraps it for one-off matches.
/// [`matches`](LikeMatcher::matches) is iterative with backtrack pointers (O(pattern × value), no
/// recursion) so adversarial patterns like `%a%a%a…` cannot blow up or overflow the stack.
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
        // allocates nothing. `_` still consumes exactly one *character*: every advance steps by
        // the decoded char's UTF-8 width.
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
mod tests;

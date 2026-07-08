//! Retry-policy tuning options (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`).
//!
//! Every data-plane RPC the driver issues is retried by the pinned Spanner client under a default
//! policy — AIP-194 strict, additionally retrying transport / IO errors on idempotent requests (the
//! client's private `SpannerRetryPolicy`, see
//! `.../google-cloud-rust-*/src/spanner/src/retry_policy.rs`). That default has **no** attempt or
//! elapsed-time cap, so a persistently `UNAVAILABLE` backend is retried until the operation-wide
//! [RPC timeout](crate::timeout) (if any) fires. These two options let a caller *bound* the client's
//! retrying instead — mirroring the gax convention of an attempt count and an overall elapsed-time
//! limit:
//!
//! - [`OPTION_RETRY_MAX_ATTEMPTS`](crate::OPTION_RETRY_MAX_ATTEMPTS) — the maximum number of
//!   attempts (the first try plus retries), a positive integer. `1` disables retrying.
//! - [`OPTION_RETRY_MAX_ELAPSED_SECONDS`](crate::OPTION_RETRY_MAX_ELAPSED_SECONDS) — an upper bound,
//!   in seconds, on the total wall-clock time spent across attempts before the last error is
//!   surfaced as permanent.
//!
//! The two are independent and may be combined (the retry loop stops at whichever limit is reached
//! first). When neither is set the client keeps its default (unbounded) policy — so this feature is
//! purely opt-in and, by default, changes nothing.
//!
//! **Preserving the client's behaviour under a limit.** Setting a policy on a request builder
//! *replaces* the client's default `SpannerRetryPolicy`, so to keep the transport-error-on-idempotent
//! retrying while adding a bound, the base policy applied here re-implements that same decoration
//! ([`SpannerRetryPolicy`] below) and layers the configured
//! [`with_attempt_limit`](google_cloud_gax::retry_policy::RetryPolicyExt::with_attempt_limit) /
//! [`with_time_limit`](google_cloud_gax::retry_policy::RetryPolicyExt::with_time_limit) wrappers on
//! top. The policy is applied to every user statement/DML builder, the read/write transaction
//! runner's begin+commit RPCs, the bulk-ingest write-only transaction, and the `ExecuteBatchDml`
//! batch — the same builder sites the request priority/tag options cover.
//!
//! Both options exist at connection **and** statement level; a connection's values become the
//! default for statements it creates (which may override them), an empty string unsets, and every
//! option round-trips through `get_option` (and `get_option_int` / `get_option_double`). This
//! bounds the client's *per-attempt* retrying; the overall per-operation deadline is the separate
//! [RPC timeout](crate::timeout) family.

use std::time::Duration;

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::retry_policy::{
    Aip194Strict, RetryPolicy, RetryPolicyArg, RetryPolicyExt as _,
};
use google_cloud_gax::retry_result::RetryResult;
use google_cloud_gax::retry_state::RetryState;
use google_cloud_gax::throttle_result::ThrottleResult;
use google_cloud_spanner::builder::{
    BatchDmlBuilder, TransactionRunnerBuilder, WriteOnlyTransactionBuilder,
};
use google_cloud_spanner::statement::StatementBuilder;

use crate::error::invalid_argument;

/// A driver-local copy of the pinned client's private `SpannerRetryPolicy`
/// (`.../src/spanner/src/retry_policy.rs`): AIP-194 strict, but additionally retrying transport / IO
/// errors on **idempotent** requests.
///
/// Replicated here because setting a retry policy on a request builder *replaces* the client's
/// default one, and we want opting into an attempt / elapsed-time limit to keep — not silently drop
/// — the client's transport-error-on-idempotent retrying. The attempt / time-limit wrappers are
/// then layered on top of this base.
#[derive(Clone, Debug, Default)]
struct SpannerRetryPolicy;

impl RetryPolicy for SpannerRetryPolicy {
    fn on_error(&self, state: &RetryState, error: GaxError) -> RetryResult {
        match Aip194Strict.on_error(state, error) {
            // AIP-194 classifies a post-headers transport/IO error as permanent; Spanner allows
            // retrying it when the request is idempotent (all the driver's data-plane RPCs are).
            RetryResult::Permanent(error)
                if state.idempotent && (error.is_transport() || error.is_io()) =>
            {
                RetryResult::Continue(error)
            }
            other => other,
        }
    }

    fn on_throttle(&self, state: &RetryState, error: GaxError) -> ThrottleResult {
        Aip194Strict.on_throttle(state, error)
    }

    fn remaining_time(&self, state: &RetryState) -> Option<Duration> {
        Aip194Strict.remaining_time(state)
    }
}

/// The retry-tuning configuration held by a connection or statement
/// (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`).
///
/// A connection's value is cloned into each statement it creates (which may then override either
/// knob), mirroring how [`ReadStaleness`](crate::staleness::ReadStaleness) and
/// [`RpcTimeouts`](crate::timeout::RpcTimeouts) are inherited.
///
/// Values are stored exactly as configured so `get_option` / `get_option_int` /
/// `get_option_double` round-trip them; [`retry_policy_arg`](Self::retry_policy_arg) turns them into
/// a gax [`RetryPolicyArg`] (or `None`, leaving the client's default policy) at apply time.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RetryConfig {
    /// `spanner.retry.max_attempts`, when set: the maximum number of attempts (>= 1).
    max_attempts: Option<u32>,
    /// `spanner.retry.max_elapsed_seconds`, when set: the total wall-clock retry budget, in seconds.
    max_elapsed_seconds: Option<f64>,
}

impl RetryConfig {
    /// Handle a `set_option` for `spanner.retry.max_attempts`. An empty string unsets it.
    pub(crate) fn set_max_attempts(&mut self, value: OptionValue) -> Result<()> {
        self.max_attempts = parse_max_attempts(value)?;
        Ok(())
    }

    /// Handle a `set_option` for `spanner.retry.max_elapsed_seconds`. An empty string unsets it.
    pub(crate) fn set_max_elapsed_seconds(&mut self, value: OptionValue) -> Result<()> {
        self.max_elapsed_seconds = parse_max_elapsed_seconds(value)?;
        Ok(())
    }

    /// The canonical `spanner.retry.max_attempts` value, for `get_option` round-trip.
    pub(crate) fn max_attempts_string(&self) -> Option<String> {
        self.max_attempts.map(|n| n.to_string())
    }

    /// The canonical `spanner.retry.max_elapsed_seconds` value, for `get_option` round-trip.
    pub(crate) fn max_elapsed_seconds_string(&self) -> Option<String> {
        self.max_elapsed_seconds.map(|s| s.to_string())
    }

    /// The effective total retry budget as a [`Duration`] (`None` when unset). Conversion cannot
    /// fail — [`parse_max_elapsed_seconds`] validated it at set time.
    fn max_elapsed_duration(&self) -> Option<Duration> {
        self.max_elapsed_seconds
            .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
    }

    /// The gax retry policy for this configuration, or `None` when neither knob is set (leaving the
    /// client's default `SpannerRetryPolicy` in place). When either is set the driver's equivalent
    /// base policy is bounded by the configured attempt / elapsed-time limits.
    pub(crate) fn retry_policy_arg(&self) -> Option<RetryPolicyArg> {
        match (self.max_attempts, self.max_elapsed_duration()) {
            (None, None) => None,
            (Some(attempts), None) => Some(SpannerRetryPolicy.with_attempt_limit(attempts).into()),
            (None, Some(elapsed)) => Some(SpannerRetryPolicy.with_time_limit(elapsed).into()),
            (Some(attempts), Some(elapsed)) => Some(
                SpannerRetryPolicy
                    .with_time_limit(elapsed)
                    .with_attempt_limit(attempts)
                    .into(),
            ),
        }
    }

    /// Apply the retry policy to a statement builder (queries and DML alike).
    pub(crate) fn apply_to_statement(&self, builder: StatementBuilder) -> StatementBuilder {
        match self.retry_policy_arg() {
            Some(policy) => builder.with_retry_policy(policy),
            None => builder,
        }
    }

    /// Apply the retry policy to an `ExecuteBatchDml` batch builder.
    pub(crate) fn apply_to_batch_dml(&self, builder: BatchDmlBuilder) -> BatchDmlBuilder {
        match self.retry_policy_arg() {
            Some(policy) => builder.with_retry_policy(policy),
            None => builder,
        }
    }

    /// Apply the retry policy to a read/write transaction runner builder (its Begin and Commit
    /// RPCs). The transaction-level abort retry (Spanner's optimistic-concurrency re-run) is a
    /// separate policy left at the client default.
    pub(crate) fn apply_to_runner(
        &self,
        builder: TransactionRunnerBuilder,
    ) -> TransactionRunnerBuilder {
        match self.retry_policy_arg() {
            Some(policy) => builder
                .with_begin_retry_policy(policy.clone())
                .with_commit_retry_policy(policy),
            None => builder,
        }
    }

    /// Apply the retry policy to a write-only transaction builder (the bulk-ingest commit path):
    /// its Begin and Commit RPCs.
    pub(crate) fn apply_to_write_only(
        &self,
        builder: WriteOnlyTransactionBuilder,
    ) -> WriteOnlyTransactionBuilder {
        match self.retry_policy_arg() {
            Some(policy) => builder
                .with_begin_retry_policy(policy.clone())
                .with_commit_retry_policy(policy),
            None => builder,
        }
    }
}

/// Parse a `spanner.retry.max_attempts` value: a positive integer (the first attempt plus retries;
/// `1` disables retrying), accepted as an integer, a whole-valued double, or a numeric string.
/// Zero, negatives, fractions, values above [`u32::MAX`] and non-numeric input are rejected with
/// `InvalidArguments`; an empty string yields `None` (unset).
fn parse_max_attempts(value: OptionValue) -> Result<Option<u32>> {
    let reject = || {
        invalid_argument(format!(
            "option {} must be a positive integer number of attempts (>= 1)",
            crate::OPTION_RETRY_MAX_ATTEMPTS
        ))
    };
    let attempts: i64 = match value {
        OptionValue::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed.parse::<i64>().map_err(|_| reject())?
        }
        OptionValue::Int(i) => i,
        // Accept the `set_option_double` shape only when it is a whole number.
        OptionValue::Double(d) if d.is_finite() && d.fract() == 0.0 => d as i64,
        _ => return Err(reject()),
    };
    if !(1..=i64::from(u32::MAX)).contains(&attempts) {
        return Err(reject());
    }
    Ok(Some(attempts as u32))
}

/// Parse a `spanner.retry.max_elapsed_seconds` value: a finite, strictly positive number of seconds
/// (fractions allowed), accepted as a numeric string, an integer, or a double. Zero, `NaN`, the
/// infinities, negatives, values too large for a [`Duration`], and non-numeric input are rejected
/// with `InvalidArguments`; an empty string yields `None` (unset).
fn parse_max_elapsed_seconds(value: OptionValue) -> Result<Option<f64>> {
    let reject = || {
        invalid_argument(format!(
            "option {} must be a finite, strictly positive number of seconds",
            crate::OPTION_RETRY_MAX_ELAPSED_SECONDS
        ))
    };
    let seconds = match value {
        OptionValue::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed.parse::<f64>().map_err(|_| reject())?
        }
        OptionValue::Double(d) => d,
        OptionValue::Int(i) => i as f64,
        _ => return Err(reject()),
    };
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(reject());
    }
    // Enforce Duration-representability at set time so `max_elapsed_duration` can never fail later.
    if Duration::try_from_secs_f64(seconds).is_err() {
        return Err(reject());
    }
    Ok(Some(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    fn s(v: &str) -> OptionValue {
        OptionValue::String(v.to_string())
    }

    #[test]
    fn parses_attempts_from_strings_ints_and_whole_doubles() {
        let mut config = RetryConfig::default();
        config.set_max_attempts(s(" 3 ")).unwrap();
        assert_eq!(config.max_attempts_string().as_deref(), Some("3"));
        config.set_max_attempts(OptionValue::Int(5)).unwrap();
        assert_eq!(config.max_attempts_string().as_deref(), Some("5"));
        config.set_max_attempts(OptionValue::Double(2.0)).unwrap();
        assert_eq!(config.max_attempts_string().as_deref(), Some("2"));
        // 1 is valid: one attempt, no retries.
        config.set_max_attempts(s("1")).unwrap();
        assert_eq!(config.max_attempts_string().as_deref(), Some("1"));
    }

    #[test]
    fn parses_elapsed_from_strings_ints_and_doubles() {
        let mut config = RetryConfig::default();
        config.set_max_elapsed_seconds(s(" 2.5 ")).unwrap();
        assert_eq!(config.max_elapsed_seconds_string().as_deref(), Some("2.5"));
        assert_eq!(
            config.max_elapsed_duration(),
            Some(Duration::from_millis(2500))
        );
        config
            .set_max_elapsed_seconds(OptionValue::Int(30))
            .unwrap();
        assert_eq!(config.max_elapsed_seconds_string().as_deref(), Some("30"));
        config
            .set_max_elapsed_seconds(OptionValue::Double(0.05))
            .unwrap();
        assert_eq!(
            config.max_elapsed_duration(),
            Some(Duration::from_millis(50))
        );
    }

    #[test]
    fn empty_string_unsets_each_independently() {
        let mut config = RetryConfig::default();
        config.set_max_attempts(s("4")).unwrap();
        config.set_max_elapsed_seconds(s("10")).unwrap();
        config.set_max_attempts(s("")).unwrap();
        assert_eq!(config.max_attempts_string(), None);
        assert_eq!(config.max_elapsed_seconds_string().as_deref(), Some("10"));
        // Whitespace-only counts as empty too.
        config.set_max_elapsed_seconds(s("  ")).unwrap();
        assert_eq!(config.max_elapsed_seconds_string(), None);
    }

    #[test]
    fn rejects_bad_attempts() {
        let mut config = RetryConfig::default();
        config.set_max_attempts(s("3")).unwrap();
        let bad = [
            OptionValue::Int(0),
            OptionValue::Int(-1),
            s("0"),
            s("-2"),
            s("1.5"),                 // fractional
            OptionValue::Double(2.5), // fractional double
            OptionValue::Double(f64::NAN),
            s("abc"),
            OptionValue::Bytes(vec![1]),
            // Above u32::MAX.
            OptionValue::Int(i64::from(u32::MAX) + 1),
        ];
        for value in bad {
            let error = config.set_max_attempts(value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
            // The stored value is left untouched.
            assert_eq!(config.max_attempts_string().as_deref(), Some("3"));
        }
    }

    #[test]
    fn rejects_bad_elapsed() {
        let mut config = RetryConfig::default();
        config.set_max_elapsed_seconds(s("5")).unwrap();
        let bad = [
            OptionValue::Double(0.0), // zero budget is degenerate
            s("0"),
            OptionValue::Double(-1.0),
            OptionValue::Int(-3),
            OptionValue::Double(f64::NAN),
            OptionValue::Double(f64::INFINITY),
            s("inf"),
            s("abc"),
            s("1s"),
            OptionValue::Double(1e300), // too large for Duration
            OptionValue::Bytes(vec![1, 2]),
        ];
        for value in bad {
            let error = config.set_max_elapsed_seconds(value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
            assert_eq!(config.max_elapsed_seconds_string().as_deref(), Some("5"));
        }
    }

    #[test]
    fn retry_policy_arg_is_none_until_configured() {
        let mut config = RetryConfig::default();
        assert!(config.retry_policy_arg().is_none());
        config.set_max_attempts(s("3")).unwrap();
        assert!(config.retry_policy_arg().is_some());
        config.set_max_attempts(s("")).unwrap();
        assert!(config.retry_policy_arg().is_none());
        config.set_max_elapsed_seconds(s("10")).unwrap();
        assert!(config.retry_policy_arg().is_some());
        // Both set: still a policy (the loop stops at whichever limit fires first).
        config.set_max_attempts(s("5")).unwrap();
        assert!(config.retry_policy_arg().is_some());
    }

    /// Statement inheritance is a plain copy of the connection's config (mirroring `RpcTimeouts`):
    /// the copy starts with the connection's values and overrides independently.
    #[test]
    fn copied_config_inherits_then_overrides_independently() {
        let mut connection = RetryConfig::default();
        connection.set_max_attempts(s("10")).unwrap();
        connection.set_max_elapsed_seconds(s("20")).unwrap();

        let mut statement = connection;
        assert_eq!(statement.max_attempts_string().as_deref(), Some("10"));
        assert_eq!(
            statement.max_elapsed_seconds_string().as_deref(),
            Some("20")
        );

        statement.set_max_attempts(s("2")).unwrap();
        statement.set_max_elapsed_seconds(s("")).unwrap();
        assert_eq!(statement.max_attempts_string().as_deref(), Some("2"));
        assert_eq!(statement.max_elapsed_seconds_string(), None);
        // The connection is unaffected by statement-level overrides.
        assert_eq!(connection.max_attempts_string().as_deref(), Some("10"));
        assert_eq!(
            connection.max_elapsed_seconds_string().as_deref(),
            Some("20")
        );
    }

    /// The driver's base policy mirrors the client's private `SpannerRetryPolicy`: transport / IO
    /// errors are retried on idempotent requests and treated as permanent otherwise. (Uses an IO
    /// error, which `Aip194Strict` also classifies as permanent, so the decoration is exercised
    /// without depending on the `http` crate for a transport error's header map.)
    #[test]
    fn base_policy_retries_io_errors_only_when_idempotent() {
        let policy = SpannerRetryPolicy;
        let io = || {
            GaxError::io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "closed",
            ))
        };
        assert!(
            policy.on_error(&RetryState::new(true), io()).is_continue(),
            "idempotent IO error should be retried"
        );
        assert!(
            policy
                .on_error(&RetryState::new(false), io())
                .is_permanent(),
            "non-idempotent IO error should be permanent"
        );
    }
}

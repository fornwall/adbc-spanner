//! Retry-policy tuning options (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`,
//! plus the backoff knobs `spanner.retry.backoff.{initial_seconds,max_seconds,multiplier}`).
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
//! Independently, three options tune the *delay between* attempts (the client's truncated
//! exponential backoff with jitter), each opt-in and applied at the same builder sites:
//!
//! - [`OPTION_RETRY_BACKOFF_INITIAL_SECONDS`](crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS) — the
//!   first inter-attempt delay, in seconds.
//! - [`OPTION_RETRY_BACKOFF_MAX_SECONDS`](crate::OPTION_RETRY_BACKOFF_MAX_SECONDS) — the ceiling the
//!   growing delay is truncated at, in seconds.
//! - [`OPTION_RETRY_BACKOFF_MULTIPLIER`](crate::OPTION_RETRY_BACKOFF_MULTIPLIER) — the per-attempt
//!   growth factor applied to the delay.
//!
//! Setting any one of them replaces the client's default backoff with a gax
//! [`ExponentialBackoff`](google_cloud_gax::exponential_backoff::ExponentialBackoff): the unset
//! knobs fall back to the client's defaults (initial 1s, maximum 60s, multiplier 2.0) and the
//! combination is clamped to the gax recommended ranges (so it can never fail to build). These are
//! orthogonal to the attempt / elapsed-time limits above — either family may be set without the
//! other.
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
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
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
/// (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds` and the backoff knobs
/// `spanner.retry.backoff.{initial_seconds,max_seconds,multiplier}`).
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
    /// `spanner.retry.backoff.initial_seconds`, when set: the first inter-attempt delay, in seconds.
    backoff_initial_seconds: Option<f64>,
    /// `spanner.retry.backoff.max_seconds`, when set: the ceiling on the inter-attempt delay, in
    /// seconds.
    backoff_max_seconds: Option<f64>,
    /// `spanner.retry.backoff.multiplier`, when set: the per-attempt growth factor for the delay.
    backoff_multiplier: Option<f64>,
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

    /// Handle a `set_option` for `spanner.retry.backoff.initial_seconds`. An empty string unsets it.
    pub(crate) fn set_backoff_initial_seconds(&mut self, value: OptionValue) -> Result<()> {
        self.backoff_initial_seconds =
            parse_backoff_seconds(value, crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS)?;
        Ok(())
    }

    /// Handle a `set_option` for `spanner.retry.backoff.max_seconds`. An empty string unsets it.
    pub(crate) fn set_backoff_max_seconds(&mut self, value: OptionValue) -> Result<()> {
        self.backoff_max_seconds =
            parse_backoff_seconds(value, crate::OPTION_RETRY_BACKOFF_MAX_SECONDS)?;
        Ok(())
    }

    /// Handle a `set_option` for `spanner.retry.backoff.multiplier`. An empty string unsets it.
    pub(crate) fn set_backoff_multiplier(&mut self, value: OptionValue) -> Result<()> {
        self.backoff_multiplier = parse_backoff_multiplier(value)?;
        Ok(())
    }

    /// The canonical `spanner.retry.backoff.initial_seconds` value, for `get_option` round-trip.
    pub(crate) fn backoff_initial_seconds_string(&self) -> Option<String> {
        self.backoff_initial_seconds.map(|s| s.to_string())
    }

    /// The canonical `spanner.retry.backoff.max_seconds` value, for `get_option` round-trip.
    pub(crate) fn backoff_max_seconds_string(&self) -> Option<String> {
        self.backoff_max_seconds.map(|s| s.to_string())
    }

    /// The canonical `spanner.retry.backoff.multiplier` value, for `get_option` round-trip.
    pub(crate) fn backoff_multiplier_string(&self) -> Option<String> {
        self.backoff_multiplier.map(|m| m.to_string())
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

    /// The gax backoff policy for this configuration, or `None` when none of the three backoff knobs
    /// is set (leaving the client's default exponential backoff in place). When any is set, the
    /// unset knobs fall back to the client's defaults (initial 1s, maximum 60s, multiplier 2.0) and
    /// the combination is clamped to the gax recommended ranges via
    /// [`ExponentialBackoffBuilder::clamp`] — so building it can never fail (initial delay ≥ 1ms,
    /// maximum delay in `[1s, 24h]` and ≥ the initial delay, multiplier in `[1.0, 32.0]`).
    ///
    /// This is independent of [`retry_policy_arg`](Self::retry_policy_arg): a caller may tune the
    /// backoff without bounding the attempt / elapsed-time limits, and vice versa.
    fn backoff_policy_arg(&self) -> Option<BackoffPolicyArg> {
        if self.backoff_initial_seconds.is_none()
            && self.backoff_max_seconds.is_none()
            && self.backoff_multiplier.is_none()
        {
            return None;
        }
        let mut builder = ExponentialBackoffBuilder::new();
        if let Some(initial) = self.backoff_initial_seconds {
            // `parse_backoff_seconds` validated Duration-representability, so this cannot fail.
            builder = builder.with_initial_delay(Duration::from_secs_f64(initial));
        }
        if let Some(maximum) = self.backoff_max_seconds {
            builder = builder.with_maximum_delay(Duration::from_secs_f64(maximum));
        }
        if let Some(multiplier) = self.backoff_multiplier {
            builder = builder.with_scaling(multiplier);
        }
        Some(builder.clamp().into())
    }

    /// Apply the retry and backoff policies to a statement builder (queries and DML alike).
    #[must_use]
    pub(crate) fn apply_to_statement(&self, mut builder: StatementBuilder) -> StatementBuilder {
        if let Some(policy) = self.retry_policy_arg() {
            builder = builder.with_retry_policy(policy);
        }
        if let Some(backoff) = self.backoff_policy_arg() {
            builder = builder.with_backoff_policy(backoff);
        }
        builder
    }

    /// Apply the retry and backoff policies to an `ExecuteBatchDml` batch builder.
    #[must_use]
    pub(crate) fn apply_to_batch_dml(&self, mut builder: BatchDmlBuilder) -> BatchDmlBuilder {
        if let Some(policy) = self.retry_policy_arg() {
            builder = builder.with_retry_policy(policy);
        }
        if let Some(backoff) = self.backoff_policy_arg() {
            builder = builder.with_backoff_policy(backoff);
        }
        builder
    }

    /// Apply the retry and backoff policies to a read/write transaction runner builder (its Begin
    /// and Commit RPCs). The transaction-level abort retry (Spanner's optimistic-concurrency re-run)
    /// is a separate policy left at the client default.
    #[must_use]
    pub(crate) fn apply_to_runner(
        &self,
        mut builder: TransactionRunnerBuilder,
    ) -> TransactionRunnerBuilder {
        if let Some(policy) = self.retry_policy_arg() {
            builder = builder
                .with_begin_retry_policy(policy.clone())
                .with_commit_retry_policy(policy);
        }
        if let Some(backoff) = self.backoff_policy_arg() {
            builder = builder
                .with_begin_backoff_policy(backoff.clone())
                .with_commit_backoff_policy(backoff);
        }
        builder
    }

    /// Apply the retry and backoff policies to a write-only transaction builder (the bulk-ingest
    /// commit path): its Begin and Commit RPCs.
    #[must_use]
    pub(crate) fn apply_to_write_only(
        &self,
        mut builder: WriteOnlyTransactionBuilder,
    ) -> WriteOnlyTransactionBuilder {
        if let Some(policy) = self.retry_policy_arg() {
            builder = builder
                .with_begin_retry_policy(policy.clone())
                .with_commit_retry_policy(policy);
        }
        if let Some(backoff) = self.backoff_policy_arg() {
            builder = builder
                .with_begin_backoff_policy(backoff.clone())
                .with_commit_backoff_policy(backoff);
        }
        builder
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

/// Parse a `spanner.retry.backoff.{initial,max}_seconds` value: a finite, strictly positive number
/// of seconds (fractions allowed), accepted as a numeric string, an integer, or a double. Zero,
/// `NaN`, the infinities, negatives, values too large for a [`Duration`], and non-numeric input are
/// rejected with `InvalidArguments`; an empty string yields `None` (unset). `option` names the key
/// for the error message.
fn parse_backoff_seconds(value: OptionValue, option: &str) -> Result<Option<f64>> {
    let reject = || {
        invalid_argument(format!(
            "option {option} must be a finite, strictly positive number of seconds"
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
    // Enforce Duration-representability at set time so `backoff_policy_arg` can never fail later.
    if Duration::try_from_secs_f64(seconds).is_err() {
        return Err(reject());
    }
    Ok(Some(seconds))
}

/// Parse a `spanner.retry.backoff.multiplier` value: a finite, strictly positive growth factor,
/// accepted as a numeric string, an integer, or a double. `NaN`, the infinities, zero, negatives and
/// non-numeric input are rejected with `InvalidArguments`; an empty string yields `None` (unset).
/// A value below `1.0` is floored to `1.0` (a constant backoff) when the policy is built.
fn parse_backoff_multiplier(value: OptionValue) -> Result<Option<f64>> {
    let reject = || {
        invalid_argument(format!(
            "option {} must be a finite, strictly positive number",
            crate::OPTION_RETRY_BACKOFF_MULTIPLIER
        ))
    };
    let multiplier = match value {
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
    if !multiplier.is_finite() || multiplier <= 0.0 {
        return Err(reject());
    }
    Ok(Some(multiplier))
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

    #[test]
    fn parses_backoff_knobs_from_strings_ints_and_doubles() {
        let mut config = RetryConfig::default();
        config.set_backoff_initial_seconds(s(" 0.5 ")).unwrap();
        assert_eq!(
            config.backoff_initial_seconds_string().as_deref(),
            Some("0.5")
        );
        config
            .set_backoff_max_seconds(OptionValue::Int(30))
            .unwrap();
        assert_eq!(config.backoff_max_seconds_string().as_deref(), Some("30"));
        config
            .set_backoff_multiplier(OptionValue::Double(1.5))
            .unwrap();
        assert_eq!(config.backoff_multiplier_string().as_deref(), Some("1.5"));
    }

    #[test]
    fn empty_string_unsets_each_backoff_knob_independently() {
        let mut config = RetryConfig::default();
        config.set_backoff_initial_seconds(s("1")).unwrap();
        config.set_backoff_max_seconds(s("10")).unwrap();
        config.set_backoff_multiplier(s("2")).unwrap();

        config.set_backoff_initial_seconds(s("")).unwrap();
        assert_eq!(config.backoff_initial_seconds_string(), None);
        assert_eq!(config.backoff_max_seconds_string().as_deref(), Some("10"));
        assert_eq!(config.backoff_multiplier_string().as_deref(), Some("2"));
        // Whitespace-only counts as empty too.
        config.set_backoff_max_seconds(s("  ")).unwrap();
        assert_eq!(config.backoff_max_seconds_string(), None);
        config.set_backoff_multiplier(s("")).unwrap();
        assert_eq!(config.backoff_multiplier_string(), None);
    }

    #[test]
    fn rejects_bad_backoff_seconds() {
        for setter in [
            RetryConfig::set_backoff_initial_seconds as fn(&mut RetryConfig, OptionValue) -> _,
            RetryConfig::set_backoff_max_seconds,
        ] {
            let mut config = RetryConfig::default();
            setter(&mut config, s("2")).unwrap();
            let bad = [
                OptionValue::Double(0.0),
                s("0"),
                OptionValue::Double(-1.0),
                OptionValue::Int(-3),
                OptionValue::Double(f64::NAN),
                OptionValue::Double(f64::INFINITY),
                s("inf"),
                s("abc"),
                s("1s"),
                OptionValue::Double(1e300), // too large for Duration
                OptionValue::Bytes(vec![1]),
            ];
            for value in bad {
                let error = setter(&mut config, value.clone()).unwrap_err();
                assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
            }
        }
    }

    #[test]
    fn rejects_bad_backoff_multiplier() {
        let mut config = RetryConfig::default();
        config.set_backoff_multiplier(s("2")).unwrap();
        let bad = [
            OptionValue::Double(0.0),
            s("0"),
            OptionValue::Double(-1.0),
            OptionValue::Int(-3),
            OptionValue::Double(f64::NAN),
            OptionValue::Double(f64::INFINITY),
            s("inf"),
            s("abc"),
            OptionValue::Bytes(vec![1]),
        ];
        for value in bad {
            let error = config.set_backoff_multiplier(value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
            // The stored value is left untouched.
            assert_eq!(config.backoff_multiplier_string().as_deref(), Some("2"));
        }
        // Sub-1.0 multipliers are accepted (floored to 1.0 at build time), not rejected.
        config
            .set_backoff_multiplier(OptionValue::Double(0.5))
            .unwrap();
        assert_eq!(config.backoff_multiplier_string().as_deref(), Some("0.5"));
    }

    #[test]
    fn backoff_policy_arg_is_none_until_configured() {
        let mut config = RetryConfig::default();
        assert!(config.backoff_policy_arg().is_none());
        // The attempt / elapsed-time limits alone do not produce a backoff policy.
        config.set_max_attempts(s("3")).unwrap();
        config.set_max_elapsed_seconds(s("10")).unwrap();
        assert!(config.backoff_policy_arg().is_none());
        // Each backoff knob on its own is enough.
        config.set_backoff_initial_seconds(s("0.25")).unwrap();
        assert!(config.backoff_policy_arg().is_some());
        config.set_backoff_initial_seconds(s("")).unwrap();
        assert!(config.backoff_policy_arg().is_none());
        config.set_backoff_max_seconds(s("30")).unwrap();
        assert!(config.backoff_policy_arg().is_some());
        config.set_backoff_max_seconds(s("")).unwrap();
        assert!(config.backoff_policy_arg().is_none());
        config.set_backoff_multiplier(s("3")).unwrap();
        assert!(config.backoff_policy_arg().is_some());
    }

    /// A backoff-only configuration builds a policy but leaves the retry (attempt/elapsed) policy
    /// untouched, and vice versa — the two families are independent.
    #[test]
    fn retry_and_backoff_are_independent() {
        let mut backoff_only = RetryConfig::default();
        backoff_only.set_backoff_multiplier(s("4")).unwrap();
        assert!(backoff_only.retry_policy_arg().is_none());
        assert!(backoff_only.backoff_policy_arg().is_some());

        let mut retry_only = RetryConfig::default();
        retry_only.set_max_attempts(s("5")).unwrap();
        assert!(retry_only.retry_policy_arg().is_some());
        assert!(retry_only.backoff_policy_arg().is_none());
    }

    /// Backoff knobs inherit into a copied (statement) config and override independently, mirroring
    /// the attempt / elapsed-time inheritance test above.
    #[test]
    fn copied_config_inherits_then_overrides_backoff_independently() {
        let mut connection = RetryConfig::default();
        connection.set_backoff_initial_seconds(s("0.5")).unwrap();
        connection.set_backoff_max_seconds(s("40")).unwrap();
        connection.set_backoff_multiplier(s("3")).unwrap();

        let mut statement = connection;
        assert_eq!(
            statement.backoff_initial_seconds_string().as_deref(),
            Some("0.5")
        );
        assert_eq!(
            statement.backoff_max_seconds_string().as_deref(),
            Some("40")
        );
        assert_eq!(statement.backoff_multiplier_string().as_deref(), Some("3"));

        statement.set_backoff_max_seconds(s("")).unwrap();
        statement.set_backoff_multiplier(s("2")).unwrap();
        assert_eq!(statement.backoff_max_seconds_string(), None);
        assert_eq!(statement.backoff_multiplier_string().as_deref(), Some("2"));
        // The connection is unaffected by statement-level overrides.
        assert_eq!(
            connection.backoff_max_seconds_string().as_deref(),
            Some("40")
        );
        assert_eq!(connection.backoff_multiplier_string().as_deref(), Some("3"));
    }
}

//! Retry-policy tuning options (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`,
//! plus the backoff knobs `spanner.retry.backoff.{initial_seconds,max_seconds,multiplier}`).
//!
//! Every data-plane RPC the driver issues is retried by the pinned Spanner client under a default
//! policy — AIP-194 strict, additionally retrying transport / IO errors on idempotent requests (the
//! client's [`SpannerRetryPolicy`]), uncapped on the unary paths. These options *bound* that
//! retrying instead; unset, they change nothing. Their grammar, defaults and round-trip behaviour
//! are documented on the `OPTION_RETRY_*` constants and in `docs/options.md`.
//!
//! Setting a policy on a request builder *replaces* the client's default, so the base policy applied
//! here is that very same [`SpannerRetryPolicy`] with the configured
//! [`with_attempt_limit`](google_cloud_gax::retry_policy::RetryPolicyExt::with_attempt_limit) /
//! [`with_time_limit`](google_cloud_gax::retry_policy::RetryPolicyExt::with_time_limit) wrappers on
//! top, keeping the transport-error-on-idempotent retrying. The backoff knobs likewise build a gax
//! [`ExponentialBackoff`](google_cloud_gax::exponential_backoff::ExponentialBackoff) whose unset
//! knobs fall back to the client's defaults (1s, 60s, ×2.0), clamped to the gax recommended ranges
//! so it can never fail to build. Both families reach the same builder sites the request
//! priority/tag options cover.
//!
//! # `max_elapsed_seconds` is inert on the streaming query path
//!
//! The pinned client runs **two** retry loops. Unary RPCs (`ExecuteSql`, `ExecuteBatchDml`,
//! `BeginTransaction`, `Commit`) go through gax's `retry_loop`, where both limits are exact. The
//! streaming query path (`ExecuteStreamingSql`, i.e. every read-only query) is dispatched *outside*
//! it and hand-rolls resumption in `ResultSet::check_retry`, building a fresh `RetryState` per
//! decision: it seeds the attempt count with `1 + retry_count`, so `max_attempts = N` is exact there
//! too, but it re-takes `start` as `Instant::now()` every time, so the elapsed-time decorator never
//! exhausts. That is an upstream defect (REVIEW.md **UP-14**) the driver cannot correct — the same
//! `RetryPolicyArg` feeds both loops, and no policy can recover a loop start the caller re-takes. A
//! streaming caller who needs a wall-clock bound has a working one in the separate
//! [RPC timeout](crate::timeout) family (`spanner.rpc.timeout_seconds.{query,fetch}`). The client's
//! default on this path is `with_attempt_limit(10)` rather than uncapped. The exact numbers are
//! pinned by the `retry_max_*` tests in `tests/mock_spanner.rs`.

use std::time::Duration;

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::retry_policy::{RetryPolicyArg, RetryPolicyExt as _};
use google_cloud_spanner::builder::{
    BatchDmlBuilder, TransactionRunnerBuilder, WriteOnlyTransactionBuilder,
};
use google_cloud_spanner::retry_policy::SpannerRetryPolicy;
use google_cloud_spanner::statement::StatementBuilder;

use crate::error::invalid_argument;

/// Emit an `apply_to_*` method applying the retry and backoff policies to the Begin and Commit RPCs
/// of one of the client's commit builders.
///
/// [`TransactionRunnerBuilder`] and [`WriteOnlyTransactionBuilder`] expose these four setters with
/// identical signatures but share no common trait, so the body lives here once rather than as two
/// byte-identical copies naming different types.
macro_rules! apply_to_commit_builder {
    ($(#[$attr:meta])* $name:ident($builder:ty)) => {
        $(#[$attr])*
        #[must_use]
        pub(crate) fn $name(&self, mut builder: $builder) -> $builder {
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
    };
}

/// Emit an `apply_to_*` method applying the retry and backoff policies to one of the client's
/// request builders — the same reasoning as [`apply_to_commit_builder`], for the builders that take
/// a single (non-commit) retry policy.
macro_rules! apply_to_request_builder {
    ($(#[$attr:meta])* $name:ident($builder:ty)) => {
        $(#[$attr])*
        #[must_use]
        pub(crate) fn $name(&self, mut builder: $builder) -> $builder {
            if let Some(policy) = self.retry_policy_arg() {
                builder = builder.with_retry_policy(policy);
            }
            if let Some(backoff) = self.backoff_policy_arg() {
                builder = builder.with_backoff_policy(backoff);
            }
            builder
        }
    };
}

/// The retry-tuning configuration held by a connection or statement
/// (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds` and the backoff knobs
/// `spanner.retry.backoff.{initial_seconds,max_seconds,multiplier}`).
///
/// A connection's value is cloned into each statement it creates (which may then override either
/// knob). Values are stored exactly as configured so `get_option` / `get_option_int` /
/// `get_option_double` round-trip them; [`retry_policy_arg`](Self::retry_policy_arg) turns them into
/// a gax [`RetryPolicyArg`] (or `None`, leaving the client's default policy) at apply time.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RetryConfig {
    /// `spanner.retry.max_attempts`, when set: the maximum number of attempts (>= 1).
    pub(crate) max_attempts: Option<u32>,
    /// `spanner.retry.max_elapsed_seconds`, when set: the total wall-clock retry budget, in seconds.
    pub(crate) max_elapsed_seconds: Option<f64>,
    /// `spanner.retry.backoff.initial_seconds`, when set: the first inter-attempt delay, in seconds.
    pub(crate) backoff_initial_seconds: Option<f64>,
    /// `spanner.retry.backoff.max_seconds`, when set: the ceiling on the inter-attempt delay, in
    /// seconds.
    pub(crate) backoff_max_seconds: Option<f64>,
    /// `spanner.retry.backoff.multiplier`, when set: the per-attempt growth factor for the delay.
    pub(crate) backoff_multiplier: Option<f64>,
}

impl RetryConfig {
    /// Handle a `set_option` for `spanner.retry.max_attempts`. An empty string unsets it. The one
    /// option here whose parse is more than a coercion, so the dispatch goes through this setter
    /// rather than assigning the field itself.
    pub(crate) fn set_max_attempts(&mut self, value: OptionValue) -> Result<()> {
        self.max_attempts = parse_max_attempts(value)?;
        Ok(())
    }

    /// The effective total retry budget as a [`Duration`] (`None` when unset). Conversion cannot
    /// fail — [`f64_option`](crate::options::f64_option) validated it at set time.
    fn max_elapsed_duration(&self) -> Option<Duration> {
        self.max_elapsed_seconds
            .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
    }

    /// The gax retry policy for this configuration, or `None` when neither knob is set (leaving the
    /// client's default [`SpannerRetryPolicy`] in place). When either is set that same policy is
    /// re-applied explicitly, bounded by the configured attempt / elapsed-time limits.
    fn retry_policy_arg(&self) -> Option<RetryPolicyArg> {
        match (self.max_attempts, self.max_elapsed_duration()) {
            (None, None) => None,
            (Some(attempts), None) => Some(
                SpannerRetryPolicy::new()
                    .with_attempt_limit(attempts)
                    .into(),
            ),
            (None, Some(elapsed)) => {
                Some(SpannerRetryPolicy::new().with_time_limit(elapsed).into())
            }
            (Some(attempts), Some(elapsed)) => Some(
                SpannerRetryPolicy::new()
                    .with_time_limit(elapsed)
                    .with_attempt_limit(attempts)
                    .into(),
            ),
        }
    }

    /// The gax backoff policy for this configuration, or `None` when none of the three backoff knobs
    /// is set (leaving the client's default exponential backoff in place). Unset knobs fall back to
    /// the client's defaults (1s, 60s, ×2.0) and the combination is clamped via
    /// [`ExponentialBackoffBuilder::clamp`] — initial delay ≥ 1ms, maximum in `[1s, 24h]` and ≥ the
    /// initial, multiplier in `[1.0, 32.0]` — so building it can never fail. Independent of
    /// [`retry_policy_arg`](Self::retry_policy_arg).
    fn backoff_policy_arg(&self) -> Option<BackoffPolicyArg> {
        if self.backoff_initial_seconds.is_none()
            && self.backoff_max_seconds.is_none()
            && self.backoff_multiplier.is_none()
        {
            return None;
        }
        let mut builder = ExponentialBackoffBuilder::new();
        if let Some(initial) = self.backoff_initial_seconds {
            // `f64_option` validated Duration-representability, so this cannot fail.
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

    apply_to_request_builder! {
        /// Apply the retry and backoff policies to a statement builder (queries and DML alike).
        apply_to_statement(StatementBuilder)
    }

    apply_to_request_builder! {
        /// Apply the retry and backoff policies to an `ExecuteBatchDml` batch builder.
        apply_to_batch_dml(BatchDmlBuilder)
    }

    apply_to_commit_builder! {
        /// Apply the retry and backoff policies to a read/write transaction runner builder (its
        /// Begin and Commit RPCs). The transaction-level abort retry (Spanner's
        /// optimistic-concurrency re-run) is a separate policy left at the client default.
        apply_to_runner(TransactionRunnerBuilder)
    }

    apply_to_commit_builder! {
        /// Apply the retry and backoff policies to a write-only transaction builder (the bulk-ingest
        /// commit path): its Begin and Commit RPCs.
        apply_to_write_only(WriteOnlyTransactionBuilder)
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

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    use crate::options::{F64Range, f64_option};

    fn s(v: &str) -> OptionValue {
        OptionValue::String(v.to_string())
    }

    /// The `impl_shared_option_dispatch` arms that set these fields, as functions the tests can
    /// call: each parses the value with the parser and range the dispatch uses, and stores it —
    /// leaving the previous value in place when the parse is rejected.
    fn set_max_elapsed_seconds(config: &mut RetryConfig, value: OptionValue) -> Result<()> {
        config.max_elapsed_seconds = f64_option(
            value,
            crate::OPTION_RETRY_MAX_ELAPSED_SECONDS,
            F64Range::PositiveSeconds,
        )?;
        Ok(())
    }

    fn set_backoff_initial_seconds(config: &mut RetryConfig, value: OptionValue) -> Result<()> {
        config.backoff_initial_seconds = f64_option(
            value,
            crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS,
            F64Range::PositiveSeconds,
        )?;
        Ok(())
    }

    fn set_backoff_max_seconds(config: &mut RetryConfig, value: OptionValue) -> Result<()> {
        config.backoff_max_seconds = f64_option(
            value,
            crate::OPTION_RETRY_BACKOFF_MAX_SECONDS,
            F64Range::PositiveSeconds,
        )?;
        Ok(())
    }

    fn set_backoff_multiplier(config: &mut RetryConfig, value: OptionValue) -> Result<()> {
        config.backoff_multiplier = f64_option(
            value,
            crate::OPTION_RETRY_BACKOFF_MULTIPLIER,
            F64Range::PositiveFactor,
        )?;
        Ok(())
    }

    /// The canonical `get_option` string of a stored value, as the dispatch's getter arms render
    /// it.
    fn string<T: std::fmt::Display>(value: Option<T>) -> Option<String> {
        value.map(|v| v.to_string())
    }

    #[test]
    fn parses_attempts_from_strings_ints_and_whole_doubles() {
        let mut config = RetryConfig::default();
        config.set_max_attempts(s(" 3 ")).unwrap();
        assert_eq!(string(config.max_attempts).as_deref(), Some("3"));
        config.set_max_attempts(OptionValue::Int(5)).unwrap();
        assert_eq!(string(config.max_attempts).as_deref(), Some("5"));
        config.set_max_attempts(OptionValue::Double(2.0)).unwrap();
        assert_eq!(string(config.max_attempts).as_deref(), Some("2"));
        // 1 is valid: one attempt, no retries.
        config.set_max_attempts(s("1")).unwrap();
        assert_eq!(string(config.max_attempts).as_deref(), Some("1"));
    }

    #[test]
    fn parses_elapsed_from_strings_ints_and_doubles() {
        let mut config = RetryConfig::default();
        set_max_elapsed_seconds(&mut config, s(" 2.5 ")).unwrap();
        assert_eq!(string(config.max_elapsed_seconds).as_deref(), Some("2.5"));
        assert_eq!(
            config.max_elapsed_duration(),
            Some(Duration::from_millis(2500))
        );
        set_max_elapsed_seconds(&mut config, OptionValue::Int(30)).unwrap();
        assert_eq!(string(config.max_elapsed_seconds).as_deref(), Some("30"));
        set_max_elapsed_seconds(&mut config, OptionValue::Double(0.05)).unwrap();
        assert_eq!(
            config.max_elapsed_duration(),
            Some(Duration::from_millis(50))
        );
    }

    #[test]
    fn empty_string_unsets_each_independently() {
        let mut config = RetryConfig::default();
        config.set_max_attempts(s("4")).unwrap();
        set_max_elapsed_seconds(&mut config, s("10")).unwrap();
        config.set_max_attempts(s("")).unwrap();
        assert_eq!(string(config.max_attempts), None);
        assert_eq!(string(config.max_elapsed_seconds).as_deref(), Some("10"));
        // Whitespace-only counts as empty too.
        set_max_elapsed_seconds(&mut config, s("  ")).unwrap();
        assert_eq!(string(config.max_elapsed_seconds), None);
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
            assert_eq!(string(config.max_attempts).as_deref(), Some("3"));
        }
    }

    #[test]
    fn rejects_bad_elapsed() {
        let mut config = RetryConfig::default();
        set_max_elapsed_seconds(&mut config, s("5")).unwrap();
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
            let error = set_max_elapsed_seconds(&mut config, value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
            assert_eq!(string(config.max_elapsed_seconds).as_deref(), Some("5"));
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
        set_max_elapsed_seconds(&mut config, s("10")).unwrap();
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
        set_max_elapsed_seconds(&mut connection, s("20")).unwrap();

        let mut statement = connection;
        assert_eq!(string(statement.max_attempts).as_deref(), Some("10"));
        assert_eq!(string(statement.max_elapsed_seconds).as_deref(), Some("20"));

        statement.set_max_attempts(s("2")).unwrap();
        set_max_elapsed_seconds(&mut statement, s("")).unwrap();
        assert_eq!(string(statement.max_attempts).as_deref(), Some("2"));
        assert_eq!(string(statement.max_elapsed_seconds), None);
        // The connection is unaffected by statement-level overrides.
        assert_eq!(string(connection.max_attempts).as_deref(), Some("10"));
        assert_eq!(
            string(connection.max_elapsed_seconds).as_deref(),
            Some("20")
        );
    }

    #[test]
    fn parses_backoff_knobs_from_strings_ints_and_doubles() {
        let mut config = RetryConfig::default();
        set_backoff_initial_seconds(&mut config, s(" 0.5 ")).unwrap();
        assert_eq!(
            string(config.backoff_initial_seconds).as_deref(),
            Some("0.5")
        );
        set_backoff_max_seconds(&mut config, OptionValue::Int(30)).unwrap();
        assert_eq!(string(config.backoff_max_seconds).as_deref(), Some("30"));
        set_backoff_multiplier(&mut config, OptionValue::Double(1.5)).unwrap();
        assert_eq!(string(config.backoff_multiplier).as_deref(), Some("1.5"));
    }

    #[test]
    fn empty_string_unsets_each_backoff_knob_independently() {
        let mut config = RetryConfig::default();
        set_backoff_initial_seconds(&mut config, s("1")).unwrap();
        set_backoff_max_seconds(&mut config, s("10")).unwrap();
        set_backoff_multiplier(&mut config, s("2")).unwrap();

        set_backoff_initial_seconds(&mut config, s("")).unwrap();
        assert_eq!(string(config.backoff_initial_seconds), None);
        assert_eq!(string(config.backoff_max_seconds).as_deref(), Some("10"));
        assert_eq!(string(config.backoff_multiplier).as_deref(), Some("2"));
        // Whitespace-only counts as empty too.
        set_backoff_max_seconds(&mut config, s("  ")).unwrap();
        assert_eq!(string(config.backoff_max_seconds), None);
        set_backoff_multiplier(&mut config, s("")).unwrap();
        assert_eq!(string(config.backoff_multiplier), None);
    }

    #[test]
    fn rejects_bad_backoff_seconds() {
        for setter in [
            set_backoff_initial_seconds as fn(&mut RetryConfig, OptionValue) -> _,
            set_backoff_max_seconds,
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
        set_backoff_multiplier(&mut config, s("2")).unwrap();
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
            let error = set_backoff_multiplier(&mut config, value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
            // The stored value is left untouched.
            assert_eq!(string(config.backoff_multiplier).as_deref(), Some("2"));
        }
        // Sub-1.0 multipliers are accepted (floored to 1.0 at build time), not rejected.
        set_backoff_multiplier(&mut config, OptionValue::Double(0.5)).unwrap();
        assert_eq!(string(config.backoff_multiplier).as_deref(), Some("0.5"));
    }

    #[test]
    fn backoff_policy_arg_is_none_until_configured() {
        let mut config = RetryConfig::default();
        assert!(config.backoff_policy_arg().is_none());
        // The attempt / elapsed-time limits alone do not produce a backoff policy.
        config.set_max_attempts(s("3")).unwrap();
        set_max_elapsed_seconds(&mut config, s("10")).unwrap();
        assert!(config.backoff_policy_arg().is_none());
        // Each backoff knob on its own is enough.
        set_backoff_initial_seconds(&mut config, s("0.25")).unwrap();
        assert!(config.backoff_policy_arg().is_some());
        set_backoff_initial_seconds(&mut config, s("")).unwrap();
        assert!(config.backoff_policy_arg().is_none());
        set_backoff_max_seconds(&mut config, s("30")).unwrap();
        assert!(config.backoff_policy_arg().is_some());
        set_backoff_max_seconds(&mut config, s("")).unwrap();
        assert!(config.backoff_policy_arg().is_none());
        set_backoff_multiplier(&mut config, s("3")).unwrap();
        assert!(config.backoff_policy_arg().is_some());
    }

    /// A backoff-only configuration builds a policy but leaves the retry (attempt/elapsed) policy
    /// untouched, and vice versa — the two families are independent.
    #[test]
    fn retry_and_backoff_are_independent() {
        let mut backoff_only = RetryConfig::default();
        set_backoff_multiplier(&mut backoff_only, s("4")).unwrap();
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
        set_backoff_initial_seconds(&mut connection, s("0.5")).unwrap();
        set_backoff_max_seconds(&mut connection, s("40")).unwrap();
        set_backoff_multiplier(&mut connection, s("3")).unwrap();

        let mut statement = connection;
        assert_eq!(
            string(statement.backoff_initial_seconds).as_deref(),
            Some("0.5")
        );
        assert_eq!(string(statement.backoff_max_seconds).as_deref(), Some("40"));
        assert_eq!(string(statement.backoff_multiplier).as_deref(), Some("3"));

        set_backoff_max_seconds(&mut statement, s("")).unwrap();
        set_backoff_multiplier(&mut statement, s("2")).unwrap();
        assert_eq!(string(statement.backoff_max_seconds), None);
        assert_eq!(string(statement.backoff_multiplier).as_deref(), Some("2"));
        // The connection is unaffected by statement-level overrides.
        assert_eq!(
            string(connection.backoff_max_seconds).as_deref(),
            Some("40")
        );
        assert_eq!(string(connection.backoff_multiplier).as_deref(), Some("3"));
    }

    /// The three backoff knobs are opaque `f64`s at the option layer, so out-of-range values reach
    /// the gax builder; [`ExponentialBackoffBuilder::clamp`] is what keeps building the policy
    /// infallible. Nothing pinned that, and a switch to `build()` would turn a value the option
    /// layer accepts into a failure (or a panic) at the first retried RPC.
    #[test]
    fn backoff_knobs_are_clamped_into_the_gax_recommended_ranges() {
        let rendered = |config: &RetryConfig| format!("{:?}", config.backoff_policy_arg().unwrap());

        // Everything below the floor: initial delay >= 1ms, maximum delay >= 1s, multiplier >= 1.0.
        let mut low = RetryConfig::default();
        set_backoff_initial_seconds(&mut low, s("0.0005")).unwrap();
        set_backoff_max_seconds(&mut low, s("0.1")).unwrap();
        set_backoff_multiplier(&mut low, OptionValue::Double(0.5)).unwrap();
        assert_eq!(
            rendered(&low),
            "BackoffPolicyArg(ExponentialBackoff { initial_delay: 1ms, maximum_delay: 1s, \
             scaling: 1.0 })"
        );

        // Everything above the ceiling: maximum delay <= 24h, multiplier <= 32.0.
        let mut high = RetryConfig::default();
        set_backoff_max_seconds(&mut high, s("200000")).unwrap();
        set_backoff_multiplier(&mut high, s("100")).unwrap();
        assert_eq!(
            rendered(&high),
            "BackoffPolicyArg(ExponentialBackoff { initial_delay: 1s, maximum_delay: 86400s, \
             scaling: 32.0 })"
        );

        // An initial delay past the maximum is an empty range, which `build()` rejects outright;
        // clamping collapses it onto the maximum instead.
        let mut inverted = RetryConfig::default();
        set_backoff_initial_seconds(&mut inverted, s("30")).unwrap();
        set_backoff_max_seconds(&mut inverted, s("5")).unwrap();
        assert_eq!(
            rendered(&inverted),
            "BackoffPolicyArg(ExponentialBackoff { initial_delay: 5s, maximum_delay: 5s, \
             scaling: 2.0 })"
        );

        // Setting one knob leaves the other two at the client's own defaults (1s / 60s / x2).
        let mut one = RetryConfig::default();
        set_backoff_multiplier(&mut one, s("3")).unwrap();
        assert_eq!(
            rendered(&one),
            "BackoffPolicyArg(ExponentialBackoff { initial_delay: 1s, maximum_delay: 60s, \
             scaling: 3.0 })"
        );
    }
}

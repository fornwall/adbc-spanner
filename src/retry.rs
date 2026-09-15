//! Retry-policy tuning options (`spanner.retry.max_attempts` / `spanner.retry.max_elapsed_seconds`,
//! plus the backoff knobs `spanner.retry.backoff.{initial_seconds,max_seconds,multiplier}`).
//!
//! Every data-plane RPC the driver issues is retried by the pinned Spanner client under a default
//! policy — AIP-194 strict, additionally retrying transport / IO errors on idempotent requests (the
//! client's [`SpannerRetryPolicy`]). On the unary RPC paths that default
//! has **no** attempt or elapsed-time cap, so a persistently `UNAVAILABLE` backend is retried until
//! the operation-wide [RPC timeout](crate::timeout) (if any) fires. These two options let a caller
//! *bound* the client's retrying instead — mirroring the gax convention of an attempt count and an
//! overall elapsed-time limit:
//!
//! - [`OPTION_RETRY_MAX_ATTEMPTS`](crate::OPTION_RETRY_MAX_ATTEMPTS) — the maximum number of
//!   attempts (the first try plus retries), a positive integer. `1` disables retrying.
//! - [`OPTION_RETRY_MAX_ELAPSED_SECONDS`](crate::OPTION_RETRY_MAX_ELAPSED_SECONDS) — an upper bound,
//!   in seconds, on the total wall-clock time spent across attempts before the last error is
//!   surfaced as permanent.
//!
//! The two are independent and may be combined (the retry loop stops at whichever limit is reached
//! first). When neither is set the client keeps its own default policy — so this feature is purely
//! opt-in and, by default, changes nothing.
//!
//! Those are the gax knobs' meanings, and what the driver asks for. The attempt limit is delivered
//! faithfully everywhere; the *elapsed* limit is not delivered on the streaming query path — the
//! next section is the authoritative statement, and both bullets above hold exactly on the unary
//! paths.
//!
//! # What the limits actually deliver, per RPC path
//!
//! The pinned client runs **two different retry loops**. They now agree on attempt accounting but
//! not on elapsed time, so `max_elapsed_seconds` lands differently depending on which RPC carries
//! the work. That gap is an upstream defect (REVIEW.md **UP-14**), not something the driver can
//! correct: the same [`RetryPolicyArg`] is handed to both kinds of loop, so no single compensation
//! is right for both — and nothing a policy can do recovers a loop start the caller re-takes on
//! every decision. The exact numbers below are pinned by `retry_max_attempts_*` /
//! `retry_max_elapsed_seconds_*` in `tests/mock_spanner.rs`, which fail loudly if a
//! `google-cloud-rust` rev bump changes them.
//!
//! - **Unary RPCs** — `ExecuteSql` (DML), `ExecuteBatchDml`, `BeginTransaction`, `Commit` — run
//!   through gax's `retry_loop`, which increments `RetryState::attempt_count` *before* each attempt
//!   and pins `RetryState::start` to the real start of the loop. Both limits are **exact**:
//!   `max_attempts = N` permits `N` attempts and `1` genuinely disables retrying; the elapsed budget
//!   bounds the loop as documented. The client's default policy here is uncapped.
//! - **The streaming query path** — `ExecuteStreamingSql`, i.e. every read-only query — is
//!   dispatched *outside* `retry_loop` (`server_streaming/builder.rs`'s `send()` has no retry loop
//!   of its own, so an error returned as the RPC's *initial* status is never retried at all). Stream
//!   resumption is hand-rolled in `ResultSet::check_retry` (`.../src/spanner/src/result_set.rs`),
//!   which builds a fresh [`RetryState`] per resume decision. It seeds that state with
//!   `1 + retry_count`, so:
//!   - `max_attempts = N` permits exactly **`N`** attempts, matching the unary paths; `1` disables
//!     retrying here too. (Before the `google-cloud-rust` rev that fixed this, the seed was the bare
//!     `retry_count` — *retries so far*, hence `0` on the first failure — and `N` permitted `N + 1`.)
//!   - `max_elapsed_seconds` is still **inert**: the fresh state resets `start` to `Instant::now()`,
//!     so the gax elapsed-time decorator forever compares now against a deadline one whole budget in
//!     the future and never exhausts. A streaming caller who needs a wall-clock bound has a working
//!     one in the separate [RPC timeout](crate::timeout) family
//!     (`spanner.rpc.timeout_seconds.{query,fetch}`), which does bound this path.
//!
//!   The client's default policy on this path is *not* uncapped either — it is
//!   `SpannerRetryPolicy::new().with_attempt_limit(10)` (`result_set.rs`'s `apply_defaults`), i.e. 10
//!   attempts — so setting `max_attempts` here replaces a cap rather than introducing one.
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
//! *replaces* the client's default [`SpannerRetryPolicy`], so to keep the
//! transport-error-on-idempotent retrying while adding a bound, the base policy applied here is that
//! very same client policy (public since googleapis/google-cloud-rust#6048), with the configured
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
/// byte-identical copies naming different types — the same reasoning as the macro of the same name
/// in `request.rs`, kept file-local since the two bodies apply different settings.
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
/// knob), mirroring how [`ReadStaleness`](crate::staleness::ReadStaleness) and
/// [`RpcTimeouts`](crate::timeout::RpcTimeouts) are inherited.
///
/// Values are stored exactly as configured so `get_option` / `get_option_int` /
/// `get_option_double` round-trip them; [`retry_policy_arg`](Self::retry_policy_arg) turns them into
/// a gax [`RetryPolicyArg`] (or `None`, leaving the client's default policy) at apply time.
///
/// The fields are set and read directly by
/// [`impl_shared_option_dispatch`](crate::options::impl_shared_option_dispatch): the seconds knobs
/// through [`f64_option`](crate::options::f64_option), the attempt count through
/// [`parse_max_attempts`].
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

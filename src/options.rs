//! The option surface shared by more than one ADBC object: coercions from an [`OptionValue`] into
//! concrete Rust types, the [`SharedConfig`] bundle a connection and its statements both carry, and
//! the [`impl_shared_option_dispatch`] macro that routes option keys to that bundle for both.
//!
//! The driver, connection and statement all accept the same handful of option shapes — booleans
//! (as exactly the string `true`/`false`), plain strings, positive integers (as an integer or a
//! numeric string), and `f64` seconds ([`f64_option`]). They live here so every level parses an
//! option identically and returns the same `InvalidArguments` status on bad input.
//!
//! Each helper takes a `what` label naming the offending option. That label is always the option's
//! **full key** (e.g. `"option spanner.emulator"`, not a short name like `"max_partitions"`): a
//! caller reading the error needs the exact string they must fix, and a key spelled anywhere else
//! can drift from the one actually dispatched on (IDIO-7). Callers whose key is an enum derive it —
//! `format!("option {}", key.as_ref())`. [`f64_option`] is the exception: it takes the bare option
//! key and prefixes `option ` itself.

use std::time::Duration;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_spanner::model::transaction_options::IsolationLevel;

use crate::conversion::TimestampPrecision;
use crate::directed_read::DirectedRead;
use crate::error::invalid_argument;
use crate::query_options::QueryOptionsConfig;
use crate::request::{CommitStats, RequestConfig};
use crate::retry::RetryConfig;
use crate::staleness::ReadStaleness;
use crate::timeout::RpcTimeouts;

/// Parse a boolean option, accepted as exactly the string `true` or `false` (lowercase — the
/// ADBC canonical spellings, matching `adbc_core`'s own `TryFrom<OptionValue> for bool` and the
/// reference C++ drivers; no case folding, no alternative spellings — COR-7). Anything else —
/// including an int-typed value — is rejected with `InvalidArguments`.
///
/// Int-typed sets are deliberately rejected rather than coerced: no surveyed ADBC driver accepts
/// `SetOptionInt` for a boolean option (the C++ framework's `Option::AsBool`, Go's driverbase and
/// `adbc_core`'s `TryFrom<OptionValue> for bool` all reject it), and accepting one would break the
/// spec's set/get type symmetry, since the getters serve the canonical `"true"`/`"false"` string
/// (COR-4).
pub(crate) fn bool_option(value: OptionValue, what: &str) -> Result<bool> {
    match value {
        OptionValue::String(s) => match s.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            other => Err(invalid_argument(format!(
                "{what} expects \"true\" or \"false\", got {other:?}"
            ))),
        },
        _ => Err(invalid_argument(format!(
            "{what} is a boolean option and takes the strings \"true\"/\"false\" \
             (int- and other non-string-typed values are not accepted)"
        ))),
    }
}

/// Parse a plain string option; any other value kind is rejected with `InvalidArguments`.
pub(crate) fn string_option(value: OptionValue, what: &str) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(invalid_argument(format!("{what} requires a string value"))),
    }
}

/// Parse a plain string option that spells "unset" as the empty string: `None` for `""`, `Some`
/// otherwise (as [`string_option`] for any non-string value kind). The value is stored verbatim —
/// callers whose grammar tolerates surrounding whitespace trim it themselves.
pub(crate) fn non_empty_string_option(value: OptionValue, what: &str) -> Result<Option<String>> {
    Ok(Some(string_option(value, what)?).filter(|s| !s.is_empty()))
}

/// The accepted range of an [`f64_option`], which also fixes the wording of its rejection.
///
/// The two `*Seconds` variants additionally require the value to be representable as a
/// [`Duration`], so the sites that convert one with `Duration::from_secs_f64` can never fail: the
/// (astronomically large) overflow is rejected at set time instead. [`PositiveFactor`] is a bare
/// number, not a duration, so it carries no such bound.
///
/// [`PositiveFactor`]: F64Range::PositiveFactor
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum F64Range {
    /// Finite, strictly positive seconds: zero is rejected.
    PositiveSeconds,
    /// Finite, non-negative seconds: zero is accepted (the RPC timeouts read it as "disabled").
    NonNegativeSeconds,
    /// A finite, strictly positive plain factor (no `Duration` bound, and the error says "number"
    /// rather than "number of seconds").
    PositiveFactor,
}

/// Parse an `f64` option in `range` — the shape shared by every "seconds" knob in the driver
/// (`spanner.rpc.timeout_seconds.*`, `spanner.retry.max_elapsed_seconds`,
/// `spanner.retry.backoff.*`): a numeric string (trimmed; fractions allowed), an integer, or a
/// double. An empty string yields `None` (unset); `NaN`, the infinities, out-of-range values,
/// other value kinds and non-numeric input are rejected with `InvalidArguments`, naming `what`.
pub(crate) fn f64_option(value: OptionValue, what: &str, range: F64Range) -> Result<Option<f64>> {
    let (bound, unit) = match range {
        F64Range::PositiveSeconds => ("strictly positive", " of seconds"),
        F64Range::NonNegativeSeconds => ("non-negative", " of seconds"),
        F64Range::PositiveFactor => ("strictly positive", ""),
    };
    let reject = || {
        invalid_argument(format!(
            "option {what} must be a finite, {bound} number{unit}"
        ))
    };
    let n = match value {
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
    let in_range = match range {
        F64Range::NonNegativeSeconds => n >= 0.0,
        F64Range::PositiveSeconds | F64Range::PositiveFactor => n > 0.0,
    };
    if !n.is_finite() || !in_range {
        return Err(reject());
    }
    if range != F64Range::PositiveFactor && Duration::try_from_secs_f64(n).is_err() {
        return Err(reject());
    }
    Ok(Some(n))
}

/// Parse a strictly-positive `i64` option, accepted as an integer or a numeric string. Zero,
/// negatives, non-numeric strings and other value kinds are rejected with `InvalidArguments`.
pub(crate) fn positive_i64(value: OptionValue, what: &str) -> Result<i64> {
    let reject = || invalid_argument(format!("{what} must be a positive integer"));
    let n = match value {
        OptionValue::Int(i) => i,
        OptionValue::String(s) => s.parse::<i64>().map_err(|_| reject())?,
        _ => return Err(reject()),
    };
    if n > 0 { Ok(n) } else { Err(reject()) }
}

/// Parse a strictly-positive `usize` option (as [`positive_i64`], narrowed to `usize`).
pub(crate) fn positive_usize(value: OptionValue, what: &str) -> Result<usize> {
    let n = positive_i64(value, what)?;
    usize::try_from(n)
        .ok()
        .filter(|&n| n > 0)
        .ok_or_else(|| invalid_argument(format!("{what} must be a positive integer")))
}

/// Reinterpret a `get_option_string` lookup as `get_option_int`.
///
/// Every gettable option in this driver has a canonical string form, so the typed getters at all
/// three levels (database / connection / statement) delegate to `get_option_string` and parse the
/// result here. The string lookup's error is propagated **unchanged** — per ADBC, `NotFound` means
/// "option unset/unknown", never "wrong type" — while an option that IS set but whose value cannot
/// be represented as an integer is reported as `InvalidArguments`.
pub(crate) fn int_from_stored_string(stored: Result<String>, what: &str) -> Result<i64> {
    let value = stored?;
    value
        .parse::<i64>()
        .map_err(|_| invalid_argument(format!("{what} value {value:?} is not an integer")))
}

/// Reinterpret a `get_option_string` lookup as `get_option_double`; the same contract as
/// [`int_from_stored_string`], parsing to `f64` (so integer-valued options are served as doubles).
pub(crate) fn double_from_stored_string(stored: Result<String>, what: &str) -> Result<f64> {
    let value = stored?;
    value
        .parse::<f64>()
        .map_err(|_| invalid_argument(format!("{what} value {value:?} is not a double")))
}

/// Generate the three typed `Optionable` getters — `get_option_bytes`, `get_option_int` and
/// `get_option_double` — that `SpannerDatabase`, `SpannerConnection` and `SpannerStatement` share
/// verbatim.
///
/// Every gettable option has a canonical string form, so each level's typed getters are pure
/// reinterpretations of its own `get_option_string`: bytes are its UTF-8, ints and doubles are it
/// parsed by [`int_from_stored_string`] / [`double_from_stored_string`] (which propagate the string
/// lookup's `NotFound` unchanged and report a set-but-unparsable value as `InvalidArguments`). Only
/// `get_option_string` differs per level; the key type is `Self::Option`, whose `AsRef<str>` names
/// the key for the error message.
///
/// This deliberately covers only the *typed* getters. The read-only
/// `spanner.commit_stats.mutation_count` key and friends live in `get_option_string` (via
/// [`impl_shared_option_dispatch`]) and so are served through these bodies without any per-level
/// special-casing here.
macro_rules! impl_typed_option_getters {
    () => {
        fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
            Ok(self.get_option_string(key)?.into_bytes())
        }

        fn get_option_int(&self, key: Self::Option) -> Result<i64> {
            let what = format!("option {}", key.as_ref());
            crate::options::int_from_stored_string(self.get_option_string(key), &what)
        }

        fn get_option_double(&self, key: Self::Option) -> Result<f64> {
            let what = format!("option {}", key.as_ref());
            crate::options::double_from_stored_string(self.get_option_string(key), &what)
        }
    };
}
pub(crate) use impl_typed_option_getters;

/// The configuration a [`SpannerConnection`](crate::connection::SpannerConnection) and every
/// [`SpannerStatement`](crate::statement::SpannerStatement) it creates both carry: everything
/// option-settable that is *not* a client handle.
///
/// A connection starts from [`Default`] and applies its own options on top; each new statement
/// takes an [`inherit`](Self::inherit)ed copy, and may then override the fields it also exposes
/// (the "staleness pattern"). [`impl_shared_option_dispatch`] emits the key→setter / key→getter
/// dispatch for these fields once, for both objects — which is why both must name their field
/// `config`.
///
/// One struct because the values already travel together: the connection hands all of them to
/// `SpannerStatement::new`, and both objects hand the commit-relevant ones to the shared
/// [`run_batch_dml`](crate::connection::run_batch_dml) /
/// [`run_batch_txn`](crate::connection::run_batch_txn) /
/// [`write_mutations_txn`](crate::connection::write_mutations_txn) helpers. Bundled, adding an
/// option touches this struct and the macro, and no signature at all (IDIO-2).
#[derive(Debug, Clone)]
pub(crate) struct SharedConfig {
    /// The standard `adbc.connection.readonly` flag: a connection that has it set rejects all
    /// writes (DML/DDL/ingest fail with `InvalidState`; queries still run), including the *commit*
    /// of already-buffered work. Behind an `Arc` and read at execution time rather than snapshotted
    /// — see [`is_read_only`](Self::is_read_only).
    pub(crate) read_only: Arc<AtomicBool>,
    /// Isolation level applied to read/write transactions (autocommit DML and the manual-mode
    /// commit), set via the standard `adbc.connection.transaction.isolation_level` option. It
    /// reaches only the DML paths — queries take a timestamp bound instead (see
    /// [`apply_isolation`](crate::connection::apply_isolation)) — and
    /// [`IsolationLevel::Unspecified`] (the default) sends no level, which Spanner reads as
    /// `SERIALIZABLE`. Connection-set only: a statement inherits it but exposes no setter of its
    /// own.
    pub(crate) isolation: IsolationLevel,
    /// Read bound for read-only queries (`spanner.read.staleness`). The default is a strong read.
    pub(crate) read_staleness: ReadStaleness,
    /// Request priority and request/transaction tags (`spanner.request.priority` /
    /// `spanner.request.tag` / `spanner.transaction.tag`), plus the commit knobs
    /// `spanner.commit.max_delay` and `spanner.commit_stats`. Unset by default. A statement may
    /// override the priority and request tag; the transaction tag is connection-level only, but
    /// rides along for the read/write transaction runners a statement builds.
    pub(crate) request: RequestConfig,
    /// Directed-read replica selection for read-only queries (`spanner.directed_read`). Unset by
    /// default (Spanner's own routing).
    pub(crate) directed_read: DirectedRead,
    /// Query optimizer options (`spanner.query.optimizer_version` /
    /// `spanner.query.optimizer_statistics_package`). Unset by default; applied to every query
    /// statement builder (via `SpannerStatement::sql_builder`).
    pub(crate) query_options: QueryOptionsConfig,
    /// How `TIMESTAMP` columns map to Arrow (`spanner.max_timestamp_precision`): nanoseconds that
    /// error on out-of-range instants (the default) or microseconds covering Spanner's full range.
    /// Applied uniformly to every result path — `execute` (plain and bound queries), DML
    /// `THEN RETURN` rows, `execute_schema`, the `execute_partitions` schema probe — and, on the
    /// connection, to `get_table_schema` and `read_partition` (which have no statement).
    pub(crate) timestamp_precision: TimestampPrecision,
    /// RPC timeouts (`spanner.rpc.timeout_seconds.{query,update,fetch}`). Unset by default (no
    /// deadline); an expired deadline fails with `Status::Timeout`. The connection applies the
    /// update timeout to its commit paths and the query/fetch timeouts to `read_partition`.
    pub(crate) timeouts: RpcTimeouts,
    /// Retry-policy and backoff tuning (`spanner.retry.*`). Unset by default, leaving the client's
    /// own policy; when set it bounds the client's retrying on every statement/DML/transaction
    /// builder the owning object produces.
    pub(crate) retry: RetryConfig,
    /// Mutation count captured from this object's most recent commit that requested commit
    /// statistics (`spanner.commit_stats`), read back via `spanner.commit_stats.mutation_count`.
    /// Per-object rather than inherited — a statement records its autocommit DML / bulk-ingest
    /// commits here, a connection its manual-mode commit — so [`inherit`](Self::inherit) resets it.
    pub(crate) commit_stats: CommitStats,
}

impl Default for SharedConfig {
    fn default() -> Self {
        Self {
            read_only: Arc::new(AtomicBool::new(false)),
            // `IsolationLevel` is a `#[non_exhaustive]` client enum with no `Default` impl, which
            // is the only reason this whole impl is hand-written rather than derived.
            isolation: IsolationLevel::Unspecified,
            read_staleness: ReadStaleness::default(),
            request: RequestConfig::default(),
            directed_read: DirectedRead::default(),
            query_options: QueryOptionsConfig::default(),
            timestamp_precision: TimestampPrecision::default(),
            timeouts: RpcTimeouts::default(),
            retry: RetryConfig::default(),
            commit_stats: CommitStats::default(),
        }
    }
}

impl SharedConfig {
    /// The config a statement created on this connection starts from.
    ///
    /// Everything is inherited except [`commit_stats`](Self::commit_stats), which is per-object:
    /// the statement's own commits record there, never into the connection's cell. Note the
    /// difference in how the two `Arc` fields come across — `read_only` is deliberately *aliased*
    /// (a later toggle on the connection reaches statements it has already created), while
    /// `commit_stats` starts fresh.
    pub(crate) fn inherit(&self) -> Self {
        Self {
            commit_stats: CommitStats::default(),
            ..self.clone()
        }
    }

    /// The *live* value of the `adbc.connection.readonly` flag. Loaded on each check, never cached,
    /// so a toggle on the connection applies immediately to the statements it already created.
    pub(crate) fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Acquire)
    }
}

/// Generate the two shared option-dispatch helpers that `SpannerConnection` and `SpannerStatement`
/// share verbatim.
///
/// Both objects carry a [`SharedConfig`] — as a field named `config` — whose fields have identical
/// setters/getters on either object, so this macro emits the key→setter / key→getter dispatch once
/// (rather than ~20 near-identical match arms in each of `connection.rs` and `statement.rs`) as two
/// inherent methods:
///
/// - `set_shared_option(key, value)` applies a shared option, returning `Ok(Some(()))` when the key
///   was handled and `Ok(None)` when it is not a shared key (so the caller can fall through to its
///   own options / error). The value's own parse errors propagate unchanged.
/// - `shared_option_string(key)` reports a shared option's canonical string, returning the
///   `NotFound` error for an unset (or non-shared) key exactly as the hand-written arms did.
///
/// Object-specific options (ingest/bind/batch on the statement, `transaction.tag` on the
/// connection, catalog/schema, autocommit, …) stay as explicit arms in each caller; only the
/// mechanical glue lives here. The referenced names (`TimestampPrecision`, `err`, `Status`,
/// `OptionValue`, `Result`) resolve at the expansion site, where both callers already import them.
macro_rules! impl_shared_option_dispatch {
    () => {
        /// Apply one of the shared "staleness-pattern" options. `Ok(Some(()))` = handled;
        /// `Ok(None)` = `key` is not a shared option. See [`impl_shared_option_dispatch`].
        fn set_shared_option(&mut self, key: &str, value: OptionValue) -> Result<Option<()>> {
            match key {
                crate::OPTION_READ_STALENESS => self.config.read_staleness.set_staleness(value)?,
                crate::OPTION_REQUEST_PRIORITY => self.config.request.set_priority(value)?,
                crate::OPTION_REQUEST_TAG => self.config.request.set_request_tag(value)?,
                crate::OPTION_DIRECTED_READ => self.config.directed_read.set(value)?,
                crate::OPTION_MAX_COMMIT_DELAY => {
                    self.config.request.set_max_commit_delay(value)?
                }
                crate::OPTION_COMMIT_STATS => self.config.request.set_commit_stats(value)?,
                crate::OPTION_QUERY_OPTIMIZER_VERSION => {
                    self.config.query_options.set_optimizer_version(value)?
                }
                crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE => self
                    .config
                    .query_options
                    .set_optimizer_statistics_package(value)?,
                crate::OPTION_MAX_TIMESTAMP_PRECISION => {
                    self.config.timestamp_precision = TimestampPrecision::parse_option(value)?
                }
                crate::OPTION_RPC_TIMEOUT_QUERY => self.config.timeouts.set_query(value)?,
                crate::OPTION_RPC_TIMEOUT_UPDATE => self.config.timeouts.set_update(value)?,
                crate::OPTION_RPC_TIMEOUT_FETCH => self.config.timeouts.set_fetch(value)?,
                crate::OPTION_RETRY_MAX_ATTEMPTS => self.config.retry.set_max_attempts(value)?,
                crate::OPTION_RETRY_MAX_ELAPSED_SECONDS => {
                    self.config.retry.set_max_elapsed_seconds(value)?
                }
                crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS => {
                    self.config.retry.set_backoff_initial_seconds(value)?
                }
                crate::OPTION_RETRY_BACKOFF_MAX_SECONDS => {
                    self.config.retry.set_backoff_max_seconds(value)?
                }
                crate::OPTION_RETRY_BACKOFF_MULTIPLIER => {
                    self.config.retry.set_backoff_multiplier(value)?
                }
                _ => return Ok(None),
            }
            Ok(Some(()))
        }

        /// Report a shared option's canonical string, or a `NotFound` error when it is unset or not
        /// a shared option. See [`impl_shared_option_dispatch`].
        fn shared_option_string(&self, key: &str) -> Result<String> {
            let value: Option<String> = match key {
                crate::OPTION_READ_STALENESS => self
                    .config
                    .read_staleness
                    .staleness_string()
                    .map(str::to_string),
                crate::OPTION_REQUEST_PRIORITY => {
                    self.config.request.priority_string().map(str::to_string)
                }
                crate::OPTION_REQUEST_TAG => {
                    self.config.request.request_tag_string().map(str::to_string)
                }
                crate::OPTION_DIRECTED_READ => self
                    .config
                    .directed_read
                    .option_string()
                    .map(str::to_string),
                crate::OPTION_MAX_COMMIT_DELAY => self
                    .config
                    .request
                    .max_commit_delay_string()
                    .map(str::to_string),
                // A plain boolean; always reports the effective value ("true"/"false", default
                // "false").
                crate::OPTION_COMMIT_STATS => {
                    Some(self.config.request.commit_stats_string().to_string())
                }
                // The captured mutation count from the most recent commit that requested commit
                // stats; None → NotFound below.
                crate::OPTION_COMMIT_STATS_MUTATION_COUNT => self
                    .config
                    .commit_stats
                    .mutation_count()
                    .map(|n| n.to_string()),
                crate::OPTION_QUERY_OPTIMIZER_VERSION => self
                    .config
                    .query_options
                    .optimizer_version_string()
                    .map(str::to_string),
                crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE => self
                    .config
                    .query_options
                    .optimizer_statistics_package_string()
                    .map(str::to_string),
                // Always set (there is a default mode), so the effective value is always reported.
                crate::OPTION_MAX_TIMESTAMP_PRECISION => {
                    Some(self.config.timestamp_precision.as_str().to_string())
                }
                crate::OPTION_RPC_TIMEOUT_QUERY => self.config.timeouts.query_string(),
                crate::OPTION_RPC_TIMEOUT_UPDATE => self.config.timeouts.update_string(),
                crate::OPTION_RPC_TIMEOUT_FETCH => self.config.timeouts.fetch_string(),
                crate::OPTION_RETRY_MAX_ATTEMPTS => self.config.retry.max_attempts_string(),
                crate::OPTION_RETRY_MAX_ELAPSED_SECONDS => {
                    self.config.retry.max_elapsed_seconds_string()
                }
                crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS => {
                    self.config.retry.backoff_initial_seconds_string()
                }
                crate::OPTION_RETRY_BACKOFF_MAX_SECONDS => {
                    self.config.retry.backoff_max_seconds_string()
                }
                crate::OPTION_RETRY_BACKOFF_MULTIPLIER => {
                    self.config.retry.backoff_multiplier_string()
                }
                _ => None,
            };
            value.ok_or_else(|| err(format!("option {key} is not set"), Status::NotFound))
        }
    };
}
pub(crate) use impl_shared_option_dispatch;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::err;
    use adbc_core::error::Status;

    #[test]
    fn int_getter_parses_integer_valued_options() {
        let get = |v: &str| int_from_stored_string(Ok(v.to_string()), "option o");
        assert_eq!(get("8192").unwrap(), 8192);
        assert_eq!(get("-5").unwrap(), -5);
        assert_eq!(get("0").unwrap(), 0);
    }

    #[test]
    fn bool_option_accepts_exact_true_false_and_rejects_int_typed_values() {
        // The string forms are exactly "true"/"false" (COR-7): lenient spellings (case variants,
        // 1/0, yes/no) are rejected with an error naming the option and the expected spellings.
        assert!(bool_option(OptionValue::String("true".into()), "option o").unwrap());
        assert!(!bool_option(OptionValue::String("false".into()), "option o").unwrap());
        for s in ["TRUE", "1", "yes", "0", "No", "maybe"] {
            let error = bool_option(OptionValue::String(s.into()), "option o").unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "string {s:?}");
            assert!(error.message.contains("option o"), "{}", error.message);
            assert!(
                error.message.contains("\"true\" or \"false\""),
                "{}",
                error.message
            );
        }
        // An int-typed set is rejected too (COR-4): the getters serve the canonical
        // "true"/"false" string, so accepting SetOptionInt(k, 1) would break the spec's set/get
        // type symmetry — and no surveyed ADBC driver accepts an int set for a boolean option.
        for i in [0, 1, -1] {
            let error = bool_option(OptionValue::Int(i), "option o").unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "Int({i})");
            assert!(error.message.contains("option o"), "{}", error.message);
            assert!(
                error.message.contains("\"true\"/\"false\""),
                "{}",
                error.message
            );
        }
    }

    #[test]
    fn int_getter_reports_set_but_non_integer_values_as_invalid_arguments() {
        // A value that exists but is not an integer must NOT be NotFound (that would read as
        // "option unset"): it is an InvalidArguments error naming the option and the value.
        for value in ["true", "max:10s", "1.5", "", "projects/p"] {
            let error = int_from_stored_string(Ok(value.to_string()), "option o").unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
            assert!(error.message.contains("option o"), "{}", error.message);
            assert!(
                error.message.contains(&format!("{value:?}")),
                "{}",
                error.message
            );
        }
    }

    #[test]
    fn typed_getters_propagate_the_string_lookup_error_unchanged() {
        // Unset (NotFound) and unknown-key errors from get_option_string pass through as-is.
        let unset = || err("option o is not set", Status::NotFound);
        let error = int_from_stored_string(Err(unset()), "option o").unwrap_err();
        assert_eq!(error.status, Status::NotFound);
        assert_eq!(error.message, "option o is not set");
        let error = double_from_stored_string(Err(unset()), "option o").unwrap_err();
        assert_eq!(error.status, Status::NotFound);
        assert_eq!(error.message, "option o is not set");
    }

    #[test]
    fn double_getter_parses_and_rejects() {
        let get = |v: &str| double_from_stored_string(Ok(v.to_string()), "option o");
        assert_eq!(get("1.5").unwrap(), 1.5);
        // Integer-valued options can always be represented as doubles.
        assert_eq!(get("3600").unwrap(), 3600.0);
        let error = get("true").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("option o"), "{}", error.message);
    }
}

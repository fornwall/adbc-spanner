//! Shared coercions from an ADBC [`OptionValue`] into concrete Rust types.
//!
//! The driver, connection and statement all accept the same handful of option shapes — booleans
//! (as bool-ish strings or integers), plain strings, and positive integers (as an integer or a
//! numeric string). These used to be copy-pasted (with slightly divergent error text) into
//! `driver.rs`, `connection.rs` and `statement.rs`; they live here so every level parses an option
//! identically and returns the same `InvalidArguments` status on bad input.
//!
//! Each helper takes a `what` label describing the option (e.g. `"option spanner.emulator"` or
//! `"max_partitions"`) so the shared error message names the offending option.

use adbc_core::error::Result;
use adbc_core::options::OptionValue;

use crate::error::invalid_argument;

/// Parse a boolean option, accepted as a bool-ish string (`true`/`false`/`1`/`0`/`yes`/`no`,
/// case-insensitive) or an integer (`0` = false, any non-zero = true). Anything else is rejected
/// with `InvalidArguments`.
pub(crate) fn bool_option(value: OptionValue, what: &str) -> Result<bool> {
    match value {
        OptionValue::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            other => Err(invalid_argument(format!(
                "{what} expects a boolean, got {other:?}"
            ))),
        },
        OptionValue::Int(i) => Ok(i != 0),
        _ => Err(invalid_argument(format!("{what} requires a boolean value"))),
    }
}

/// Parse a plain string option; any other value kind is rejected with `InvalidArguments`.
pub(crate) fn string_option(value: OptionValue, what: &str) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(invalid_argument(format!("{what} requires a string value"))),
    }
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

/// Generate the two shared option-dispatch helpers that `SpannerConnection` and `SpannerStatement`
/// share verbatim.
///
/// Both objects carry the same "staleness-pattern" config fields (`read_staleness`, `request`,
/// `directed_read`, `query_options`, `timestamp_precision`, `timeouts`, `retry`, `commit_stats`)
/// with identical setters/getters, so the key→setter and key→getter dispatch for those options was
/// duplicated as ~20 near-identical `Other(k) if k == OPTION_X => …` match arms in each of
/// `connection.rs` and `statement.rs`. This macro emits that dispatch once as two inherent methods:
///
/// - `set_shared_option(key, value)` applies a shared option, returning `Ok(Some(()))` when the key
///   was handled and `Ok(None)` when it is not a shared key (so the caller can fall through to its
///   own options / error). The value's own parse errors propagate unchanged.
/// - `shared_option_string(key)` reports a shared option's canonical string, returning the
///   `NotFound` error for an unset (or non-shared) key exactly as the hand-written arms did.
///
/// Object-specific options (ingest/bind/batch on the statement, `transaction.tag` on the
/// connection, catalog/schema, autocommit, …) stay as explicit arms in each caller; only the
/// mechanical glue lives here. The
/// referenced names (`TimestampPrecision`, `err`, `Status`, `OptionValue`, `Result`) resolve at the
/// expansion site, where both callers already import them, so the generated bodies read exactly like
/// the arms they replace.
macro_rules! impl_shared_option_dispatch {
    () => {
        /// Apply one of the shared "staleness-pattern" options. `Ok(Some(()))` = handled;
        /// `Ok(None)` = `key` is not a shared option. See [`impl_shared_option_dispatch`].
        fn set_shared_option(&mut self, key: &str, value: OptionValue) -> Result<Option<()>> {
            match key {
                crate::OPTION_READ_STALENESS => self.read_staleness.set_staleness(value)?,
                crate::OPTION_REQUEST_PRIORITY => self.request.set_priority(value)?,
                crate::OPTION_REQUEST_TAG => self.request.set_request_tag(value)?,
                crate::OPTION_DIRECTED_READ => self.directed_read.set(value)?,
                crate::OPTION_MAX_COMMIT_DELAY => self.request.set_max_commit_delay(value)?,
                crate::OPTION_COMMIT_STATS => self.request.set_commit_stats(value)?,
                crate::OPTION_QUERY_OPTIMIZER_VERSION => {
                    self.query_options.set_optimizer_version(value)?
                }
                crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE => {
                    self.query_options.set_optimizer_statistics_package(value)?
                }
                crate::OPTION_MAX_TIMESTAMP_PRECISION => {
                    self.timestamp_precision = TimestampPrecision::parse_option(value)?
                }
                crate::OPTION_RPC_TIMEOUT_QUERY => self.timeouts.set_query(value)?,
                crate::OPTION_RPC_TIMEOUT_UPDATE => self.timeouts.set_update(value)?,
                crate::OPTION_RPC_TIMEOUT_FETCH => self.timeouts.set_fetch(value)?,
                crate::OPTION_RETRY_MAX_ATTEMPTS => self.retry.set_max_attempts(value)?,
                crate::OPTION_RETRY_MAX_ELAPSED_SECONDS => {
                    self.retry.set_max_elapsed_seconds(value)?
                }
                crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS => {
                    self.retry.set_backoff_initial_seconds(value)?
                }
                crate::OPTION_RETRY_BACKOFF_MAX_SECONDS => {
                    self.retry.set_backoff_max_seconds(value)?
                }
                crate::OPTION_RETRY_BACKOFF_MULTIPLIER => {
                    self.retry.set_backoff_multiplier(value)?
                }
                _ => return Ok(None),
            }
            Ok(Some(()))
        }

        /// Report a shared option's canonical string, or a `NotFound` error when it is unset or not
        /// a shared option. See [`impl_shared_option_dispatch`].
        fn shared_option_string(&self, key: &str) -> Result<String> {
            let value: Option<String> = match key {
                crate::OPTION_READ_STALENESS => {
                    self.read_staleness.staleness_string().map(str::to_string)
                }
                crate::OPTION_REQUEST_PRIORITY => {
                    self.request.priority_string().map(str::to_string)
                }
                crate::OPTION_REQUEST_TAG => self.request.request_tag_string().map(str::to_string),
                crate::OPTION_DIRECTED_READ => {
                    self.directed_read.option_string().map(str::to_string)
                }
                crate::OPTION_MAX_COMMIT_DELAY => {
                    self.request.max_commit_delay_string().map(str::to_string)
                }
                // A plain boolean; always reports the effective value ("true"/"false", default
                // "false").
                crate::OPTION_COMMIT_STATS => Some(self.request.commit_stats_string().to_string()),
                // The captured mutation count from the most recent commit that requested commit
                // stats; None → NotFound below.
                crate::OPTION_COMMIT_STATS_MUTATION_COUNT => {
                    self.commit_stats.mutation_count().map(|n| n.to_string())
                }
                crate::OPTION_QUERY_OPTIMIZER_VERSION => self
                    .query_options
                    .optimizer_version_string()
                    .map(str::to_string),
                crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE => self
                    .query_options
                    .optimizer_statistics_package_string()
                    .map(str::to_string),
                // Always set (there is a default mode), so the effective value is always reported.
                crate::OPTION_MAX_TIMESTAMP_PRECISION => {
                    Some(self.timestamp_precision.as_str().to_string())
                }
                crate::OPTION_RPC_TIMEOUT_QUERY => self.timeouts.query_string(),
                crate::OPTION_RPC_TIMEOUT_UPDATE => self.timeouts.update_string(),
                crate::OPTION_RPC_TIMEOUT_FETCH => self.timeouts.fetch_string(),
                crate::OPTION_RETRY_MAX_ATTEMPTS => self.retry.max_attempts_string(),
                crate::OPTION_RETRY_MAX_ELAPSED_SECONDS => self.retry.max_elapsed_seconds_string(),
                crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS => {
                    self.retry.backoff_initial_seconds_string()
                }
                crate::OPTION_RETRY_BACKOFF_MAX_SECONDS => self.retry.backoff_max_seconds_string(),
                crate::OPTION_RETRY_BACKOFF_MULTIPLIER => self.retry.backoff_multiplier_string(),
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

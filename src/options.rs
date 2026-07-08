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
    if n > 0 {
        Ok(n)
    } else {
        Err(reject())
    }
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

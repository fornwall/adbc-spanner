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

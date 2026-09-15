//! The read-bound option for read-only queries.
//!
//! By default every query reads at a **strong** bound (`TimestampBound::strong`) — it sees the
//! effects of every transaction that committed before the read started. Spanner also supports
//! **stale reads**, which pick an older read timestamp so the read can be served locally without a
//! cross-replica quorum: cheaper and lock-free, ideal for analytics. This module parses
//! [`OPTION_READ_STALENESS`](crate::OPTION_READ_STALENESS) (`spanner.read.staleness`), whose value
//! grammar is documented on that constant, and maps it onto the client's [`TimestampBound`]:
//!
//! - `exact:<duration>` → [`TimestampBound::exact_staleness`]
//! - `max:<duration>` → [`TimestampBound::max_staleness`] (single-use reads only)
//! - `read:<rfc3339>` → [`TimestampBound::read_timestamp`]
//! - `min:<rfc3339>` → [`TimestampBound::min_read_timestamp`] (single-use reads only)
//!
//! The four prefixes are distinct, so a value is unambiguous; like every option value in this
//! driver they are lowercase and matched exactly. Malformed values are rejected with
//! `InvalidArgument`, and an empty string unsets (which is also how a statement clears a bound
//! inherited from its connection).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use chrono::{DateTime, Utc};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::transaction::{
    MultiUseReadOnlyTransaction, SingleUseReadOnlyTransaction, TimestampBound,
};

use crate::error::{from_spanner, invalid_argument};
use crate::options::RawParsed;

/// Build a single-use read-only transaction, applying an optional non-strong timestamp bound.
/// `None` leaves the client default (a strong read).
pub(crate) fn single_use(
    client: &DatabaseClient,
    bound: Option<TimestampBound>,
) -> SingleUseReadOnlyTransaction {
    let builder = client.single_use();
    match bound {
        Some(b) => builder.set_timestamp_bound(b).build(),
        None => builder.build(),
    }
}

/// Build a multi-use read-only transaction, applying an optional timestamp bound (already pinned
/// to a multi-use-legal kind by [`ReadStaleness::multi_use_timestamp_bound`]); `None` leaves the
/// client default (a strong read). Building issues no RPC — the begin is inline on the first query.
pub(crate) async fn multi_use(
    client: &DatabaseClient,
    bound: Option<TimestampBound>,
) -> Result<MultiUseReadOnlyTransaction> {
    let builder = client.read_only_transaction();
    match bound {
        Some(b) => builder.set_timestamp_bound(b),
        None => builder,
    }
    .build()
    .await
    .map_err(from_spanner)
}

/// A parsed read bound, before it is turned into a client [`TimestampBound`]. Kept as a small,
/// pure value so the option parsing can be unit-tested offline.
#[derive(Debug, Clone, PartialEq)]
enum ReadBound {
    /// Read exactly this far in the past (`exact:<duration>`).
    ExactStaleness(Duration),
    /// Read at any timestamp within this window of now (`max:<duration>`).
    MaxStaleness(Duration),
    /// Read exactly as of this timestamp (`read:<rfc3339>` / bare).
    ReadTimestamp(DateTime<Utc>),
    /// Read at this timestamp or later (`min:<rfc3339>`).
    MinReadTimestamp(DateTime<Utc>),
}

impl ReadBound {
    /// The equivalent bound for a **multi-use** read-only transaction.
    ///
    /// Spanner only accepts strong / exact-staleness / read-timestamp bounds when beginning a
    /// multi-use read-only transaction — the bounded-staleness kinds are single-use only (the
    /// server rejects them in `BeginTransaction`). Those two are therefore pinned to the *most
    /// stale* timestamp their window allows, which is always a legal choice under the original
    /// bound: `max:<d>` becomes exact staleness `<d>`, and `min:<t>` becomes read timestamp `<t>`.
    /// The already-exact kinds pass through unchanged.
    fn pinned_for_multi_use(&self) -> ReadBound {
        match self {
            ReadBound::MaxStaleness(d) => ReadBound::ExactStaleness(*d),
            ReadBound::MinReadTimestamp(t) => ReadBound::ReadTimestamp(*t),
            other => other.clone(),
        }
    }

    /// Build the client [`TimestampBound`] for this read bound.
    fn to_timestamp_bound(&self) -> Result<TimestampBound> {
        match self {
            ReadBound::ExactStaleness(d) => TimestampBound::try_exact_staleness(*d)
                .map_err(|e| invalid_argument(format!("read staleness out of range: {e}"))),
            ReadBound::MaxStaleness(d) => TimestampBound::try_max_staleness(*d)
                .map_err(|e| invalid_argument(format!("read staleness out of range: {e}"))),
            ReadBound::ReadTimestamp(t) => TimestampBound::try_read_timestamp(to_system_time(*t))
                .map_err(|e| invalid_argument(format!("read timestamp out of range: {e}"))),
            ReadBound::MinReadTimestamp(t) => {
                TimestampBound::try_min_read_timestamp(to_system_time(*t))
                    .map_err(|e| invalid_argument(format!("read timestamp out of range: {e}")))
            }
        }
    }
}

/// The read-bound configuration held by a connection or statement: the raw
/// `spanner.read.staleness` string (so `get_option` round-trips exactly what was set) beside the
/// bound it parsed to.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadStaleness {
    bound: RawParsed<ReadBound>,
}

impl ReadStaleness {
    /// Handle a `set_option` for `spanner.read.staleness`. An empty value unsets it (a strong
    /// read); any non-empty value replaces the current bound.
    pub(crate) fn set_staleness(&mut self, value: OptionValue) -> Result<()> {
        self.bound
            .set(value, crate::OPTION_READ_STALENESS, parse_read_bound)
    }

    /// The raw `spanner.read.staleness` value, for `get_option` round-trip.
    pub(crate) fn staleness_string(&self) -> Option<&str> {
        self.bound.raw()
    }

    /// The client [`TimestampBound`] to apply, or `None` for a strong read.
    pub(crate) fn timestamp_bound(&self) -> Result<Option<TimestampBound>> {
        self.bound
            .parsed()
            .map(ReadBound::to_timestamp_bound)
            .transpose()
    }

    /// The client [`TimestampBound`] to apply to a **multi-use** read-only transaction, or `None`
    /// for a strong read. The single-use-only bounded-staleness kinds are pinned to a legal
    /// equivalent first — see [`ReadBound::pinned_for_multi_use`].
    pub(crate) fn multi_use_timestamp_bound(&self) -> Result<Option<TimestampBound>> {
        self.bound
            .parsed()
            .map(|b| b.pinned_for_multi_use().to_timestamp_bound())
            .transpose()
    }
}

/// The four accepted `spanner.read.staleness` forms, each with a worked example — the tail of
/// every grammar rejection.
const GRAMMAR_FORMS: &str = "\"exact:<duration>\" (e.g. \"exact:10s\"), \"max:<duration>\" \
     (e.g. \"max:500ms\"), \"read:<rfc3339>\" (e.g. \"read:2026-07-07T00:00:00Z\") or \
     \"min:<rfc3339>\" (e.g. \"min:2026-07-07T00:00:00+02:00\")";

/// Reject `value` as not matching the [`GRAMMAR_FORMS`], echoing it so an empty or whitespace
/// value is visible and the caller can see which of the four prefixes they missed.
fn grammar_err(value: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "option {}: {value:?} is not a valid read bound; expected {GRAMMAR_FORMS}",
        crate::OPTION_READ_STALENESS
    ))
}

/// Parse a `spanner.read.staleness` value into a [`ReadBound`]. Accepts the four prefixed forms —
/// the *relative* `exact:<duration>` / `max:<duration>` and the *absolute* `read:<rfc3339>` /
/// `min:<rfc3339>` — plus a bare `<rfc3339>` (equivalent to `read:`). The four prefixes are
/// distinct, so the value is unambiguous. They are matched exactly (lowercase): ADBC option values
/// are exact-match canonical strings across the driver ecosystem, so an uppercase prefix is
/// rejected with the grammar error rather than case-folded.
fn parse_read_bound(value: &str) -> Result<ReadBound> {
    // RFC 3339 timestamps themselves contain colons, but only *after* the date part, so splitting
    // at the first colon cleanly separates a kind prefix from its argument: `read:<rfc3339>` keeps
    // the timestamp's own colons in `arg`, while a bare timestamp's pseudo-kind (`2026-07-07T00`)
    // matches no arm and falls through.
    if let Some((kind, arg)) = value.split_once(':') {
        match kind.trim() {
            "exact" => return Ok(ReadBound::ExactStaleness(parse_duration(arg.trim())?)),
            "max" => return Ok(ReadBound::MaxStaleness(parse_duration(arg.trim())?)),
            // An explicit timestamp prefix: the caller clearly meant an RFC 3339 timestamp, so the
            // parser's own complaint about it is the useful part and is forwarded.
            "read" => return parse_rfc3339(value, arg.trim()).map(ReadBound::ReadTimestamp),
            "min" => return parse_rfc3339(value, arg.trim()).map(ReadBound::MinReadTimestamp),
            // Not a known kind — fall through and try a bare RFC 3339 timestamp (which also
            // contains colons), else report the grammar error.
            _ => {}
        }
    }
    // No recognised prefix: the likely mistake is the prefix, not the timestamp, so report the
    // grammar rather than a chrono complaint about text that was never meant to be a timestamp.
    DateTime::parse_from_rfc3339(value)
        .map(|t| ReadBound::ReadTimestamp(t.with_timezone(&Utc)))
        .map_err(|_| grammar_err(value))
}

/// Parse the RFC 3339 `timestamp` argument of a `read:`/`min:`-prefixed `value` into a UTC
/// [`DateTime`], quoting the whole option value and forwarding the parser's own diagnosis.
fn parse_rfc3339(value: &str, timestamp: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(timestamp)
        .map_err(|e| {
            invalid_argument(format!(
                "option {}: {value:?} does not carry a valid RFC 3339 timestamp ({e}); expected \
                 {GRAMMAR_FORMS}",
                crate::OPTION_READ_STALENESS
            ))
        })?
        .with_timezone(&Utc))
}

/// Parse a non-negative duration for the `spanner.read.staleness` option — a thin wrapper over
/// [`parse_duration_for`]. Any other option must name itself via that function instead.
pub(crate) fn parse_duration(value: &str) -> Result<Duration> {
    parse_duration_for(value, crate::OPTION_READ_STALENESS)
}

/// Parse a non-negative duration with an optional unit suffix (`s` default, `ms`, `us`/`µs`, `ns`,
/// `m`, `h`), naming `key` in the rejection.
///
/// The grammar is shared between `spanner.read.staleness` and `spanner.commit.max_delay` (see
/// [`crate::request`]), so the option key is a parameter: a rejection that named a fixed option
/// would point the caller at a key they never set.
pub(crate) fn parse_duration_for(value: &str, key: &str) -> Result<Duration> {
    let bad = || {
        invalid_argument(format!(
            "option {key}: {value:?} is not a valid duration; expected a number with an optional \
             unit suffix (s [default], ms, us/µs, ns, m, h), e.g. \"500ms\""
        ))
    };
    // Order matters: check the two-letter suffixes before the single-letter ones.
    let (number, unit_secs): (&str, f64) = if let Some(n) = value.strip_suffix("ms") {
        (n, 1e-3)
    } else if let Some(n) = value
        .strip_suffix("us")
        .or_else(|| value.strip_suffix("µs"))
    {
        (n, 1e-6)
    } else if let Some(n) = value.strip_suffix("ns") {
        (n, 1e-9)
    } else if let Some(n) = value.strip_suffix('s') {
        (n, 1.0)
    } else if let Some(n) = value.strip_suffix('m') {
        (n, 60.0)
    } else if let Some(n) = value.strip_suffix('h') {
        (n, 3600.0)
    } else {
        (value, 1.0)
    };
    let magnitude: f64 = number.trim().parse().map_err(|_| bad())?;
    let seconds = magnitude * unit_secs;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(bad());
    }
    // `try_from_secs_f64` rejects (rather than panics on) durations too large for `Duration`,
    // e.g. "exact:1e20".
    Duration::try_from_secs_f64(seconds).map_err(|_| bad())
}

/// Convert a UTC timestamp to [`SystemTime`] without relying on chrono's optional `SystemTime`
/// conversions (works for timestamps before the Unix epoch too).
fn to_system_time(dt: DateTime<Utc>) -> SystemTime {
    let secs = dt.timestamp();
    let nanos = dt.timestamp_subsec_nanos(); // always in [0, 1e9), even for pre-epoch times
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nanos)
    } else {
        UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs()) + Duration::from_nanos(nanos as u64)
    }
}

#[cfg(test)]
mod tests;

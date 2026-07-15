//! The read-bound option for read-only queries.
//!
//! By default every query reads at a **strong** bound (`TimestampBound::strong`) — it sees the
//! effects of every transaction that committed before the read started. Spanner also supports
//! **stale reads**, which pick an older read timestamp so the read can be served locally without a
//! cross-replica quorum: cheaper and lock-free, ideal for analytics. This module parses the single
//! driver option that requests a non-strong bound and maps it onto the client's [`TimestampBound`].
//!
//! [`OPTION_READ_STALENESS`](crate::OPTION_READ_STALENESS) (`spanner.read.staleness`) selects the
//! bound. Its value is one of four prefixed forms — two *relative* (a duration) and two *absolute*
//! (an RFC 3339 timestamp):
//!
//! - `exact:<duration>` → [`TimestampBound::exact_staleness`]: read exactly `<duration>` in the
//!   past (a single, repeatable timestamp).
//! - `max:<duration>` → [`TimestampBound::max_staleness`]: read at any timestamp within
//!   `<duration>` of now (bounded staleness; the server picks, single-use reads only).
//! - `read:<rfc3339>` → [`TimestampBound::read_timestamp`]: read exactly as of that timestamp.
//! - `min:<rfc3339>` → [`TimestampBound::min_read_timestamp`]: read at that timestamp or later
//!   (bounded staleness; single-use reads only).
//!
//! `<duration>` is a non-negative number optionally suffixed with a unit — `s` (seconds, the
//! default), `ms`, `us`/`µs`, `ns`, `m` (minutes) or `h` (hours). Examples: `exact:10`, `exact:2.5s`,
//! `max:500ms`, `max:1m`, `read:2026-07-07T00:00:00Z`, `min:2026-07-07T00:00:00+02:00`.
//!
//! The four prefixes are distinct, so a single value is unambiguous; like every option value in
//! this driver they are lowercase and matched exactly. Malformed values are rejected
//! with `InvalidArgument`. Set the option to an empty string to unset it (which is also how a
//! statement clears a bound inherited from its connection).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use chrono::{DateTime, Utc};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::transaction::{SingleUseReadOnlyTransaction, TimestampBound};

use crate::error::invalid_argument;
use crate::options::string_option;

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

/// A parsed read bound, before it is turned into a client [`TimestampBound`]. Kept as a small,
/// pure value so the option parsing can be unit-tested offline.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ReadBound {
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
    pub(crate) fn pinned_for_multi_use(&self) -> ReadBound {
        match self {
            ReadBound::MaxStaleness(d) => ReadBound::ExactStaleness(*d),
            ReadBound::MinReadTimestamp(t) => ReadBound::ReadTimestamp(*t),
            other => other.clone(),
        }
    }

    /// Build the client [`TimestampBound`] for this read bound.
    pub(crate) fn to_timestamp_bound(&self) -> Result<TimestampBound> {
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

/// The read-bound configuration held by a connection or statement.
///
/// Stores the raw option string (so `get_option` round-trips exactly what was set) alongside the
/// parsed bound. `bound` mirrors the raw string: both are `Some` together or `None` together.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadStaleness {
    /// Raw `spanner.read.staleness` value, when set.
    staleness: Option<String>,
    /// The parsed bound (`None` means a strong read).
    bound: Option<ReadBound>,
}

impl ReadStaleness {
    /// Handle a `set_option` for `spanner.read.staleness`. An empty value unsets it (a strong
    /// read); any non-empty value replaces the current bound.
    pub(crate) fn set_staleness(&mut self, value: OptionValue) -> Result<()> {
        let raw = string_option(value, crate::OPTION_READ_STALENESS)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            self.staleness = None;
            self.bound = None;
            return Ok(());
        }
        let bound = parse_read_bound(trimmed)?;
        self.staleness = Some(trimmed.to_string());
        self.bound = Some(bound);
        Ok(())
    }

    /// The raw `spanner.read.staleness` value, for `get_option` round-trip.
    pub(crate) fn staleness_string(&self) -> Option<&str> {
        self.staleness.as_deref()
    }

    /// The client [`TimestampBound`] to apply, or `None` for a strong read.
    pub(crate) fn timestamp_bound(&self) -> Result<Option<TimestampBound>> {
        self.bound
            .as_ref()
            .map(ReadBound::to_timestamp_bound)
            .transpose()
    }

    /// The client [`TimestampBound`] to apply to a **multi-use** read-only transaction, or `None`
    /// for a strong read. The single-use-only bounded-staleness kinds are pinned to a legal
    /// equivalent first — see [`ReadBound::pinned_for_multi_use`].
    pub(crate) fn multi_use_timestamp_bound(&self) -> Result<Option<TimestampBound>> {
        self.bound
            .as_ref()
            .map(|b| b.pinned_for_multi_use().to_timestamp_bound())
            .transpose()
    }
}

/// Error describing the accepted `spanner.read.staleness` grammar.
const GRAMMAR_MSG: &str = "spanner.read.staleness must be one of \"exact:<duration>\", \
     \"max:<duration>\", \"read:<rfc3339>\" or \"min:<rfc3339>\" (e.g. \"exact:10s\", \
     \"max:500ms\", \"read:2026-07-07T00:00:00Z\", \"min:2026-07-07T00:00:00+02:00\")";

/// Parse a `spanner.read.staleness` value into a [`ReadBound`]. Accepts the four prefixed forms —
/// the *relative* `exact:<duration>` / `max:<duration>` and the *absolute* `read:<rfc3339>` /
/// `min:<rfc3339>` — plus a bare `<rfc3339>` (equivalent to `read:`). The four prefixes are
/// distinct, so the value is unambiguous. They are matched exactly (lowercase): ADBC option values
/// are exact-match canonical strings across the driver ecosystem, so an uppercase prefix is
/// rejected with the grammar error rather than case-folded.
pub(crate) fn parse_read_bound(value: &str) -> Result<ReadBound> {
    // RFC 3339 timestamps themselves contain colons, but only *after* the date part, so splitting
    // at the first colon cleanly separates a kind prefix from its argument: `read:<rfc3339>` keeps
    // the timestamp's own colons in `arg`, while a bare timestamp's pseudo-kind (`2026-07-07T00`)
    // matches no arm and falls through.
    if let Some((kind, arg)) = value.split_once(':') {
        match kind.trim() {
            "exact" => return Ok(ReadBound::ExactStaleness(parse_duration(arg.trim())?)),
            "max" => return Ok(ReadBound::MaxStaleness(parse_duration(arg.trim())?)),
            "read" => return Ok(ReadBound::ReadTimestamp(parse_rfc3339(arg.trim())?)),
            "min" => return Ok(ReadBound::MinReadTimestamp(parse_rfc3339(arg.trim())?)),
            // Not a known kind — fall through and try a bare RFC 3339 timestamp (which also
            // contains colons), else report the grammar error.
            _ => {}
        }
    }
    parse_rfc3339(value).map(ReadBound::ReadTimestamp)
}

/// Parse an RFC 3339 timestamp into a UTC [`DateTime`].
fn parse_rfc3339(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|e| invalid_argument(format!("{GRAMMAR_MSG}: {e}")))?
        .with_timezone(&Utc))
}

/// Parse a non-negative duration with an optional unit suffix (`s` default, `ms`, `us`/`µs`, `ns`,
/// `m`, `h`). Shared with the `spanner.commit.max_delay` option (see [`crate::request`]).
pub(crate) fn parse_duration(value: &str) -> Result<Duration> {
    let bad = || invalid_argument(format!("invalid staleness duration {value:?}"));
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
mod tests {
    use super::*;
    use adbc_core::error::Status;

    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn parses_exact_and_max_staleness_with_units() {
        assert_eq!(
            parse_read_bound("exact:10").unwrap(),
            ReadBound::ExactStaleness(Duration::from_secs(10))
        );
        assert_eq!(
            parse_read_bound("exact:2.5s").unwrap(),
            ReadBound::ExactStaleness(Duration::from_secs_f64(2.5))
        );
        assert_eq!(
            parse_read_bound("max:500ms").unwrap(),
            ReadBound::MaxStaleness(Duration::from_millis(500))
        );
        assert_eq!(
            parse_read_bound("max:1m").unwrap(),
            ReadBound::MaxStaleness(Duration::from_secs(60))
        );
        assert_eq!(
            parse_read_bound(" exact : 1h ").unwrap(),
            ReadBound::ExactStaleness(Duration::from_secs(3600))
        );
    }

    #[test]
    fn parses_read_and_min_timestamp() {
        // Bare RFC 3339 is accepted as an exact read timestamp (equivalent to `read:`).
        assert_eq!(
            parse_read_bound("2026-07-07T00:00:00Z").unwrap(),
            ReadBound::ReadTimestamp(dt("2026-07-07T00:00:00Z"))
        );
        assert_eq!(
            parse_read_bound("read:2026-07-07T00:00:00Z").unwrap(),
            ReadBound::ReadTimestamp(dt("2026-07-07T00:00:00Z"))
        );
        assert_eq!(
            parse_read_bound("min:2026-07-07T00:00:00+02:00").unwrap(),
            ReadBound::MinReadTimestamp(dt("2026-07-07T00:00:00+02:00"))
        );
        // The absolute prefixes tolerate whitespace around the prefix, like ` exact : 1h ` — all
        // four kinds share one grammar (COR-7).
        assert_eq!(
            parse_read_bound("read : 2026-07-07T00:00:00Z").unwrap(),
            ReadBound::ReadTimestamp(dt("2026-07-07T00:00:00Z"))
        );
    }

    /// Prefixes are exact lowercase, like every other option value in the driver (and the ADBC
    /// ecosystem, which exact-matches option values): any case variant is rejected with the
    /// grammar error, uniformly across all four kinds (COR-7).
    #[test]
    fn rejects_uppercase_and_mixed_case_prefixes() {
        for bad in [
            "EXACT:10s",
            "Exact:10s",
            "MAX:1m",
            "READ:2026-07-07T00:00:00Z",
            "Read:2026-07-07T00:00:00Z",
            "MIN:2026-07-07T00:00:00+02:00",
            "Min:2026-07-07T00:00:00+02:00",
        ] {
            let error = parse_read_bound(bad).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{bad}");
        }
    }

    /// All four prefixes plus the bare form dispatch to the right kind through the one entry point.
    #[test]
    fn rejects_bad_read_bound() {
        for bad in [
            "10s",        // no kind, not a timestamp
            "exact:",     // no duration
            "exact:abc",  // non-numeric duration
            "exact:-5",   // negative duration
            "soon:10s",   // unknown duration kind
            "exact:1x",   // unknown unit (parsed as number "1x" → error)
            "not-a-time", // not a timestamp
            "read:",      // empty timestamp
            "2026-07-07", // date only, not a full RFC 3339 timestamp
            "min:12345",  // not a timestamp
        ] {
            assert!(parse_read_bound(bad).is_err(), "expected error for {bad:?}");
        }
    }

    /// Durations too large for `std::time::Duration` (roughly above 1.8e19 seconds) must be
    /// rejected with `InvalidArguments`, not panic in `Duration::from_secs_f64`. The unit suffix
    /// multiplies before the conversion, so `1e19h` overflows even though `1e19` alone would not.
    #[test]
    fn rejects_oversized_duration_instead_of_panicking() {
        for bad in ["exact:1e20", "max:1e20", "exact:1e19h"] {
            let error = parse_read_bound(bad).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{bad}");
        }
    }

    /// A single option holds one bound at a time; setting a new value replaces the old, and an
    /// empty value clears it. All four kinds round-trip through the one `spanner.read.staleness` key.
    #[test]
    fn single_option_holds_one_bound_and_can_be_replaced() {
        let mut s = ReadStaleness::default();
        assert!(s.timestamp_bound().unwrap().is_none());

        s.set_staleness(OptionValue::String("exact:10s".into()))
            .unwrap();
        assert_eq!(s.staleness_string(), Some("exact:10s"));
        assert!(s.timestamp_bound().unwrap().is_some());

        // Setting a timestamp value on the same key replaces the staleness bound (no conflict).
        s.set_staleness(OptionValue::String("read:2026-07-07T00:00:00Z".into()))
            .unwrap();
        assert_eq!(s.staleness_string(), Some("read:2026-07-07T00:00:00Z"));
        assert!(s.timestamp_bound().unwrap().is_some());

        // An empty value clears the bound (a strong read again).
        s.set_staleness(OptionValue::String(String::new())).unwrap();
        assert_eq!(s.staleness_string(), None);
        assert!(s.timestamp_bound().unwrap().is_none());
    }

    /// Pinning for a multi-use read-only transaction: the exact kinds pass through unchanged,
    /// while the single-use-only bounded kinds are pinned to the most stale timestamp their window
    /// allows (`max:<d>` → exact staleness `<d>`, `min:<t>` → read timestamp `<t>`).
    #[test]
    fn multi_use_pins_bounded_staleness_kinds() {
        let d = Duration::from_secs(10);
        let t = dt("2026-07-07T00:00:00Z");
        assert_eq!(
            ReadBound::ExactStaleness(d).pinned_for_multi_use(),
            ReadBound::ExactStaleness(d)
        );
        assert_eq!(
            ReadBound::ReadTimestamp(t).pinned_for_multi_use(),
            ReadBound::ReadTimestamp(t)
        );
        assert_eq!(
            ReadBound::MaxStaleness(d).pinned_for_multi_use(),
            ReadBound::ExactStaleness(d)
        );
        assert_eq!(
            ReadBound::MinReadTimestamp(t).pinned_for_multi_use(),
            ReadBound::ReadTimestamp(t)
        );

        // Through ReadStaleness: a strong (unset) bound stays None, a bounded kind still builds a
        // client TimestampBound.
        let mut s = ReadStaleness::default();
        assert!(s.multi_use_timestamp_bound().unwrap().is_none());
        s.set_staleness(OptionValue::String("max:500ms".into()))
            .unwrap();
        assert!(s.multi_use_timestamp_bound().unwrap().is_some());
    }

    #[test]
    fn to_system_time_round_trips_via_bound() {
        // Ensure the client accepts our SystemTime conversion for realistic timestamps.
        let bound = ReadBound::ReadTimestamp(dt("2026-07-07T12:34:56.789Z"));
        assert!(bound.to_timestamp_bound().is_ok());
        let bound = ReadBound::MinReadTimestamp(dt("1999-12-31T23:59:59Z"));
        assert!(bound.to_timestamp_bound().is_ok());
    }
}

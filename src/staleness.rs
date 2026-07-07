//! Read staleness / timestamp-bound options for read-only queries.
//!
//! By default every query reads at a **strong** bound (`TimestampBound::strong`) — it sees the
//! effects of every transaction that committed before the read started. Spanner also supports
//! **stale reads**, which pick an older read timestamp so the read can be served locally without a
//! cross-replica quorum: cheaper and lock-free, ideal for analytics. This module parses the two
//! driver options that request a non-strong bound and maps them onto the client's
//! [`TimestampBound`].
//!
//! The two options are **mutually exclusive** — only one read bound can apply to a query:
//!
//! - [`OPTION_READ_STALENESS`](crate::OPTION_READ_STALENESS) (`spanner.read.staleness`) — a
//!   *relative* bound, `"<kind>:<duration>"`:
//!   - `exact:<duration>` → [`TimestampBound::exact_staleness`]: read exactly `<duration>` in the
//!     past (a single, repeatable timestamp).
//!   - `max:<duration>` → [`TimestampBound::max_staleness`]: read at any timestamp within
//!     `<duration>` of now (bounded staleness; the server picks, single-use reads only).
//!
//!   `<duration>` is a non-negative number optionally suffixed with a unit — `s` (seconds,
//!   the default), `ms`, `us`/`µs`, `ns`, `m` (minutes) or `h` (hours). Examples: `exact:10`,
//!   `exact:2.5s`, `max:500ms`, `max:1m`.
//!
//! - [`OPTION_READ_TIMESTAMP`](crate::OPTION_READ_TIMESTAMP) (`spanner.read.timestamp`) — an
//!   *absolute* bound, an RFC 3339 timestamp optionally prefixed to select the mode:
//!   - `read:<rfc3339>` (or bare `<rfc3339>`) → [`TimestampBound::read_timestamp`]: read exactly as
//!     of that timestamp.
//!   - `min:<rfc3339>` → [`TimestampBound::min_read_timestamp`]: read at that timestamp or later
//!     (bounded staleness; single-use reads only).
//!
//!   Examples: `2026-07-07T00:00:00Z`, `read:2026-07-07T00:00:00Z`, `min:2026-07-07T00:00:00+02:00`.
//!
//! Malformed values are rejected with `InvalidArgument`. Because the two options are mutually
//! exclusive, setting one while the other is already set is rejected as a conflict; set the other to
//! an empty string first to unset it (which is also how a statement clears a bound inherited from
//! its connection).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use chrono::{DateTime, Utc};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::transaction::{SingleUseReadOnlyTransaction, TimestampBound};

use crate::error::invalid_argument;

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

/// Message used when both read-bound options would be set at once.
const CONFLICT_MSG: &str = "spanner.read.staleness and spanner.read.timestamp are mutually \
     exclusive (only one read bound can apply); unset the other with an empty value first";

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

/// The read staleness/timestamp configuration held by a connection or statement.
///
/// Stores the raw option strings (so `get_option` round-trips exactly what was set) alongside the
/// parsed bound. The two options are mutually exclusive, so at most one of `staleness`/`timestamp`
/// is ever `Some`, and `bound` mirrors whichever it is.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadStaleness {
    /// Raw `spanner.read.staleness` value, when set.
    staleness: Option<String>,
    /// Raw `spanner.read.timestamp` value, when set.
    timestamp: Option<String>,
    /// The parsed bound (`None` means a strong read).
    bound: Option<ReadBound>,
}

impl ReadStaleness {
    /// Handle a `set_option` for `spanner.read.staleness`. An empty value unsets it; a non-empty
    /// value is rejected if `spanner.read.timestamp` is already set (see [`CONFLICT_MSG`]).
    pub(crate) fn set_staleness(&mut self, value: OptionValue) -> Result<()> {
        let raw = as_string(value)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            self.staleness = None;
            self.bound = None;
            return Ok(());
        }
        if self.timestamp.is_some() {
            return Err(invalid_argument(CONFLICT_MSG));
        }
        let bound = parse_staleness(trimmed)?;
        self.staleness = Some(trimmed.to_string());
        self.bound = Some(bound);
        Ok(())
    }

    /// Handle a `set_option` for `spanner.read.timestamp`. An empty value unsets it; a non-empty
    /// value is rejected if `spanner.read.staleness` is already set (see [`CONFLICT_MSG`]).
    pub(crate) fn set_timestamp(&mut self, value: OptionValue) -> Result<()> {
        let raw = as_string(value)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            self.timestamp = None;
            self.bound = None;
            return Ok(());
        }
        if self.staleness.is_some() {
            return Err(invalid_argument(CONFLICT_MSG));
        }
        let bound = parse_timestamp(trimmed)?;
        self.timestamp = Some(trimmed.to_string());
        self.bound = Some(bound);
        Ok(())
    }

    /// The raw `spanner.read.staleness` value, for `get_option` round-trip.
    pub(crate) fn staleness_string(&self) -> Option<&str> {
        self.staleness.as_deref()
    }

    /// The raw `spanner.read.timestamp` value, for `get_option` round-trip.
    pub(crate) fn timestamp_string(&self) -> Option<&str> {
        self.timestamp.as_deref()
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

/// Extract a string from an option value, erroring on any other value kind.
fn as_string(value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(invalid_argument(
            "read staleness/timestamp options require a string value",
        )),
    }
}

/// Parse a `spanner.read.staleness` value: `"exact:<duration>"` or `"max:<duration>"`.
pub(crate) fn parse_staleness(value: &str) -> Result<ReadBound> {
    let (kind, arg) = value.split_once(':').ok_or_else(|| {
        invalid_argument(
            "spanner.read.staleness must be \"exact:<duration>\" or \"max:<duration>\" \
             (e.g. \"exact:10s\", \"max:500ms\")",
        )
    })?;
    let duration = parse_duration(arg.trim())?;
    match kind.trim().to_ascii_lowercase().as_str() {
        "exact" => Ok(ReadBound::ExactStaleness(duration)),
        "max" => Ok(ReadBound::MaxStaleness(duration)),
        other => Err(invalid_argument(format!(
            "unknown staleness kind {other:?}; expected \"exact\" or \"max\""
        ))),
    }
}

/// Parse a `spanner.read.timestamp` value: an RFC 3339 timestamp optionally prefixed `read:`
/// (exact, the default) or `min:` (minimum / bounded staleness).
pub(crate) fn parse_timestamp(value: &str) -> Result<ReadBound> {
    // RFC 3339 timestamps themselves contain colons, so only a leading `read:`/`min:` prefix is
    // treated specially — never a plain `split_once(':')`.
    let (min, rest) = if let Some(r) = value.strip_prefix("min:") {
        (true, r.trim())
    } else if let Some(r) = value.strip_prefix("read:") {
        (false, r.trim())
    } else {
        (false, value)
    };
    let dt = DateTime::parse_from_rfc3339(rest)
        .map_err(|e| {
            invalid_argument(format!(
                "spanner.read.timestamp must be an RFC 3339 timestamp, optionally prefixed \
                 \"read:\" or \"min:\": {e}"
            ))
        })?
        .with_timezone(&Utc);
    Ok(if min {
        ReadBound::MinReadTimestamp(dt)
    } else {
        ReadBound::ReadTimestamp(dt)
    })
}

/// Parse a non-negative duration with an optional unit suffix (`s` default, `ms`, `us`/`µs`, `ns`,
/// `m`, `h`).
fn parse_duration(value: &str) -> Result<Duration> {
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
    Ok(Duration::from_secs_f64(seconds))
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

    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn parses_exact_and_max_staleness_with_units() {
        assert_eq!(
            parse_staleness("exact:10").unwrap(),
            ReadBound::ExactStaleness(Duration::from_secs(10))
        );
        assert_eq!(
            parse_staleness("exact:2.5s").unwrap(),
            ReadBound::ExactStaleness(Duration::from_secs_f64(2.5))
        );
        assert_eq!(
            parse_staleness("max:500ms").unwrap(),
            ReadBound::MaxStaleness(Duration::from_millis(500))
        );
        assert_eq!(
            parse_staleness("MAX:1m").unwrap(),
            ReadBound::MaxStaleness(Duration::from_secs(60))
        );
        assert_eq!(
            parse_staleness(" exact : 1h ").unwrap(),
            ReadBound::ExactStaleness(Duration::from_secs(3600))
        );
    }

    #[test]
    fn rejects_bad_staleness() {
        for bad in [
            "10s",       // no kind
            "exact:",    // no duration
            "exact:abc", // non-numeric
            "exact:-5",  // negative
            "soon:10s",  // unknown kind
            "exact:1x",  // unknown unit (parsed as number "1x" → error)
        ] {
            assert!(parse_staleness(bad).is_err(), "expected error for {bad:?}");
        }
    }

    #[test]
    fn parses_read_and_min_timestamp() {
        assert_eq!(
            parse_timestamp("2026-07-07T00:00:00Z").unwrap(),
            ReadBound::ReadTimestamp(dt("2026-07-07T00:00:00Z"))
        );
        assert_eq!(
            parse_timestamp("read:2026-07-07T00:00:00Z").unwrap(),
            ReadBound::ReadTimestamp(dt("2026-07-07T00:00:00Z"))
        );
        assert_eq!(
            parse_timestamp("min:2026-07-07T00:00:00+02:00").unwrap(),
            ReadBound::MinReadTimestamp(dt("2026-07-07T00:00:00+02:00"))
        );
    }

    #[test]
    fn rejects_bad_timestamp() {
        for bad in ["not-a-timestamp", "read:", "2026-07-07", "min:12345"] {
            assert!(parse_timestamp(bad).is_err(), "expected error for {bad:?}");
        }
    }

    #[test]
    fn mutually_exclusive_options_conflict_and_can_be_switched() {
        let mut s = ReadStaleness::default();
        assert!(s.timestamp_bound().unwrap().is_none());

        s.set_staleness(OptionValue::String("exact:10s".into()))
            .unwrap();
        assert_eq!(s.staleness_string(), Some("exact:10s"));
        assert!(s.timestamp_bound().unwrap().is_some());

        // Setting the other bound while one is active is rejected.
        let err = s
            .set_timestamp(OptionValue::String("2026-07-07T00:00:00Z".into()))
            .unwrap_err();
        assert_eq!(err.status, adbc_core::error::Status::InvalidArguments);

        // Unset the staleness, then the timestamp is accepted.
        s.set_staleness(OptionValue::String(String::new())).unwrap();
        assert_eq!(s.staleness_string(), None);
        assert!(s.timestamp_bound().unwrap().is_none());
        s.set_timestamp(OptionValue::String("2026-07-07T00:00:00Z".into()))
            .unwrap();
        assert_eq!(s.timestamp_string(), Some("2026-07-07T00:00:00Z"));
        assert!(s.timestamp_bound().unwrap().is_some());
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

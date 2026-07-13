#![no_main]

use adbc_spanner::fuzzing::{parse_date_days, parse_numeric_i128, parse_timestamp_nanos};
use chrono::{DateTime, Datelike, Duration, NaiveDate};
use libfuzzer_sys::fuzz_target;

// Fuzz the Spanner value-string parsers (DATE / TIMESTAMP / NUMERIC). They decode wire-format
// strings and must never panic on malformed input (only return None).
//
// Beyond "never panics", each success is checked with a round-trip oracle: render the parsed value
// back to its canonical string and re-parse it; the second parse must yield the identical value.
// This catches truncation, overflow, and sign bugs that a panic-only target would miss.
//
// The date/timestamp round-trips are restricted to 4-digit years (1..=9999) — the RFC 3339 domain,
// which is also Spanner's actual DATE/TIMESTAMP range. Outside it, chrono's own format→parse is not
// invertible (e.g. a `+05:00` year-0000 instant renders as a pre-year-0 UTC string it cannot
// re-parse), which would be an artifact of the oracle, not a bug in the parser under test.
fuzz_target!(|s: String| {
    if let Some(days) = parse_date_days(&s) {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        // `days` came from a real date, so it is back in range.
        let date = epoch + Duration::days(days as i64);
        if (1..=9999).contains(&date.year()) {
            let canonical = date.format("%Y-%m-%d").to_string();
            assert_eq!(
                parse_date_days(&canonical),
                Some(days),
                "date round-trip: {s:?}"
            );
        }
    }

    if let Some(nanos) = parse_timestamp_nanos(&s) {
        // `nanos` came from a parsed timestamp, so it is representable (and thus in range).
        let dt: DateTime<chrono::Utc> = DateTime::from_timestamp_nanos(nanos);
        if (1..=9999).contains(&dt.year()) {
            // Render with a `Z` suffix (not the `+00:00` offset `to_rfc3339` emits): the parser
            // only accepts Spanner's actual wire form. Per the Spanner `TypeCode::TIMESTAMP`
            // contract the time zone "must be present, and must be `\"Z\"`", so a numeric offset
            // never appears on the wire and re-parsing a `+00:00` canonical would be an oracle
            // artifact (like the year-range restriction above), not a parser bug.
            let canonical = dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
            assert_eq!(
                parse_timestamp_nanos(&canonical),
                Some(nanos),
                "timestamp round-trip: {s:?}"
            );
        }
    }

    if let Some(unscaled) = parse_numeric_i128(&s) {
        // Reconstruct the canonical scale-9 decimal string and re-parse it.
        let neg = unscaled < 0;
        let mag = unscaled.unsigned_abs();
        let int = mag / 1_000_000_000;
        let frac = mag % 1_000_000_000;
        let canonical = format!("{}{int}.{frac:09}", if neg { "-" } else { "" });
        assert_eq!(
            parse_numeric_i128(&canonical),
            Some(unscaled),
            "numeric round-trip: {s:?}"
        );
    }
});

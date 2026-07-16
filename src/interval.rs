//! Conversion between Arrow `Interval(MonthDayNano)` and the ISO 8601 duration string Spanner uses
//! on the wire for its `INTERVAL` type.
//!
//! Spanner `INTERVAL` and Arrow `Interval(MonthDayNano)` share the **same** three-component model —
//! months, days, and a sub-day nanosecond count — and neither normalizes across the boundaries
//! (5 months is not 150 days, 1 day is not 24 hours), so the two round-trip exactly. Spanner
//! encodes an `INTERVAL` value as an ISO 8601 duration string (e.g. `P1Y2M3DT4H5M6.5S`), so the
//! write path ([`format_month_day_nano`]) formats the Arrow components into that string and the read
//! path ([`parse_month_day_nano`]) parses Spanner's canonical output back into them.
//!
//! Only `Interval(MonthDayNano)` is mapped: it is the one Arrow interval layout that carries all
//! three components, matching Spanner's model 1:1. `Interval(YearMonth)` / `Interval(DayTime)` are
//! not bound.
//!
//! **Emulator caveat.** The Cloud Spanner *emulator* does not support the `INTERVAL` column type
//! (a `CREATE TABLE` declaring one fails with a backend `GOOGLESQL_RET_CHECK`), so this mapping is
//! exercised offline by the codec unit tests below and end-to-end only against **real** Spanner —
//! the C++ `adbc_validation` `SqlIngestInterval` case stays excluded in
//! `scripts/run-adbc-validation.sh` for exactly this reason.

/// Format Arrow `Interval(MonthDayNano)` components as the ISO 8601 duration string Spanner accepts
/// for an `INTERVAL` value.
///
/// Each component keeps its own sign — Spanner does not normalize between months, days, and the time
/// portion, so they are emitted independently: `P<months>M<days>DT<seconds>S`. The seconds field
/// carries the whole nanosecond count (Spanner's time portion has nanosecond precision), formatted
/// with up to nine fractional digits and trailing zeros trimmed.
pub(crate) fn format_month_day_nano(months: i32, days: i32, nanos: i64) -> String {
    format!("P{months}M{days}DT{}S", format_seconds(nanos))
}

/// Format a nanosecond count as a signed decimal number of seconds (up to nine fractional digits,
/// trailing zeros trimmed). The sign is applied to the whole value, so `-42_000` ns (which has a
/// zero integer-seconds part) still renders as `-0.000042`, not `0.000042`.
fn format_seconds(nanos: i64) -> String {
    let sign = if nanos < 0 { "-" } else { "" };
    let magnitude = (nanos as i128).unsigned_abs();
    let secs = magnitude / 1_000_000_000;
    let frac = (magnitude % 1_000_000_000) as u64;
    if frac == 0 {
        format!("{sign}{secs}")
    } else {
        let frac = format!("{frac:09}");
        format!("{sign}{secs}.{}", frac.trim_end_matches('0'))
    }
}

/// Parse a Spanner `INTERVAL` ISO 8601 duration string into Arrow `Interval(MonthDayNano)`
/// components `(months, days, nanos)`.
///
/// Handles the full canonical grammar Spanner can emit — `P[<n>Y][<n>M][<n>W][<n>D][T[<n>H][<n>M]
/// [<n>S]]` — with each component optional and independently signed. Years fold into months (×12)
/// and weeks into days (×7), matching how Spanner reports them (it keeps a single months field that
/// subsumes years). Returns `None` on any malformed input or on month/day overflow of `i32`.
pub(crate) fn parse_month_day_nano(s: &str) -> Option<(i32, i32, i64)> {
    let body = s.strip_prefix('P')?;
    let (date_part, time_part) = match body.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (body, None),
    };

    let mut months: i64 = 0;
    let mut days: i64 = 0;
    for (num, unit) in components(date_part) {
        let n: i64 = num.parse().ok()?;
        match unit {
            'Y' => months = months.checked_add(n.checked_mul(12)?)?,
            'M' => months = months.checked_add(n)?,
            'W' => days = days.checked_add(n.checked_mul(7)?)?,
            'D' => days = days.checked_add(n)?,
            _ => return None,
        }
    }

    let mut nanos: i64 = 0;
    if let Some(time) = time_part {
        for (num, unit) in components(time) {
            let add = match unit {
                'H' => num.parse::<i64>().ok()?.checked_mul(3_600_000_000_000)?,
                'M' => num.parse::<i64>().ok()?.checked_mul(60_000_000_000)?,
                'S' => seconds_to_nanos(num)?,
                _ => return None,
            };
            nanos = nanos.checked_add(add)?;
        }
    }

    Some((
        i32::try_from(months).ok()?,
        i32::try_from(days).ok()?,
        nanos,
    ))
}

/// Split an ISO 8601 duration part (either the date or the time segment) into its `(number, unit)`
/// tokens, e.g. `-5M-5D` → `[("-5", 'M'), ("-5", 'D')]`. A number token is a run of sign / digit /
/// decimal-point characters terminated by its unit letter; a trailing run with no unit letter is
/// dropped (so a caller's `parse` of a malformed token surfaces as `None` upstream, never a panic).
fn components(part: &str) -> Vec<(&str, char)> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, c) in part.char_indices() {
        if c.is_ascii_alphabetic() {
            out.push((&part[start..i], c));
            start = i + c.len_utf8();
        }
    }
    out
}

/// Parse the seconds field of an ISO 8601 duration (a signed decimal, e.g. `6.789` or `-0.000042`)
/// into a nanosecond count. Fractions beyond nine digits are truncated. Returns `None` on malformed
/// input or `i64` overflow.
fn seconds_to_nanos(s: &str) -> Option<i64> {
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    // An empty integer part is allowed only when there is a fraction (".5"); "" alone is malformed.
    let secs: i64 = if int_part.is_empty() {
        if frac_part.is_empty() {
            return None;
        }
        0
    } else {
        int_part.parse().ok()?
    };
    if !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Pad/truncate the fraction to exactly nine digits (nanosecond resolution).
    let mut frac_digits = frac_part.to_string();
    frac_digits.truncate(9);
    while frac_digits.len() < 9 {
        frac_digits.push('0');
    }
    let frac: i64 = if frac_digits.is_empty() {
        0
    } else {
        frac_digits.parse().ok()?
    };
    let total = secs.checked_mul(1_000_000_000)?.checked_add(frac)?;
    Some(if neg { -total } else { total })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_and_parses_the_validation_suite_values() {
        // The exact (months, days, nanos) triples the adbc_validation SqlIngestInterval case ingests.
        for (m, d, n) in [(-5, -5, -42_000i64), (0, 0, 0), (5, 5, 42_000)] {
            let s = format_month_day_nano(m, d, n);
            assert_eq!(
                parse_month_day_nano(&s),
                Some((m, d, n)),
                "round-trip failed for {m},{d},{n} via {s:?}"
            );
        }
    }

    #[test]
    fn formats_negative_subsecond_with_sign() {
        // -42_000 ns has a zero integer-seconds part; the sign must survive.
        assert_eq!(format_month_day_nano(-5, -5, -42_000), "P-5M-5DT-0.000042S");
        assert_eq!(format_month_day_nano(0, 0, 0), "P0M0DT0S");
        assert_eq!(format_month_day_nano(5, 5, 42_000), "P5M5DT0.000042S");
    }

    #[test]
    fn parses_spanner_canonical_forms() {
        // Years fold into months (×12), weeks into days (×7); components are independently signed.
        assert_eq!(parse_month_day_nano("P1Y2M3D"), Some((14, 3, 0)));
        assert_eq!(parse_month_day_nano("P1W"), Some((0, 7, 0)));
        assert_eq!(
            parse_month_day_nano("P1Y2M3DT4H5M6.789S"),
            Some((
                14,
                3,
                4 * 3_600_000_000_000 + 5 * 60_000_000_000 + 6_789_000_000
            )),
        );
        // A pure time interval with a leading-zero integer part and a fraction.
        assert_eq!(parse_month_day_nano("PT0.5S"), Some((0, 0, 500_000_000)));
        assert_eq!(parse_month_day_nano("PT-0.000042S"), Some((0, 0, -42_000)));
    }

    #[test]
    fn rejects_malformed_input() {
        assert_eq!(parse_month_day_nano("garbage"), None); // no leading P
        assert_eq!(parse_month_day_nano("P5X"), None); // unknown unit
        assert_eq!(parse_month_day_nano("PT.S"), None); // empty seconds
    }
}

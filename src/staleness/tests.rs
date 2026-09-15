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
    // four kinds share one grammar.
    assert_eq!(
        parse_read_bound("read : 2026-07-07T00:00:00Z").unwrap(),
        ReadBound::ReadTimestamp(dt("2026-07-07T00:00:00Z"))
    );
}

/// Prefixes are exact lowercase, like every other option value in the driver (and the ADBC
/// ecosystem, which exact-matches option values): any case variant is rejected with the
/// grammar error, uniformly across all four kinds.
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

/// A value with no recognised prefix must echo itself and enumerate the four forms — the mistake
/// is the prefix, so a chrono complaint about text that was never a timestamp only misleads.
#[test]
fn grammar_rejection_echoes_the_value_and_omits_the_timestamp_parser_detail() {
    let error = parse_read_bound("stale:10s").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "option spanner.read.staleness: \"stale:10s\" is not a valid read bound; expected \
         \"exact:<duration>\" (e.g. \"exact:10s\"), \"max:<duration>\" (e.g. \"max:500ms\"), \
         \"read:<rfc3339>\" (e.g. \"read:2026-07-07T00:00:00Z\") or \"min:<rfc3339>\" \
         (e.g. \"min:2026-07-07T00:00:00+02:00\")"
    );
    assert!(
        !error.message.contains("invalid characters"),
        "{}",
        error.message
    );
}

/// With an explicit `read:`/`min:` prefix the caller did mean a timestamp, so the parser's own
/// diagnosis is the useful part and is forwarded alongside the grammar.
#[test]
fn timestamp_rejection_keeps_the_parser_detail() {
    for bad in ["read:nope", "min:12345"] {
        let error = parse_read_bound(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
        assert!(
            error.message.starts_with(&format!(
                "option spanner.read.staleness: {bad:?} does not carry a valid RFC 3339 timestamp ("
            )),
            "{}",
            error.message
        );
        assert!(
            error.message.contains("expected \"exact:<duration>\""),
            "{}",
            error.message
        );
    }
}

/// The duration grammar is shared with `spanner.commit.max_delay`, so the rejection names the key
/// the caller actually set and lists the accepted unit suffixes.
#[test]
fn duration_rejection_names_its_option_and_the_units() {
    let error = parse_duration_for("1x", crate::OPTION_MAX_COMMIT_DELAY).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "option spanner.commit.max_delay: \"1x\" is not a valid duration; expected a number with \
         an optional unit suffix (s [default], ms, us/µs, ns, m, h), e.g. \"500ms\""
    );
    // The staleness wrapper names the staleness key instead.
    assert!(
        parse_duration("1x")
            .unwrap_err()
            .message
            .starts_with("option spanner.read.staleness: \"1x\" is not a valid duration;")
    );
}

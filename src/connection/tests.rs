//! Offline tests for the connection's own surfaces: isolation-level parsing/rendering, the
//! fixed "current" catalog/schema, and the opaque partition-descriptor envelope.

use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use google_cloud_spanner::model::transaction_options::IsolationLevel;

use super::{
    PARTITION_DESCRIPTOR_VERSION, check_fixed_catalog_or_schema, decode_partition,
    encode_partition, isolation_to_adbc_string, parse_isolation_level,
};

#[test]
fn parses_supported_isolation_levels() {
    use adbc_core::constants::*;
    let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE).unwrap(),
        IsolationLevel::Serializable
    );
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ).unwrap(),
        IsolationLevel::RepeatableRead
    );
    // Spanner implements REPEATABLE_READ as snapshot isolation (its proto definition matches
    // ADBC's `snapshot` almost verbatim), so `snapshot` is a native mapping, not a promotion.
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT).unwrap(),
        IsolationLevel::RepeatableRead
    );
    // `default` maps to the client's unspecified level: no level is sent, and Spanner reads
    // that as SERIALIZABLE.
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_DEFAULT).unwrap(),
        IsolationLevel::Unspecified
    );
}

#[test]
fn promotes_unsupported_isolation_levels() {
    use adbc_core::constants::*;
    let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
    // Spec levels Spanner does not natively expose are promoted upward to the weakest
    // supported level that still satisfies their guarantees (never rejected). `snapshot` is
    // not among them — it maps natively (see `parses_supported_isolation_levels`).
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED).unwrap(),
        IsolationLevel::RepeatableRead
    );
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED).unwrap(),
        IsolationLevel::RepeatableRead
    );
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE).unwrap(),
        IsolationLevel::Serializable
    );
    // A completely unknown value is still an invalid argument.
    assert_eq!(
        parse("not-a-level").unwrap_err().status,
        Status::InvalidArguments
    );
    // A non-string option value is rejected.
    assert_eq!(
        parse_isolation_level(OptionValue::Int(1))
            .unwrap_err()
            .status,
        Status::InvalidArguments
    );
}

#[test]
fn isolation_level_round_trips_to_adbc_string() {
    use adbc_core::constants::*;
    assert_eq!(
        isolation_to_adbc_string(&IsolationLevel::Serializable),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
    );
    assert_eq!(
        isolation_to_adbc_string(&IsolationLevel::RepeatableRead),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        isolation_to_adbc_string(&IsolationLevel::Unspecified),
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT
    );
}

#[test]
fn promoted_isolation_level_round_trips_to_effective_level() {
    use adbc_core::constants::*;
    // `get_option` reports the effective level that will actually run, not the input that was
    // set: parse then render must land on a level Spanner exposes.
    let effective = |s: &str| {
        let level = parse_isolation_level(OptionValue::String(s.to_string())).expect("parses");
        isolation_to_adbc_string(&level)
    };
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    // `snapshot` reports as `repeatable_read` — the native level it maps onto, which is what
    // Spanner will run.
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
    );
}

#[test]
fn setting_current_catalog_or_schema_accepts_only_the_reported_value() {
    let set = |s: &str| {
        check_fixed_catalog_or_schema(
            OptionValue::String(s.to_string()),
            "current catalog",
            "adbc-test",
        )
    };
    // The current catalog is the connection's database, and the current schema the unnamed one:
    // setting either to the value `get_option` reports is a no-op success.
    assert!(set("adbc-test").is_ok());
    assert!(
        check_fixed_catalog_or_schema(OptionValue::String(String::new()), "current schema", "")
            .is_ok()
    );
    // Neither is switchable, so any other value is unsupported → NotImplemented (aligned with the
    // C++ PostgreSQL driver's `set` on this class). The one legal value is named in the error.
    let err = set("foo").unwrap_err();
    assert_eq!(err.status, Status::NotImplemented);
    assert!(err.message.contains("\"foo\""), "{}", err.message);
    assert!(err.message.contains("\"adbc-test\""), "{}", err.message);
    // A non-string option value is a malformed argument, rejected as InvalidArguments.
    assert_eq!(
        check_fixed_catalog_or_schema(OptionValue::Int(1), "current schema", "")
            .unwrap_err()
            .status,
        Status::InvalidArguments
    );
}

/// A garbage partition descriptor — `read_partition`'s input is caller-supplied opaque bytes —
/// must be rejected as `InvalidArguments` by the decode step (before anything executes), never
/// panic. Covers empty input, non-JSON bytes, truncated JSON, and well-formed JSON that is not
/// a partition descriptor.
#[test]
fn garbage_partition_descriptors_error_cleanly() {
    let cases: [&[u8]; 6] = [
        b"",                      // empty
        b"\xff\xfe\x00 not json", // non-UTF-8, non-JSON bytes
        b"{",                     // truncated JSON
        b"{}",                    // valid JSON object missing every descriptor field
        br#"{"hello": "world"}"#, // valid JSON object of the wrong shape
        b"[1, 2, 3]",             // valid JSON that is not even an object
    ];
    for descriptor in cases {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(
            error.status,
            Status::InvalidArguments,
            "descriptor {descriptor:?}"
        );
        assert!(
            error.message.contains("invalid partition descriptor"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
}

/// `encode_partition` writes the versioned envelope, and decode → encode is a byte-for-byte
/// fixed point.
#[test]
fn partition_descriptor_envelope_round_trips() {
    let descriptor: &[u8] = br#"{"v":1,"partition":{"inner":{"Query":{"sql":"SELECT 1"}}}}"#;
    let partition = decode_partition(descriptor).expect("enveloped descriptor decodes");

    let encoded = encode_partition(&partition).expect("encode");
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(value["v"], PARTITION_DESCRIPTOR_VERSION);
    assert!(
        value.get("partition").is_some(),
        "envelope carries the partition payload: {value}"
    );

    // The enveloped form is canonical: decode → encode reproduces it exactly.
    let again = decode_partition(&encoded).expect("enveloped descriptor decodes");
    assert_eq!(encode_partition(&again).expect("re-encode"), encoded);
}

/// A pre-envelope bare descriptor (no `"v"` key) is now rejected — the driver has never had
/// users, so there are no legacy descriptors to accept, and a descriptor carries a live
/// session/transaction identity that could not outlive a driver upgrade anyway.
#[test]
fn bare_partition_descriptor_is_rejected() {
    let bare: &[u8] = br#"{"inner":{"Query":{"sql":"SELECT 1"}}}"#;
    let error = decode_partition(bare).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("missing \"v\" version field"),
        "unexpected message: {}",
        error.message
    );
}

/// An envelope with an unknown version must be rejected up front with a clean
/// `InvalidArguments` naming the version — not fail on its (unknown-format) payload shape.
#[test]
fn unsupported_partition_descriptor_version_errors_cleanly() {
    for descriptor in [
        br#"{"v":2,"partition":{"future":"format"}}"#.as_slice(),
        br#"{"v":0}"#.as_slice(),
    ] {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("partition descriptor version")
                && error.message.contains("not supported by this driver"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
    // The version 2 rejection names the version.
    let error = decode_partition(br#"{"v":2,"partition":{}}"#).unwrap_err();
    assert!(
        error
            .message
            .contains("partition descriptor version 2 not supported by this driver"),
        "{}",
        error.message
    );
}

/// Malformed envelopes — non-integer version, or a supported version with a missing/wrong
/// payload — are `InvalidArguments`, never a panic.
#[test]
fn malformed_partition_descriptor_envelopes_error_cleanly() {
    let cases: [&[u8]; 4] = [
        br#"{"v":"one"}"#,            // non-integer version
        br#"{"v":-1}"#,               // negative version
        br#"{"v":1}"#,                // missing "partition" payload
        br#"{"v":1,"partition":{}}"#, // payload of the wrong shape
    ];
    for descriptor in cases {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(
            error.status,
            Status::InvalidArguments,
            "descriptor {descriptor:?}"
        );
        assert!(
            error.message.contains("invalid partition descriptor"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
}

#[test]
fn partition_descriptor_round_trips_large_floats() {
    // Regression for a nightly fuzz find (spanner-adbc#188): a descriptor whose payload
    // carries an integer literal too large for i64/u64 is parsed to f64, so re-encoding emits
    // a ryu float. serde_json's default float parser is fast-but-imprecise (up to one ULP
    // off), so `parse(ryu(x)) != x` and each decode → encode pass drifted to an adjacent ULP —
    // the fixed point `read_partition` relies on never settled. The `float_roundtrip` feature
    // makes the parser exact, so a single re-encode is already the fixed point.
    //
    // The unknown keys land in the generated request's `_unknown_fields` as `serde_json::Value`
    // numbers, exercising exactly that float path.
    let descriptor = br#"{"v": 1, "partition": {"inner": {"Query": {"con[": 44444424444444444249, "/+%n":4444440000000000000000074074764, "/s%n": "prns/s"}}}}"#;

    let partition = decode_partition(descriptor).expect("descriptor decodes");
    let first = encode_partition(&partition).expect("a decoded partition re-encodes");
    // `encode_partition`'s own output must be a byte-stable fixed point under decode → encode.
    let normalized = decode_partition(&first).expect("re-encoded descriptor decodes");
    let again = encode_partition(&normalized).expect("a decoded partition re-encodes");
    assert_eq!(
        first, again,
        "decode → encode of an encoder's output must be byte-stable"
    );
}

use super::*;
use adbc_core::error::Status;

pub(super) fn new_database() -> SpannerDatabase {
    SpannerDatabase::new(new_runtime().unwrap())
}

#[test]
fn ensure_scheme_adds_http_prefix() {
    assert_eq!(ensure_scheme("localhost:9010"), "http://localhost:9010");
    assert_eq!(ensure_scheme("http://host:1"), "http://host:1");
    assert_eq!(ensure_scheme("https://host:1"), "https://host:1");
}

#[test]
fn database_options_round_trip() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        OptionValue::String("http://localhost:9010".into()),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_EMULATOR.into()),
        OptionValue::String("true".into()),
    )
    .unwrap();

    assert_eq!(
        db.get_option_string(OptionDatabase::Uri).unwrap(),
        "projects/p/instances/i/databases/d"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_ENDPOINT.into()))
            .unwrap(),
        "http://localhost:9010"
    );
    assert!(db.emulator);
}

#[test]
fn debug_redacts_credential_fields() {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.keyfile = Some("/etc/secret/key.json".into());
    db.keyfile_json = Some(r#"{"private_key":"SUPER-SECRET-PRIVATE-KEY"}"#.into());
    db.access_token = Some("ya29.LIVE-BEARER-TOKEN".into());

    let rendered = format!("{db:?}");

    // The secret values never appear in cleartext.
    assert!(
        !rendered.contains("SUPER-SECRET-PRIVATE-KEY"),
        "keyfile_json leaked: {rendered}"
    );
    assert!(
        !rendered.contains("ya29.LIVE-BEARER-TOKEN"),
        "access_token leaked: {rendered}"
    );
    assert!(
        !rendered.contains("/etc/secret/key.json"),
        "keyfile leaked: {rendered}"
    );

    // Presence is shown via the redaction placeholder, not the value.
    assert!(
        rendered.contains(r#"keyfile: Some("<redacted>")"#),
        "keyfile presence not shown: {rendered}"
    );
    assert!(
        rendered.contains(r#"keyfile_json: Some("<redacted>")"#),
        "keyfile_json presence not shown: {rendered}"
    );
    assert!(
        rendered.contains(r#"access_token: Some("<redacted>")"#),
        "access_token presence not shown: {rendered}"
    );

    // Non-secret fields render normally.
    assert!(
        rendered.contains("projects/p/instances/i/databases/d"),
        "database not shown: {rendered}"
    );
}

#[test]
fn debug_shows_none_for_absent_credentials() {
    let db = new_database();
    let rendered = format!("{db:?}");
    assert!(
        rendered.contains("keyfile: None"),
        "absent keyfile not shown: {rendered}"
    );
    assert!(
        rendered.contains("access_token: None"),
        "absent access_token not shown: {rendered}"
    );
}

#[test]
fn debug_renders_the_client_stack_cache_presence_only() {
    // The cached client stack must never delegate to the client types' own `Debug` (an
    // external surface that could render endpoint/credential internals): unbuilt it renders
    // as `connected: None`, and once built it renders as the fixed `<client stack>` marker
    // (asserted end-to-end in tests/mock_spanner.rs, where a real stack exists). Redaction
    // of the credential fields alongside it is covered by `debug_redacts_credential_fields`.
    let db = new_database();
    let rendered = format!("{db:?}");
    assert!(
        rendered.contains("connected: None"),
        "absent client stack not shown: {rendered}"
    );
    // The cache must not cost `SpannerDatabase` its `Send + Sync` (the ADBC database is
    // shared across threads by driver managers; the client handles are both).
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SpannerDatabase>();
}

#[test]
fn typed_option_getters_distinguish_unset_from_non_integer() {
    let mut db = new_database();

    // Genuinely unset: NotFound ("option not set"), same as get_option_string.
    let error = db
        .get_option_int(OptionDatabase::Other(OPTION_ENDPOINT.into()))
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert!(error.message.contains("is not set"), "{}", error.message);

    // An integer-valued option is served by get_option_int (and as a double).
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()),
        OptionValue::String("900".into()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_int(OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()))
            .unwrap(),
        900
    );
    assert_eq!(
        db.get_option_double(OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()))
            .unwrap(),
        900.0
    );

    // Set, but the value is not an integer: InvalidArguments, NOT NotFound (which must mean
    // "option unset/unknown").
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
    )
    .unwrap();
    let error = db.get_option_int(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("is not an integer"),
        "{}",
        error.message
    );
    let error = db.get_option_double(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn boolean_options_reject_int_typed_sets() {
    // COR-4: boolean options take the strings "true"/"false", never OptionValue::Int — an
    // int set would not round-trip through the getters (which serve the canonical string),
    // and no surveyed ADBC driver accepts SetOptionInt for a boolean option.
    let mut db = new_database();
    let emulator = || OptionDatabase::Other(OPTION_EMULATOR.into());

    for i in [0, 1] {
        let error = db.set_option(emulator(), OptionValue::Int(i)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "Int({i})");
        assert!(error.message.contains(OPTION_EMULATOR), "{}", error.message);
        assert!(
            error.message.contains("\"true\"/\"false\""),
            "{}",
            error.message
        );
    }

    // The string forms still work and read back canonically.
    db.set_option(emulator(), OptionValue::String("true".into()))
        .unwrap();
    assert_eq!(db.get_option_string(emulator()).unwrap(), "true");
    db.set_option(emulator(), OptionValue::String("false".into()))
        .unwrap();
    assert_eq!(db.get_option_string(emulator()).unwrap(), "false");
}

#[test]
fn connecting_without_a_database_path_is_an_error() {
    let db = new_database();
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
}

#[test]
fn a_non_string_uri_is_rejected() {
    let mut db = new_database();
    let error = db
        .set_option(OptionDatabase::Uri, OptionValue::Int(42))
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn keyfile_path_round_trips_but_inline_json_is_write_only() {
    // SEC-1: `spanner.auth.keyfile` is a filesystem path (not a secret) and reads back
    // verbatim; `spanner.auth.keyfile_json` holds a live private key, so `get_option` reports
    // `NotFound` — never the key material — whether or not it is set.
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_KEYFILE.into()),
        OptionValue::String("/path/to/key.json".into()),
    )
    .unwrap();
    let secret_json = r#"{"type":"service_account","private_key":"SUPER-SECRET-PRIVATE-KEY"}"#;
    db.set_option(
        OptionDatabase::Other(OPTION_KEYFILE_JSON.into()),
        OptionValue::String(secret_json.into()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_KEYFILE.into()))
            .unwrap(),
        "/path/to/key.json"
    );
    let error = db
        .get_option_string(OptionDatabase::Other(OPTION_KEYFILE_JSON.into()))
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert!(error.message.contains("write-only"), "{}", error.message);
    assert!(
        !error.message.contains("SUPER-SECRET-PRIVATE-KEY"),
        "keyfile_json leaked through get_option: {}",
        error.message
    );
    // The stored key is still in effect — only reading it back is refused.
    assert_eq!(db.keyfile_json.as_deref(), Some(secret_json));
}

#[test]
fn unknown_database_option_is_not_implemented() {
    // ADBC: setting an unrecognised option reports NotImplemented (not InvalidArguments), so a
    // driver manager can tell "I don't support this option" from "this value is wrong".
    let mut db = new_database();
    let error = db
        .set_option(
            OptionDatabase::Other("this_option_does_not_exist".into()),
            OptionValue::String("x".into()),
        )
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

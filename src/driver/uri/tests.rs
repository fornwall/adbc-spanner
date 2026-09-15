use super::*;
use crate::driver::tests::new_database;
use adbc_core::error::Status;

// --- Connection URIs (`spanner:` scheme with query-parameter options) ---

const DB_PATH: &str = "projects/p/instances/i/databases/d";

fn set_uri(db: &mut SpannerDatabase, uri: &str) -> Result<()> {
    db.set_option(OptionDatabase::Uri, OptionValue::String(uri.into()))
}

#[test]
fn a_bare_database_path_is_rejected() {
    // The `uri` option requires the `spanner://` scheme; a bare path is no longer accepted, and
    // the error echoes back the wrapped form so the fix is obvious.
    let mut db = new_database();
    let error = set_uri(&mut db, DB_PATH).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains(&format!("spanner:///{DB_PATH}")));
}

#[test]
fn a_scheme_uri_sets_the_database_path() {
    // The accepted no-endpoint spelling is the three-slash `spanner:///` form (empty authority).
    for uri in [
        format!("spanner:///{DB_PATH}"),
        format!("Spanner:///{DB_PATH}"), // schemes are case-insensitive
    ] {
        let mut db = new_database();
        set_uri(&mut db, &uri).unwrap();
        assert_eq!(db.database.as_deref(), Some(DB_PATH), "uri: {uri}");
        assert_eq!(db.endpoint, None, "uri: {uri}");
    }
}

#[test]
fn the_documented_quickstart_uri_example_parses() {
    // The exact `uri=` example string shown in the quickstart docs (src/ffi.rs module doc,
    // docs/adbc.md, python/adbc_driver_spanner/dbapi.py) must stay a form the driver accepts,
    // so the docs can't silently rot into a rejected spelling again.
    let mut db = new_database();
    set_uri(&mut db, "spanner:///projects/p/instances/i/databases/d").unwrap();
    assert_eq!(db.database.as_deref(), Some(DB_PATH));
    assert_eq!(db.endpoint, None);
}

#[test]
fn a_scheme_uri_without_two_slashes_is_rejected() {
    // `spanner://` is required — the scheme-only (`spanner:path`) and single-slash
    // (`spanner:/path`) spellings are rejected.
    for uri in [format!("spanner:{DB_PATH}"), format!("spanner:/{DB_PATH}")] {
        let mut db = new_database();
        let error = set_uri(&mut db, &uri).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "uri: {uri}");
        assert!(error.message.contains("spanner://"), "uri: {uri}");
    }
}

#[test]
fn a_cloudspanner_scheme_is_not_recognised() {
    // Only `spanner:` is a connection-URI scheme; `cloudspanner:` (the JDBC convention) is
    // deliberately not supported. Not being a `spanner://` URI, it is rejected.
    let mut db = new_database();
    let uri = format!("cloudspanner:///{DB_PATH}?spanner.emulator=true");
    let error = set_uri(&mut db, &uri).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn a_host_authority_becomes_the_endpoint() {
    // `spanner://host:port/projects/...` — the authority is the gRPC endpoint, taken verbatim
    // (exactly as if passed as the `spanner.endpoint` option).
    let mut db = new_database();
    set_uri(&mut db, &format!("spanner://emu-host:9010/{DB_PATH}")).unwrap();
    assert_eq!(db.database.as_deref(), Some(DB_PATH));
    assert_eq!(db.endpoint.as_deref(), Some("emu-host:9010"));
}

#[test]
fn query_parameters_set_database_options() {
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!(
            "spanner:///{DB_PATH}?spanner.endpoint=http://localhost:9010\
             &spanner.emulator=true"
        ),
    )
    .unwrap();
    assert_eq!(db.database.as_deref(), Some(DB_PATH));
    assert_eq!(db.endpoint.as_deref(), Some("http://localhost:9010"));
    assert!(db.emulator);
}

#[test]
fn every_non_secret_database_level_option_is_accepted_as_a_query_parameter() {
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!(
            "spanner:///{DB_PATH}\
             ?spanner.auth.keyfile=/path/key.json\
             &spanner.auth.impersonate.target_principal=target%40p.iam.gserviceaccount.com\
             &spanner.auth.impersonate.delegates=a%40p.iam.gserviceaccount.com,b%40p.iam.gserviceaccount.com\
             &spanner.auth.impersonate.scopes=https://www.googleapis.com/auth/cloud-platform\
             &spanner.auth.impersonate.lifetime=900\
             &spanner.auth.quota_project=billing-project"
        ),
    )
    .unwrap();
    // `spanner.auth.keyfile` is a path, not a secret, so — as with `get_option` — it stays a
    // legal query parameter; the two secret-holding keys are covered by
    // `secret_bearing_query_parameters_are_rejected`.
    assert_eq!(db.keyfile.as_deref(), Some("/path/key.json"));
    assert_eq!(db.quota_project.as_deref(), Some("billing-project"));
    assert_eq!(
        db.impersonate_target_principal.as_deref(),
        Some("target@p.iam.gserviceaccount.com")
    );
    assert_eq!(
        db.impersonate_delegates,
        vec![
            "a@p.iam.gserviceaccount.com".to_string(),
            "b@p.iam.gserviceaccount.com".to_string()
        ]
    );
    assert_eq!(
        db.impersonate_scopes,
        vec!["https://www.googleapis.com/auth/cloud-platform".to_string()]
    );
    assert_eq!(db.impersonate_lifetime_secs, Some(900));
}

#[test]
fn an_explicit_option_set_after_the_uri_wins() {
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?spanner.endpoint=http://from-uri:9010"),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        OptionValue::String("http://explicit:9010".into()),
    )
    .unwrap();
    assert_eq!(db.endpoint.as_deref(), Some("http://explicit:9010"));
}

#[test]
fn a_uri_set_after_an_explicit_option_overwrites_only_what_it_carries() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        OptionValue::String("http://explicit:9010".into()),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_EMULATOR.into()),
        OptionValue::String("true".into()),
    )
    .unwrap();
    // The URI names an endpoint but says nothing about the emulator flag: the endpoint is
    // overwritten, the emulator flag survives.
    set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?spanner.endpoint=http://from-uri:9010"),
    )
    .unwrap();
    assert_eq!(db.endpoint.as_deref(), Some("http://from-uri:9010"));
    assert!(db.emulator);
    // A URI with no query parameters at all leaves both untouched.
    set_uri(&mut db, "spanner:///projects/p2/instances/i2/databases/d2").unwrap();
    assert_eq!(
        db.database.as_deref(),
        Some("projects/p2/instances/i2/databases/d2")
    );
    assert_eq!(db.endpoint.as_deref(), Some("http://from-uri:9010"));
    assert!(db.emulator);
}

#[test]
fn a_query_parameter_beats_the_host_authority() {
    // Both name an endpoint; the query parameter applies after the authority, so it wins.
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!("spanner://authority:9010/{DB_PATH}?spanner.endpoint=http://param:9010"),
    )
    .unwrap();
    assert_eq!(db.endpoint.as_deref(), Some("http://param:9010"));
}

#[test]
fn an_unknown_query_parameter_is_rejected_by_name() {
    let mut db = new_database();
    let error = set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?spanner.databoost=1"),
    )
    .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("spanner.databoost"));
    // The database-path key is not a query parameter either — the URI path is the one way to
    // name the database.
    let error = set_uri(&mut db, &format!("spanner:///{DB_PATH}?uri=x")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("uri"));
}

#[test]
fn secret_bearing_query_parameters_are_rejected() {
    // A URI is the most-logged config artifact there is (shell history, `ps`, tracing
    // spans), so the two options whose value is a live secret cannot travel in one. They are
    // rejected by name — not silently accepted, and not lumped in with unknown keys — and the
    // error points at the option itself, which is the supported way to supply them.
    for (key, value) in [
        (
            OPTION_KEYFILE_JSON,
            "%7B%22type%22%3A%22service_account%22%7D",
        ),
        (OPTION_ACCESS_TOKEN, "ya29.uri-token"),
    ] {
        let mut db = new_database();
        let error = set_uri(&mut db, &format!("spanner:///{DB_PATH}?{key}={value}")).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "key: {key}");
        assert!(error.message.contains(key), "{}", error.message);
        assert!(error.message.contains("secret"), "{}", error.message);
        // Rejected, so nothing was stored — the URI never reaches the option fields.
        assert_eq!(db.keyfile_json, None, "key: {key}");
        assert_eq!(db.access_token, None, "key: {key}");

        // The option itself still works: only the URI route is closed.
        let decoded = percent_decode(value).unwrap();
        db.set_option(
            OptionDatabase::Other(key.into()),
            OptionValue::String(decoded.clone()),
        )
        .unwrap();
        let stored = match key {
            OPTION_KEYFILE_JSON => db.keyfile_json.as_deref(),
            _ => db.access_token.as_deref(),
        };
        assert_eq!(stored, Some(decoded.as_str()), "key: {key}");
    }
}

#[test]
fn a_rejected_uri_leaves_the_configuration_untouched() {
    let mut db = new_database();
    set_uri(&mut db, &format!("spanner:///{DB_PATH}")).unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        OptionValue::String("http://kept:9010".into()),
    )
    .unwrap();
    for bad in [
        "spanner:///projects/p2/instances/i2/databases/d2?bogus.key=1".to_string(),
        // A bad *value* for a known key must also leave everything untouched (it is validated
        // against a scratch instance before any field is mutated).
        "spanner:///projects/p2/instances/i2/databases/d2?spanner.emulator=maybe".to_string(),
        "spanner://host:9010/projects/p2/instances/i2/databases/d2?spanner.auth.keyfile=%G1"
            .to_string(),
        // A refused secret-holding key is no different: the whole URI is rejected
        // before any field is mutated, authority included.
        format!(
            "spanner://host:9010/projects/p2/instances/i2/databases/d2?{OPTION_ACCESS_TOKEN}=ya29.x"
        ),
    ] {
        let error = set_uri(&mut db, &bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "uri: {bad}");
        assert_eq!(db.database.as_deref(), Some(DB_PATH), "uri: {bad}");
        assert_eq!(
            db.endpoint.as_deref(),
            Some("http://kept:9010"),
            "uri: {bad}"
        );
        assert!(!db.emulator, "uri: {bad}");
    }
}

#[test]
fn malformed_percent_encoding_is_rejected() {
    for bad in ["%G1", "%1", "%", "a%+5b"] {
        let error = percent_decode(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "input: {bad}");
        assert!(error.message.contains("percent-encoding"), "input: {bad}");
    }
    // Percent-decoding is RFC 3986: `+` stays a literal plus (form-encoding would corrupt e.g.
    // a keyfile path or an endpoint containing one).
    assert_eq!(percent_decode("a+b%20c%3D1").unwrap(), "a+b c=1");
    // A decoded byte sequence that is not UTF-8 is rejected, not lossily replaced.
    let error = percent_decode("%FF%FE").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("UTF-8"));
}

#[test]
fn a_uri_with_a_bad_database_path_is_rejected() {
    let mut db = new_database();
    for bad in [
        "spanner:///",
        "spanner:///projects/p",
        "spanner:///projects//instances/i/databases/d",
        "spanner:///databases/d/instances/i/projects/p",
    ] {
        let error = set_uri(&mut db, bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "uri: {bad}");
        assert!(error.message.contains("database path"), "uri: {bad}");
    }
    // The classic trap: two slashes make `projects` a host authority. The error says so.
    let error = set_uri(&mut db, &format!("spanner://{DB_PATH}")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("host authority"));
    assert!(error.message.contains("spanner:///projects/"));
}

#[test]
fn a_uri_fragment_is_rejected() {
    let mut db = new_database();
    let error = set_uri(&mut db, &format!("spanner:///{DB_PATH}#frag")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("#fragment"));
}

#[test]
fn get_option_uri_returns_the_uri_verbatim() {
    // adbc.h: `GetOption` serves the option *value*, so the URI comes back exactly as set —
    // query parameters included — while what it expanded into reads back under its own keys.
    let uri = format!(
        "spanner:///{DB_PATH}?spanner.endpoint=http://localhost:9010&spanner.emulator=true"
    );
    let mut db = new_database();
    set_uri(&mut db, &uri).unwrap();
    assert_eq!(db.get_option_string(OptionDatabase::Uri).unwrap(), uri);
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_ENDPOINT.into()))
            .unwrap(),
        "http://localhost:9010"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_EMULATOR.into()))
            .unwrap(),
        "true"
    );
}

#[test]
fn get_option_uri_is_not_found_until_a_uri_is_set() {
    // The database path has no other source, so an unset `uri` reports `NotFound` rather than
    // some invented value.
    let db = new_database();
    let error = db.get_option_string(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::NotFound);
}

#[test]
fn the_last_uri_set_is_the_one_returned() {
    let mut db = new_database();
    set_uri(&mut db, &format!("spanner:///{DB_PATH}")).unwrap();
    let last = "spanner://host:9010/projects/p2/instances/i2/databases/d2?spanner.emulator=true";
    set_uri(&mut db, last).unwrap();
    assert_eq!(db.get_option_string(OptionDatabase::Uri).unwrap(), last);
    assert_eq!(
        db.database.as_deref(),
        Some("projects/p2/instances/i2/databases/d2")
    );
}

#[test]
fn a_uri_survives_a_set_get_set_replay() {
    // The property that makes profile dump-and-replay work: feeding `get_option("uri")` straight
    // back into `set_option` is accepted and lands in the identical state.
    let uri = format!(
        "spanner://authority:9010/{DB_PATH}\
         ?spanner.endpoint=http%3A%2F%2Flocalhost%3A9010\
         &spanner.emulator=true\
         &spanner.auth.keyfile=/path/key.json"
    );
    let mut db = new_database();
    set_uri(&mut db, &uri).unwrap();
    let dumped = db.get_option_string(OptionDatabase::Uri).unwrap();
    assert_eq!(dumped, uri);

    let mut replayed = new_database();
    set_uri(&mut replayed, &dumped).unwrap();
    assert_eq!(
        replayed.get_option_string(OptionDatabase::Uri).unwrap(),
        uri
    );
    assert_eq!(replayed.database, db.database);
    assert_eq!(replayed.endpoint, db.endpoint);
    assert_eq!(replayed.emulator, db.emulator);
    assert_eq!(replayed.keyfile, db.keyfile);
}

#[test]
fn a_rejected_uri_is_never_retrievable() {
    // Validation happens before the URI is stored, so a refusal — a secret-bearing parameter
    // above all — leaves nothing behind for a config dump to find.
    let mut db = new_database();
    for bad in [
        format!("spanner:///{DB_PATH}?{OPTION_ACCESS_TOKEN}=ya29.uri-token"),
        format!("spanner:///{DB_PATH}?{OPTION_KEYFILE_JSON}=%7B%7D"),
        format!("spanner:///{DB_PATH}?bogus.key=1"),
        format!("spanner:/{DB_PATH}"),
        DB_PATH.to_string(),
    ] {
        assert!(set_uri(&mut db, &bad).is_err(), "uri: {bad}");
        let error = db.get_option_string(OptionDatabase::Uri).unwrap_err();
        assert_eq!(error.status, Status::NotFound, "uri: {bad}");
    }

    // ... and a rejected URI does not disturb an already-stored one either.
    let good = format!("spanner:///{DB_PATH}");
    set_uri(&mut db, &good).unwrap();
    assert!(set_uri(&mut db, &format!("spanner:///{DB_PATH}?bogus.key=1")).is_err());
    assert_eq!(db.get_option_string(OptionDatabase::Uri).unwrap(), good);
}

#[test]
fn uri_query_parameter_values_are_percent_decoded() {
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?spanner.endpoint=http%3A%2F%2Flocalhost%3A9010"),
    )
    .unwrap();
    assert_eq!(db.endpoint.as_deref(), Some("http://localhost:9010"));
    // Keys are decoded too, and empty `&&` segments are tolerated.
    set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?&spanner%2Eemulator=true&"),
    )
    .unwrap();
    assert!(db.emulator);
}

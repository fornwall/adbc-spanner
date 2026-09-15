use super::*;
use crate::driver::tests::new_database;
use crate::runtime::new_runtime;
use crate::{OPTION_IMPERSONATE_DELEGATES, OPTION_IMPERSONATE_LIFETIME, OPTION_IMPERSONATE_SCOPES};
use adbc_core::Optionable;
use adbc_core::error::Status;
use adbc_core::options::{OptionDatabase, OptionValue};

// Emulator mode + explicitly configured credentials is refused at connect() time instead of
// silently downgrading to anonymous plaintext credentials. The guard fires before any network
// or runtime work, so these tests run offline. `spanner.emulator=true` is used to enter
// emulator mode (env vars cannot be mutated safely in parallel tests); the
// `SPANNER_EMULATOR_HOST` path resolves to the same `emulator` flag and hits the same guard.
#[test]
fn emulator_mode_with_an_explicit_keyfile_is_refused() {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.keyfile = Some("/path/to/key.json".into());
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains("emulator mode"));
    assert!(error.message.contains(OPTION_KEYFILE));
    assert!(error.message.contains("`spanner.emulator` option"));
}

#[test]
fn emulator_mode_with_explicit_keyfile_json_is_refused() {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.keyfile_json = Some("{\"type\":\"service_account\"}".into());
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains(OPTION_KEYFILE_JSON));
}

#[test]
fn emulator_mode_with_an_impersonation_target_is_refused() {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.impersonate_target_principal = Some("target@project.iam.gserviceaccount.com".into());
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains(OPTION_IMPERSONATE_TARGET_PRINCIPAL));
}

#[test]
fn emulator_mode_with_an_access_token_is_refused() {
    // An explicit access token trips the same emulator guard as the keyfile options: emulator
    // mode forces anonymous credentials, so silently dropping the token would be a downgrade.
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.access_token = Some("ya29.test-token".into());
    assert_eq!(db.explicit_credential_option(), Some(OPTION_ACCESS_TOKEN));
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains("emulator mode"));
    assert!(error.message.contains(OPTION_ACCESS_TOKEN));
}

#[test]
fn access_token_is_write_only() {
    // SEC-1: the token is a live bearer credential, so `get_option` reports `NotFound`
    // whether the option is set or not — the token is never returned (matching the
    // keyfile_json convention and the `Debug` redaction).
    let mut db = new_database();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()))
            .unwrap_err()
            .status,
        Status::NotFound
    );
    db.set_option(
        OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()),
        OptionValue::String("ya29.LIVE-BEARER-TOKEN".into()),
    )
    .unwrap();
    let error = db
        .get_option_string(OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()))
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert!(error.message.contains("write-only"), "{}", error.message);
    assert!(
        !error.message.contains("ya29.LIVE-BEARER-TOKEN"),
        "access_token leaked through get_option: {}",
        error.message
    );
    // The bytes getter funnels through the same guard.
    let error = db
        .get_option_bytes(OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()))
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    // The stored token is still in effect — only reading it back is refused.
    assert_eq!(db.access_token.as_deref(), Some("ya29.LIVE-BEARER-TOKEN"));
}

#[test]
fn access_token_conflicts_with_other_credential_options() {
    // An access token is a complete credential; combining it with a keyfile, inline keyfile
    // JSON, or an impersonation target is a conflict that `connect()` refuses (InvalidState),
    // naming the offender. The conflict is decided by `conflicting_credential_with_access_token`
    // — we assert on it directly rather than through `connect()`, because CI sets
    // `SPANNER_EMULATOR_HOST` (which the module note above explains cannot be unset in parallel
    // tests), and its emulator guard fires first inside `connect()`, masking this branch.
    // keyfile_json is checked first, so it wins when several are set.
    let base = || {
        let mut db = new_database();
        db.access_token = Some("ya29.test-token".into());
        db
    };
    // An access token on its own is a complete credential, not a conflict.
    assert_eq!(base().conflicting_credential_with_access_token(), None);
    for (mutate, expected) in [
        (
            Box::new(|db: &mut SpannerDatabase| db.keyfile = Some("/path/key.json".into()))
                as Box<dyn Fn(&mut SpannerDatabase)>,
            OPTION_KEYFILE,
        ),
        (
            Box::new(|db: &mut SpannerDatabase| {
                db.keyfile_json = Some("{\"type\":\"service_account\"}".into())
            }),
            OPTION_KEYFILE_JSON,
        ),
        (
            Box::new(|db: &mut SpannerDatabase| {
                db.impersonate_target_principal =
                    Some("target@project.iam.gserviceaccount.com".into())
            }),
            OPTION_IMPERSONATE_TARGET_PRINCIPAL,
        ),
    ] {
        let mut db = base();
        mutate(&mut db);
        assert_eq!(
            db.conflicting_credential_with_access_token(),
            Some(expected),
            "conflict: {expected}"
        );
    }
}

#[test]
fn access_token_credentials_send_a_bearer_authorization_header() {
    // The custom static-token credential emits `Authorization: Bearer <token>` verbatim, marks
    // it sensitive, and reports "not modified" for a matching cache tag. Runs inside a runtime
    // because `headers()` is async (though it does no I/O).
    let credentials = build_static_token_credentials("ya29.the-token", None).unwrap();
    let runtime = new_runtime().unwrap();
    runtime.block_on(async {
        let resource = credentials.headers(Extensions::new()).await.unwrap();
        let (headers, tag) = match resource {
            CacheableResource::New { entity_tag, data } => (data, entity_tag),
            CacheableResource::NotModified => panic!("expected fresh headers"),
        };
        let value = headers.get(AUTHORIZATION).expect("authorization header");
        assert_eq!(value.to_str().unwrap(), "Bearer ya29.the-token");
        assert!(
            value.is_sensitive(),
            "the bearer token must be marked sensitive"
        );
        // No quota project was requested, so no billing header is attached.
        assert!(headers.get(QUOTA_PROJECT_HEADER).is_none());

        // A request carrying the same entity tag is told the headers have not changed.
        let mut extensions = Extensions::new();
        extensions.insert(tag);
        assert!(matches!(
            credentials.headers(extensions).await.unwrap(),
            CacheableResource::NotModified
        ));
    });
}

#[test]
fn access_token_with_illegal_header_characters_is_rejected_without_leaking() {
    // A token containing characters illegal in an HTTP header value (here a newline) is rejected
    // up front, and the token material never appears in the error message.
    const TOKEN: &str = "bad\ntoken-SECRET-do-not-leak";
    let error = build_static_token_credentials(TOKEN, None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains(OPTION_ACCESS_TOKEN),
        "{}",
        error.message
    );
    assert!(
        !error.message.contains("SECRET"),
        "access-token error leaked token material: {}",
        error.message
    );
}

#[test]
fn quota_project_option_round_trips_and_unsets() {
    // The billing project is a bare project id (not a secret), so it round-trips verbatim; it is
    // unset by default and `""` clears it, back to NotFound — the house "" pattern.
    let mut db = new_database();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()))
            .unwrap_err()
            .status,
        Status::NotFound
    );
    db.set_option(
        OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()),
        OptionValue::String("my-billing-project".into()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()))
            .unwrap(),
        "my-billing-project"
    );
    // `""` unsets.
    db.set_option(
        OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()),
        OptionValue::String(String::new()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()))
            .unwrap_err()
            .status,
        Status::NotFound
    );
}

#[test]
fn quota_project_renders_in_debug_and_is_not_a_secret() {
    // Unlike the credential fields, the billing project is not redacted (it is a project id).
    let mut db = new_database();
    db.quota_project = Some("my-billing-project".into());
    let rendered = format!("{db:?}");
    assert!(
        rendered.contains(r#"quota_project: Some("my-billing-project")"#),
        "quota_project not shown verbatim: {rendered}"
    );
}

#[test]
fn emulator_mode_with_a_quota_project_is_refused() {
    // The emulator forces anonymous credentials and ignores billing, so a configured quota
    // project would be silently dropped — refused at connect() like the credential options.
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.quota_project = Some("my-billing-project".into());
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains("emulator mode"));
    assert!(error.message.contains(OPTION_QUOTA_PROJECT));
}

#[test]
fn access_token_credentials_send_the_quota_project_header() {
    // With a quota project, the static-token credential attaches `x-goog-user-project` alongside
    // the bearer token — the actual on-the-wire billing header — and leaves it non-sensitive
    // (a project id, not a secret). This is the strongest offline assertion of header emission;
    // the builder paths (ADC/keyfile/impersonation) emit the identical header but only mint
    // tokens over the network, so they cannot be exercised offline.
    let credentials =
        build_static_token_credentials("ya29.the-token", Some("my-billing-project")).unwrap();
    let runtime = new_runtime().unwrap();
    runtime.block_on(async {
        let resource = credentials.headers(Extensions::new()).await.unwrap();
        let headers = match resource {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => panic!("expected fresh headers"),
        };
        let value = headers
            .get(QUOTA_PROJECT_HEADER)
            .expect("x-goog-user-project header");
        assert_eq!(value.to_str().unwrap(), "my-billing-project");
        assert!(
            !value.is_sensitive(),
            "the quota project is a project id, not a secret"
        );
        // The bearer token is still present and sensitive.
        let auth = headers.get(AUTHORIZATION).expect("authorization header");
        assert_eq!(auth.to_str().unwrap(), "Bearer ya29.the-token");
        assert!(auth.is_sensitive());
    });
}

#[test]
fn quota_project_does_not_conflict_with_an_access_token() {
    // The billing project composes with every credential path, including the access token — it
    // is not a credential, so it is absent from the access-token conflict set.
    let mut db = new_database();
    db.access_token = Some("ya29.test-token".into());
    db.quota_project = Some("my-billing-project".into());
    assert_eq!(db.conflicting_credential_with_access_token(), None);
    assert_eq!(db.explicit_credential_option(), Some(OPTION_ACCESS_TOKEN));
}

// Only explicit driver options count as credentials: a fresh database (which would fall back to
// ambient ADC, e.g. GOOGLE_APPLICATION_CREDENTIALS) reports none, so plain emulator use — the
// integration-test path — is not refused. Inert `spanner.auth.impersonate.*` options (no target
// principal) do not count either.
#[test]
fn ambient_adc_and_inert_impersonation_options_do_not_trip_the_emulator_guard() {
    let mut db = new_database();
    assert_eq!(db.explicit_credential_option(), None);
    db.impersonate_delegates = vec!["delegate@p.iam.gserviceaccount.com".into()];
    db.impersonate_scopes = vec!["https://www.googleapis.com/auth/cloud-platform".into()];
    db.impersonate_lifetime_secs = Some(900);
    assert_eq!(db.explicit_credential_option(), None);
    db.keyfile = Some("/path/to/key.json".into());
    assert_eq!(db.explicit_credential_option(), Some(OPTION_KEYFILE));
}

#[test]
fn the_credential_ladder_selects_one_flow_in_precedence_order() {
    // Nothing configured: ambient Application Default Credentials.
    let mut db = new_database();
    assert_eq!(db.credential_choice(false), CredentialChoice::Adc);
    // A quota project does not select a flow — it rides on whichever one wins (here ADC,
    // which `build_credentials` then builds explicitly to attach the header to).
    db.quota_project = Some("billing-project".into());
    assert_eq!(db.credential_choice(false), CredentialChoice::Adc);

    // A keyfile — by path, or inline JSON — selects the keyfile flow.
    db.keyfile = Some("/path/to/key.json".into());
    assert_eq!(db.credential_choice(false), CredentialChoice::Keyfile);
    db.keyfile = None;
    db.keyfile_json = Some("{\"type\":\"authorized_user\"}".into());
    assert_eq!(db.credential_choice(false), CredentialChoice::Keyfile);

    // Impersonation outranks the keyfile rather than conflicting with it: the keyfile becomes
    // the *source* credential that the impersonated one wraps.
    db.impersonate_target_principal = Some("target@p.iam.gserviceaccount.com".into());
    assert_eq!(db.credential_choice(false), CredentialChoice::Impersonate);

    // An access token outranks both — though only nominally: `connect` refuses that
    // combination up front (`access_token_conflicts_with_other_credential_options`).
    db.access_token = Some("ya29.token".into());
    assert_eq!(db.credential_choice(false), CredentialChoice::AccessToken);

    // Resolved emulator mode wins outright over everything configured above. `connect`'s guard
    // refuses that combination first, so the ladder never silently drops a credential — but it
    // must not silently *use* one either.
    assert_eq!(db.credential_choice(true), CredentialChoice::Anonymous);
}

#[test]
fn the_credential_ladder_ignores_impersonation_options_without_a_target() {
    // The `impersonate.*` knobs are inert without a target principal (as
    // `explicit_credential_option` also holds), so the flow stays ADC — or the keyfile's, with
    // no impersonation wrapped around it.
    let mut db = new_database();
    db.impersonate_delegates = vec!["delegate@p.iam.gserviceaccount.com".into()];
    db.impersonate_scopes = vec!["https://www.googleapis.com/auth/cloud-platform".into()];
    db.impersonate_lifetime_secs = Some(900);
    assert_eq!(db.credential_choice(false), CredentialChoice::Adc);
    db.keyfile = Some("/path/to/key.json".into());
    assert_eq!(db.credential_choice(false), CredentialChoice::Keyfile);
}

#[test]
fn missing_keyfile_is_an_error() {
    let mut db = new_database();
    db.keyfile = Some("/no/such/keyfile-does-not-exist.json".into());
    let error = db.credentials_json().unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn inline_keyfile_json_takes_precedence_over_path() {
    let mut db = new_database();
    db.keyfile = Some("/ignored/path.json".into());
    db.keyfile_json = Some("{\"inline\":true}".into());
    assert_eq!(
        db.credentials_json().unwrap(),
        Some("{\"inline\":true}".to_string())
    );
}

#[test]
fn malformed_credential_json_is_rejected() {
    let error = build_credentials_from_json("{ not valid json", None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("invalid credential JSON key"));
}

#[test]
fn credential_json_without_a_type_is_rejected() {
    let error = build_credentials_from_json("{\"private_key\":\"x\"}", None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("missing a string `type` field"));
    assert!(error.message.contains("service_account"));
}

#[test]
fn credential_json_with_a_non_string_type_is_rejected() {
    let error = build_credentials_from_json("{\"type\":42}", None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("missing a string `type` field"));
}

#[test]
fn credential_json_with_an_unknown_type_is_rejected() {
    let error =
        build_credentials_from_json("{\"type\":\"gdch_service_account\"}", None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("unsupported credential `type`"));
    assert!(error.message.contains("gdch_service_account"));
    assert!(error.message.contains("external_account"));
}

// An `authorized_user` (end-user ADC) keyfile with all required fields is accepted and routed to
// the user-account flow — no service-account private key required, and no network is touched
// (token exchange happens lazily on first use). The builder spawns a token-cache task, so it
// must run inside a Tokio runtime — exactly as `connect()` does inside its `block_on`.
#[test]
fn authorized_user_credential_json_is_accepted() {
    let json = r#"{
        "type": "authorized_user",
        "client_id": "test-client-id.apps.googleusercontent.com",
        "client_secret": "test-client-secret",
        "refresh_token": "test-refresh-token"
    }"#;
    let runtime = new_runtime().unwrap();
    runtime.block_on(async { assert!(build_credentials_from_json(json, None).is_ok()) });
}

// A `service_account` keyfile is still routed to the service-account flow. A key with an invalid
// private key fails inside that builder — and, crucially, the error names the detected type,
// proving the dispatch reached the service-account path rather than being rejected as unknown.
#[test]
fn service_account_credential_json_is_routed_to_the_service_account_flow() {
    let error = build_credentials_from_json(
        "{\"type\":\"service_account\",\"private_key\":\"not-a-key\"}",
        None,
    )
    .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error
            .message
            .contains("failed to build service_account credentials")
    );
}

// A credential-build failure must never echo the credential JSON body into the ADBC error
// message: the auth crate's `Display` — never interpolated, see `scrub_credential_error` — can
// carry `serde_json`-derived fragments of the input, and the input holds the private key. Here
// a `service_account` key carries a recognizable fake secret but omits the required
// `client_email`, so `.build()` fails; the surfaced message must name the detected type and a
// safe category, and must not contain the secret material.
#[test]
fn credential_build_failure_never_leaks_key_material() {
    const SECRET: &str = "SUPER-SECRET-PRIVATE-KEY-DO-NOT-LEAK-abc123";
    let json = format!(
        "{{\"type\":\"service_account\",\"private_key\":\"{SECRET}\",\"private_key_id\":42}}"
    );
    let error = build_credentials_from_json(&json, None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    // The message names the detected credential type (safe, user-supplied config) ...
    assert!(
        error
            .message
            .contains("failed to build service_account credentials"),
        "message should name the detected type: {}",
        error.message
    );
    // ... but never the secret key material carried in the credential JSON body.
    assert!(
        !error.message.contains(SECRET),
        "credential-build error leaked key material: {}",
        error.message
    );
}

// The scrubber turns a raw `google-cloud-auth` builder error into a fixed, secret-free phrase.
// We drive a real builder to produce a genuine `build_errors::Error` (its constructors are
// crate-private, so this is the only way to obtain one), then confirm the scrubbed phrase is a
// constant and carries none of the secret-bearing body the raw error was built from.
#[test]
fn scrub_credential_error_returns_fixed_phrase() {
    const SECRET: &str = "leak-me-if-you-can-9f8e7d";
    // A `service_account` body carrying a fake secret but missing the required `client_email`:
    // `.build()` fails deserializing it, yielding a real `build_errors::Error`.
    let raw = ServiceAccountCredentials::new(serde_json::json!({
        "type": "service_account",
        "private_key": SECRET,
    }))
    .build()
    .unwrap_err();
    let scrubbed = scrub_credential_error(&raw);
    assert_eq!(scrubbed, "the credential JSON could not be parsed");
    assert!(
        !scrubbed.contains(SECRET),
        "scrubbed phrase must be a fixed string, got: {scrubbed}"
    );
}

#[test]
fn impersonation_options_round_trip_and_split() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()),
        OptionValue::String("target@project.iam.gserviceaccount.com".into()),
    )
    .unwrap();
    // Delegates and scopes are comma-separated; surrounding whitespace and a trailing comma are
    // tolerated.
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_DELEGATES.into()),
        OptionValue::String("a@p.iam.gserviceaccount.com, b@p.iam.gserviceaccount.com,".into()),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_SCOPES.into()),
        OptionValue::String(
            "https://www.googleapis.com/auth/spanner.data,https://www.googleapis.com/auth/cloud-platform".into(),
        ),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()),
        OptionValue::String("1800".into()),
    )
    .unwrap();

    assert_eq!(
        db.impersonate_target_principal.as_deref(),
        Some("target@project.iam.gserviceaccount.com")
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
        vec![
            "https://www.googleapis.com/auth/spanner.data".to_string(),
            "https://www.googleapis.com/auth/cloud-platform".to_string()
        ]
    );
    assert_eq!(db.impersonate_lifetime_secs, Some(1800));

    // Round-trips back out through get_option_string (delegates/scopes re-joined with commas).
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()
        ))
        .unwrap(),
        "target@project.iam.gserviceaccount.com"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_IMPERSONATE_DELEGATES.into()))
            .unwrap(),
        "a@p.iam.gserviceaccount.com,b@p.iam.gserviceaccount.com"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()))
            .unwrap(),
        "1800"
    );
}

#[test]
fn impersonation_lifetime_defaults_to_one_hour_when_unset() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()),
        OptionValue::String("target@project.iam.gserviceaccount.com".into()),
    )
    .unwrap();
    // With a target set but no explicit lifetime, the effective lifetime is the 3600s default —
    // resolved exactly as `connect()` does.
    assert_eq!(db.impersonate_lifetime_secs, None);
    let effective = Duration::from_secs(
        db.impersonate_lifetime_secs
            .unwrap_or(DEFAULT_IMPERSONATION_LIFETIME_SECS),
    );
    assert_eq!(effective, Duration::from_secs(3600));
}

#[test]
fn impersonation_target_is_disabled_by_default() {
    let db = new_database();
    assert!(db.impersonate_target_principal.is_none());
    assert!(db.impersonate_delegates.is_empty());
    assert!(db.impersonate_scopes.is_empty());
    // Unset options report "not set".
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()
        ))
        .unwrap_err()
        .status,
        Status::NotFound
    );
}

#[test]
fn a_non_numeric_impersonation_lifetime_is_rejected() {
    let mut db = new_database();
    let error = db
        .set_option(
            OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()),
            OptionValue::String("not-a-number".into()),
        )
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("non-negative integer"));
}

#[test]
fn an_integer_impersonation_lifetime_is_accepted() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()),
        OptionValue::Int(900),
    )
    .unwrap();
    assert_eq!(db.impersonate_lifetime_secs, Some(900));
}

// Building impersonated credentials on top of a valid base credential succeeds without any
// network I/O: the `impersonated` builder clones the source credential and constructs a lazy
// token provider — the IAM `generateAccessToken` call only happens on first token use. We use an
// `authorized_user` base (which itself builds offline, like #23's test) and must run inside a
// Tokio runtime because the builders spawn token-cache tasks, exactly as `connect()` does.
#[test]
fn impersonated_credentials_build_without_network() {
    let source_json = r#"{
        "type": "authorized_user",
        "client_id": "test-client-id.apps.googleusercontent.com",
        "client_secret": "test-client-secret",
        "refresh_token": "test-refresh-token"
    }"#;
    let runtime = new_runtime().unwrap();
    runtime.block_on(async {
        let source = build_credentials_from_json(source_json, None).unwrap();
        let result = build_impersonated_credentials(
            source,
            "target@project.iam.gserviceaccount.com",
            &["delegate@project.iam.gserviceaccount.com".to_string()],
            &["https://www.googleapis.com/auth/cloud-platform".to_string()],
            Duration::from_secs(1200),
            Some("my-billing-project"),
        );
        assert!(result.is_ok());
    });
}

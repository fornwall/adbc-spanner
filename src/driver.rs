//! The [`SpannerDriver`] and [`SpannerDatabase`] — the two top levels of the ADBC hierarchy.

use adbc_core::error::{Result, Status};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_auth::credentials::external_account::Builder as ExternalAccountCredentials;
use google_cloud_auth::credentials::impersonated::Builder as ImpersonatedCredentials;
use google_cloud_auth::credentials::service_account::Builder as ServiceAccountCredentials;
use google_cloud_auth::credentials::user_account::Builder as UserAccountCredentials;
use google_cloud_auth::credentials::Credentials;
use google_cloud_spanner::client::{DatabaseClient, Spanner};

use crate::connection::SpannerConnection;
use crate::error::{err, from_builder, from_spanner, invalid_argument, invalid_state};
use crate::runtime::{new_runtime, SharedRuntime};
use crate::{
    OPTION_DATABASE, OPTION_EMULATOR, OPTION_ENDPOINT, OPTION_KEYFILE, OPTION_KEYFILE_JSON,
};

/// The Spanner ADBC driver — the entry point for creating [`SpannerDatabase`] instances.
///
/// The driver owns the shared Tokio runtime used to drive the asynchronous Spanner client, so a
/// single driver instance should be reused for the lifetime of the application.
pub struct SpannerDriver {
    runtime: SharedRuntime,
}

impl SpannerDriver {
    /// Create a new driver, initialising its Tokio runtime.
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            runtime: new_runtime()?,
        })
    }
}

impl Default for SpannerDriver {
    /// Create a driver with a fresh runtime.
    ///
    /// Required by the C FFI driver exporter, which cannot surface a fallible constructor. Panics
    /// only if the Tokio runtime cannot be created (catastrophic OS resource exhaustion); prefer
    /// [`SpannerDriver::try_new`] in Rust code.
    fn default() -> Self {
        Self::try_new().expect("failed to initialize the Spanner ADBC driver Tokio runtime")
    }
}

impl Driver for SpannerDriver {
    type DatabaseType = SpannerDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(SpannerDatabase::new(self.runtime.clone()))
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = SpannerDatabase::new(self.runtime.clone());
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

/// A configured, but not yet connected, Spanner database.
///
/// Holds the connection parameters (the database path and, optionally, an emulator endpoint) and
/// mints [`SpannerConnection`]s from them.
pub struct SpannerDatabase {
    runtime: SharedRuntime,
    database: Option<String>,
    endpoint: Option<String>,
    emulator: bool,
    keyfile: Option<String>,
    keyfile_json: Option<String>,
}

impl SpannerDatabase {
    pub(crate) fn new(runtime: SharedRuntime) -> Self {
        Self {
            runtime,
            database: None,
            endpoint: None,
            emulator: false,
            keyfile: None,
            keyfile_json: None,
        }
    }

    /// Resolve the inline credential JSON to use, reading the key file if a path was given. The
    /// credential flow is auto-detected from the JSON's `"type"` field in [`build_credentials_from_json`].
    /// Inline JSON ([`OPTION_KEYFILE_JSON`]) takes precedence over a file path ([`OPTION_KEYFILE`]).
    fn credentials_json(&self) -> Result<Option<String>> {
        if let Some(json) = &self.keyfile_json {
            Ok(Some(json.clone()))
        } else if let Some(path) = &self.keyfile {
            let json = std::fs::read_to_string(path).map_err(|e| {
                err(
                    format!("failed to read keyfile {path:?}: {e}"),
                    Status::InvalidArguments,
                )
            })?;
            Ok(Some(json))
        } else {
            Ok(None)
        }
    }

    /// Resolve the effective configuration and establish a connection.
    ///
    /// Emulator handling: if `SPANNER_EMULATOR_HOST` is set it supplies the endpoint (unless one was
    /// given explicitly) and forces anonymous credentials.
    pub(crate) fn connect(&self) -> Result<Connected> {
        let database = self.database.clone().ok_or_else(|| {
            invalid_state(
                "Spanner database path is not set; provide the `uri` or \
                 `adbc.spanner.database` option (projects/<p>/instances/<i>/databases/<d>)",
            )
        })?;

        let mut endpoint = self.endpoint.clone();
        let mut emulator = self.emulator;
        if let Ok(host) = std::env::var("SPANNER_EMULATOR_HOST") {
            if !host.is_empty() {
                if endpoint.is_none() {
                    endpoint = Some(ensure_scheme(&host));
                }
                emulator = true;
            }
        }

        // Resolve the credential JSON up front (reads the key file, if any); the flow is detected
        // from its `"type"` below. Ignored in emulator mode, which always uses anonymous credentials.
        let credentials_json = if emulator {
            None
        } else {
            self.credentials_json()?
        };

        self.runtime.block_on(async move {
            let mut builder = Spanner::builder();
            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if emulator {
                builder = builder.with_credentials(AnonymousCredentials::new().build());
            } else if let Some(json) = credentials_json {
                builder = builder.with_credentials(build_credentials_from_json(&json)?);
            }
            // Otherwise: Application Default Credentials.
            let spanner = builder.build().await.map_err(from_builder)?;
            let client = spanner
                .database_client(database.clone())
                .build()
                .await
                .map_err(from_spanner)?;
            Ok(Connected {
                client,
                spanner,
                database,
            })
        })
    }
}

/// An established connection's handles: the data-plane [`DatabaseClient`], the [`Spanner`] client
/// (used to reach the Database Admin API for DDL), and the resolved database path.
#[derive(Debug)]
pub(crate) struct Connected {
    pub(crate) client: DatabaseClient,
    pub(crate) spanner: Spanner,
    pub(crate) database: String,
}

impl Optionable for SpannerDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionDatabase::Uri => self.database = Some(string_value(&key, value)?),
            OptionDatabase::Other(name) if name == OPTION_DATABASE => {
                self.database = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_ENDPOINT => {
                self.endpoint = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_EMULATOR => {
                self.emulator = bool_value(&key, value)?
            }
            OptionDatabase::Other(name) if name == OPTION_KEYFILE => {
                self.keyfile = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_KEYFILE_JSON => {
                self.keyfile_json = Some(string_value(&key, value)?)
            }
            other => {
                return Err(invalid_argument(format!(
                    "unsupported Spanner database option: {}",
                    option_name(other)
                )))
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let value = match &key {
            OptionDatabase::Uri => self.database.clone(),
            OptionDatabase::Other(name) if name == OPTION_DATABASE => self.database.clone(),
            OptionDatabase::Other(name) if name == OPTION_ENDPOINT => self.endpoint.clone(),
            OptionDatabase::Other(name) if name == OPTION_EMULATOR => {
                Some(self.emulator.to_string())
            }
            OptionDatabase::Other(name) if name == OPTION_KEYFILE => self.keyfile.clone(),
            OptionDatabase::Other(name) if name == OPTION_KEYFILE_JSON => self.keyfile_json.clone(),
            _ => None,
        };
        value.ok_or_else(|| {
            err(
                format!("option {} is not set", option_name(&key)),
                Status::NotFound,
            )
        })
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Ok(self.get_option_string(key)?.into_bytes())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(err(
            format!("option {} is not an integer", option_name(&key)),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(err(
            format!("option {} is not a double", option_name(&key)),
            Status::NotFound,
        ))
    }
}

impl Database for SpannerDatabase {
    type ConnectionType = SpannerConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(SpannerConnection::new(
            self.runtime.clone(),
            self.connect()?,
        ))
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (adbc_core::options::OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = self.new_connection()?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

/// Prefix a bare `host:port` emulator address with an `http://` scheme, as expected by the gRPC
/// transport.
pub(crate) fn ensure_scheme(host: &str) -> String {
    if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    }
}

fn option_name(key: &OptionDatabase) -> String {
    key.as_ref().to_string()
}

fn string_value(key: &OptionDatabase, value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(invalid_argument(format!(
            "option {} requires a string value",
            option_name(key)
        ))),
    }
}

/// The credential `type` values we accept in a keyfile JSON, for use in error messages.
const SUPPORTED_CREDENTIAL_TYPES: &str =
    "service_account, authorized_user, impersonated_service_account, external_account";

/// Build Google credentials from an inline JSON key, auto-detecting the credential flow from the
/// JSON's top-level `"type"` field, mirroring the BigQuery ADBC driver and Google's own auth
/// libraries.
///
/// Standard Google credential JSON carries a `"type"` discriminator; each value maps to a distinct
/// auth flow with its own required fields:
///
/// - `service_account` — a service-account key (`private_key` / `client_email`).
/// - `authorized_user` — end-user Application Default Credentials from `gcloud auth
///   application-default login`.
/// - `impersonated_service_account` — impersonation of a target service account.
/// - `external_account` — Workload/Workforce Identity Federation.
///
/// The underlying `google-cloud-auth` top-level `Builder` already dispatches on this field, but only
/// for credentials it loads itself from the environment (the `GOOGLE_APPLICATION_CREDENTIALS` var or
/// the well-known ADC file). It offers no entry point that takes inline JSON, so the dispatch has to
/// happen here for the JSON supplied through the `adbc.spanner.keyfile` / `adbc.spanner.keyfile_json`
/// options. Previously every keyfile was forced through the `service_account` builder, which failed
/// (or misbehaved) for any other credential type.
fn build_credentials_from_json(json: &str) -> Result<Credentials> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|e| {
        err(
            format!("invalid credential JSON key: {e}"),
            Status::InvalidArguments,
        )
    })?;

    let credential_type = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            invalid_argument(format!(
                "credential JSON is missing a string `type` field; expected one of \
                 {SUPPORTED_CREDENTIAL_TYPES}"
            ))
        })?
        .to_owned();

    let result = match credential_type.as_str() {
        "service_account" => ServiceAccountCredentials::new(value).build(),
        "authorized_user" => UserAccountCredentials::new(value).build(),
        "impersonated_service_account" => ImpersonatedCredentials::new(value).build(),
        "external_account" => ExternalAccountCredentials::new(value).build(),
        other => {
            return Err(invalid_argument(format!(
                "unsupported credential `type` {other:?}; expected one of \
                 {SUPPORTED_CREDENTIAL_TYPES}"
            )));
        }
    };

    result.map_err(|e| {
        err(
            format!("failed to build {credential_type} credentials: {e}"),
            Status::InvalidArguments,
        )
    })
}

fn bool_value(key: &OptionDatabase, value: OptionValue) -> Result<bool> {
    match value {
        OptionValue::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            other => Err(invalid_argument(format!(
                "option {} expects a boolean, got {other:?}",
                option_name(key)
            ))),
        },
        OptionValue::Int(i) => Ok(i != 0),
        _ => Err(invalid_argument(format!(
            "option {} requires a boolean value",
            option_name(key)
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    fn new_database() -> SpannerDatabase {
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
            OptionValue::String("projects/p/instances/i/databases/d".into()),
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
    fn the_database_option_is_an_alias_for_uri() {
        let mut db = new_database();
        db.set_option(
            OptionDatabase::Other(OPTION_DATABASE.into()),
            OptionValue::String("projects/p/instances/i/databases/d".into()),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Uri).unwrap(),
            "projects/p/instances/i/databases/d"
        );
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
    fn keyfile_options_round_trip() {
        let mut db = new_database();
        db.set_option(
            OptionDatabase::Other(OPTION_KEYFILE.into()),
            OptionValue::String("/path/to/key.json".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(OPTION_KEYFILE_JSON.into()),
            OptionValue::String("{\"type\":\"service_account\"}".into()),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(OPTION_KEYFILE.into()))
                .unwrap(),
            "/path/to/key.json"
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(OPTION_KEYFILE_JSON.into()))
                .unwrap(),
            "{\"type\":\"service_account\"}"
        );
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
        let error = build_credentials_from_json("{ not valid json").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("invalid credential JSON key"));
    }

    #[test]
    fn credential_json_without_a_type_is_rejected() {
        let error = build_credentials_from_json("{\"private_key\":\"x\"}").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("missing a string `type` field"));
        assert!(error.message.contains("service_account"));
    }

    #[test]
    fn credential_json_with_a_non_string_type_is_rejected() {
        let error = build_credentials_from_json("{\"type\":42}").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("missing a string `type` field"));
    }

    #[test]
    fn credential_json_with_an_unknown_type_is_rejected() {
        let error = build_credentials_from_json("{\"type\":\"gdch_service_account\"}").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("unsupported credential `type`"));
        assert!(error.message.contains("gdch_service_account"));
        assert!(error.message.contains("external_account"));
    }

    // An `authorized_user` (end-user ADC) keyfile with all required fields is accepted and routed to
    // the user-account flow — no service-account private key required, and no network is touched
    // (token exchange happens lazily on first use). This is exactly the case the previous
    // service-account-only code path mishandled. The builder spawns a token-cache task, so it must
    // run inside a Tokio runtime — exactly as `connect()` does inside its `block_on`.
    #[test]
    fn authorized_user_credential_json_is_accepted() {
        let json = r#"{
            "type": "authorized_user",
            "client_id": "test-client-id.apps.googleusercontent.com",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token"
        }"#;
        let runtime = new_runtime().unwrap();
        runtime.block_on(async { assert!(build_credentials_from_json(json).is_ok()) });
    }

    // A `service_account` keyfile is still routed to the service-account flow. A key with an invalid
    // private key fails inside that builder — and, crucially, the error names the detected type,
    // proving the dispatch reached the service-account path rather than being rejected as unknown.
    #[test]
    fn service_account_credential_json_is_routed_to_the_service_account_flow() {
        let error = build_credentials_from_json(
            "{\"type\":\"service_account\",\"private_key\":\"not-a-key\"}",
        )
        .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error
            .message
            .contains("failed to build service_account credentials"));
    }
}

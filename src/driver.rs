//! The [`SpannerDriver`] and [`SpannerDatabase`] — the two top levels of the ADBC hierarchy.

use adbc_core::error::{Result, Status};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_auth::credentials::service_account::Builder as ServiceAccountCredentials;
use google_cloud_spanner::client::{DatabaseClient, Spanner};

use crate::connection::SpannerConnection;
use crate::error::{
    err, from_builder, from_spanner, invalid_argument, invalid_state, redact_url_query,
};
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

    /// Resolve the inline service-account JSON to use, reading the key file if a path was given.
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

        // Resolve service-account credentials up front (reads the key file, if any). Ignored in
        // emulator mode, which always uses anonymous credentials.
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
                let key = parse_service_account_key(&json)?;
                let credentials = ServiceAccountCredentials::new(key).build().map_err(|e| {
                    err(
                        format!(
                            "failed to build service-account credentials: {}",
                            redact_url_query(&e.to_string())
                        ),
                        Status::InvalidArguments,
                    )
                })?;
                builder = builder.with_credentials(credentials);
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

/// Parse a service-account JSON key, mapping errors to `InvalidArguments`.
fn parse_service_account_key(json: &str) -> Result<serde_json::Value> {
    serde_json::from_str(json).map_err(|e| {
        err(
            format!("invalid service-account JSON key: {e}"),
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
    fn invalid_service_account_json_is_rejected() {
        assert_eq!(
            parse_service_account_key("{ not valid json")
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
        assert!(parse_service_account_key("{\"type\":\"service_account\"}").is_ok());
    }
}

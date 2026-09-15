//! The [`SpannerDriver`] and [`SpannerDatabase`] — the two top levels of the ADBC hierarchy.

mod credentials;
mod uri;

pub(crate) use uri::ensure_scheme;

use adbc_core::error::{Result, Status};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};
use google_cloud_spanner::client::{DatabaseAdmin, DatabaseClient, Spanner};

use crate::connection::SpannerConnection;
use crate::error::{
    err, from_builder, from_spanner, invalid_argument, invalid_state, not_implemented,
};
use crate::options::impl_typed_option_getters;
use crate::runtime::{SharedRuntime, new_runtime};
use crate::{
    OPTION_ACCESS_TOKEN, OPTION_EMULATOR, OPTION_ENDPOINT, OPTION_IMPERSONATE_DELEGATES,
    OPTION_IMPERSONATE_LIFETIME, OPTION_IMPERSONATE_SCOPES, OPTION_IMPERSONATE_TARGET_PRINCIPAL,
    OPTION_KEYFILE, OPTION_KEYFILE_JSON, OPTION_QUOTA_PROJECT,
};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// The Spanner ADBC driver — the entry point for creating [`SpannerDatabase`] instances.
///
/// The driver owns the shared Tokio runtime used to drive the asynchronous Spanner client, so a
/// single driver instance should be reused for the lifetime of the application.
#[derive(Debug)]
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
///
/// The underlying Spanner client stack — the gRPC channel pool, the resolved credentials, and the
/// [`DatabaseClient`] with its multiplexed session — is built **once**, lazily, on the first
/// connection and *shared* by every connection this database mints: the client's own docs describe
/// a `DatabaseClient` as a long-lived, one-per-database object whose clones cheaply share the
/// session and channels, and ADBC's `Database` is exactly that owner. Setting **any** database
/// option invalidates the cached stack (options affect the endpoint / credentials / database
/// path), so the next connection rebuilds it from the new configuration. One consequence: the
/// `SPANNER_EMULATOR_HOST` environment variable is consulted when the stack is *built* — on the
/// first connection, or the first after a `set_option` — not once per connection.
///
/// [`Debug`] is hand-written rather than derived so the three credential fields (`keyfile`,
/// `keyfile_json` — a full service-account private key — and `access_token` — a live OAuth bearer
/// token) never render in cleartext: each is shown as `Some("<redacted>")` / `None`, exposing only
/// presence, never the secret. This mirrors `StaticTokenCredentials`, whose token lives in a
/// sensitive `HeaderValue` for the same reason. `get_option` matches: the two secret-*holding*
/// options (`keyfile_json`, `access_token`) are write-only and always report `NotFound`, and a
/// connection URI may not carry them as query parameters (`URI_SECRET_OPTIONS`), while `keyfile` —
/// a path, not a secret — both reads back normally and stays a legal query parameter.
pub struct SpannerDatabase {
    runtime: SharedRuntime,
    /// The `uri` option exactly as the caller set it, returned verbatim by `get_option`. The
    /// fields below hold what it expanded into; this is only the string itself.
    uri: Option<String>,
    database: Option<String>,
    endpoint: Option<String>,
    emulator: bool,
    keyfile: Option<String>,
    keyfile_json: Option<String>,
    /// The service account to impersonate. When `Some`, impersonation is layered on top of the base
    /// credentials (keyfile or ADC); when `None`, authentication is unchanged.
    impersonate_target_principal: Option<String>,
    /// Optional delegation chain for impersonation (empty = none).
    impersonate_delegates: Vec<String>,
    /// Optional OAuth scopes for the impersonated token (empty = the auth crate's cloud-platform default).
    impersonate_scopes: Vec<String>,
    /// Optional impersonated-token lifetime in seconds (`None` = [`credentials::DEFAULT_IMPERSONATION_LIFETIME_SECS`]).
    impersonate_lifetime_secs: Option<u64>,
    /// A caller-supplied OAuth 2.0 access token. When `Some`, the driver authenticates with this
    /// bearer token directly (no refresh); it is mutually exclusive with the keyfile/impersonation
    /// options and with emulator mode.
    access_token: Option<String>,
    /// The quota / billing project charged for API usage (`spanner.auth.quota_project`), sent as the
    /// `x-goog-user-project` header. `None` = the credential's own project. Not a secret (a bare
    /// project id), so it renders in `Debug` and round-trips through `get_option`. Composes with
    /// every non-emulator credential path; refused in emulator mode.
    quota_project: Option<String>,
    /// The lazily-built client stack shared by every connection (see the struct docs). `None`
    /// until the first successful [`Database::new_connection`]; reset to `None` by any
    /// `set_option`. Rendered presence-only in `Debug` — the client's own `Debug` output is a
    /// client-crate surface outside this crate's control, so it must not be interpolated here.
    connected: Mutex<Option<Connected>>,
}

impl std::fmt::Debug for SpannerDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Presence-only for the three credential fields; never the secret value (see the struct
        // docs). Every other field renders normally.
        let redact = |value: &Option<String>| value.as_ref().map(|_| "<redacted>");
        f.debug_struct("SpannerDatabase")
            .field("runtime", &self.runtime)
            .field("uri", &self.uri)
            .field("database", &self.database)
            .field("endpoint", &self.endpoint)
            .field("emulator", &self.emulator)
            .field("keyfile", &redact(&self.keyfile))
            .field("keyfile_json", &redact(&self.keyfile_json))
            .field(
                "impersonate_target_principal",
                &self.impersonate_target_principal,
            )
            .field("impersonate_delegates", &self.impersonate_delegates)
            .field("impersonate_scopes", &self.impersonate_scopes)
            .field("impersonate_lifetime_secs", &self.impersonate_lifetime_secs)
            .field("access_token", &redact(&self.access_token))
            .field("quota_project", &self.quota_project)
            // Presence-only, like the credential fields: never delegate to the cached client's
            // own `Debug` (an external surface that could render endpoint/credential internals).
            .field(
                "connected",
                &self
                    .connected
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|_| "<client stack>"),
            )
            .finish()
    }
}

impl SpannerDatabase {
    pub(crate) fn new(runtime: SharedRuntime) -> Self {
        Self {
            runtime,
            uri: None,
            database: None,
            endpoint: None,
            emulator: false,
            keyfile: None,
            keyfile_json: None,
            impersonate_target_principal: None,
            impersonate_delegates: Vec::new(),
            impersonate_scopes: Vec::new(),
            impersonate_lifetime_secs: None,
            access_token: None,
            quota_project: None,
            connected: Mutex::new(None),
        }
    }

    /// Resolve the effective configuration and establish a connection.
    ///
    /// Emulator handling: if `SPANNER_EMULATOR_HOST` is set it supplies the endpoint (unless one was
    /// given explicitly) and forces anonymous credentials. Combining emulator mode with explicitly
    /// configured credentials is refused (see below) instead of silently downgrading them.
    pub(crate) fn connect(&self) -> Result<Connected> {
        let database = self.database.clone().ok_or_else(|| {
            invalid_state(
                "Spanner database path is not set; provide the `uri` option \
                 (projects/<p>/instances/<i>/databases/<d>)",
            )
        })?;

        let mut endpoint = self.endpoint.clone();
        let mut emulator = self.emulator;
        if let Ok(host) = std::env::var("SPANNER_EMULATOR_HOST")
            && !host.is_empty()
        {
            if endpoint.is_none() {
                endpoint = Some(ensure_scheme(&host));
            }
            emulator = true;
        }

        // Emulator mode forces anonymous credentials over plaintext `http://`. Silently dropping
        // credentials the user explicitly configured would be an environment-controlled security
        // downgrade (a stray `SPANNER_EMULATOR_HOST` redirecting real-database traffic, sans auth,
        // to an attacker-chosen endpoint), so the combination is refused instead. Ambient ADC does
        // not trip this — only explicit driver options do. The quota (billing) project is refused
        // on the same grounds: the emulator ignores it, so it would be silently dropped.
        if emulator {
            let cause = if self.emulator {
                "the `spanner.emulator` option"
            } else {
                "the `SPANNER_EMULATOR_HOST` environment variable"
            };
            if let Some(option) = self.explicit_credential_option() {
                return Err(invalid_state(format!(
                    "emulator mode (enabled by {cause}) forces anonymous plaintext credentials \
                     and would silently ignore the configured `{option}` option; unset the \
                     credential option(s) or disable emulator mode"
                )));
            }
            if self.quota_project.is_some() {
                return Err(invalid_state(format!(
                    "emulator mode (enabled by {cause}) forces anonymous plaintext credentials \
                     and would silently ignore the configured `{OPTION_QUOTA_PROJECT}` option; \
                     unset it or disable emulator mode"
                )));
            }
        }

        // An explicit access token is a complete credential on its own — it *is* the bearer token,
        // not a way to obtain one — so it cannot be combined with a keyfile or impersonation, which
        // describe a *different* credential source. Reject the combination (naming the conflicting
        // option, in the emulator-guard style) rather than silently letting one path win.
        if self.access_token.is_some()
            && let Some(conflict) = self.conflicting_credential_with_access_token()
        {
            return Err(invalid_state(format!(
                "the `{OPTION_ACCESS_TOKEN}` option supplies a complete OAuth2 credential and \
                 cannot be combined with the `{conflict}` option; set only one"
            )));
        }

        // Read the key file, if any, before entering the runtime (blocking I/O). In emulator mode
        // the guard above guarantees the credential options are unset, so this stays `None` and
        // anonymous credentials win.
        let credentials_json = self.credentials_json()?;

        self.runtime.block_on(async move {
            let mut builder = Spanner::builder();
            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            // Inside the runtime on purpose — see `build_credentials`. `None` means no explicit
            // credential: Application Default Credentials, resolved by the client.
            if let Some(credentials) = self.build_credentials(emulator, credentials_json)? {
                builder = builder.with_credentials(credentials);
            }
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
                admin: Arc::new(OnceCell::new()),
            })
        })
    }

    /// Return the shared client stack, building it via [`Self::connect`] on first use.
    ///
    /// The expensive parts of `connect` — the gRPC channel pool, credential resolution, and the
    /// `CreateSession` RPC with its background session-maintenance task — are per-*database*
    /// costs, so the stack is cached here and cheaply [`Clone`]d into every connection (the
    /// clones share the multiplexed session and channels). `set_option` invalidates the cache. A
    /// failed build caches nothing, so the next connection attempt retries from scratch.
    fn connect_shared(&self) -> Result<Connected> {
        // Hold the lock across the build so two concurrent `new_connection` calls cannot build
        // the stack twice. This cannot deadlock: the lock is only ever taken on caller (sync
        // ADBC) threads — never by anything running *on* the shared runtime that `connect`'s
        // `block_on` drives — so the losing thread simply parks until the winner finishes.
        let mut cached = self.connected.lock().unwrap();
        if let Some(connected) = cached.as_ref() {
            return Ok(connected.clone());
        }
        let connected = self.connect()?;
        *cached = Some(connected.clone());
        Ok(connected)
    }
}

/// An established connection's handles: the data-plane [`DatabaseClient`], the [`Spanner`] client
/// (used to reach the Database Admin API for DDL), the resolved database path, and the shared
/// [Database Admin client cell](SharedDatabaseAdmin).
///
/// `Clone` is cheap by design — the client types share their channel pool and multiplexed session
/// across clones (and the admin cell its `Arc`) — which is what lets [`SpannerDatabase`] cache one
/// stack and hand a clone to every connection.
#[derive(Clone, Debug)]
pub(crate) struct Connected {
    pub(crate) client: DatabaseClient,
    pub(crate) spanner: Spanner,
    pub(crate) database: String,
    pub(crate) admin: SharedDatabaseAdmin,
}

/// The lazily-built [`DatabaseAdmin`] client (the DDL path — `UpdateDatabaseDdl`, including the
/// `CREATE TABLE` a create-mode ingest issues), shared via `Arc` by every connection and statement
/// minted from one cached [`Connected`] stack.
///
/// Like [`DatabaseClient`], `DatabaseAdmin` holds its connection pool behind an internal `Arc` and
/// its docs advise creating one and reusing it, so it is built **once** on the first DDL statement
/// (no admin connection is opened for workloads that never run DDL) and cheaply cloned thereafter.
/// Living inside [`Connected`] ties its lifetime to the data-plane stack's: when a database option
/// invalidates the cached stack, the rebuilt stack starts with a fresh empty cell, so the admin
/// client is rebuilt from the new endpoint/credentials too. A failed build caches nothing
/// (`get_or_try_init`), so the next DDL statement retries from scratch.
pub(crate) type SharedDatabaseAdmin = Arc<OnceCell<DatabaseAdmin>>;

impl Optionable for SpannerDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        // Any database option can change the endpoint / credentials / database path the cached
        // client stack was built from, so drop it up front (even if the set fails below — a
        // spurious rebuild is harmless); the next connection rebuilds from the new configuration.
        *self.connected.get_mut().unwrap() = None;
        match &key {
            OptionDatabase::Uri => {
                let value = string_value(&key, value)?;
                self.set_uri_option(value)?
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
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_TARGET_PRINCIPAL => {
                self.impersonate_target_principal = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_DELEGATES => {
                self.impersonate_delegates = comma_separated(&string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_SCOPES => {
                self.impersonate_scopes = comma_separated(&string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_LIFETIME => {
                self.impersonate_lifetime_secs = Some(u64_seconds_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_ACCESS_TOKEN => {
                self.access_token = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_QUOTA_PROJECT => {
                // `""` unsets, back to the credential's own project (the house "" pattern).
                let project = string_value(&key, value)?;
                self.quota_project = (!project.is_empty()).then_some(project);
            }
            other => {
                return Err(not_implemented(&format!(
                    "unsupported Spanner database option: {}",
                    option_name(other)
                )));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        // The two secret-holding options are **write-only**: `spanner.auth.keyfile_json` is a full
        // service-account private key and `spanner.auth.access_token` a live bearer token, so
        // reading either back is always `NotFound` — whether set or not — and tooling that dumps
        // connection options can never print a usable credential (SEC-1). This mirrors the `Debug`
        // redaction of the same fields; `spanner.auth.keyfile` (a filesystem path, not a secret)
        // stays readable.
        if let OptionDatabase::Other(name) = &key
            && (name == OPTION_KEYFILE_JSON || name == OPTION_ACCESS_TOKEN)
        {
            return Err(err(
                format!("option {name} is write-only (it holds a secret) and cannot be read back"),
                Status::NotFound,
            ));
        }
        let value = match &key {
            // adbc.h: `GetOption` serves *the option value*, so the URI comes back exactly as
            // it was set; what it expanded into reads back under the expanded options' own keys.
            OptionDatabase::Uri => self.uri.clone(),
            OptionDatabase::Other(name) if name == OPTION_ENDPOINT => self.endpoint.clone(),
            OptionDatabase::Other(name) if name == OPTION_EMULATOR => {
                Some(self.emulator.to_string())
            }
            OptionDatabase::Other(name) if name == OPTION_KEYFILE => self.keyfile.clone(),
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_TARGET_PRINCIPAL => {
                self.impersonate_target_principal.clone()
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_DELEGATES => {
                (!self.impersonate_delegates.is_empty())
                    .then(|| self.impersonate_delegates.join(","))
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_SCOPES => {
                (!self.impersonate_scopes.is_empty()).then(|| self.impersonate_scopes.join(","))
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_LIFETIME => {
                self.impersonate_lifetime_secs.map(|secs| secs.to_string())
            }
            OptionDatabase::Other(name) if name == OPTION_QUOTA_PROJECT => {
                self.quota_project.clone()
            }
            _ => None,
        };
        value.ok_or_else(|| {
            err(
                format!("option {} is not set", option_name(&key)),
                Status::NotFound,
            )
        })
    }

    impl_typed_option_getters!();
}

impl Database for SpannerDatabase {
    type ConnectionType = SpannerConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(SpannerConnection::new(
            self.runtime.clone(),
            self.connect_shared()?,
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

fn option_name(key: &OptionDatabase) -> String {
    key.as_ref().to_string()
}

fn string_value(key: &OptionDatabase, value: OptionValue) -> Result<String> {
    crate::options::string_option(value, &format!("option {}", option_name(key)))
}

/// Split a comma-separated option value (delegates, scopes) into a list, trimming surrounding
/// whitespace and dropping empty entries so a trailing comma or spaces are harmless.
fn comma_separated(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Parse an option carrying a non-negative integer number of seconds (the impersonation lifetime).
/// Accepts an integer option value directly or a numeric string; anything else is rejected with a
/// clear `InvalidArguments` error.
fn u64_seconds_value(key: &OptionDatabase, value: OptionValue) -> Result<u64> {
    match value {
        OptionValue::Int(seconds) if seconds >= 0 => Ok(seconds as u64),
        OptionValue::String(seconds) => seconds.trim().parse::<u64>().map_err(|_| {
            invalid_argument(format!(
                "option {} expects a non-negative integer number of seconds, got {seconds:?}",
                option_name(key)
            ))
        }),
        _ => Err(invalid_argument(format!(
            "option {} expects a non-negative integer number of seconds",
            option_name(key)
        ))),
    }
}

fn bool_value(key: &OptionDatabase, value: OptionValue) -> Result<bool> {
    crate::options::bool_option(value, &format!("option {}", option_name(key)))
}

#[cfg(test)]
mod tests;

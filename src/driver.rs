//! The [`SpannerDriver`] and [`SpannerDatabase`] — the two top levels of the ADBC hierarchy.

use adbc_core::error::{Result, Status};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};
use google_cloud_auth::credentials::Builder as AdcCredentials;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_auth::credentials::external_account::Builder as ExternalAccountCredentials;
use google_cloud_auth::credentials::impersonated::Builder as ImpersonatedCredentials;
use google_cloud_auth::credentials::service_account::Builder as ServiceAccountCredentials;
use google_cloud_auth::credentials::user_account::Builder as UserAccountCredentials;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use http::header::{AUTHORIZATION, HeaderValue};
use http::{Extensions, HeaderMap};

use crate::connection::SpannerConnection;
use crate::error::{
    err, from_builder, from_spanner, invalid_argument, invalid_state, not_implemented,
};
use crate::runtime::{SharedRuntime, new_runtime};
use crate::{
    OPTION_ACCESS_TOKEN, OPTION_DATABASE, OPTION_EMULATOR, OPTION_ENDPOINT,
    OPTION_IMPERSONATE_DELEGATES, OPTION_IMPERSONATE_LIFETIME, OPTION_IMPERSONATE_SCOPES,
    OPTION_IMPERSONATE_TARGET_PRINCIPAL, OPTION_KEYFILE, OPTION_KEYFILE_JSON,
};
use std::time::Duration;

/// The default lifetime, in seconds, of an impersonated access token when
/// [`OPTION_IMPERSONATE_LIFETIME`] is left unset — one hour, matching the `google-cloud-auth`
/// `impersonated` builder's own default (and gcloud's `--lifetime` default).
const DEFAULT_IMPERSONATION_LIFETIME_SECS: u64 = 3600;

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
/// [`Debug`] is hand-written rather than derived so the three credential fields (`keyfile`,
/// `keyfile_json` — a full service-account private key — and `access_token` — a live OAuth bearer
/// token) never render in cleartext: each is shown as `Some("<redacted>")` / `None`, exposing only
/// presence, never the secret. This mirrors `StaticTokenCredentials`, whose token lives in a
/// sensitive `HeaderValue` for the same reason.
pub struct SpannerDatabase {
    runtime: SharedRuntime,
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
    /// Optional impersonated-token lifetime in seconds (`None` = [`DEFAULT_IMPERSONATION_LIFETIME_SECS`]).
    impersonate_lifetime_secs: Option<u64>,
    /// A caller-supplied OAuth 2.0 access token. When `Some`, the driver authenticates with this
    /// bearer token directly (no refresh); it is mutually exclusive with the keyfile/impersonation
    /// options and with emulator mode.
    access_token: Option<String>,
}

impl std::fmt::Debug for SpannerDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the three credential fields: show presence (`Some("<redacted>")` / `None`) but
        // never the secret value (`keyfile_json` is a private key, `access_token` a live bearer
        // token). Every other field renders normally.
        let redact = |value: &Option<String>| value.as_ref().map(|_| "<redacted>");
        f.debug_struct("SpannerDatabase")
            .field("runtime", &self.runtime)
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
            .finish()
    }
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
            impersonate_target_principal: None,
            impersonate_delegates: Vec::new(),
            impersonate_scopes: Vec::new(),
            impersonate_lifetime_secs: None,
            access_token: None,
        }
    }

    /// Handle a value set through the standard `uri` option or its [`OPTION_DATABASE`] alias.
    ///
    /// Two forms are accepted:
    ///
    /// - A bare database path, `projects/<p>/instances/<i>/databases/<d>` — stored verbatim, exactly
    ///   as before connection URIs existed.
    /// - A **connection URI**, recognised by a `spanner:` scheme — parsed by
    ///   [`parse_connection_uri`] and *expanded immediately* into the underlying option fields, as
    ///   if each part had been passed as an individual database option.
    ///
    /// Because the URI is expanded eagerly at `set_option` time, option precedence is purely
    /// **last-writer-wins and order-deterministic**: an explicit option set *after* the URI
    /// overrides what the URI carried, and setting the URI *after* an explicit option overwrites
    /// only the fields the URI actually carries (its path, its `//host` authority, and its query
    /// parameters — in that order, so a `spanner.endpoint` query parameter beats the authority).
    ///
    /// `get_option("uri")` intentionally returns the stored **database path**, not a reconstruction
    /// of the full URI; the expanded options are readable under their own keys.
    ///
    /// The whole URI is validated before any field is mutated, so a rejected URI leaves the
    /// configuration untouched.
    fn set_database_or_uri(&mut self, value: String) -> Result<()> {
        match connection_uri_remainder(&value) {
            Some(remainder) => self.apply_connection_uri(remainder),
            None => {
                self.database = Some(value);
                Ok(())
            }
        }
    }

    /// Expand a parsed connection URI (see [`parse_connection_uri`]) into this database's option
    /// fields: path → database, authority → [`OPTION_ENDPOINT`], query parameters → the options
    /// they name (validated against a scratch instance first, so failure leaves `self` unchanged).
    fn apply_connection_uri(&mut self, remainder: &str) -> Result<()> {
        let parsed = parse_connection_uri(remainder)?;
        // Dry-run the query parameters against a scratch database so a bad *value* (e.g.
        // `spanner.emulator=maybe`) is caught before `self` is touched at all.
        let mut scratch = SpannerDatabase::new(self.runtime.clone());
        for (key, value) in &parsed.params {
            scratch.set_option(
                OptionDatabase::Other(key.clone()),
                OptionValue::String(value.clone()),
            )?;
        }

        self.database = Some(parsed.database);
        if let Some(endpoint) = parsed.endpoint {
            self.endpoint = Some(endpoint);
        }
        for (key, value) in parsed.params {
            // Cannot fail: the identical calls just succeeded on `scratch`.
            self.set_option(OptionDatabase::Other(key), OptionValue::String(value))?;
        }
        Ok(())
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

    /// The name of the first explicitly-configured credential option, if any.
    ///
    /// Only *driver-level* credential configuration counts: a keyfile (path or inline JSON), an
    /// impersonation target, or an explicit access token. Ambient Application Default Credentials
    /// (e.g. the
    /// `GOOGLE_APPLICATION_CREDENTIALS` environment variable or a gcloud login) are deliberately
    /// *not* reported — they are the environment's business, not an explicit driver option, and
    /// must not prevent emulator use. The remaining `spanner.impersonate.*` options are inert
    /// without a target principal, so they do not count either.
    fn explicit_credential_option(&self) -> Option<&'static str> {
        if self.keyfile_json.is_some() {
            Some(OPTION_KEYFILE_JSON)
        } else if self.keyfile.is_some() {
            Some(OPTION_KEYFILE)
        } else if self.impersonate_target_principal.is_some() {
            Some(OPTION_IMPERSONATE_TARGET_PRINCIPAL)
        } else if self.access_token.is_some() {
            Some(OPTION_ACCESS_TOKEN)
        } else {
            None
        }
    }

    /// The name of the other explicit credential option that conflicts with an
    /// [`OPTION_ACCESS_TOKEN`], if any: a keyfile (path or inline JSON) or an impersonation target.
    /// An access token is a complete credential, so combining it with any of these is refused.
    fn conflicting_credential_with_access_token(&self) -> Option<&'static str> {
        if self.keyfile_json.is_some() {
            Some(OPTION_KEYFILE_JSON)
        } else if self.keyfile.is_some() {
            Some(OPTION_KEYFILE)
        } else if self.impersonate_target_principal.is_some() {
            Some(OPTION_IMPERSONATE_TARGET_PRINCIPAL)
        } else {
            None
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
                "Spanner database path is not set; provide the `uri` or \
                 `spanner.database` option (projects/<p>/instances/<i>/databases/<d>)",
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
        // not trip this — only explicit driver options do.
        if emulator && let Some(option) = self.explicit_credential_option() {
            let cause = if self.emulator {
                "the `spanner.emulator` option"
            } else {
                "the `SPANNER_EMULATOR_HOST` environment variable"
            };
            return Err(invalid_state(format!(
                "emulator mode (enabled by {cause}) forces anonymous plaintext credentials \
                 and would silently ignore the configured `{option}` option; unset the \
                 credential option(s) or disable emulator mode"
            )));
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

        // Resolve the credential JSON up front (reads the key file, if any); the flow is detected
        // from its `"type"` below. In emulator mode the guard above guarantees these are unset, so
        // both resolve to `None` and anonymous credentials win.
        let credentials_json = self.credentials_json()?;
        let access_token = self.access_token.clone();

        // Impersonation config, applied on top of the base credentials below when a target is set.
        let impersonate_target = self.impersonate_target_principal.clone();
        let impersonate_delegates = self.impersonate_delegates.clone();
        let impersonate_scopes = self.impersonate_scopes.clone();
        let impersonate_lifetime = Duration::from_secs(
            self.impersonate_lifetime_secs
                .unwrap_or(DEFAULT_IMPERSONATION_LIFETIME_SECS),
        );

        self.runtime.block_on(async move {
            let mut builder = Spanner::builder();
            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if emulator {
                builder = builder.with_credentials(AnonymousCredentials::new().build());
            } else if let Some(token) = access_token {
                // A caller-supplied OAuth2 bearer token, sent verbatim with no refresh. Mutual
                // exclusion with the keyfile/impersonation options was checked above.
                builder = builder.with_credentials(build_static_token_credentials(&token)?);
            } else if let Some(target) = impersonate_target {
                // Build the base credential exactly as the non-impersonated path does — an explicit
                // keyfile, or ADC when none is given — then wrap it so it is only used to mint a
                // short-lived token for `target` (optionally through a delegation chain).
                let source = match credentials_json {
                    Some(json) => build_credentials_from_json(&json)?,
                    None => AdcCredentials::default().build().map_err(|e| {
                        err(
                            format!(
                                "failed to build Application Default Credentials to impersonate \
                                 {target:?}: {}",
                                scrub_credential_error(&e)
                            ),
                            Status::InvalidArguments,
                        )
                    })?,
                };
                let credentials = build_impersonated_credentials(
                    source,
                    &target,
                    &impersonate_delegates,
                    &impersonate_scopes,
                    impersonate_lifetime,
                )?;
                builder = builder.with_credentials(credentials);
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
            OptionDatabase::Uri => {
                let value = string_value(&key, value)?;
                self.set_database_or_uri(value)?
            }
            OptionDatabase::Other(name) if name == OPTION_DATABASE => {
                let value = string_value(&key, value)?;
                self.set_database_or_uri(value)?
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
        let value = match &key {
            OptionDatabase::Uri => self.database.clone(),
            OptionDatabase::Other(name) if name == OPTION_DATABASE => self.database.clone(),
            OptionDatabase::Other(name) if name == OPTION_ENDPOINT => self.endpoint.clone(),
            OptionDatabase::Other(name) if name == OPTION_EMULATOR => {
                Some(self.emulator.to_string())
            }
            OptionDatabase::Other(name) if name == OPTION_KEYFILE => self.keyfile.clone(),
            OptionDatabase::Other(name) if name == OPTION_KEYFILE_JSON => self.keyfile_json.clone(),
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
            // Round-trips verbatim, matching the keyfile_json convention (which likewise returns the
            // stored secret unchanged); ADBC has no notion of a write-only option.
            OptionDatabase::Other(name) if name == OPTION_ACCESS_TOKEN => self.access_token.clone(),
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
        let what = format!("option {}", option_name(&key));
        crate::options::int_from_stored_string(self.get_option_string(key), &what)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let what = format!("option {}", option_name(&key));
        crate::options::double_from_stored_string(self.get_option_string(key), &what)
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

/// The database-level option names a connection URI may carry as query parameters.
///
/// Exactly the options that configure a [`SpannerDatabase`] besides the database path itself; the
/// path aliases (`uri` / [`OPTION_DATABASE`]) are deliberately absent — the URI's path component is
/// the one way to name the database. Unknown keys are rejected with `InvalidArguments`.
const URI_QUERY_OPTIONS: [&str; 9] = [
    OPTION_ENDPOINT,
    OPTION_EMULATOR,
    OPTION_KEYFILE,
    OPTION_KEYFILE_JSON,
    OPTION_IMPERSONATE_TARGET_PRINCIPAL,
    OPTION_IMPERSONATE_DELEGATES,
    OPTION_IMPERSONATE_SCOPES,
    OPTION_IMPERSONATE_LIFETIME,
    OPTION_ACCESS_TOKEN,
];

/// If `value` is a connection URI — it starts with the `spanner:` scheme (ASCII case-insensitive,
/// per RFC 3986) — return the remainder after the scheme. A bare database path (or any other
/// scheme) returns `None` and is used verbatim.
fn connection_uri_remainder(value: &str) -> Option<&str> {
    const SCHEME: &str = "spanner:";
    value
        .get(..SCHEME.len())
        .filter(|prefix| prefix.eq_ignore_ascii_case(SCHEME))
        .map(|_| &value[SCHEME.len()..])
}

/// The components of a parsed connection URI: the database path, the optional `//host` authority
/// (an endpoint), and the decoded query parameters in source order.
struct ParsedConnectionUri {
    database: String,
    endpoint: Option<String>,
    params: Vec<(String, String)>,
}

/// Parse the remainder of a `spanner:` connection URI (everything after the scheme), e.g.
///
/// ```text
/// spanner:///projects/p/instances/i/databases/d?spanner.endpoint=localhost:9010&spanner.emulator=true
/// spanner://emulator-host:9010/projects/p/instances/i/databases/d
/// ```
///
/// - The **path** must be a full database path, `projects/<p>/instances/<i>/databases/<d>`; a
///   leading `/` is tolerated (`spanner:projects/…`, `spanner:/projects/…` and
///   `spanner:///projects/…` are equivalent).
/// - An optional `//host[:port]` **authority** names the gRPC endpoint; it is taken verbatim as
///   the [`OPTION_ENDPOINT`] value.
/// - **Query parameters** are full driver option names from [`URI_QUERY_OPTIONS`]; unknown keys are
///   rejected. Keys and values are percent-decoded ([`percent_decode`]; `+` is *not* a space).
/// - A `#fragment` is meaningless here and rejected rather than silently dropped.
fn parse_connection_uri(remainder: &str) -> Result<ParsedConnectionUri> {
    let (remainder, fragment) = match remainder.split_once('#') {
        Some((rest, fragment)) => (rest, Some(fragment)),
        None => (remainder, None),
    };
    if fragment.is_some() {
        return Err(invalid_argument(
            "connection URI must not carry a #fragment",
        ));
    }

    let (before_query, query) = match remainder.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (remainder, None),
    };

    // `//authority/path`; an empty authority (`spanner:///…`) means "no endpoint". Without the
    // `//`, tolerate one leading `/` before the database path.
    let (authority, path) = match before_query.strip_prefix("//") {
        Some(after) => match after.split_once('/') {
            Some((authority, path)) => (Some(authority), path),
            None => (Some(after), ""),
        },
        None => (None, before_query.strip_prefix('/').unwrap_or(before_query)),
    };
    let endpoint = authority
        .filter(|authority| !authority.is_empty())
        .map(str::to_owned);

    let database = match path.split('/').collect::<Vec<_>>().as_slice() {
        ["projects", p, "instances", i, "databases", d]
            if !p.is_empty() && !i.is_empty() && !d.is_empty() =>
        {
            path.to_owned()
        }
        _ => {
            return Err(invalid_argument(format!(
                "connection URI path {path:?} is not a Spanner database path \
                 (projects/<project>/instances/<instance>/databases/<database>); note that in \
                 `spanner://projects/...` the `projects` segment is parsed as a host authority — \
                 write `spanner:///projects/...` (or `spanner:/projects/...`) when no endpoint \
                 host is intended"
            )));
        }
    };

    let mut params = Vec::new();
    for pair in query.unwrap_or("").split('&').filter(|s| !s.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        if !URI_QUERY_OPTIONS.contains(&key.as_str()) {
            return Err(invalid_argument(format!(
                "unknown connection URI query parameter {key:?}; supported parameters: {}",
                URI_QUERY_OPTIONS.join(", ")
            )));
        }
        params.push((key, percent_decode(value)?));
    }

    Ok(ParsedConnectionUri {
        database,
        endpoint,
        params,
    })
}

/// Percent-decode a connection-URI component (RFC 3986): each `%XX` hex escape becomes one byte,
/// everything else passes through unchanged. Notably `+` is **not** decoded to a space (that is the
/// `application/x-www-form-urlencoded` convention, not RFC 3986) — an inline keyfile JSON or an
/// RFC 3339 timestamp may legitimately contain a literal `+`. Malformed escapes and non-UTF-8
/// results are rejected with `InvalidArguments`.
fn percent_decode(s: &str) -> Result<String> {
    if !s.contains('%') {
        return Ok(s.to_owned());
    }
    let malformed = || {
        invalid_argument(format!(
            "malformed percent-encoding in connection URI component {s:?}"
        ))
    };
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3).ok_or_else(malformed)?;
            // `from_str_radix` tolerates a leading `+`/`-`, which is not valid percent-encoding.
            if !hex.iter().all(u8::is_ascii_hexdigit) {
                return Err(malformed());
            }
            let hex = std::str::from_utf8(hex).map_err(|_| malformed())?;
            out.push(u8::from_str_radix(hex, 16).map_err(|_| malformed())?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| {
        invalid_argument(format!(
            "connection URI component {s:?} percent-decodes to invalid UTF-8"
        ))
    })
}

fn option_name(key: &OptionDatabase) -> String {
    key.as_ref().to_string()
}

fn string_value(key: &OptionDatabase, value: OptionValue) -> Result<String> {
    crate::options::string_option(value, &format!("option {}", option_name(key)))
}

/// The credential `type` values we accept in a keyfile JSON, for use in error messages.
const SUPPORTED_CREDENTIAL_TYPES: &str =
    "service_account, authorized_user, impersonated_service_account, external_account";

/// Build Google credentials from an inline JSON key, auto-detecting the credential flow from the
/// JSON's top-level `"type"` field, as Google's own auth libraries (and gcloud) do.
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
/// happen here for the JSON supplied through the `spanner.keyfile` / `spanner.keyfile_json`
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
            format!(
                "failed to build {credential_type} credentials: {}",
                scrub_credential_error(&e)
            ),
            Status::InvalidArguments,
        )
    })
}

/// Reduce a `google-cloud-auth` credential-builder error to a fixed, secret-free category phrase.
///
/// The auth crate's own `Display` (and the `#[source]` chain behind it) is outside this crate's
/// control: its `Parsing` / `Loading` variants wrap the underlying `serde_json` error produced while
/// deserializing the credential JSON, which — depending on the failure mode, and on future versions
/// of the crate — can echo fragments of the very JSON it was reading (potentially `private_key` or
/// `refresh_token` material). So we never interpolate that `Display` into an ADBC error message.
/// Instead we classify the failure with the crate's own public predicates and surface only one of a
/// handful of fixed phrases, guaranteeing no key material can reach an error message regardless of
/// what the auth crate puts in its `Display` now or later. The credential *type* and (on the keyfile
/// path) the file *path* are still reported by the callers — those are user-supplied configuration,
/// not secrets — upholding the driver's rule that keyfile JSON bodies never appear in error
/// messages.
fn scrub_credential_error(error: &google_cloud_auth::build_errors::Error) -> &'static str {
    if error.is_missing_field() {
        "a required field is missing or has the wrong type"
    } else if error.is_parsing() {
        "the credential JSON could not be parsed"
    } else if error.is_unknown_type() {
        "the credential type is unknown or invalid"
    } else if error.is_not_supported() {
        "the credential type is not supported for this use"
    } else if error.is_loading() {
        "the credentials could not be loaded"
    } else {
        "the credentials could not be built"
    }
}

/// Wrap a base credential with service-account impersonation using the `google-cloud-auth`
/// `impersonated` builder.
///
/// The base credentials (built as usual from a keyfile or ADC) become the *source*: they are used to
/// call the IAM Credentials `generateAccessToken` API and mint a short-lived token for
/// `target_principal`. `delegates` is an optional delegation chain; `scopes` overrides the default
/// `cloud-platform` scope when non-empty; `lifetime` bounds the minted token. The `impersonate.*`
/// option group follows gcloud's `--impersonate-service-account` / this `impersonated` builder.
fn build_impersonated_credentials(
    source: Credentials,
    target_principal: &str,
    delegates: &[String],
    scopes: &[String],
    lifetime: Duration,
) -> Result<Credentials> {
    let mut builder = ImpersonatedCredentials::from_source_credentials(source)
        .with_target_principal(target_principal)
        .with_lifetime(lifetime);
    if !delegates.is_empty() {
        builder = builder.with_delegates(delegates.iter().cloned());
    }
    if !scopes.is_empty() {
        builder = builder.with_scopes(scopes.iter().cloned());
    }
    builder.build().map_err(|e| {
        err(
            format!(
                "failed to build impersonated credentials for {target_principal:?}: {}",
                scrub_credential_error(&e)
            ),
            Status::InvalidArguments,
        )
    })
}

/// A minimal `google-cloud-auth` [`Credentials`] backed by a fixed, caller-supplied OAuth 2.0
/// bearer token.
///
/// The pinned auth crate ships no static-token credential builder, so we implement the public
/// [`CredentialsProvider`] trait directly: every request gets the same pre-built
/// `Authorization: Bearer <token>` header, and there is no refresh — the caller owns token
/// validity. The `Authorization` header value is marked sensitive so it is redacted from any
/// header logging the transport might do.
#[derive(Debug)]
struct StaticTokenCredentials {
    /// The pre-built headers (`Authorization: Bearer <token>`), returned verbatim on every call.
    headers: HeaderMap,
    /// A stable cache tag so callers using the `EntityTag` fast-path see "not modified" — the token
    /// never changes for the lifetime of these credentials.
    entity_tag: EntityTag,
}

impl CredentialsProvider for StaticTokenCredentials {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> std::result::Result<
        CacheableResource<HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        match extensions.get::<EntityTag>() {
            Some(tag) if self.entity_tag.eq(tag) => Ok(CacheableResource::NotModified),
            _ => Ok(CacheableResource::New {
                data: self.headers.clone(),
                entity_tag: self.entity_tag.clone(),
            }),
        }
    }

    async fn universe_domain(&self) -> Option<String> {
        // `None` means the default `googleapis.com` universe.
        None
    }
}

/// Build [`Credentials`] that authenticate with a fixed OAuth 2.0 bearer token.
///
/// The token is pre-formatted into an `Authorization: Bearer <token>` header once, here, so a
/// malformed token (one carrying characters illegal in an HTTP header value) is rejected up front
/// with a clean `InvalidArguments` — and the token itself is never interpolated into the error, so
/// no token material can leak (the `scrub_credential_error` discipline).
fn build_static_token_credentials(token: &str) -> Result<Credentials> {
    let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
        invalid_argument(format!(
            "the `{OPTION_ACCESS_TOKEN}` option contains characters that are not valid in an HTTP \
             Authorization header value"
        ))
    })?;
    value.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, value);
    Ok(Credentials::from(StaticTokenCredentials {
        headers,
        entity_tag: EntityTag::new(),
    }))
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
            OptionValue::String("projects/p/instances/i/databases/d".into()),
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
    fn access_token_option_round_trips() {
        // Matches the keyfile_json convention: the stored token round-trips verbatim through
        // get_option (ADBC has no write-only option), and is unset by default.
        let mut db = new_database();
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()))
                .unwrap_err()
                .status,
            Status::NotFound
        );
        db.set_option(
            OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()),
            OptionValue::String("ya29.a-bearer-token".into()),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()))
                .unwrap(),
            "ya29.a-bearer-token"
        );
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
        let credentials = build_static_token_credentials("ya29.the-token").unwrap();
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
        let error = build_static_token_credentials(TOKEN).unwrap_err();
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

    // Only explicit driver options count as credentials: a fresh database (which would fall back to
    // ambient ADC, e.g. GOOGLE_APPLICATION_CREDENTIALS) reports none, so plain emulator use — the
    // integration-test path — is not refused. Inert `spanner.impersonate.*` options (no target
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
        assert!(
            error
                .message
                .contains("failed to build service_account credentials")
        );
    }

    // A credential-build failure must never echo the credential JSON body into the ADBC error
    // message: the auth crate's `Display` (which we no longer interpolate — see
    // `scrub_credential_error`) can carry `serde_json`-derived fragments of the input, and the
    // input holds the private key. Here a `service_account` key carries a recognizable fake secret
    // but omits the required `client_email`, so `.build()` fails; the surfaced message must name the
    // detected type and a safe category, and must not contain the secret material.
    #[test]
    fn credential_build_failure_never_leaks_key_material() {
        const SECRET: &str = "SUPER-SECRET-PRIVATE-KEY-DO-NOT-LEAK-abc123";
        let json = format!(
            "{{\"type\":\"service_account\",\"private_key\":\"{SECRET}\",\"private_key_id\":42}}"
        );
        let error = build_credentials_from_json(&json).unwrap_err();
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
            let source = build_credentials_from_json(source_json).unwrap();
            let result = build_impersonated_credentials(
                source,
                "target@project.iam.gserviceaccount.com",
                &["delegate@project.iam.gserviceaccount.com".to_string()],
                &["https://www.googleapis.com/auth/cloud-platform".to_string()],
                Duration::from_secs(1200),
            );
            assert!(result.is_ok());
        });
    }

    // --- Connection URIs (`spanner:` scheme with query-parameter options) ---

    const DB_PATH: &str = "projects/p/instances/i/databases/d";

    fn set_uri(db: &mut SpannerDatabase, uri: &str) -> Result<()> {
        db.set_option(OptionDatabase::Uri, OptionValue::String(uri.into()))
    }

    #[test]
    fn a_bare_database_path_is_stored_verbatim() {
        // The pre-URI form keeps working exactly as before, even with URI-ish characters in it.
        let mut db = new_database();
        set_uri(&mut db, DB_PATH).unwrap();
        assert_eq!(db.database.as_deref(), Some(DB_PATH));
        assert_eq!(db.endpoint, None);
        assert!(!db.emulator);
        // Not a recognised scheme → not parsed as a URI, stored as-is (and rejected only later, by
        // Spanner itself).
        let odd = "projects/p/instances/i/databases/d?x=y";
        set_uri(&mut db, odd).unwrap();
        assert_eq!(db.database.as_deref(), Some(odd));
    }

    #[test]
    fn a_scheme_uri_sets_the_database_path() {
        // All the tolerated path spellings: no slash, one slash, and an empty `//` authority.
        for uri in [
            format!("spanner:{DB_PATH}"),
            format!("spanner:/{DB_PATH}"),
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
    fn a_cloudspanner_scheme_is_not_recognised() {
        // Only `spanner:` is a connection-URI scheme; `cloudspanner:` (the JDBC convention) is
        // deliberately not supported. Like any other unknown scheme it is not parsed as a URI —
        // the value is stored verbatim (and rejected only later, by Spanner itself).
        let mut db = new_database();
        let uri = format!("cloudspanner:///{DB_PATH}?spanner.emulator=true");
        set_uri(&mut db, &uri).unwrap();
        assert_eq!(db.database.as_deref(), Some(uri.as_str()));
        assert_eq!(db.endpoint, None);
        assert!(!db.emulator);
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
    fn every_database_level_option_is_accepted_as_a_query_parameter() {
        let mut db = new_database();
        set_uri(
            &mut db,
            &format!(
                "spanner:///{DB_PATH}\
                 ?spanner.keyfile=/path/key.json\
                 &spanner.keyfile_json=%7B%22type%22%3A%22service_account%22%7D\
                 &spanner.impersonate.target_principal=target%40p.iam.gserviceaccount.com\
                 &spanner.impersonate.delegates=a%40p.iam.gserviceaccount.com,b%40p.iam.gserviceaccount.com\
                 &spanner.impersonate.scopes=https://www.googleapis.com/auth/cloud-platform\
                 &spanner.impersonate.lifetime=900\
                 &spanner.access_token=ya29.uri-token"
            ),
        )
        .unwrap();
        assert_eq!(db.keyfile.as_deref(), Some("/path/key.json"));
        assert_eq!(db.access_token.as_deref(), Some("ya29.uri-token"));
        assert_eq!(
            db.keyfile_json.as_deref(),
            Some("{\"type\":\"service_account\"}")
        );
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
        // The database-path aliases are not query parameters either — the URI path is the one way
        // to name the database.
        for alias in ["uri", OPTION_DATABASE] {
            let error = set_uri(&mut db, &format!("spanner:///{DB_PATH}?{alias}=x")).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(error.message.contains(alias));
        }
    }

    #[test]
    fn a_rejected_uri_leaves_the_configuration_untouched() {
        let mut db = new_database();
        set_uri(&mut db, DB_PATH).unwrap();
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
            "spanner://host:9010/projects/p2/instances/i2/databases/d2?spanner.keyfile=%G1"
                .to_string(),
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
        // base64 in an inline keyfile JSON).
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
            "spanner:",
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
    fn get_option_uri_returns_the_database_path_after_a_uri() {
        // Documented: `get_option("uri")` reports the stored database path, not the original URI;
        // the expanded options round-trip under their own keys.
        let mut db = new_database();
        set_uri(
            &mut db,
            &format!(
                "spanner:///{DB_PATH}?spanner.endpoint=http://localhost:9010&spanner.emulator=yes"
            ),
        )
        .unwrap();
        assert_eq!(db.get_option_string(OptionDatabase::Uri).unwrap(), DB_PATH);
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(OPTION_DATABASE.into()))
                .unwrap(),
            DB_PATH
        );
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
    fn the_database_alias_also_accepts_a_connection_uri() {
        let mut db = new_database();
        db.set_option(
            OptionDatabase::Other(OPTION_DATABASE.into()),
            OptionValue::String(format!("spanner:///{DB_PATH}?spanner.emulator=1")),
        )
        .unwrap();
        assert_eq!(db.database.as_deref(), Some(DB_PATH));
        assert!(db.emulator);
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
}

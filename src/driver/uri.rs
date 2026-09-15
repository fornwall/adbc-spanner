//! The `spanner:` connection URI: parsing it into a database path, an endpoint authority and
//! query-parameter options, and expanding those onto a [`SpannerDatabase`] eagerly at
//! `set_option` time.

use adbc_core::Optionable;
use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};

use super::SpannerDatabase;
use crate::error::invalid_argument;
use crate::{
    OPTION_ACCESS_TOKEN, OPTION_EMULATOR, OPTION_ENDPOINT, OPTION_IMPERSONATE_DELEGATES,
    OPTION_IMPERSONATE_LIFETIME, OPTION_IMPERSONATE_SCOPES, OPTION_IMPERSONATE_TARGET_PRINCIPAL,
    OPTION_KEYFILE, OPTION_KEYFILE_JSON, OPTION_QUOTA_PROJECT,
};

impl SpannerDatabase {
    /// Handle a value set through the standard `uri` option.
    ///
    /// The value must be a **`spanner://` connection URI**: its path is the Spanner database path,
    /// an optional `//host:port` authority becomes the endpoint, and query parameters are
    /// database-level options — except the secret-holding ones ([`URI_SECRET_OPTIONS`]), which a
    /// URI may not carry at all. A bare database path is **rejected** — the `spanner://` scheme is
    /// required, matching the ADBC BigQuery driver, whose `uri` likewise requires the `bigquery://`
    /// scheme. The URI is parsed by [`parse_connection_uri`] and *expanded immediately* into the
    /// underlying option fields, as if each part had been passed as an individual database option.
    ///
    /// Because the URI is expanded eagerly at `set_option` time, option precedence is purely
    /// **last-writer-wins and order-deterministic**: an explicit option set *after* the URI
    /// overrides what the URI carried, and setting the URI *after* an explicit option overwrites
    /// only the fields the URI actually carries (its path, its `//host` authority, and its query
    /// parameters — in that order, so a `spanner.endpoint` query parameter beats the authority).
    ///
    /// The accepted URI is *also stored verbatim*, and that exact string — query parameters and all
    /// — is what `get_option("uri")` returns, as adbc.h requires of `GetOption` (it serves the
    /// option value). So a set/get pair round-trips, and replaying a dumped configuration lands in
    /// the same state; what the URI expanded into stays readable under the expanded options' own
    /// keys.
    ///
    /// The whole URI is validated, and expanded, before it is stored, so a rejected URI leaves the
    /// configuration untouched and is never retrievable through `get_option`.
    pub(super) fn set_uri_option(&mut self, value: String) -> Result<()> {
        let Some(remainder) = connection_uri_remainder(&value) else {
            return Err(invalid_argument(format!(
                "the `uri` option value {value:?} is not a `spanner://` connection URI: a bare \
                 database path or a foreign scheme is not accepted; write `spanner:///{value}` \
                 (three slashes, no endpoint host) or `spanner://<host:port>/{value}`"
            )));
        };
        self.apply_connection_uri(&value, remainder)?;
        self.uri = Some(value);
        Ok(())
    }

    /// Expand a parsed connection URI (see [`parse_connection_uri`]) into this database's option
    /// fields: path → database, authority → [`OPTION_ENDPOINT`], query parameters → the options
    /// they name (validated against a scratch instance first, so failure leaves `self` unchanged).
    fn apply_connection_uri(&mut self, uri: &str, remainder: &str) -> Result<()> {
        let parsed = parse_connection_uri(uri, remainder)?;
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
/// The options that configure a [`SpannerDatabase`] besides the database path itself, minus the
/// secret-holding ones ([`URI_SECRET_OPTIONS`]). The path key (`uri`) is deliberately absent — the
/// URI's path component is the one way to name the database. Unknown keys are rejected with
/// `InvalidArguments`.
const URI_QUERY_OPTIONS: [&str; 8] = [
    OPTION_ENDPOINT,
    OPTION_EMULATOR,
    OPTION_KEYFILE,
    OPTION_IMPERSONATE_TARGET_PRINCIPAL,
    OPTION_IMPERSONATE_DELEGATES,
    OPTION_IMPERSONATE_SCOPES,
    OPTION_IMPERSONATE_LIFETIME,
    OPTION_QUOTA_PROJECT,
];

/// The database options whose *value is a live secret*, and which a connection URI therefore may
/// **not** carry as a query parameter: [`OPTION_KEYFILE_JSON`] (a full service-account private key)
/// and [`OPTION_ACCESS_TOKEN`] (a live OAuth bearer token).
///
/// A URI is the most-logged configuration artifact there is — it lands in shell history, process
/// listings (`ps`), connection strings pasted into tickets, and tracing spans — so embedding a
/// secret in one leaks it far beyond the driver. These two keys are rejected with a message naming
/// the key and pointing at the option itself, which is the one supported way to supply them. The
/// same two keys are write-only for `get_option` and redacted in [`SpannerDatabase`]'s [`Debug`],
/// for the same reason; this closes the remaining path by which they could travel in cleartext.
///
/// [`OPTION_KEYFILE`] — a *path*, not a secret — stays accepted, as it does for `get_option`.
const URI_SECRET_OPTIONS: [&str; 2] = [OPTION_KEYFILE_JSON, OPTION_ACCESS_TOKEN];

/// If `value` starts with the `spanner:` scheme (ASCII case-insensitive, per RFC 3986) — return the
/// remainder after the scheme. Any other value (a bare database path, or a different scheme) returns
/// `None` and is rejected: the `uri` option requires a `spanner://` connection URI.
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
/// - The `//` is **required**, so the no-endpoint spelling is the three-slash `spanner:///…`
///   (empty authority); the single-slash `spanner:/…` and scheme-only `spanner:…` forms are
///   rejected, as [docs/options.md] documents.
/// - The **path** must be a full database path, `projects/<p>/instances/<i>/databases/<d>`.
/// - An optional `//host[:port]` **authority** names the gRPC endpoint; it is taken verbatim as
///   the [`OPTION_ENDPOINT`] value.
/// - **Query parameters** are full driver option names from [`URI_QUERY_OPTIONS`]; unknown keys are
///   rejected, as are the secret-holding keys of [`URI_SECRET_OPTIONS`] (which name a dedicated
///   error). Keys and values are percent-decoded ([`percent_decode`]; `+` is *not* a space).
/// - A `#fragment` is meaningless here and rejected rather than silently dropped.
///
/// `uri` is the whole option value, carried along only so the errors can quote what the caller set.
///
/// [docs/options.md]: https://github.com/fornwall/adbc-spanner/blob/main/docs/options.md#connection-uris
fn parse_connection_uri(uri: &str, remainder: &str) -> Result<ParsedConnectionUri> {
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

    // `//authority/path`; an empty authority (`spanner:///…`) means "no endpoint". The `//` is
    // required — a scheme-only `spanner:path` or single-slash `spanner:/path` is rejected, so the
    // accepted form is always `spanner://`.
    let after_authority = before_query.strip_prefix("//").ok_or_else(|| {
        invalid_argument(format!(
            "the `uri` option value {uri:?} does not use the required `spanner://` form: the \
             scheme must be followed by two slashes; write `spanner:///projects/...` (three \
             slashes) when no endpoint host is intended"
        ))
    })?;
    let (authority, path) = match after_authority.split_once('/') {
        Some((authority, path)) => (Some(authority), path),
        None => (Some(after_authority), ""),
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
                "the `uri` option's connection URI path {path:?} is not a Spanner database path: \
                 it must be projects/<project>/instances/<instance>/databases/<database>, and in \
                 `spanner://projects/...` the `projects` segment parses as a host authority; \
                 write `spanner:///projects/...` (three slashes) when no endpoint host is intended"
            )));
        }
    };

    let mut params = Vec::new();
    for pair in query.unwrap_or("").split('&').filter(|s| !s.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        // Refuse the secret-holding keys before the unknown-key check, so they get the specific
        // "why" rather than a misleading "unknown parameter".
        if URI_SECRET_OPTIONS.contains(&key.as_str()) {
            return Err(invalid_argument(format!(
                "connection URI query parameter {key:?} is not supported because its value is a \
                 secret, and a connection URI is routinely logged (shell history, process \
                 listings, tracing spans); set the `{key}` database option directly instead"
            )));
        }
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
/// `application/x-www-form-urlencoded` convention, not RFC 3986) — a keyfile path or an endpoint
/// may legitimately contain a literal `+`. Malformed escapes and non-UTF-8 results are rejected
/// with `InvalidArguments`.
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

#[cfg(test)]
mod tests;

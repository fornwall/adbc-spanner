//! The credential ladder: which of the mutually exclusive auth flows a [`SpannerDatabase`]
//! selects from its options, and how each one is built — keyfile JSON, impersonation, a static
//! access token, or Application Default Credentials.

use adbc_core::error::{Result, Status};
use google_cloud_auth::credentials::Builder as AdcCredentials;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_auth::credentials::external_account::Builder as ExternalAccountCredentials;
use google_cloud_auth::credentials::impersonated::Builder as ImpersonatedCredentials;
use google_cloud_auth::credentials::service_account::Builder as ServiceAccountCredentials;
use google_cloud_auth::credentials::user_account::Builder as UserAccountCredentials;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use http::{Extensions, HeaderMap};

use super::SpannerDatabase;
use crate::error::{err, invalid_argument};
use crate::{
    OPTION_ACCESS_TOKEN, OPTION_IMPERSONATE_TARGET_PRINCIPAL, OPTION_KEYFILE, OPTION_KEYFILE_JSON,
    OPTION_QUOTA_PROJECT,
};
use std::time::Duration;

/// The HTTP request header carrying the quota / billing project (`spanner.auth.quota_project`).
/// Matches the `google-cloud-auth` `QUOTA_PROJECT_KEY` that its `with_quota_project_id` emits, so
/// the credential-builder and the access-token paths attach the identical header.
const QUOTA_PROJECT_HEADER: &str = "x-goog-user-project";

/// The default lifetime, in seconds, of an impersonated access token when
/// [`OPTION_IMPERSONATE_LIFETIME`](crate::OPTION_IMPERSONATE_LIFETIME) is left unset — one hour, matching the `google-cloud-auth`
/// `impersonated` builder's own default (and gcloud's `--lifetime` default).
pub(super) const DEFAULT_IMPERSONATION_LIFETIME_SECS: u64 = 3600;

/// Which of the five mutually exclusive credential flows a [`SpannerDatabase`] uses, as selected
/// from its configured options by [`SpannerDatabase::credential_choice`] and built by
/// [`SpannerDatabase::build_credentials`].
///
/// Naming the choice splits the *ladder* — pure precedence rules, unit-tested offline — from
/// building the credential it names, which reaches for the ambient environment. Deliberately
/// payload-free: every variant's input is re-read from the database, so a bearer token never lands
/// in a `Debug`-derived type.
#[derive(Debug, PartialEq, Eq)]
enum CredentialChoice {
    /// Emulator mode: anonymous credentials over plaintext `http://`. Wins outright — the guards in
    /// [`SpannerDatabase::connect`] refuse emulator mode combined with any credential option, so
    /// nothing below it can be configured at the same time.
    Anonymous,
    /// [`OPTION_ACCESS_TOKEN`]: the caller's own bearer token. Outranks the two below only
    /// nominally — `connect` refuses that combination outright, so it is reachable alone.
    AccessToken,
    /// [`OPTION_IMPERSONATE_TARGET_PRINCIPAL`]: a keyfile-or-ADC base credential, wrapped to mint
    /// short-lived tokens for the target principal. Outranks [`Keyfile`](Self::Keyfile), which it
    /// *uses* as its base credential when both are set.
    Impersonate,
    /// [`OPTION_KEYFILE_JSON`] / [`OPTION_KEYFILE`]: an explicit credential built from key JSON.
    Keyfile,
    /// Application Default Credentials — no credential option was configured.
    Adc,
}

impl SpannerDatabase {
    /// Resolve the inline credential JSON to use, reading the key file if a path was given. The
    /// credential flow is auto-detected from the JSON's `"type"` field in [`build_credentials_from_json`].
    /// Inline JSON ([`OPTION_KEYFILE_JSON`]) takes precedence over a file path ([`OPTION_KEYFILE`]).
    pub(super) fn credentials_json(&self) -> Result<Option<String>> {
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
    /// are deliberately *not* reported — they are the environment's business, not an explicit
    /// driver option, and must not prevent emulator use.
    ///
    /// The ladder's order is load-bearing: [`OPTION_ACCESS_TOKEN`] comes last so that
    /// [`conflicting_credential_with_access_token`](Self::conflicting_credential_with_access_token)
    /// is this same ladder with the token filtered out.
    pub(super) fn explicit_credential_option(&self) -> Option<&'static str> {
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
    ///
    /// This is [`explicit_credential_option`](Self::explicit_credential_option) minus the token
    /// itself: that ladder reports [`OPTION_ACCESS_TOKEN`] *last*, hence only when no other
    /// credential option is set, so any other name it yields is by definition a conflict.
    pub(super) fn conflicting_credential_with_access_token(&self) -> Option<&'static str> {
        self.explicit_credential_option()
            .filter(|option| *option != OPTION_ACCESS_TOKEN)
    }

    /// Which credential flow this configuration selects; see [`CredentialChoice`]. `emulator` is
    /// the **resolved** emulator mode — the [`OPTION_EMULATOR`](crate::OPTION_EMULATOR) option *or* `SPANNER_EMULATOR_HOST`
    /// — not the raw option.
    fn credential_choice(&self, emulator: bool) -> CredentialChoice {
        if emulator {
            CredentialChoice::Anonymous
        } else if self.access_token.is_some() {
            CredentialChoice::AccessToken
        } else if self.impersonate_target_principal.is_some() {
            CredentialChoice::Impersonate
        } else if self.keyfile_json.is_some() || self.keyfile.is_some() {
            CredentialChoice::Keyfile
        } else {
            CredentialChoice::Adc
        }
    }

    /// Build the credential [`credential_choice`](Self::credential_choice) selects — the *how* to
    /// that ladder's *which* — or `None` to leave the client to resolve Application Default
    /// Credentials itself. `credentials_json` is the already-read key JSON, whose flow is detected
    /// from its `"type"` in [`build_credentials_from_json`].
    ///
    /// Must run inside the Tokio runtime: the `google-cloud-auth` builders spawn token-cache tasks.
    /// None of them does I/O here — the first token is only fetched on use.
    pub(super) fn build_credentials(
        &self,
        emulator: bool,
        credentials_json: Option<String>,
    ) -> Result<Option<Credentials>> {
        // The quota / billing project, attached to whichever non-emulator credential wins below via
        // `with_quota_project_id` (ADC / keyfile / impersonation) or the `x-goog-user-project`
        // header (access token). `None` leaves the credential's own project in charge.
        let quota_project = self.quota_project.as_deref();
        match self.credential_choice(emulator) {
            // Anonymous over plaintext `http://`; `connect`'s guard has already refused every
            // credential option, so nothing is dropped here.
            CredentialChoice::Anonymous => Ok(Some(AnonymousCredentials::new().build())),
            // A caller-supplied OAuth2 bearer token, sent verbatim with no refresh (plus the
            // quota-project header, if any). Mutual exclusion with the keyfile/impersonation
            // options is checked in `connect`.
            CredentialChoice::AccessToken => self
                .access_token
                .as_deref()
                .map(|token| build_static_token_credentials(token, quota_project))
                .transpose(),
            CredentialChoice::Impersonate => self
                .impersonate_target_principal
                .as_deref()
                .map(|target| {
                    // Build the base credential exactly as the non-impersonated path does — an
                    // explicit keyfile, or ADC when none is given — then wrap it so it is only used
                    // to mint a short-lived token for `target` (optionally through a delegation
                    // chain). The quota project rides on the impersonated builder, not the source.
                    let source = match &credentials_json {
                        Some(json) => build_credentials_from_json(json, None)?,
                        None => AdcCredentials::default().build().map_err(|e| {
                            err(
                                format!(
                                    "failed to build Application Default Credentials to \
                                     impersonate {target:?}: {}",
                                    scrub_credential_error(&e)
                                ),
                                Status::InvalidArguments,
                            )
                        })?,
                    };
                    build_impersonated_credentials(
                        source,
                        target,
                        &self.impersonate_delegates,
                        &self.impersonate_scopes,
                        Duration::from_secs(
                            self.impersonate_lifetime_secs
                                .unwrap_or(DEFAULT_IMPERSONATION_LIFETIME_SECS),
                        ),
                        quota_project,
                    )
                })
                .transpose(),
            CredentialChoice::Keyfile => credentials_json
                .map(|json| build_credentials_from_json(&json, quota_project))
                .transpose(),
            // With a quota (billing) project, Application Default Credentials must be built
            // *explicitly* so `with_quota_project_id` can attach the `x-goog-user-project` header;
            // without one, `None` lets the client resolve ADC itself, with nothing to attach.
            CredentialChoice::Adc => quota_project
                .map(|project| {
                    AdcCredentials::default()
                        .with_quota_project_id(project)
                        .build()
                        .map_err(|e| {
                            err(
                                format!(
                                    "failed to build Application Default Credentials with quota \
                                     project {project:?}: {}",
                                    scrub_credential_error(&e)
                                ),
                                Status::InvalidArguments,
                            )
                        })
                })
                .transpose(),
        }
    }
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
/// happen here for the JSON supplied through the `spanner.auth.keyfile` / `spanner.auth.keyfile_json`
/// options — do not funnel every keyfile through the `service_account` builder instead: any other
/// type then fails or misbehaves.
///
/// `quota_project`, when `Some`, is attached to whichever builder is selected via its
/// `with_quota_project_id`, so the resulting credentials send the `x-goog-user-project` billing
/// header (the `spanner.auth.quota_project` option).
fn build_credentials_from_json(json: &str, quota_project: Option<&str>) -> Result<Credentials> {
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

    // Each credential-type branch has a differently-typed builder, all sharing a
    // `with_quota_project_id` method; this local macro applies the quota project uniformly.
    macro_rules! with_quota_project {
        ($builder:expr) => {{
            let builder = $builder;
            match quota_project {
                Some(project) => builder.with_quota_project_id(project),
                None => builder,
            }
        }};
    }
    let result = match credential_type.as_str() {
        "service_account" => with_quota_project!(ServiceAccountCredentials::new(value)).build(),
        "authorized_user" => with_quota_project!(UserAccountCredentials::new(value)).build(),
        "impersonated_service_account" => {
            with_quota_project!(ImpersonatedCredentials::new(value)).build()
        }
        "external_account" => with_quota_project!(ExternalAccountCredentials::new(value)).build(),
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
/// The auth crate's `Display` (and the `#[source]` chain behind it) wraps the `serde_json` error
/// from deserializing the credential JSON, which can echo fragments of it (potentially
/// `private_key` or `refresh_token` material). So it is never interpolated into an ADBC error
/// message; the failure is classified with the crate's own public predicates into one of a handful
/// of fixed phrases instead, whatever that `Display` does now or later. The credential *type* and
/// (on the keyfile path) the file *path* are still reported by the callers — user-supplied
/// configuration, not secrets.
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
/// The base credentials (from a keyfile or ADC) become the *source*: they call the IAM Credentials
/// `generateAccessToken` API to mint a short-lived token for `target_principal`. `delegates` is an
/// optional delegation chain, `scopes` overrides the default `cloud-platform` scope when non-empty,
/// and `lifetime` bounds the minted token.
fn build_impersonated_credentials(
    source: Credentials,
    target_principal: &str,
    delegates: &[String],
    scopes: &[String],
    lifetime: Duration,
    quota_project: Option<&str>,
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
    // The quota project belongs on the final (impersonated) credential; the auth crate gives the
    // builder value precedence over any carried by the source.
    if let Some(project) = quota_project {
        builder = builder.with_quota_project_id(project);
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
/// `Authorization: Bearer <token>` header and there is no refresh. The header value is marked
/// sensitive so it is redacted from any header logging the transport might do.
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
/// malformed token is rejected up front with a clean `InvalidArguments` — and the token itself is
/// never interpolated into the error (the `scrub_credential_error` discipline).
///
/// `quota_project`, when `Some`, adds the `x-goog-user-project` billing header, attached manually
/// since the static-token provider uses no builder. It is a bare project id, not a secret, so it is
/// not marked sensitive.
fn build_static_token_credentials(token: &str, quota_project: Option<&str>) -> Result<Credentials> {
    let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
        invalid_argument(format!(
            "the `{OPTION_ACCESS_TOKEN}` option contains characters that are not valid in an HTTP \
             Authorization header value"
        ))
    })?;
    value.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, value);
    if let Some(project) = quota_project {
        let project = HeaderValue::from_str(project).map_err(|_| {
            invalid_argument(format!(
                "the `{OPTION_QUOTA_PROJECT}` option contains characters that are not valid in an \
                 HTTP header value"
            ))
        })?;
        headers.insert(HeaderName::from_static(QUOTA_PROJECT_HEADER), project);
    }
    Ok(Credentials::from(StaticTokenCredentials {
        headers,
        entity_tag: EntityTag::new(),
    }))
}

#[cfg(test)]
mod tests;

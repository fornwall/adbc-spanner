//! Helpers for producing [`adbc_core`] errors and translating Spanner client errors.

use adbc_core::error::{Error, Status};

/// Build an ADBC error with the given message and status.
pub(crate) fn err(message: impl Into<String>, status: Status) -> Error {
    Error::with_message_and_status(message, status)
}

/// A `NotImplemented` error for functionality this driver does not (yet) support.
pub(crate) fn not_implemented(what: &str) -> Error {
    err(
        format!("{what} is not supported by the Spanner ADBC driver"),
        Status::NotImplemented,
    )
}

/// An `InvalidState` error, used when the caller invokes an operation out of order
/// (for example executing a statement before setting its query).
pub(crate) fn invalid_state(message: impl Into<String>) -> Error {
    err(message, Status::InvalidState)
}

/// An `InvalidArguments` error.
pub(crate) fn invalid_argument(message: impl Into<String>) -> Error {
    err(message, Status::InvalidArguments)
}

/// Translate an error coming from the Spanner client into an ADBC error.
///
/// The Spanner preview client (and its LRO poller) surface every failure as
/// `google_cloud_spanner::Error` (a re-export of `google_cloud_gax::error::Error`). When that error
/// carries a gRPC status, we map its canonical code onto the closest [`Status`] variant so callers
/// can distinguish, say, "table not found" from a backend failure instead of collapsing everything
/// to [`Status::Internal`]. Errors without a status (transport/serialization/etc.) fall back to
/// [`Status::Internal`].
///
/// The error text is passed through [`redact_url_query`] first so that any credential-bearing URL
/// query strings (OAuth/token endpoints, signed URLs, ...) are not stored in the message.
pub(crate) fn from_spanner(error: google_cloud_spanner::Error) -> Error {
    // `Error::status()` yields the structured gRPC status when present. We match on the code's
    // canonical name (e.g. "NOT_FOUND"), which is derived from the enum by the client itself — this
    // is not fragile string-parsing of a `Display` message, and it avoids taking a direct
    // dependency on the transitive `google-cloud-gax` crate just to name its `Code` enum.
    let status = error
        .status()
        .map(|status| status_for_grpc_code(status.code.name()))
        .unwrap_or(Status::Internal);
    err(
        format!("Spanner error: {}", redact_url_query(&error.to_string())),
        status,
    )
}

/// Translate a Spanner *client/admin builder* construction error into an ADBC error.
///
/// The top-level `Spanner` client builder and the admin builders fail with
/// `google_cloud_gax::client_builder::Error`, a distinct type that (unlike a service
/// [`google_cloud_spanner::Error`]) carries no gRPC status — it reports credential, transport or
/// universe-domain-mismatch setup problems. It has no code to map, so these collapse to
/// [`Status::Internal`]. Kept generic over [`std::fmt::Display`] so we do not need a direct
/// dependency on the transitive `google-cloud-gax` crate just to name the builder error type.
///
/// As with [`from_spanner`], the error text is passed through [`redact_url_query`] first so that
/// credential-bearing URL query strings do not leak into the stored message.
pub(crate) fn from_builder<E: std::fmt::Display>(error: E) -> Error {
    err(
        format!("Spanner error: {}", redact_url_query(&error.to_string())),
        Status::Internal,
    )
}

/// Strip the query string from any `http://` / `https://` URL found in `msg`.
///
/// Google API / OAuth errors can embed URLs whose query string carries access tokens or other
/// credentials; surfacing that raw text in an ADBC error (which callers commonly log) would leak
/// the secret. As a defensive measure we drop everything from the `?` up to the next whitespace,
/// leaving the scheme, host and path intact. This mirrors what the BigQuery Go driver does by
/// blanking `url.RawQuery` before surfacing errors.
///
/// This is a deliberately small, conservative hand-rolled scan rather than a full URL parser: it
/// only touches text that begins with a recognised scheme and leaves everything else untouched.
pub(crate) fn redact_url_query(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    let mut rest = msg;
    while let Some(idx) = find_scheme(rest) {
        // Copy everything up to the start of the URL verbatim.
        out.push_str(&rest[..idx]);
        let url = &rest[idx..];
        // The URL ends at the first whitespace character (or the end of the string).
        let url_end = url.find(char::is_whitespace).unwrap_or(url.len());
        let (url, after) = (&url[..url_end], &url[url_end..]);
        match url.find('?') {
            Some(q) => out.push_str(&url[..q]),
            None => out.push_str(url),
        }
        rest = after;
    }
    out.push_str(rest);
    out
}

/// Find the byte index of the next `http://` or `https://` scheme in `s`, if any.
fn find_scheme(s: &str) -> Option<usize> {
    let http = s.find("http://");
    let https = s.find("https://");
    match (http, https) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

/// Map a canonical gRPC status code name onto the closest ADBC [`Status`].
///
/// Factored out from [`from_spanner`] as a pure function so the mapping can be unit-tested without
/// constructing real gax error values. Codes with no closely matching ADBC variant (and the
/// unexpected `OK`) fall back to [`Status::Internal`].
fn status_for_grpc_code(code_name: &str) -> Status {
    match code_name {
        "NOT_FOUND" => Status::NotFound,
        "ALREADY_EXISTS" => Status::AlreadyExists,
        // ADBC distinguishes the two: failed authentication vs. an authenticated-but-forbidden call.
        "UNAUTHENTICATED" => Status::Unauthenticated,
        "PERMISSION_DENIED" => Status::Unauthorized,
        "INVALID_ARGUMENT" | "OUT_OF_RANGE" => Status::InvalidArguments,
        // "The preconditions for the operation are not met" — matches ADBC's InvalidState.
        "FAILED_PRECONDITION" => Status::InvalidState,
        "DEADLINE_EXCEEDED" => Status::Timeout,
        "CANCELLED" => Status::Cancelled,
        // ADBC's IO status documents "a remote service may be unavailable".
        "UNAVAILABLE" => Status::IO,
        // ABORTED, RESOURCE_EXHAUSTED, INTERNAL, UNKNOWN, DATA_LOSS, OK and anything unrecognised.
        _ => Status::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_grpc_codes_to_adbc_status() {
        assert_eq!(status_for_grpc_code("NOT_FOUND"), Status::NotFound);
        assert_eq!(
            status_for_grpc_code("ALREADY_EXISTS"),
            Status::AlreadyExists
        );
        assert_eq!(
            status_for_grpc_code("UNAUTHENTICATED"),
            Status::Unauthenticated
        );
        assert_eq!(
            status_for_grpc_code("PERMISSION_DENIED"),
            Status::Unauthorized
        );
        assert_eq!(
            status_for_grpc_code("INVALID_ARGUMENT"),
            Status::InvalidArguments
        );
        assert_eq!(
            status_for_grpc_code("OUT_OF_RANGE"),
            Status::InvalidArguments
        );
        assert_eq!(
            status_for_grpc_code("FAILED_PRECONDITION"),
            Status::InvalidState
        );
        assert_eq!(status_for_grpc_code("DEADLINE_EXCEEDED"), Status::Timeout);
        assert_eq!(status_for_grpc_code("CANCELLED"), Status::Cancelled);
        assert_eq!(status_for_grpc_code("UNAVAILABLE"), Status::IO);
    }

    #[test]
    fn unmapped_and_unknown_codes_fall_back_to_internal() {
        for code in [
            "ABORTED",
            "RESOURCE_EXHAUSTED",
            "INTERNAL",
            "UNKNOWN",
            "DATA_LOSS",
            "OK",
            "",
        ] {
            assert_eq!(status_for_grpc_code(code), Status::Internal);
        }
    }

    #[test]
    fn maps_a_real_gax_status_error() {
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};
        use google_cloud_gax::error::Error as GaxError;

        let gax = GaxError::service(
            RpcStatus::default()
                .set_code(Code::NotFound)
                .set_message("Table not found: Nope"),
        );
        let adbc = from_spanner(gax);
        assert_eq!(adbc.status, Status::NotFound);
        assert!(adbc.message.starts_with("Spanner error:"));
    }

    #[test]
    fn redacts_query_string() {
        assert_eq!(
            redact_url_query(
                "failed calling https://oauth2.example.com/token?access_token=SECRET&x=1"
            ),
            "failed calling https://oauth2.example.com/token"
        );
    }

    #[test]
    fn leaves_url_without_query_unchanged() {
        let msg = "could not reach https://spanner.googleapis.com/v1/projects/p";
        assert_eq!(redact_url_query(msg), msg);
    }

    #[test]
    fn leaves_message_without_url_unchanged() {
        let msg = "deadline exceeded while running query (id=abc?def)";
        assert_eq!(redact_url_query(msg), msg);
    }

    #[test]
    fn preserves_text_after_url() {
        assert_eq!(
            redact_url_query(
                "auth error at https://accounts.google.com/o/oauth2/token?client_secret=SHH please retry"
            ),
            "auth error at https://accounts.google.com/o/oauth2/token please retry"
        );
    }

    #[test]
    fn redacts_plain_http_and_multiple_urls() {
        assert_eq!(
            redact_url_query("see http://a.example/p?t=1 and https://b.example/q?u=2 done"),
            "see http://a.example/p and https://b.example/q done"
        );
    }
}

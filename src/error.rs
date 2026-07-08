//! Helpers for producing [`adbc_core`] errors and translating Spanner client errors.

use adbc_core::error::{Error, Status};
use google_cloud_gax::error::rpc::StatusDetails;

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
/// to [`Status::Internal`] — and preserve the **numeric gRPC code** in the ADBC error's
/// `vendor_code`, so callers can recover exactly what failed (e.g. a retry loop looking for
/// `ABORTED` = 10) even where several codes share one ADBC status. Errors without a status
/// (transport/serialization/etc.) fall back to [`Status::Internal`] with `vendor_code` 0.
///
/// # Structured error details
///
/// A `google.rpc.Status` may also carry structured *details* — most importantly
/// `google.rpc.RetryInfo` on `ABORTED`, where Spanner recommends how long to back off before
/// retrying the aborted transaction. Each detail is forwarded into the ADBC error's `details`
/// vector as a `(key, value)` pair:
///
/// - **key** — the lowercased fully-qualified protobuf type name of the detail message, e.g.
///   `google.rpc.retryinfo`, `google.rpc.errorinfo`, `google.rpc.badrequest`, `google.rpc.help`.
///   This follows the Flight SQL / gRPC metadata key style; there is no `-bin` suffix because the
///   value is UTF-8 text, not binary protobuf (`-bin` marks binary values in that convention).
/// - **value** — the detail's **ProtoJSON** encoding as UTF-8 bytes, self-describing via its
///   `"@type"` field — i.e. the canonical JSON form of the `google.protobuf.Any` that carried the
///   detail on the wire, e.g. `{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"0.010s"}`.
///   ProtoJSON (rather than binary protobuf) because the preview client decodes details into
///   serde-modelled types whose only supported wire encoding is ProtoJSON; the format is a stable,
///   documented protobuf encoding that any JSON parser can consume.
///
/// Together with `vendor_code` this completes the retry story: a retry loop detects an exhausted
/// abort via `vendor_code == 10` (`ABORTED`) and, when present, honours the server-recommended
/// delay in the `google.rpc.retryinfo` detail's `retryDelay`. Errors without a gRPC status, and
/// statuses without details, leave `details` as `None` (never `Some(vec![])`).
pub(crate) fn from_spanner(error: google_cloud_spanner::Error) -> Error {
    // `Error::status()` yields the structured gRPC status when present. We match on the code's
    // canonical name (e.g. "NOT_FOUND"), which is derived from the enum by the client itself — this
    // is not fragile string-parsing of a `Display` message.
    let (status, vendor_code, details) = error
        .status()
        .map(|status| {
            (
                status_for_grpc_code(status.code.name()),
                status.code as i32,
                details_for_adbc(&status.details),
            )
        })
        .unwrap_or((Status::Internal, 0, None));
    let mut adbc = err(format!("Spanner error: {error}"), status);
    adbc.vendor_code = vendor_code;
    adbc.details = details;
    adbc
}

/// Map a gRPC status' `google.rpc.Status` details onto ADBC error details.
///
/// Returns `None` (rather than `Some(vec![])`) when nothing mapped, so an error without details
/// keeps the `details: None` shape callers expect. See [`from_spanner`] for the key/value format
/// contract. Serialization failures and details without a usable type name are skipped — a detail
/// is diagnostic garnish, never worth failing (or panicking) the error path for.
fn details_for_adbc(details: &[StatusDetails]) -> Option<Vec<(String, Vec<u8>)>> {
    let mapped: Vec<(String, Vec<u8>)> = details
        .iter()
        .filter_map(|detail| Some((detail_key(detail)?, serde_json::to_vec(detail).ok()?)))
        .collect();
    (!mapped.is_empty()).then_some(mapped)
}

/// The ADBC detail key for one `google.rpc.Status` detail: the lowercased fully-qualified
/// protobuf type name (e.g. `google.rpc.retryinfo`).
///
/// The well-known `google.rpc` detail types get static keys; an unrecognised detail
/// ([`StatusDetails::Other`]) derives its key from the `Any` type URL (the part after the final
/// `/`, lowercased), or is skipped if the URL is missing.
fn detail_key(detail: &StatusDetails) -> Option<String> {
    let name = match detail {
        StatusDetails::BadRequest(_) => "google.rpc.badrequest",
        StatusDetails::DebugInfo(_) => "google.rpc.debuginfo",
        StatusDetails::ErrorInfo(_) => "google.rpc.errorinfo",
        StatusDetails::Help(_) => "google.rpc.help",
        StatusDetails::LocalizedMessage(_) => "google.rpc.localizedmessage",
        StatusDetails::PreconditionFailure(_) => "google.rpc.preconditionfailure",
        StatusDetails::QuotaFailure(_) => "google.rpc.quotafailure",
        StatusDetails::RequestInfo(_) => "google.rpc.requestinfo",
        StatusDetails::ResourceInfo(_) => "google.rpc.resourceinfo",
        StatusDetails::RetryInfo(_) => "google.rpc.retryinfo",
        StatusDetails::Other(any) => {
            let url = any.type_url()?;
            return Some(url.rsplit('/').next().unwrap_or(url).to_ascii_lowercase());
        }
        // `StatusDetails` is #[non_exhaustive]; skip any detail kind added later rather than
        // inventing a key for a type we cannot name.
        _ => return None,
    };
    Some(name.to_string())
}

/// Translate a Spanner *client/admin builder* construction error into an ADBC error.
///
/// The top-level `Spanner` client builder and the admin builders fail with
/// `google_cloud_gax::client_builder::Error`, a distinct type that (unlike a service
/// [`google_cloud_spanner::Error`]) carries no gRPC status — it reports credential, transport or
/// universe-domain-mismatch setup problems. It has no code to map, so these collapse to
/// [`Status::Internal`]. Kept generic over [`std::fmt::Display`] so we do not need a direct
/// dependency on the transitive `google-cloud-gax` crate just to name the builder error type.
pub(crate) fn from_builder<E: std::fmt::Display>(error: E) -> Error {
    err(format!("Spanner error: {error}"), Status::Internal)
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
        // ABORTED is Spanner's routine "transaction contended, please retry" signal, seen by
        // callers only after the client's read/write runner has exhausted its retries. It is
        // transient and environmental, not a driver or database defect, so it maps to IO rather
        // than Internal (which reads as "driver bug"). ADBC has no closer variant; the exact code
        // survives in `vendor_code` (ABORTED = 10) for callers with their own retry logic.
        "ABORTED" => Status::IO,
        // RESOURCE_EXHAUSTED, INTERNAL, UNKNOWN, DATA_LOSS, OK and anything unrecognised.
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
        // Transient contention, not a driver/database defect: IO, not Internal.
        assert_eq!(status_for_grpc_code("ABORTED"), Status::IO);
    }

    #[test]
    fn unmapped_and_unknown_codes_fall_back_to_internal() {
        for code in [
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
        // The numeric gRPC code survives in vendor_code (NOT_FOUND = 5).
        assert_eq!(adbc.vendor_code, Code::NotFound as i32);
        assert_eq!(adbc.vendor_code, 5);
        // A status without details keeps details = None, not Some(vec![]).
        assert_eq!(adbc.details, None);
    }

    #[test]
    fn aborted_keeps_its_grpc_code_in_vendor_code() {
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};
        use google_cloud_gax::error::Error as GaxError;

        let gax = GaxError::service(
            RpcStatus::default()
                .set_code(Code::Aborted)
                .set_message("Transaction was aborted"),
        );
        let adbc = from_spanner(gax);
        // Retry loops can detect ABORTED (10) exactly, whatever the ADBC status says.
        assert_eq!(adbc.status, Status::IO);
        assert_eq!(adbc.vendor_code, Code::Aborted as i32);
        assert_eq!(adbc.vendor_code, 10);
    }

    #[test]
    fn errors_without_a_grpc_status_have_no_vendor_code() {
        use google_cloud_gax::error::Error as GaxError;
        let adbc = from_spanner(GaxError::deser("no structured status here"));
        assert_eq!(adbc.status, Status::Internal);
        assert_eq!(adbc.vendor_code, 0);
        // Non-service errors (transport, deserialization, ...) carry no details.
        assert_eq!(adbc.details, None);
    }

    /// Build a `StatusDetails` from its ProtoJSON (`Any`) encoding — the same wire shape the
    /// client itself deserializes, and the shape our mapped detail values re-serialize to.
    fn detail_from_json(value: serde_json::Value) -> StatusDetails {
        serde_json::from_value(value).expect("valid StatusDetails ProtoJSON")
    }

    #[test]
    fn aborted_forwards_retry_info_detail() {
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};
        use google_cloud_gax::error::Error as GaxError;

        let retry_info = serde_json::json!({
            "@type": "type.googleapis.com/google.rpc.RetryInfo",
            "retryDelay": "1s",
        });
        let gax = GaxError::service(
            RpcStatus::default()
                .set_code(Code::Aborted)
                .set_message("Transaction was aborted")
                .set_details([detail_from_json(retry_info.clone())]),
        );
        let adbc = from_spanner(gax);
        assert_eq!(adbc.status, Status::IO);
        assert_eq!(adbc.vendor_code, Code::Aborted as i32);

        let details = adbc.details.expect("RetryInfo detail forwarded");
        assert_eq!(details.len(), 1);
        let (key, value) = &details[0];
        assert_eq!(key, "google.rpc.retryinfo");
        // The value is the detail's ProtoJSON bytes, self-describing via "@type": a retry loop
        // can parse it with any JSON parser and honour the recommended delay.
        let parsed: serde_json::Value = serde_json::from_slice(value).expect("UTF-8 JSON value");
        assert_eq!(parsed, retry_info);
        assert_eq!(parsed["retryDelay"], "1s");
    }

    #[test]
    fn multiple_details_forward_in_order_under_typed_keys() {
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};
        use google_cloud_gax::error::Error as GaxError;

        let gax = GaxError::service(
            RpcStatus::default()
                .set_code(Code::InvalidArgument)
                .set_message("Bad query")
                .set_details([
                    detail_from_json(serde_json::json!({
                        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                        "reason": "TEST_REASON",
                        "domain": "spanner.googleapis.com",
                    })),
                    detail_from_json(serde_json::json!({
                        "@type": "type.googleapis.com/google.rpc.BadRequest",
                        "fieldViolations": [{"field": "sql", "description": "syntax error"}],
                    })),
                    detail_from_json(serde_json::json!({
                        "@type": "type.googleapis.com/google.rpc.Help",
                        "links": [{"description": "docs", "url": "https://example.invalid"}],
                    })),
                ]),
        );
        let adbc = from_spanner(gax);
        let details = adbc.details.expect("details forwarded");
        let keys: Vec<&str> = details.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "google.rpc.errorinfo",
                "google.rpc.badrequest",
                "google.rpc.help"
            ]
        );
        // Spot-check one payload round-trips its fields.
        let error_info: serde_json::Value = serde_json::from_slice(&details[0].1).unwrap();
        assert_eq!(error_info["reason"], "TEST_REASON");
        assert_eq!(error_info["domain"], "spanner.googleapis.com");
    }

    #[test]
    fn unrecognised_detail_keys_off_its_type_url() {
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};
        use google_cloud_gax::error::Error as GaxError;

        // Not one of the well-known google.rpc detail types: lands in StatusDetails::Other and
        // takes its key from the Any type URL (final path segment, lowercased).
        let custom = serde_json::json!({
            "@type": "type.googleapis.com/mycompany.CustomDetail",
            "foo": "bar",
        });
        let gax = GaxError::service(
            RpcStatus::default()
                .set_code(Code::Internal)
                .set_message("boom")
                .set_details([detail_from_json(custom.clone())]),
        );
        let adbc = from_spanner(gax);
        let details = adbc.details.expect("custom detail forwarded");
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].0, "mycompany.customdetail");
        let parsed: serde_json::Value = serde_json::from_slice(&details[0].1).unwrap();
        assert_eq!(parsed, custom);
    }
}

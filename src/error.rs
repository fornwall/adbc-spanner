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
/// to [`Status::Internal`] — and preserve the **numeric gRPC code** in the ADBC error's
/// `vendor_code`, so callers can recover exactly what failed (e.g. a retry loop looking for
/// `ABORTED` = 10) even where several codes share one ADBC status. Errors without a status
/// (transport/serialization/etc.) fall back to [`Status::Internal`] with `vendor_code` 0.
pub(crate) fn from_spanner(error: google_cloud_spanner::Error) -> Error {
    // `Error::status()` yields the structured gRPC status when present. We match on the code's
    // canonical name (e.g. "NOT_FOUND"), which is derived from the enum by the client itself — this
    // is not fragile string-parsing of a `Display` message.
    let (status, vendor_code) = error
        .status()
        .map(|status| (status_for_grpc_code(status.code.name()), status.code as i32))
        .unwrap_or((Status::Internal, 0));
    let mut adbc = err(format!("Spanner error: {error}"), status);
    adbc.vendor_code = vendor_code;
    adbc
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
    }
}

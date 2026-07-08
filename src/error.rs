//! Helpers for producing [`adbc_core`] errors and translating Spanner client errors.
//!
//! Every error also carries a coarse **SQLSTATE** class code (see [`sqlstate_for_status`]) so
//! ODBC/JDBC bridges layered on an ADBC driver manager get a meaningful five-character code
//! instead of the unset `00000`. The mapping is deliberately coarse — standard SQL:2011 class
//! codes plus the X/Open CLI codes ODBC expects — and is derived from the ADBC [`Status`] (with a
//! couple of gRPC-code refinements in [`from_spanner`]), so it is applied uniformly in the
//! centralized constructors and no ad-hoc error site can miss it.

use std::os::raw::c_char;

use adbc_core::error::{Error, Status};

/// Build an ADBC error with the given message and status.
///
/// Also stamps the coarse SQLSTATE for the status (see [`sqlstate_for_status`]); all other
/// constructors in this module funnel through here, so every driver error carries one.
pub(crate) fn err(message: impl Into<String>, status: Status) -> Error {
    let mut error = Error::with_message_and_status(message, status);
    error.sqlstate = sqlstate_for_status(status);
    error
}

/// Convert a five-byte ASCII SQLSTATE literal into the `[c_char; 5]` form `adbc_core` stores.
const fn sqlstate(code: &[u8; 5]) -> [c_char; 5] {
    [
        code[0] as c_char,
        code[1] as c_char,
        code[2] as c_char,
        code[3] as c_char,
        code[4] as c_char,
    ]
}

/// The coarse SQLSTATE class code for an ADBC [`Status`].
///
/// Codes are standard SQL:2011 class codes where one fits, and X/Open CLI (`HY…`/`…S0…`) codes —
/// the ones ODBC defines — where the standard has no class (sequence errors, cancellation,
/// timeouts, missing tables):
///
/// | Status | SQLSTATE | meaning |
/// |---|---|---|
/// | `NotImplemented` | `0A000` | feature not supported |
/// | `NotFound` | `42S02` | base table or view not found (X/Open CLI) |
/// | `AlreadyExists` | `42S01` | base table or view already exists (X/Open CLI) |
/// | `InvalidArguments` | `42000` | syntax error or access rule violation |
/// | `InvalidState` | `HY010` | function sequence error (CLI) |
/// | `InvalidData` | `22000` | data exception |
/// | `Integrity` | `23000` | integrity constraint violation |
/// | `IO` | `08000` | connection exception |
/// | `Cancelled` | `HY008` | operation canceled (CLI) |
/// | `Timeout` | `HYT00` | timeout expired (CLI) |
/// | `Unauthenticated` | `28000` | invalid authorization specification |
/// | `Unauthorized` | `42501` | insufficient privilege |
/// | `Internal` / `Unknown` | `HY000` | general error (CLI) |
///
/// `Ok` (never constructed as an error here) stays all-zeroes, the spec's "not set".
fn sqlstate_for_status(status: Status) -> [c_char; 5] {
    let code: &[u8; 5] = match status {
        Status::Ok => return [0; 5],
        Status::NotImplemented => b"0A000",
        Status::NotFound => b"42S02",
        Status::AlreadyExists => b"42S01",
        Status::InvalidArguments => b"42000",
        Status::InvalidState => b"HY010",
        Status::InvalidData => b"22000",
        Status::Integrity => b"23000",
        Status::IO => b"08000",
        Status::Cancelled => b"HY008",
        Status::Timeout => b"HYT00",
        Status::Unauthenticated => b"28000",
        Status::Unauthorized => b"42501",
        Status::Internal | Status::Unknown => b"HY000",
    };
    sqlstate(code)
}

/// A SQLSTATE refinement for gRPC codes whose status-derived default would mislead.
///
/// Only two codes need one: `OUT_OF_RANGE` maps to ADBC `InvalidArguments` (whose default is the
/// syntax-flavoured `42000`) but is a data exception → `22000`; `ABORTED` maps to `IO` (default
/// `08000`, a connection problem) but is Spanner's retryable transaction-contention signal → the
/// standard serialization failure `40001`. Everything else keeps the status-derived code.
fn sqlstate_for_grpc_code(code_name: &str) -> Option<[c_char; 5]> {
    match code_name {
        "OUT_OF_RANGE" => Some(sqlstate(b"22000")),
        "ABORTED" => Some(sqlstate(b"40001")),
        _ => None,
    }
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
    let (status, vendor_code, sqlstate) = error
        .status()
        .map(|status| {
            (
                status_for_grpc_code(status.code.name()),
                status.code as i32,
                sqlstate_for_grpc_code(status.code.name()),
            )
        })
        .unwrap_or((Status::Internal, 0, None));
    let mut adbc = err(format!("Spanner error: {error}"), status);
    adbc.vendor_code = vendor_code;
    // `err` stamped the status-derived SQLSTATE; a few gRPC codes carry a sharper one.
    if let Some(sqlstate) = sqlstate {
        adbc.sqlstate = sqlstate;
    }
    adbc
}

/// Translate a Spanner *client/admin builder* construction error into an ADBC error.
///
/// The top-level `Spanner` client builder and the admin builders fail with
/// `google_cloud_gax::client_builder::Error`, a distinct type that (unlike a service
/// [`google_cloud_spanner::Error`]) carries no gRPC status — it reports credential, transport or
/// universe-domain-mismatch setup problems. It has no code to map, so these collapse to
/// [`Status::Internal`], but since they are by construction connection-establishment failures the
/// SQLSTATE is the more telling `08001` ("client unable to establish connection") rather than
/// Internal's general `HY000`. Kept generic over [`std::fmt::Display`] so we do not need a direct
/// dependency on the transitive `google-cloud-gax` crate just to name the builder error type.
pub(crate) fn from_builder<E: std::fmt::Display>(error: E) -> Error {
    let mut adbc = err(format!("Spanner error: {error}"), Status::Internal);
    adbc.sqlstate = sqlstate(b"08001");
    adbc
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

    /// Render a `[c_char; 5]` SQLSTATE as a `String` for readable assertions.
    fn sqlstate_str(state: [c_char; 5]) -> String {
        state.iter().map(|&c| char::from(c as u8)).collect()
    }

    #[test]
    fn every_status_gets_its_coarse_sqlstate() {
        for (status, expected) in [
            (Status::NotImplemented, "0A000"),
            (Status::NotFound, "42S02"),
            (Status::AlreadyExists, "42S01"),
            (Status::InvalidArguments, "42000"),
            (Status::InvalidState, "HY010"),
            (Status::InvalidData, "22000"),
            (Status::Integrity, "23000"),
            (Status::IO, "08000"),
            (Status::Cancelled, "HY008"),
            (Status::Timeout, "HYT00"),
            (Status::Unauthenticated, "28000"),
            (Status::Unauthorized, "42501"),
            (Status::Internal, "HY000"),
            (Status::Unknown, "HY000"),
        ] {
            assert_eq!(
                sqlstate_str(sqlstate_for_status(status)),
                expected,
                "SQLSTATE for {status:?}"
            );
        }
        // `Ok` is never an error; it keeps the spec's all-zeroes "not set" value.
        assert_eq!(sqlstate_for_status(Status::Ok), [0; 5]);
    }

    #[test]
    fn constructors_stamp_the_sqlstate() {
        assert_eq!(sqlstate_str(not_implemented("Substrait").sqlstate), "0A000");
        assert_eq!(
            sqlstate_str(invalid_state("no query set").sqlstate),
            "HY010"
        );
        assert_eq!(
            sqlstate_str(invalid_argument("bad option").sqlstate),
            "42000"
        );
        assert_eq!(
            sqlstate_str(err("boom", Status::Integrity).sqlstate),
            "23000"
        );
    }

    #[test]
    fn builder_errors_are_connection_failures() {
        let adbc = from_builder("cannot resolve credentials");
        // Status is unchanged (Internal), but the SQLSTATE says "unable to establish connection".
        assert_eq!(adbc.status, Status::Internal);
        assert_eq!(sqlstate_str(adbc.sqlstate), "08001");
    }

    #[test]
    fn grpc_code_refines_the_sqlstate_where_the_status_would_mislead() {
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};
        use google_cloud_gax::error::Error as GaxError;

        // OUT_OF_RANGE shares InvalidArguments with syntax errors, but it is a data exception.
        let out_of_range = from_spanner(GaxError::service(
            RpcStatus::default()
                .set_code(Code::OutOfRange)
                .set_message("value out of range"),
        ));
        assert_eq!(out_of_range.status, Status::InvalidArguments);
        assert_eq!(sqlstate_str(out_of_range.sqlstate), "22000");

        // INVALID_ARGUMENT keeps the status-derived syntax/access-rule class.
        let invalid = from_spanner(GaxError::service(
            RpcStatus::default()
                .set_code(Code::InvalidArgument)
                .set_message("Syntax error"),
        ));
        assert_eq!(sqlstate_str(invalid.sqlstate), "42000");
    }

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
        assert_eq!(sqlstate_str(adbc.sqlstate), "42S02");
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
        // ...and the SQLSTATE is the standard serialization failure, not IO's 08000.
        assert_eq!(sqlstate_str(adbc.sqlstate), "40001");
    }

    #[test]
    fn errors_without_a_grpc_status_have_no_vendor_code() {
        use google_cloud_gax::error::Error as GaxError;
        let adbc = from_spanner(GaxError::deser("no structured status here"));
        assert_eq!(adbc.status, Status::Internal);
        assert_eq!(adbc.vendor_code, 0);
        assert_eq!(sqlstate_str(adbc.sqlstate), "HY000");
    }
}

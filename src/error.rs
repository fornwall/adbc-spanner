//! Helpers for producing [`adbc_core`] errors and translating Spanner client errors.

use adbc_core::error::{Error, Status};
use google_cloud_gax::error::rpc::{Code, StatusDetails};

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
/// A `google.rpc.Status` may also carry structured *details* that describe *why* a call failed
/// beyond its status code — e.g. `google.rpc.QuotaFailure` on `RESOURCE_EXHAUSTED`,
/// `google.rpc.BadRequest` or `google.rpc.ErrorInfo` on `INVALID_ARGUMENT`,
/// `google.rpc.PreconditionFailure` on `FAILED_PRECONDITION`, or `google.rpc.RetryInfo` on
/// `ABORTED`. Each detail is forwarded into the ADBC error's `details` vector as a `(key, value)`
/// pair:
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
/// This per-detail, type-name-keyed ProtoJSON layout deliberately diverges from the Flight SQL ADBC
/// driver's convention (a single `grpc-status-details-bin` detail carrying the whole `google.rpc.Status`
/// as binary protobuf), so a consumer written to that convention won't interoperate; the pinned
/// preview client offers no binary-protobuf encoding of details, only ProtoJSON.
///
/// Together with `vendor_code`, these details give a caller structured diagnostics beyond the ADBC
/// status. Note that `RetryInfo` on `ABORTED` rarely reaches here in practice: the Spanner client's
/// read/write transaction runner — and the write-only mutation (bulk-ingest) path — retry aborted
/// transactions internally, *consuming* the `retryDelay` for their own backoff, under a default
/// policy that retries indefinitely (`BasicTransactionRetryPolicy::default` in the client's
/// `transaction_retry_policy` / `transaction_runner` modules; the driver installs no bounded
/// override). So an `ABORTED` normally never surfaces from a DML / commit / ingest call — it is
/// forwarded like any other detail only in the rare cases one does (a caller with its own bounded
/// retry policy can still key off `vendor_code == 10`). Errors without a gRPC status, and statuses
/// without details, leave `details` as `None` (never `Some(vec![])`).
pub(crate) fn from_spanner(error: google_cloud_spanner::Error) -> Error {
    // `Error::status()` yields the structured gRPC status when present. We map its `Code` enum
    // directly — no string round-trip, and every mapped arm is compile-checked rather than a
    // stringly-typed match on a `Display` message.
    let (status, vendor_code, details, code) =
        error
            .status()
            .map_or((Status::Internal, 0, None, None), |status| {
                (
                    status_for_grpc_code(status.code),
                    status.code as i32,
                    details_for_adbc(&status.details),
                    Some(status.code),
                )
            });
    let mut message = format!("Spanner error: {error}");
    // On PERMISSION_DENIED, append an actionable IAM hint (see [`permission_denied_hint`]). The hint
    // is only *appended* — the original message, status, vendor_code and forwarded details are all
    // left untouched.
    if code == Some(Code::PermissionDenied) {
        let hint = permission_denied_hint(&message);
        message.push_str(&hint);
    }
    let mut adbc = err(message, status);
    adbc.vendor_code = vendor_code;
    adbc.details = details;
    adbc
}

/// Base URL of the Spanner IAM documentation, appended to every permission hint.
const IAM_DOCS_URL: &str = "https://cloud.google.com/spanner/docs/iam";

/// Build the actionable IAM hint appended to a `PERMISSION_DENIED` error's message.
///
/// A Spanner `PERMISSION_DENIED` means the caller's principal lacks an IAM permission for the
/// operation. Cloud Spanner's status message usually already *names* the exact missing permission,
/// e.g. `... is missing IAM permission: spanner.databases.select on resource ...`. We turn that into
/// concrete guidance without ever *guessing*:
///
/// - **If the message names a `spanner.<resource>.<verb>` permission** (echo what Spanner itself
///   reported — the most accurate source), the hint repeats that exact permission and, when it is
///   one of Spanner's well-known data-plane permissions, points at the *least-privilege* predefined
///   role that includes it ([`role_for_permission`]) — `roles/spanner.databaseReader` for reads,
///   `roles/spanner.databaseUser` for writes/DML/DDL, `roles/spanner.databaseAdmin` for database
///   administration. A permission we don't have a mapping for is still echoed, with a generic "grant
///   a predefined Spanner role that includes it", so we never assert a role that might be wrong.
/// - **Otherwise** (the message did not name a permission — this layer cannot see which operation
///   the failed call was performing, so it cannot infer one), the hint is a still-accurate generic
///   pointer that enumerates the three common roles by operation kind.
///
/// Every hint ends with the [`IAM_DOCS_URL`] doc link. The hint is only ever *appended* to the
/// existing message — it augments, never replaces, and leaves `vendor_code`/`details` alone. The
/// emulator does not enforce IAM, so this path is exercised by the unit tests here and by a
/// mock-server test (`tests/mock_spanner.rs`) that returns a synthetic `PERMISSION_DENIED`, not by
/// the emulator integration test.
fn permission_denied_hint(message: &str) -> String {
    match extract_spanner_permission(message) {
        Some(permission) => match role_for_permission(permission) {
            Some(role) => format!(
                " (IAM hint: the caller is missing the Spanner IAM permission '{permission}'; \
                 grant a role that includes it, e.g. {role} — see {IAM_DOCS_URL})"
            ),
            None => format!(
                " (IAM hint: the caller is missing the Spanner IAM permission '{permission}'; \
                 grant a predefined Spanner role that includes it — see {IAM_DOCS_URL})"
            ),
        },
        None => format!(
            " (IAM hint: this is a Spanner IAM permission error; ensure the caller has a role \
             granting the permission this operation needs — roles/spanner.databaseReader for reads, \
             roles/spanner.databaseUser for reads plus writes (DML) and schema changes (DDL), or \
             roles/spanner.databaseAdmin for database administration — see {IAM_DOCS_URL})"
        ),
    }
}

/// Extract the first `spanner.<resource>.<verb>` IAM-permission token from an error message, if any.
///
/// Cloud Spanner's `PERMISSION_DENIED` messages name the missing permission inline (e.g.
/// `spanner.databases.select`). We split on any character that cannot appear in a permission
/// identifier (permissions are ASCII-alphanumeric segments joined by dots; verbs are camelCase like
/// `beginOrRollbackReadWriteTransaction`), trim stray dots, and return the first token shaped like a
/// Spanner permission. Deliberately dependency-free (no `regex`).
fn extract_spanner_permission(message: &str) -> Option<&str> {
    message
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '.'))
        .map(|token| token.trim_matches('.'))
        .find(|token| is_spanner_permission(token))
}

/// Is `token` shaped like a Spanner IAM permission — `spanner.<resource>.<verb>` with at least three
/// non-empty ASCII-alphanumeric segments (`spanner` plus a resource plus a verb)?
fn is_spanner_permission(token: &str) -> bool {
    let mut segments = token.split('.');
    if segments.next() != Some("spanner") {
        return false;
    }
    let rest: Vec<&str> = segments.collect();
    rest.len() >= 2
        && rest.iter().all(|segment| {
            !segment.is_empty() && segment.chars().all(|c| c.is_ascii_alphanumeric())
        })
}

/// Map a well-known Spanner data-plane IAM permission to the *least-privilege* predefined role that
/// grants it, or `None` for permissions we don't want to assert a role for.
///
/// The mapping follows Google's published Spanner IAM permission/role reference
/// (<https://cloud.google.com/spanner/docs/iam>): reads (SELECT / sessions) → `databaseReader`;
/// writes (DML), read/write-transaction begin, and DDL (`updateDdl`) → `databaseUser`; database
/// administration (create / drop / list / get DDL / IAM policy) → `databaseAdmin`. Anything not
/// listed returns `None` so the hint falls back to the generic "grant a predefined role that
/// includes it" wording rather than naming a role that might be wrong.
fn role_for_permission(permission: &str) -> Option<&'static str> {
    match permission {
        "spanner.databases.select"
        | "spanner.databases.read"
        | "spanner.databases.beginReadOnlyTransaction"
        | "spanner.databases.partitionQuery"
        | "spanner.databases.partitionRead"
        | "spanner.sessions.create"
        | "spanner.sessions.delete"
        | "spanner.sessions.get"
        | "spanner.sessions.list" => Some("roles/spanner.databaseReader"),
        "spanner.databases.write"
        | "spanner.databases.beginOrRollbackReadWriteTransaction"
        | "spanner.databases.beginPartitionedDmlTransaction"
        | "spanner.databases.updateDdl" => Some("roles/spanner.databaseUser"),
        "spanner.databases.create"
        | "spanner.databases.drop"
        | "spanner.databases.get"
        | "spanner.databases.getDdl"
        | "spanner.databases.list"
        | "spanner.databases.update"
        | "spanner.databases.getIamPolicy"
        | "spanner.databases.setIamPolicy" => Some("roles/spanner.databaseAdmin"),
        _ => None,
    }
}

/// Map a gRPC status' `google.rpc.Status` details onto ADBC error details.
///
/// Returns `None` (rather than `Some(vec![])`) when nothing mapped, so an error without details
/// keeps the `details: None` shape callers expect. See [`from_spanner`] for the key/value format
/// contract. A detail that fails to serialize, or whose ProtoJSON carries no usable `@type`, is
/// skipped — a detail is diagnostic garnish, never worth failing (or panicking) the error path for.
fn details_for_adbc(details: &[StatusDetails]) -> Option<Vec<(String, Vec<u8>)>> {
    let mapped: Vec<(String, Vec<u8>)> = details.iter().filter_map(map_detail).collect();
    (!mapped.is_empty()).then_some(mapped)
}

/// Map one `google.rpc.Status` detail to its ADBC `(key, value)` pair, or `None` to skip it.
///
/// Both halves derive from the detail's ProtoJSON form, so there is a single self-consistent path
/// for every detail kind:
///
/// - **value** — the ProtoJSON encoding as UTF-8 bytes, self-describing via its `"@type"` field.
/// - **key** — the lowercased fully-qualified protobuf type name, taken from that same `"@type"`
///   (the path segment after the final `/`), e.g. `google.rpc.retryinfo`.
///
/// Deriving the key from the serialized `@type` — rather than matching each [`StatusDetails`]
/// variant against a hand-maintained table — means the well-known `google.rpc` types and an
/// unrecognised [`StatusDetails::Other`] share one code path, *and* any new `google.rpc.*` detail
/// type added to the `#[non_exhaustive]` enum upstream is forwarded automatically instead of being
/// silently dropped. A detail that fails to serialize, or whose ProtoJSON carries no `@type`
/// string, is skipped.
fn map_detail(detail: &StatusDetails) -> Option<(String, Vec<u8>)> {
    let value = serde_json::to_value(detail).ok()?;
    let type_url = value.get("@type")?.as_str()?;
    let key = type_url
        .rsplit('/')
        .next()
        .unwrap_or(type_url)
        .to_ascii_lowercase();
    Some((key, serde_json::to_vec(&value).ok()?))
}

/// Build an ADBC error from a `google.rpc.Status`-style numeric code and message.
///
/// The BatchWrite (`spanner.ingest.batch_write`) path surfaces a failed mutation group as a
/// `google.rpc.Status` embedded in a streamed `BatchWriteResponse` — a numeric gRPC `code` plus a
/// `message` — rather than as a `google_cloud_spanner::Error`. This maps that numeric code onto the
/// closest ADBC [`Status`] through the very same [`status_for_grpc_code`] table [`from_spanner`]
/// uses (turning the number into a [`Code`] via [`Code::from`]) and keeps the numeric code in
/// `vendor_code`. A duplicate primary key therefore surfaces as [`Status::AlreadyExists`] exactly as
/// it does on the write-only commit path, so the bulk-ingest append/create error remaps still fire
/// identically for both ingest transports.
pub(crate) fn from_status_parts(code: i32, message: &str) -> Error {
    let status = status_for_grpc_code(Code::from(code));
    let mut full = format!("Spanner batch-write error: {message}");
    // A per-group `PERMISSION_DENIED` on the BatchWrite path gets the same actionable IAM hint the
    // `from_spanner` commit path adds (see [`permission_denied_hint`]); the original message and
    // `vendor_code` are preserved.
    if Code::from(code) == Code::PermissionDenied {
        let hint = permission_denied_hint(&full);
        full.push_str(&hint);
    }
    let mut adbc = err(full, status);
    adbc.vendor_code = code;
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

/// Map a canonical gRPC status [`Code`] onto the closest ADBC [`Status`].
///
/// Factored out from [`from_spanner`] as a pure function so the mapping can be unit-tested without
/// constructing real gax error values. Matching on the [`Code`] enum (rather than its string name)
/// makes every arm compile-checked, so a mis-spelled code is a build error rather than a silently
/// dead arm. Codes with no closely matching ADBC variant (and the unexpected `Ok`) fall back to
/// [`Status::Internal`]; the `#[non_exhaustive]` enum keeps the wildcard mandatory regardless.
fn status_for_grpc_code(code: Code) -> Status {
    match code {
        Code::NotFound => Status::NotFound,
        Code::AlreadyExists => Status::AlreadyExists,
        // ADBC distinguishes the two: failed authentication vs. an authenticated-but-forbidden call.
        Code::Unauthenticated => Status::Unauthenticated,
        Code::PermissionDenied => Status::Unauthorized,
        Code::InvalidArgument | Code::OutOfRange => Status::InvalidArguments,
        // "The preconditions for the operation are not met" — matches ADBC's InvalidState.
        Code::FailedPrecondition => Status::InvalidState,
        Code::DeadlineExceeded => Status::Timeout,
        Code::Cancelled => Status::Cancelled,
        // ADBC's IO status documents "a remote service may be unavailable".
        Code::Unavailable => Status::IO,
        // The operation is not implemented / not supported by the backend — ADBC has a dedicated
        // variant that fits this far better than the "driver bug" Internal fallback.
        Code::Unimplemented => Status::NotImplemented,
        // Aborted is Spanner's routine "transaction contended, please retry" signal. The client's
        // read/write runner retries it internally (indefinitely under the default policy the driver
        // uses), so a DML/commit/ingest caller does not normally see it; it reaches here only in the
        // rare case the runner surfaces it. It is transient and environmental, not a driver or
        // database defect, so it maps to IO rather than Internal (which reads as "driver bug"). ADBC
        // has no closer variant; the exact code survives in `vendor_code` (Aborted = 10) for callers
        // with their own retry logic.
        Code::Aborted => Status::IO,
        // ResourceExhausted, Internal, Unknown, DataLoss, Ok and anything unrecognised.
        _ => Status::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_grpc_codes_to_adbc_status() {
        assert_eq!(status_for_grpc_code(Code::NotFound), Status::NotFound);
        assert_eq!(
            status_for_grpc_code(Code::AlreadyExists),
            Status::AlreadyExists
        );
        assert_eq!(
            status_for_grpc_code(Code::Unauthenticated),
            Status::Unauthenticated
        );
        assert_eq!(
            status_for_grpc_code(Code::PermissionDenied),
            Status::Unauthorized
        );
        assert_eq!(
            status_for_grpc_code(Code::InvalidArgument),
            Status::InvalidArguments
        );
        assert_eq!(
            status_for_grpc_code(Code::OutOfRange),
            Status::InvalidArguments
        );
        assert_eq!(
            status_for_grpc_code(Code::FailedPrecondition),
            Status::InvalidState
        );
        assert_eq!(
            status_for_grpc_code(Code::DeadlineExceeded),
            Status::Timeout
        );
        assert_eq!(status_for_grpc_code(Code::Cancelled), Status::Cancelled);
        assert_eq!(status_for_grpc_code(Code::Unavailable), Status::IO);
        // Transient contention, not a driver/database defect: IO, not Internal.
        assert_eq!(status_for_grpc_code(Code::Aborted), Status::IO);
        // Not-supported operations map to the dedicated NotImplemented, not the Internal fallback.
        assert_eq!(
            status_for_grpc_code(Code::Unimplemented),
            Status::NotImplemented
        );
        // Codes with no close ADBC match fall through to the Internal wildcard.
        assert_eq!(
            status_for_grpc_code(Code::ResourceExhausted),
            Status::Internal
        );
        assert_eq!(status_for_grpc_code(Code::Internal), Status::Internal);
    }

    #[test]
    fn from_status_parts_maps_numeric_codes_like_from_spanner() {
        // A duplicate primary key on the BatchWrite path arrives as a numeric ALREADY_EXISTS (6)
        // and must surface as the same ADBC status the write-only path produces, so the ingest
        // append/create remaps fire identically.
        let adbc = from_status_parts(Code::AlreadyExists as i32, "Row already exists");
        assert_eq!(adbc.status, Status::AlreadyExists);
        assert_eq!(adbc.vendor_code, 6);
        assert!(adbc.message.contains("Row already exists"));
        // NOT_FOUND (5) → NotFound, INVALID_ARGUMENT (3) → InvalidArguments, and the numeric code
        // is preserved in vendor_code throughout.
        assert_eq!(
            from_status_parts(Code::NotFound as i32, "no table").status,
            Status::NotFound
        );
        assert_eq!(
            from_status_parts(Code::InvalidArgument as i32, "bad").status,
            Status::InvalidArguments
        );
        // An unmapped/unknown numeric code falls back to Internal but still keeps the code.
        let internal = from_status_parts(13, "boom");
        assert_eq!(internal.status, Status::Internal);
        assert_eq!(internal.vendor_code, 13);
    }

    #[test]
    fn unmapped_and_unknown_codes_fall_back_to_internal() {
        for code in [
            Code::ResourceExhausted,
            Code::Internal,
            Code::Unknown,
            Code::DataLoss,
            Code::Ok,
            // An out-of-range numeric decodes to some non-mapped `Code` and still hits the wildcard.
            Code::from(9999),
        ] {
            assert_eq!(status_for_grpc_code(code), Status::Internal);
        }
    }

    #[test]
    fn maps_a_real_gax_status_error() {
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

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
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

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
    fn unimplemented_maps_to_not_implemented() {
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

        let gax = GaxError::service(
            RpcStatus::default()
                .set_code(Code::Unimplemented)
                .set_message("Operation not supported"),
        );
        let adbc = from_spanner(gax);
        // A far better fit than the Internal "driver bug" fallback; the exact code still survives.
        assert_eq!(adbc.status, Status::NotImplemented);
        assert_eq!(adbc.vendor_code, Code::Unimplemented as i32);
        assert_eq!(adbc.vendor_code, 12);
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
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

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
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

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
    fn permission_denied_echoes_the_named_permission_and_maps_a_role() {
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

        // A real-shaped Spanner PERMISSION_DENIED that names the missing read permission.
        let gax = GaxError::service(
            RpcStatus::default()
                .set_code(Code::PermissionDenied)
                .set_message(
                    "Caller is missing IAM permission spanner.databases.select on resource \
                 projects/p/instances/i/databases/d.",
                ),
        );
        let adbc = from_spanner(gax);
        assert_eq!(adbc.status, Status::Unauthorized);
        assert_eq!(adbc.vendor_code, Code::PermissionDenied as i32);
        assert_eq!(adbc.vendor_code, 7);
        // The original message survives verbatim...
        assert!(
            adbc.message
                .contains("Caller is missing IAM permission spanner.databases.select")
        );
        // ...and the appended hint echoes the exact permission and names the least-privilege role.
        assert!(
            adbc.message.contains("IAM hint:"),
            "expected an IAM hint, got: {}",
            adbc.message
        );
        assert!(adbc.message.contains("spanner.databases.select"));
        assert!(
            adbc.message.contains("roles/spanner.databaseReader"),
            "a read permission must map to databaseReader, got: {}",
            adbc.message
        );
        assert!(
            adbc.message
                .contains("https://cloud.google.com/spanner/docs/iam")
        );
    }

    #[test]
    fn permission_denied_maps_write_and_ddl_permissions_to_database_user() {
        // A DML/write permission and the DDL permission both resolve to databaseUser.
        assert_eq!(
            role_for_permission("spanner.databases.write"),
            Some("roles/spanner.databaseUser")
        );
        assert_eq!(
            role_for_permission("spanner.databases.beginOrRollbackReadWriteTransaction"),
            Some("roles/spanner.databaseUser")
        );
        assert_eq!(
            role_for_permission("spanner.databases.updateDdl"),
            Some("roles/spanner.databaseUser")
        );
        // Reads → databaseReader; database administration → databaseAdmin.
        assert_eq!(
            role_for_permission("spanner.databases.select"),
            Some("roles/spanner.databaseReader")
        );
        assert_eq!(
            role_for_permission("spanner.databases.getDdl"),
            Some("roles/spanner.databaseAdmin")
        );
        // A permission we don't map returns None → the generic wording, never a guessed role.
        assert_eq!(role_for_permission("spanner.instances.get"), None);
    }

    #[test]
    fn permission_denied_without_a_named_permission_uses_the_generic_hint() {
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

        // Some PERMISSION_DENIED messages don't name a permission token at all.
        let gax = GaxError::service(
            RpcStatus::default()
                .set_code(Code::PermissionDenied)
                .set_message("Permission denied on resource database."),
        );
        let adbc = from_spanner(gax);
        assert_eq!(adbc.status, Status::Unauthorized);
        assert!(adbc.message.contains("IAM hint:"));
        // The generic hint enumerates the three common roles and the doc link — accurate without
        // asserting a single specific permission it can't see.
        assert!(adbc.message.contains("roles/spanner.databaseReader"));
        assert!(adbc.message.contains("roles/spanner.databaseUser"));
        assert!(adbc.message.contains("roles/spanner.databaseAdmin"));
        assert!(
            adbc.message
                .contains("https://cloud.google.com/spanner/docs/iam")
        );
    }

    #[test]
    fn non_permission_errors_get_no_iam_hint() {
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

        // A NOT_FOUND that incidentally mentions a spanner.* token must NOT gain an IAM hint —
        // the hint is gated strictly on the PERMISSION_DENIED code, not on message contents.
        let gax = GaxError::service(
            RpcStatus::default()
                .set_code(Code::NotFound)
                .set_message("Table not found; unrelated to spanner.databases.select"),
        );
        let adbc = from_spanner(gax);
        assert_eq!(adbc.status, Status::NotFound);
        assert!(
            !adbc.message.contains("IAM hint:"),
            "only PERMISSION_DENIED should carry the IAM hint, got: {}",
            adbc.message
        );
    }

    #[test]
    fn extracts_a_spanner_permission_token_ignoring_surrounding_punctuation() {
        // Trailing punctuation (the sentence's period) must not become part of the token.
        assert_eq!(
            extract_spanner_permission("... missing IAM permission: spanner.databases.select."),
            Some("spanner.databases.select")
        );
        // camelCase verbs are preserved intact.
        assert_eq!(
            extract_spanner_permission(
                "needs spanner.databases.beginOrRollbackReadWriteTransaction to proceed"
            ),
            Some("spanner.databases.beginOrRollbackReadWriteTransaction")
        );
        // A bare `spanner` word (no resource/verb) is not a permission.
        assert_eq!(
            extract_spanner_permission("just the word spanner here"),
            None
        );
        assert_eq!(
            extract_spanner_permission("no permission token at all"),
            None
        );
    }

    #[test]
    fn from_status_parts_adds_the_iam_hint_on_permission_denied() {
        // The BatchWrite path surfaces failures as numeric codes; a PERMISSION_DENIED group there
        // must also carry the hint, while the vendor_code and message are preserved.
        let adbc = from_status_parts(
            Code::PermissionDenied as i32,
            "Caller is missing IAM permission spanner.databases.write on resource d.",
        );
        assert_eq!(adbc.status, Status::Unauthorized);
        assert_eq!(adbc.vendor_code, 7);
        assert!(adbc.message.contains("spanner.databases.write"));
        assert!(adbc.message.contains("roles/spanner.databaseUser"));
        assert!(adbc.message.contains("IAM hint:"));
        // A non-permission numeric code stays hint-free.
        let already = from_status_parts(Code::AlreadyExists as i32, "Row already exists");
        assert!(!already.message.contains("IAM hint:"));
    }

    #[test]
    fn unrecognised_detail_keys_off_its_type_url() {
        use google_cloud_gax::error::Error as GaxError;
        use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

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

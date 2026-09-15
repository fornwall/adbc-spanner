//! Helpers for producing [`adbc_core`] errors and translating Spanner client errors.

use adbc_core::error::{Error, Status};
use google_cloud_gax::error::rpc::{Code, StatusDetails};
use google_cloud_wkt::Any;

/// Build an ADBC error with the given message and status.
pub(crate) fn err(message: impl Into<String>, status: Status) -> Error {
    Error::with_message_and_status(message, status)
}

/// A `NotImplemented` error for a **bare noun** naming functionality this driver does not (yet)
/// support, e.g. `not_implemented("ingest mode \"upsert\"")`. The noun is composed into a full
/// sentence here; a caller that already has one must use [`unsupported`] instead, or the two
/// collide into ungrammatical text.
pub(crate) fn not_implemented(what: &str) -> Error {
    err(
        format!("{what} is not supported by the Spanner ADBC driver"),
        Status::NotImplemented,
    )
}

/// A `NotImplemented` error carrying `message` **verbatim** — the sibling of [`not_implemented`]
/// for callers that have already phrased the whole sentence (typically because they name a cause
/// after a `:` or a remedy after a `;`, which no fixed suffix can follow grammatically).
pub(crate) fn unsupported(message: impl Into<String>) -> Error {
    err(message, Status::NotImplemented)
}

/// The `NotImplemented` error every `set_option` raises for a key it does not recognise.
///
/// One situation, one wording: `object_kind` is the ADBC object the key was set on (`"database"`,
/// `"connection"`, `"statement"`) and `key` is the key exactly as the caller spelled it.
pub(crate) fn unknown_option(object_kind: &str, key: &str) -> Error {
    unsupported(format!("unsupported Spanner {object_kind} option: {key}"))
}

/// The `NotFound` error every option getter raises for a key it cannot answer.
///
/// `adbc.h` licenses exactly one failure for the getters, and the driver reaches it two ways: the
/// key is recognised but unset, or it is not a key of this driver at all. The getters cannot tell
/// those apart (an unrecognised key falls through to the same arm), so the message names **both**
/// possibilities rather than telling the caller to set a key that a later `set_option` would
/// reject as unknown.
pub(crate) fn option_not_set(key: &str) -> Error {
    not_found(format!(
        "option {key} is not set or not recognized by this driver"
    ))
}

/// Rewrite an error's message **in place**, keeping every other field.
///
/// The annotating call sites — adding the offending table/column to an error raised deeper down —
/// must preserve `vendor_code` and `details`, which is what makes the annotated error still
/// carry the gRPC code and the forwarded `google.rpc.Status` details. Rebuilding the error and
/// copying those two fields back by hand does that only until a third field is added, so the
/// mutation is done once, here.
pub(crate) fn annotate(mut error: Error, f: impl FnOnce(&str) -> String) -> Error {
    error.message = f(&error.message);
    error
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

/// A `NotFound` error — the single failure `adbc.h` licenses for the option getters.
pub(crate) fn not_found(message: impl Into<String>) -> Error {
    err(message, Status::NotFound)
}

/// Walk an error and everything it wraps, outermost first.
///
/// The one consumer is the C ABI's Arrow stream export (`src/ffi/stream.rs`): a
/// [`RecordBatchReader`](arrow_array::RecordBatchReader)'s only error channel is
/// [`ArrowError`](arrow_schema::ArrowError), so the driver boxes its own [`Error`] inside
/// `ArrowError::ExternalError` and the export layer walks back down to it to recover the ADBC
/// status — which is what lets a cancelled read report `ECANCELED` rather than a generic errno.
pub(crate) fn chain<'a>(
    source: &'a (dyn std::error::Error + 'static),
) -> impl Iterator<Item = &'a (dyn std::error::Error + 'static)> {
    std::iter::successors(Some(source), |error| error.source())
}

/// Translate an error coming from the Spanner client into an ADBC error.
///
/// The Spanner preview client (and its LRO poller) surface every failure as
/// `google_cloud_spanner::Error` (a re-export of `google_cloud_gax::error::Error`). When that error
/// carries a gRPC status, its canonical code maps onto the closest [`Status`] variant (rather than
/// collapsing everything to [`Status::Internal`]) and the **numeric gRPC code** is preserved in
/// `vendor_code`, so a caller can recover exactly what failed (e.g. a retry loop looking for
/// `ABORTED` = 10) even where several codes share one ADBC status. Errors without a status
/// (transport/serialization/etc.) fall back to [`Status::Internal`] with `vendor_code` 0.
///
/// That `vendor_code` contract holds as written for Rust-native consumers and for C callers using
/// the ADBC **1.0.0** error layout, but not for one using the **1.1.0** layout: there the field is
/// not the driver's to spend. adbc.h reserves it as a discriminant — a 1.1.0 caller learns that an
/// error carries structured details by reading `ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA` (`i32::MIN`)
/// back out of it — so the export layer (`src/ffi/error.rs`) must re-stamp that sentinel over
/// whatever numeric code was stored here. Nothing is lost, because the same layout is the one with
/// a details vector: the code is handed back as an extra detail keyed `adbc.spanner.vendor_code`
/// whose value is its decimal ASCII rendering (so the `ABORTED` retry loop above becomes a lookup
/// of that key for `"10"`, reachable through `AdbcErrorGetDetail`). The entry exists only on that
/// path and only for a non-zero code; it is deliberately not added to `details` here, where every
/// consumer can already read `vendor_code` directly.
///
/// # Structured error details
///
/// A `google.rpc.Status` may also carry structured *details* — e.g. `QuotaFailure` on
/// `RESOURCE_EXHAUSTED`, `BadRequest`/`ErrorInfo` on `INVALID_ARGUMENT`, `PreconditionFailure` on
/// `FAILED_PRECONDITION`, `RetryInfo` on `ABORTED`. Each is forwarded into the ADBC error's
/// `details` vector as a `(key, value)` pair:
///
/// - **key** — the lowercased fully-qualified protobuf type name, e.g. `google.rpc.retryinfo`.
///   Flight SQL / gRPC metadata key style, but with no `-bin` suffix: that marks binary values, and
///   the value here is UTF-8 text.
/// - **value** — the detail's **ProtoJSON** encoding as UTF-8 bytes, self-describing via its
///   `"@type"` field, e.g. `{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"0.010s"}`.
///   ProtoJSON rather than binary protobuf because the preview client decodes details into
///   serde-modelled types whose only supported wire encoding is ProtoJSON.
///
/// This per-detail, type-name-keyed ProtoJSON layout deliberately diverges from the Flight SQL ADBC
/// driver's convention (one `grpc-status-details-bin` detail carrying the whole `google.rpc.Status`
/// as binary protobuf), so a consumer written to that convention won't interoperate — the pinned
/// preview client offers no binary-protobuf encoding of details.
///
/// `RetryInfo` on `ABORTED` rarely reaches here: the client's read/write transaction runner and the
/// write-only mutation (bulk-ingest) path retry aborted transactions internally, *consuming* the
/// `retryDelay`, under a default policy that retries indefinitely
/// (`BasicTransactionRetryPolicy::default`; the driver installs no bounded override). It is
/// forwarded like any other detail in the rare cases one does surface. Errors without a gRPC
/// status, and statuses without details, leave `details` as `None` (never `Some(vec![])`).
pub(crate) fn from_spanner(error: google_cloud_spanner::Error) -> Error {
    // Match the structured status' `Code` enum directly — no string round-trip, and every mapped
    // arm is compile-checked rather than a stringly-typed match on a `Display` message.
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
    // Appended only; the original message, status, vendor_code and details are untouched.
    if code == Some(Code::PermissionDenied) {
        message.push_str(PERMISSION_DENIED_GUIDANCE);
    }
    let mut adbc = err(message, status);
    adbc.vendor_code = vendor_code;
    adbc.details = details;
    adbc
}

/// Fixed IAM guidance appended to every `PERMISSION_DENIED` error message.
///
/// Spanner's own status message already *names* the missing permission (e.g. `... is missing IAM
/// permission: spanner.databases.select on resource ...`) and is preserved verbatim, so rather than
/// re-parse it we append a constant hint to grant a role including that permission, plus the IAM
/// docs link. Deliberately naming **no specific roles**: this mirrors the ADBC BigQuery driver,
/// whose only fixed auth guidance (`reauthGuidance`, a RAPT re-auth hint plus a doc link) likewise
/// leaves the server's message intact and names no roles. Enumerating roles risks steering the
/// caller to an over-broad or wrong-scoped one.
///
/// The guidance is only ever *appended* — message text, status, `vendor_code` and forwarded
/// `details` are untouched. The emulator does not enforce IAM, so this path is covered by the unit
/// tests here plus a `tests/mock_spanner.rs` synthetic `PERMISSION_DENIED`, not by the emulator
/// integration test.
const PERMISSION_DENIED_GUIDANCE: &str =
    "; grant an IAM role that includes it — see https://cloud.google.com/spanner/docs/iam";

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
/// Both halves derive from the detail's ProtoJSON form: the value is its UTF-8 bytes, the key the
/// lowercased type name taken from that same `"@type"` (the path segment after the final `/`).
///
/// Deriving the key from the serialized `@type` — rather than a hand-maintained table over the
/// [`StatusDetails`] variants — means the well-known `google.rpc` types and an unrecognised
/// [`StatusDetails::Other`] share one code path, *and* any new `google.rpc.*` type added to the
/// `#[non_exhaustive]` enum upstream is forwarded automatically instead of being silently dropped.
/// A detail that fails to serialize, or whose ProtoJSON carries no `@type` string, is skipped.
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

/// Build an ADBC error from the parts of a `google.rpc.Status`: its numeric code, message and
/// details.
///
/// The BatchWrite (`spanner.ingest.batch_write`) path surfaces a failed mutation group as a
/// `google.rpc.Status` embedded in a streamed `BatchWriteResponse` — not as a
/// `google_cloud_spanner::Error` — so it reaches the driver as loose parts instead of through
/// [`from_spanner`]. This keeps the two paths' output identical: same [`status_for_grpc_code`]
/// table (via [`Code::from`]), same `vendor_code`, same [`PERMISSION_DENIED_GUIDANCE`], and details
/// forwarded through the same [`details_for_adbc`] mapping (see [`from_spanner`] for the contract).
/// A duplicate primary key therefore surfaces as [`Status::AlreadyExists`] exactly as on the
/// write-only commit path, so the bulk-ingest append/create error remaps fire identically for both
/// ingest transports.
///
/// The `details` arrive as the wire [`Any`]s of the embedded status rather than decoded
/// [`StatusDetails`]; they are decoded here with the very conversion the client itself applies when
/// building the gax error status [`from_spanner`] reads, so a given detail maps to byte-identical
/// output on either path.
pub(crate) fn from_status_parts(code: i32, message: &str, details: &[Any]) -> Error {
    let status = status_for_grpc_code(Code::from(code));
    let mut full = format!("Spanner batch-write error: {message}");
    // Appended only, exactly as on the `from_spanner` commit path.
    if Code::from(code) == Code::PermissionDenied {
        full.push_str(PERMISSION_DENIED_GUIDANCE);
    }
    let decoded: Vec<StatusDetails> = details.iter().map(StatusDetails::from).collect();
    let mut adbc = err(full, status);
    adbc.vendor_code = code;
    adbc.details = details_for_adbc(&decoded);
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
        Code::InvalidArgument => Status::InvalidArguments,
        // Out of range is a *data* fault, not a malformed request: Spanner raises it for values the
        // query produced or consumed (numeric overflow, an index past the end), which is exactly
        // adbc.h's InvalidData ("invalid data was processed (not a programming error) ... a
        // division by zero may have occurred during query execution") and where the PostgreSQL
        // driver sends SQLSTATE class 22.
        Code::OutOfRange => Status::InvalidData,
        // "The preconditions for the operation are not met" — matches ADBC's InvalidState.
        //
        // Deliberately *not* ADBC's Integrity, even though Spanner reports foreign-key, CHECK and
        // NOT NULL violations with this code: it also reports genuine wrong-state failures with it
        // (a database still being created, a schema change in flight, an operation the current
        // schema does not allow), and one code cannot be split without sniffing the server's
        // message text, which is untyped and version-dependent. Integrity asserts "the database's
        // integrity was affected", so guessing it for a wrong-state error is the worse mislabel of
        // the two; upstream agrees the code is not a clean fit (the Flight SQL driver maps it to
        // Unknown). A caller that needs the distinction has the exact code in `vendor_code` (9) and
        // the forwarded `google.rpc.PreconditionFailure` detail, both typed.
        Code::FailedPrecondition => Status::InvalidState,
        Code::DeadlineExceeded => Status::Timeout,
        Code::Cancelled => Status::Cancelled,
        // ADBC's IO status documents "a remote service may be unavailable".
        Code::Unavailable => Status::IO,
        // The operation is not implemented / not supported by the backend — ADBC has a dedicated
        // variant that fits this far better than the "driver bug" Internal fallback.
        Code::Unimplemented => Status::NotImplemented,
        // Aborted is Spanner's routine "transaction contended, please retry" signal, normally
        // consumed by the client's read/write runner (see `from_spanner`). Transient and
        // environmental, not a driver/database defect, so IO rather than the "driver bug"-flavoured
        // Internal; ADBC has no closer variant, and the exact code survives in `vendor_code` (10).
        Code::Aborted => Status::IO,
        // ADBC has a dedicated "an unknown error occurred" status; Internal claims a driver or
        // database *defect*, which is more than this code says. The Flight SQL driver maps it so.
        Code::Unknown => Status::Unknown,
        // Unrecoverable data loss or corruption in transit — adbc.h's IO ("an I/O error occurred"),
        // as the Flight SQL driver maps it, rather than the driver-bug-flavoured Internal.
        Code::DataLoss => Status::IO,
        // ResourceExhausted, Internal, Ok and anything unrecognised.
        _ => Status::Internal,
    }
}

#[cfg(test)]
mod tests;

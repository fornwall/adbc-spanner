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
    let adbc = from_status_parts(Code::AlreadyExists as i32, "Row already exists", &[]);
    assert_eq!(adbc.status, Status::AlreadyExists);
    assert_eq!(adbc.vendor_code, 6);
    assert!(adbc.message.contains("Row already exists"));
    // A status without details keeps details = None, not Some(vec![]) — as on the
    // `from_spanner` path.
    assert_eq!(adbc.details, None);
    // NOT_FOUND (5) → NotFound, INVALID_ARGUMENT (3) → InvalidArguments, and the numeric code
    // is preserved in vendor_code throughout.
    assert_eq!(
        from_status_parts(Code::NotFound as i32, "no table", &[]).status,
        Status::NotFound
    );
    assert_eq!(
        from_status_parts(Code::InvalidArgument as i32, "bad", &[]).status,
        Status::InvalidArguments
    );
    // An unmapped/unknown numeric code falls back to Internal but still keeps the code.
    let internal = from_status_parts(13, "boom", &[]);
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
fn permission_denied_preserves_the_message_and_appends_iam_guidance() {
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
    // The server's original message — which already names the missing permission — survives
    // verbatim; we neither re-parse it nor resolve a role from it.
    assert!(
        adbc.message
            .contains("Caller is missing IAM permission spanner.databases.select")
    );
    // ...and the fixed guidance is appended: a generic "grant a role that includes it" hint
    // plus the IAM doc link — no specific role is named (BigQuery-driver parity).
    assert!(
        adbc.message.contains("grant an IAM role that includes it"),
        "expected the appended IAM guidance, got: {}",
        adbc.message
    );
    assert!(
        adbc.message
            .contains("https://cloud.google.com/spanner/docs/iam")
    );
    // The role enumeration was intentionally dropped: no predefined role names leak through.
    assert!(
        !adbc.message.contains("roles/spanner."),
        "guidance must name no specific IAM role, got: {}",
        adbc.message
    );
}

#[test]
fn permission_denied_guidance_is_added_even_without_a_named_permission() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    // Some PERMISSION_DENIED messages don't name a permission token at all — the guidance is
    // appended regardless, keyed only on the code.
    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::PermissionDenied)
            .set_message("Permission denied on resource database."),
    );
    let adbc = from_spanner(gax);
    assert_eq!(adbc.status, Status::Unauthorized);
    assert!(adbc.message.contains("grant an IAM role that includes it"));
    assert!(
        adbc.message
            .contains("https://cloud.google.com/spanner/docs/iam")
    );
    assert!(!adbc.message.contains("roles/spanner."));
}

#[test]
fn non_permission_errors_get_no_iam_guidance() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    // A NOT_FOUND that incidentally mentions a spanner.* token must NOT gain IAM guidance —
    // it is gated strictly on the PERMISSION_DENIED code, not on message contents.
    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::NotFound)
            .set_message("Table not found; unrelated to spanner.databases.select"),
    );
    let adbc = from_spanner(gax);
    assert_eq!(adbc.status, Status::NotFound);
    assert!(
        !adbc.message.contains("cloud.google.com/spanner/docs/iam"),
        "only PERMISSION_DENIED should carry the IAM guidance, got: {}",
        adbc.message
    );
}

#[test]
fn from_status_parts_adds_iam_guidance_on_permission_denied() {
    // The BatchWrite path surfaces failures as numeric codes; a PERMISSION_DENIED group there
    // must also carry the guidance, while the vendor_code and message are preserved.
    let adbc = from_status_parts(
        Code::PermissionDenied as i32,
        "Caller is missing IAM permission spanner.databases.write on resource d.",
        &[],
    );
    assert_eq!(adbc.status, Status::Unauthorized);
    assert_eq!(adbc.vendor_code, 7);
    assert!(adbc.message.contains("spanner.databases.write"));
    assert!(adbc.message.contains("grant an IAM role that includes it"));
    assert!(
        adbc.message
            .contains("https://cloud.google.com/spanner/docs/iam")
    );
    assert!(!adbc.message.contains("roles/spanner."));
    // A non-permission numeric code stays guidance-free.
    let already = from_status_parts(Code::AlreadyExists as i32, "Row already exists", &[]);
    assert!(
        !already
            .message
            .contains("cloud.google.com/spanner/docs/iam")
    );
}

/// Build a `wkt::Any` from its ProtoJSON form — the shape a `google.rpc.Status`'s details take
/// on the BatchWrite response, and the input [`from_status_parts`] decodes.
fn any_from_json(value: serde_json::Value) -> Any {
    serde_json::from_value(value).expect("valid Any ProtoJSON")
}

#[test]
fn from_status_parts_forwards_status_details_like_from_spanner() {
    // A `BatchWriteResponse`'s embedded `google.rpc.Status` carries the same structured details
    // a commit-path error does. They must reach `Error::details` under exactly the contract
    // `from_spanner` documents — lowercased proto type-name keys, ProtoJSON values, in order —
    // so a consumer cannot tell the two ingest transports apart.
    let adbc = from_status_parts(
        Code::AlreadyExists as i32,
        "Row [v0] in table MockTable already exists",
        &[
            any_from_json(serde_json::json!({
                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                "reason": "DUPLICATE_KEY",
                "domain": "spanner.googleapis.com",
            })),
            any_from_json(serde_json::json!({
                "@type": "type.googleapis.com/google.rpc.BadRequest",
                "fieldViolations": [{"field": "Id", "description": "duplicate"}],
            })),
        ],
    );
    assert_eq!(adbc.status, Status::AlreadyExists);
    assert_eq!(adbc.vendor_code, 6);

    let details = adbc
        .details
        .expect("the group status' details must be forwarded");
    let keys: Vec<&str> = details.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(keys, ["google.rpc.errorinfo", "google.rpc.badrequest"]);
    // Values are the details' self-describing ProtoJSON, exactly as on the `from_spanner` path.
    let error_info: serde_json::Value = serde_json::from_slice(&details[0].1).unwrap();
    assert_eq!(
        error_info["@type"],
        "type.googleapis.com/google.rpc.ErrorInfo"
    );
    assert_eq!(error_info["reason"], "DUPLICATE_KEY");
    assert_eq!(error_info["domain"], "spanner.googleapis.com");
    let bad_request: serde_json::Value = serde_json::from_slice(&details[1].1).unwrap();
    assert_eq!(bad_request["fieldViolations"][0]["field"], "Id");
}

#[test]
fn from_status_parts_keys_an_unrecognised_detail_off_its_type_url() {
    // A detail type outside the well-known `google.rpc` set decodes to `StatusDetails::Other`
    // and is still forwarded, keyed off its Any type URL — the same fallback `from_spanner` has.
    let custom = serde_json::json!({
        "@type": "type.googleapis.com/mycompany.CustomDetail",
        "foo": "bar",
    });
    let adbc = from_status_parts(13, "boom", &[any_from_json(custom.clone())]);
    let details = adbc.details.expect("custom detail forwarded");
    assert_eq!(details.len(), 1);
    assert_eq!(details[0].0, "mycompany.customdetail");
    let parsed: serde_json::Value = serde_json::from_slice(&details[0].1).unwrap();
    assert_eq!(parsed, custom);
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

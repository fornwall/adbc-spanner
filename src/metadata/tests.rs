//! Offline tests for the metadata plumbing: the ADBC `LIKE` matcher, the catalog check, the
//! `INFORMATION_SCHEMA` column accessor and what a metadata statement builder carries.

use std::sync::Arc;

use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::{check_lookup_catalog, like_match, metadata_sql_builder, str_col};
use crate::options::SharedConfig;

#[test]
fn like_matching() {
    assert!(like_match("", ""));
    assert!(like_match("%", ""));
    assert!(like_match("%", "anything"));
    assert!(like_match("Singers", "Singers"));
    assert!(!like_match("Singers", "singers")); // case-sensitive
    assert!(like_match("Sing%", "Singers"));
    assert!(like_match("%ers", "Singers"));
    assert!(like_match("S_ngers", "Singers"));
    assert!(like_match("%a%a%", "banana"));
    assert!(!like_match("%x%", "banana"));
    assert!(!like_match("", "x"));
    // A pattern `%` must stay a wildcard even when the value has a literal `%` where the
    // wildcard begins matching — the value starts with `%`, or a `%` follows matched literals.
    // The literal branch used to mis-consume it there, so these all failed. Found by the `like`
    // fuzz target's differential regex oracle.
    assert!(like_match("%", "%foo"));
    assert!(like_match("%", "%^%?"));
    assert!(like_match("a%", "a%b"));
}

#[test]
fn like_matching_multibyte_utf8() {
    // The matcher walks values by byte offset; `_` must still consume one *character* (of any
    // UTF-8 width), never one byte, and `%` backtracking must skip whole characters.
    assert!(like_match("_", "é")); // 2-byte char is one `_`
    assert!(!like_match("__", "é")); // ... not two
    assert!(like_match("caf_", "café"));
    assert!(like_match("_本_", "日本語")); // 3-byte chars
    assert!(!like_match("____", "日本語"));
    assert!(like_match("_", "🦀")); // 4-byte char
    assert!(like_match("%語", "日本語")); // `%` backtracks over multi-byte chars
    assert!(like_match("%本%", "日本語"));
    assert!(!like_match("%x%", "日本語"));
    assert!(like_match("日%語", "日本語"));
    assert!(like_match("é%é", "été"));
    assert!(!like_match("é_é", "été été")); // literal tail must land on the right char
}

#[test]
fn lookup_catalog_accepts_only_the_default_empty_catalog() {
    // Spanner's single catalog is the empty string; `None` means "don't filter".
    assert!(check_lookup_catalog(None).is_ok());
    assert!(check_lookup_catalog(Some("")).is_ok());
    // Any named catalog does not exist, so a lookup in it is NotFound.
    let err = check_lookup_catalog(Some("main")).unwrap_err();
    assert_eq!(err.status, Status::NotFound);
    assert!(err.message.contains("\"main\""), "{}", err.message);
}

#[test]
fn str_col_errors_on_out_of_range_index() {
    // A zero-column batch: any column index is out of range and must error, not panic.
    let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let err = str_col(&empty, 0).unwrap_err();
    assert_eq!(err.status, Status::Internal);

    // A one-column batch: index 0 is fine, index 1 is out of range.
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, true)]));
    let col: ArrayRef = Arc::new(StringArray::from(vec![Some("x")]));
    let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
    assert!(str_col(&batch, 0).is_ok());
    assert_eq!(str_col(&batch, 1).unwrap_err().status, Status::Internal);
}

/// A config with every knob `metadata_sql_builder` cares about set — plus the tags, which it
/// must *not* forward.
fn configured() -> SharedConfig {
    let mut config = SharedConfig::default();
    config
        .request
        .set_priority(OptionValue::String("low".into()))
        .unwrap();
    config
        .request
        .set_request_tag(OptionValue::String("user-request-tag".into()))
        .unwrap();
    config
        .request
        .set_transaction_tag(OptionValue::String("user-txn-tag".into()))
        .unwrap();
    config
        .directed_read
        .set(OptionValue::String("include:eu-west1:read_only".into()))
        .unwrap();
    config.retry.set_max_attempts(OptionValue::Int(3)).unwrap();
    config
}

/// The built request is the only inspection surface the client crate offers (its `Statement`
/// fields are `pub(crate)`), so these assert on its `Debug` rendering.
#[test]
fn metadata_statements_carry_priority_replicas_and_retry_bounds() {
    let built = format!(
        "{:?}",
        metadata_sql_builder(&configured(), "SELECT 1").build()
    );
    assert!(built.contains("priority: Low"), "{built}");
    assert!(built.contains("location: \"eu-west1\""), "{built}");
    assert!(built.contains("maximum_attempts: 3"), "{built}");
}

/// Tags stay off driver-internal metadata reads: they attribute the *user's* statements in
/// Spanner's introspection tables.
#[test]
fn metadata_statements_are_never_tagged() {
    let built = format!(
        "{:?}",
        metadata_sql_builder(&configured(), "SELECT 1").build()
    );
    assert!(!built.contains("user-request-tag"), "{built}");
    assert!(!built.contains("user-txn-tag"), "{built}");
    assert!(built.contains("request_tag: \"\""), "{built}");
    assert!(built.contains("transaction_tag: \"\""), "{built}");
}

/// An unconfigured connection still sends a bare statement — nothing is forced on by default.
#[test]
fn unconfigured_metadata_statements_stay_bare() {
    let built = format!(
        "{:?}",
        metadata_sql_builder(&SharedConfig::default(), "SELECT 1").build()
    );
    assert!(built.contains("request_options: None"), "{built}");
    assert!(built.contains("directed_read_options: None"), "{built}");
    assert!(built.contains("retry_policy: None"), "{built}");
}

//! Building the result of [`Connection::get_info`](adbc_core::Connection::get_info).
//!
//! ADBC `get_info` returns driver / vendor metadata as a two-column batch: `info_name` (a `u32`
//! code) and `info_value` (a dense union able to hold a string, bool, int64, bitmask, string list,
//! or int32→int32-list map — see [`GET_INFO_SCHEMA`]). Every value this driver reports is static
//! metadata known without contacting Spanner, so `get_info` needs no RPC.

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use adbc_core::options::InfoCode;
use adbc_core::schemas::GET_INFO_SCHEMA;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::DataType;

use crate::error::err;
use crate::nested::{arrow_err, dense_union};
use crate::{DRIVER_NAME, DRIVER_VERSION, VENDOR_NAME};

/// Type ids of the `info_value` union branches this driver populates (see [`GET_INFO_SCHEMA`]:
/// `string_value` = 0, `bool_value` = 1, `int64_value` = 2). The remaining branches (3–5) are
/// carried as empty children so the union type still matches the schema exactly.
const STRING_VALUE: i8 = 0;
const BOOL_VALUE: i8 = 1;
const INT64_VALUE: i8 = 2;

/// The ADBC API revision this driver targets (`1.1.0`), reported for `DriverAdbcVersion`.
const ADBC_VERSION_1_1_0: i64 = 1_001_000;

/// The version of the `arrow-array` crate this driver is built against (resolved from `Cargo.lock`
/// by `build.rs`), reported for `DriverArrowVersion` with the conventional leading `v`.
const ARROW_VERSION: &str = concat!("v", env!("ADBC_SPANNER_ARROW_VERSION"));

/// Every info code this driver recognises, in code order — the exact result set of
/// `get_info(None)` ("fetch all"). Includes the codes whose value is null (see [`value_for`]):
/// per `adbc.h`, null *is* the defined value for the Substrait version bounds when Substrait is
/// unsupported, and omission is reserved for *unrecognized* codes. Explicit requests are filtered
/// to this set, so a code answered on explicit request is always present in the all-codes result
/// and vice versa.
const REPORTED: &[InfoCode] = &[
    InfoCode::VendorName,
    InfoCode::VendorVersion,
    InfoCode::VendorArrowVersion,
    InfoCode::VendorSql,
    InfoCode::VendorSubstrait,
    InfoCode::VendorSubstraitMinVersion,
    InfoCode::VendorSubstraitMaxVersion,
    InfoCode::DriverName,
    InfoCode::DriverVersion,
    InfoCode::DriverArrowVersion,
    InfoCode::DriverAdbcVersion,
];

/// A single info value, tagged by which union branch it belongs to.
enum InfoValue {
    /// Every string this driver reports is a compile-time constant (a `const` or an `env!`), so the
    /// variant borrows rather than allocates.
    Str(&'static str),
    Bool(bool),
    Int(i64),
    /// A code we recognise but have no stable value for (e.g. a Spanner product version): emitted as
    /// a null in the `string_value` branch.
    Null,
}

/// The value this driver reports for `code`.
fn value_for(code: InfoCode) -> InfoValue {
    match code {
        InfoCode::VendorName => InfoValue::Str(VENDOR_NAME),
        // Spanner speaks SQL (GoogleSQL / PostgreSQL) but not Substrait.
        InfoCode::VendorSql => InfoValue::Bool(true),
        InfoCode::VendorSubstrait => InfoValue::Bool(false),
        InfoCode::DriverName => InfoValue::Str(DRIVER_NAME),
        InfoCode::DriverVersion => InfoValue::Str(DRIVER_VERSION),
        InfoCode::DriverArrowVersion => InfoValue::Str(ARROW_VERSION),
        InfoCode::DriverAdbcVersion => InfoValue::Int(ADBC_VERSION_1_1_0),
        // Recognised codes without a stable value, emitted as null rather than a fabricated string.
        // In particular `VendorVersion` (the Spanner *server* product version) is deliberately null:
        // Spanner exposes no user-visible server version, so reporting anything here — least of all
        // this driver's own version — would be misleading. Likewise `VendorArrowVersion` (the server
        // runs no Arrow library) and the Substrait version bounds (null is `adbc.h`'s defined value
        // when Substrait is unsupported). A future `#[non_exhaustive]` variant also lands here, but
        // never reaches this function: it is absent from [`REPORTED`], so [`build`] omits it as
        // unrecognized first.
        _ => InfoValue::Null,
    }
}

/// Build the `get_info` record batch for the requested `codes` (or the full [`REPORTED`] set when
/// `None`).
pub(crate) fn build(codes: Option<HashSet<InfoCode>>) -> Result<RecordBatch> {
    // One row per recognised code, in code order. An explicit request yields a row for each
    // requested code in [`REPORTED`] (even the null-valued ones) and — per the `adbc.h` contract —
    // silently omits unrecognized codes, keeping explicit requests and `get_info(None)` symmetric.
    let codes: Vec<InfoCode> = match codes {
        Some(set) => {
            let mut v: Vec<InfoCode> = set.into_iter().filter(|c| REPORTED.contains(c)).collect();
            v.sort_by_key(|c| u32::from(c));
            v
        }
        None => REPORTED.to_vec(),
    };

    let mut names: Vec<u32> = Vec::with_capacity(codes.len());
    let mut type_ids: Vec<i8> = Vec::with_capacity(codes.len());
    let mut offsets: Vec<i32> = Vec::with_capacity(codes.len());
    let mut strings: Vec<Option<&'static str>> = Vec::new();
    let mut bools: Vec<Option<bool>> = Vec::new();
    let mut ints: Vec<Option<i64>> = Vec::new();

    for code in codes {
        names.push(u32::from(&code));
        let (type_id, offset) = match value_for(code) {
            InfoValue::Str(s) => {
                strings.push(Some(s));
                (STRING_VALUE, strings.len() - 1)
            }
            InfoValue::Null => {
                strings.push(None);
                (STRING_VALUE, strings.len() - 1)
            }
            InfoValue::Bool(b) => {
                bools.push(Some(b));
                (BOOL_VALUE, bools.len() - 1)
            }
            InfoValue::Int(i) => {
                ints.push(Some(i));
                (INT64_VALUE, ints.len() - 1)
            }
        };
        type_ids.push(type_id);
        offsets.push(offset as i32);
    }

    // Reuse the canonical union type from the ADBC schema verbatim so the built column matches
    // GET_INFO_SCHEMA exactly (branch names, nullability and type ids all).
    let union_fields = match GET_INFO_SCHEMA.field(1).data_type() {
        DataType::Union(fields, _) => fields.clone(),
        other => {
            return Err(err(
                format!("GET_INFO_SCHEMA info_value is not a union: {other:?}"),
                Status::Internal,
            ));
        }
    };

    let string_child: ArrayRef = Arc::new(StringArray::from_iter(strings));
    let bool_child: ArrayRef = Arc::new(BooleanArray::from_iter(bools));
    let int_child: ArrayRef = Arc::new(Int64Array::from_iter(ints));
    // Only branches 0–2 carry values; `dense_union` fills the remaining branches (3–5) with empty
    // children so the union type still matches GET_INFO_SCHEMA exactly.
    let info_value = dense_union(
        &union_fields,
        &[
            (STRING_VALUE, string_child),
            (BOOL_VALUE, bool_child),
            (INT64_VALUE, int_child),
        ],
        type_ids,
        offsets,
    )?;

    RecordBatch::try_new(
        GET_INFO_SCHEMA.clone(),
        vec![Arc::new(UInt32Array::from(names)), Arc::new(info_value)],
    )
    .map_err(arrow_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, UnionArray};

    #[test]
    fn build_matches_schema_and_reports_defaults() {
        let batch = build(None).unwrap();
        assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
        assert_eq!(batch.num_rows(), REPORTED.len());
        // info_name codes are the reported ones.
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let got: Vec<u32> = (0..names.len()).map(|i| names.value(i)).collect();
        let want: Vec<u32> = REPORTED.iter().map(u32::from).collect();
        assert_eq!(got, want);
        assert!(
            want.windows(2).all(|w| w[0] < w[1]),
            "REPORTED must be in strict code order (it is the get_info(None) row order)"
        );
    }

    #[test]
    fn all_codes_result_covers_every_explicitly_answered_code() {
        // The SPEC-5 invariant (mirrors the C++ validation `MetadataGetInfoAllCodes` test): any
        // code answered for an explicit request must also appear in the `get_info(None)` result.
        let all = build(None).unwrap();
        let all_names = all
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let all_codes: HashSet<u32> = (0..all_names.len()).map(|i| all_names.value(i)).collect();

        for code in REPORTED {
            let batch = build(Some([*code].into_iter().collect())).unwrap();
            assert_eq!(batch.num_rows(), 1, "explicit request for {code:?}");
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            assert!(
                all_codes.contains(&names.value(0)),
                "{code:?} answered explicitly but missing from the all-codes result"
            );
        }
    }

    #[test]
    fn requested_codes_yield_one_row_each_in_code_order() {
        let requested = [
            InfoCode::VendorName,
            InfoCode::DriverVersion,
            InfoCode::DriverName,
            InfoCode::VendorVersion, // recognised but valued null
        ];
        let batch = build(Some(requested.iter().copied().collect())).unwrap();
        assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 4);

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let mut want: Vec<u32> = requested.iter().map(u32::from).collect();
        want.sort_unstable();
        let got: Vec<u32> = (0..names.len()).map(|i| names.value(i)).collect();
        assert_eq!(got, want, "rows are ordered by info code");

        // VendorName's string value round-trips through the union's string branch.
        let union = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        let vendor_row = want
            .iter()
            .position(|&c| c == u32::from(&InfoCode::VendorName))
            .unwrap();
        let value = union.value(vendor_row);
        let s = value.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), VENDOR_NAME);
    }

    #[test]
    fn driver_arrow_version_is_reported_with_a_leading_v() {
        // Part of the default `get_info(None)` set.
        assert!(REPORTED.contains(&InfoCode::DriverArrowVersion));

        // Requested explicitly, it carries the arrow crate version as a `v`-prefixed string.
        let batch = build(Some([InfoCode::DriverArrowVersion].into_iter().collect())).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let union = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        let value = union.value(0);
        let s = value.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!s.is_null(0));
        let version = s.value(0);
        assert!(
            version.starts_with('v') && version.len() > 1,
            "expected a v-prefixed arrow version, got {version:?}"
        );
    }

    #[test]
    fn vendor_version_is_null_not_the_driver_version() {
        // The Spanner *server* has no user-visible product version, so `VendorVersion` must never be
        // populated — least of all with this driver's own version. It is a recognised code, present
        // in the default set and answered on explicit request, always with a null string value.
        assert!(REPORTED.contains(&InfoCode::VendorVersion));
        assert!(REPORTED.contains(&InfoCode::VendorArrowVersion));

        let batch = build(Some(
            [InfoCode::VendorVersion, InfoCode::DriverVersion]
                .into_iter()
                .collect(),
        ))
        .unwrap();
        assert_eq!(batch.num_rows(), 2);

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let union = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();

        let vendor_row = (0..names.len())
            .find(|&i| names.value(i) == u32::from(&InfoCode::VendorVersion))
            .unwrap();
        let vendor_value = union.value(vendor_row);
        let vendor_str = vendor_value
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("VendorVersion lands in the string branch");
        assert!(
            vendor_str.is_null(0),
            "VendorVersion must be null, got {:?}",
            vendor_str.value(0)
        );

        // The driver version, by contrast, is a real non-null string — so the null above is a
        // deliberate absence, not a build that reports nothing at all.
        let driver_row = (0..names.len())
            .find(|&i| names.value(i) == u32::from(&InfoCode::DriverVersion))
            .unwrap();
        let driver_value = union.value(driver_row);
        let driver_str = driver_value.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!driver_str.is_null(0));
        assert_eq!(driver_str.value(0), DRIVER_VERSION);
    }

    #[test]
    fn empty_request_is_an_empty_batch() {
        let batch = build(Some(HashSet::new())).unwrap();
        assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 0);
    }
}

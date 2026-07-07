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
use arrow_array::{
    new_empty_array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, UInt32Array,
    UnionArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{ArrowError, DataType, UnionFields};

use crate::error::err;
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

/// The codes reported when the caller requests *all* info (`get_info(None)`): the ones with a
/// stable, meaningful value. Requesting a specific code outside this set still yields a row (with a
/// null value) — see [`value_for`].
const REPORTED: &[InfoCode] = &[
    InfoCode::VendorName,
    InfoCode::VendorSql,
    InfoCode::VendorSubstrait,
    InfoCode::DriverName,
    InfoCode::DriverVersion,
    InfoCode::DriverArrowVersion,
    InfoCode::DriverAdbcVersion,
];

/// A single info value, tagged by which union branch it belongs to.
enum InfoValue {
    Str(String),
    Bool(bool),
    Int(i64),
    /// A code we recognise but have no stable value for (e.g. a Spanner product version): emitted as
    /// a null in the `string_value` branch.
    Null,
}

/// The value this driver reports for `code`.
fn value_for(code: InfoCode) -> InfoValue {
    match code {
        InfoCode::VendorName => InfoValue::Str(VENDOR_NAME.to_string()),
        // Spanner speaks SQL (GoogleSQL / PostgreSQL) but not Substrait.
        InfoCode::VendorSql => InfoValue::Bool(true),
        InfoCode::VendorSubstrait => InfoValue::Bool(false),
        InfoCode::DriverName => InfoValue::Str(DRIVER_NAME.to_string()),
        InfoCode::DriverVersion => InfoValue::Str(DRIVER_VERSION.to_string()),
        InfoCode::DriverArrowVersion => InfoValue::Str(ARROW_VERSION.to_string()),
        InfoCode::DriverAdbcVersion => InfoValue::Int(ADBC_VERSION_1_1_0),
        // Recognised codes without a stable value (vendor product/Arrow versions, Substrait
        // bounds), plus any future `#[non_exhaustive]` variant.
        _ => InfoValue::Null,
    }
}

/// Build the `get_info` record batch for the requested `codes` (or the full [`REPORTED`] set when
/// `None`).
pub(crate) fn build(codes: Option<HashSet<InfoCode>>) -> Result<RecordBatch> {
    // One row per code. For an explicit request, honour exactly the codes asked for (a row each,
    // even for ones we answer with null), in a deterministic order.
    let codes: Vec<InfoCode> = match codes {
        Some(set) => {
            let mut v: Vec<InfoCode> = set.into_iter().collect();
            v.sort_by_key(|c| u32::from(c));
            v
        }
        None => REPORTED.to_vec(),
    };

    let mut names: Vec<u32> = Vec::with_capacity(codes.len());
    let mut type_ids: Vec<i8> = Vec::with_capacity(codes.len());
    let mut offsets: Vec<i32> = Vec::with_capacity(codes.len());
    let mut strings: Vec<Option<String>> = Vec::new();
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
            ))
        }
    };

    let string_child: ArrayRef = Arc::new(StringArray::from_iter(strings));
    let bool_child: ArrayRef = Arc::new(BooleanArray::from_iter(bools));
    let int_child: ArrayRef = Arc::new(Int64Array::from_iter(ints));
    let children: Vec<ArrayRef> =
        union_child_arrays(&union_fields, &string_child, &bool_child, &int_child);

    let info_value = UnionArray::try_new(
        union_fields,
        ScalarBuffer::from(type_ids),
        Some(ScalarBuffer::from(offsets)),
        children,
    )
    .map_err(arrow_err)?;

    RecordBatch::try_new(
        GET_INFO_SCHEMA.clone(),
        vec![Arc::new(UInt32Array::from(names)), Arc::new(info_value)],
    )
    .map_err(arrow_err)
}

/// Assemble the union's child arrays in field order: the branches we populate get their built
/// arrays, every other branch an empty array of the right type (no row references them).
fn union_child_arrays(
    fields: &UnionFields,
    string_child: &ArrayRef,
    bool_child: &ArrayRef,
    int_child: &ArrayRef,
) -> Vec<ArrayRef> {
    fields
        .iter()
        .map(|(id, field)| match id {
            STRING_VALUE => string_child.clone(),
            BOOL_VALUE => bool_child.clone(),
            INT64_VALUE => int_child.clone(),
            _ => new_empty_array(field.data_type()),
        })
        .collect()
}

fn arrow_err(e: ArrowError) -> adbc_core::error::Error {
    err(
        format!("failed to build get_info batch: {e}"),
        Status::Internal,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

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
    fn empty_request_is_an_empty_batch() {
        let batch = build(Some(HashSet::new())).unwrap();
        assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 0);
    }
}

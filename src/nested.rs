//! Shared helpers for assembling the nested list/struct result batches of the metadata methods
//! ([`get_objects`](adbc_core::Connection::get_objects),
//! [`get_statistics`](adbc_core::Connection::get_statistics)).
//!
//! The result schemas are `adbc_core` constants, so the shape lookups here cannot fail in
//! practice — but this crate is loaded as a cdylib, so a mismatch must surface as a
//! `Status::Internal` error rather than a panic unwinding into the C ABI.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use arrow_array::{ArrayRef, ListArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields};

use crate::error::err;

/// An `Internal` error for a metadata result batch that failed to assemble.
pub(crate) fn arrow_err(e: ArrowError) -> Error {
    err(
        format!("failed to build metadata result batch: {e}"),
        Status::Internal,
    )
}

/// An `Internal` error for a metadata result schema without the expected shape.
fn shape_err(expected: &str) -> Error {
    err(
        format!("unexpected ADBC result schema shape: expected {expected}"),
        Status::Internal,
    )
}

/// The `Field` for a named field within `fields`.
pub(crate) fn field(fields: &Fields, name: &str) -> Result<FieldRef> {
    fields
        .find(name)
        .map(|(_, f)| f.clone())
        .ok_or_else(|| shape_err(&format!("a `{name}` field")))
}

/// The item field of a `List` field.
pub(crate) fn list_item(list_field: &FieldRef) -> Result<FieldRef> {
    match list_field.data_type() {
        DataType::List(item) => Ok(item.clone()),
        _ => Err(shape_err(&format!("`{}` to be a list", list_field.name()))),
    }
}

/// The `Fields` of a `Struct` field (e.g. a list item).
pub(crate) fn struct_fields(struct_field: &FieldRef) -> Result<Fields> {
    match struct_field.data_type() {
        DataType::Struct(fs) => Ok(fs.clone()),
        _ => Err(shape_err(&format!(
            "`{}` to be a struct",
            struct_field.name()
        ))),
    }
}

/// Wrap `child` (one entry per element) into a `ListArray` grouping elements by `lengths`, one
/// non-null list per parent.
pub(crate) fn list_of(item: FieldRef, lengths: &[usize], child: ArrayRef) -> Result<ArrayRef> {
    list_of_nullable(item, lengths, child, None)
}

/// Like [`list_of`], but marks selected list entries null via `nulls` (a validity mask, one bool
/// per entry — `false` = SQL NULL). A null entry still has a zero-length slice, so its `lengths`
/// value must be 0.
pub(crate) fn list_of_nullable(
    item: FieldRef,
    lengths: &[usize],
    child: ArrayRef,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0i32);
    let mut acc = 0i32;
    for len in lengths {
        acc = i32::try_from(*len)
            .ok()
            .and_then(|len| acc.checked_add(len))
            .ok_or_else(|| {
                err(
                    "metadata result list exceeds the i32 offset range",
                    Status::Internal,
                )
            })?;
        offsets.push(acc);
    }
    let list = ListArray::try_new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        nulls,
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(list))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::Field;

    #[test]
    fn list_of_offset_overflow_is_an_error_not_a_panic() {
        let item = Arc::new(Field::new("item", DataType::Int64, false));
        let child: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let too_long = vec![usize::MAX, 1];
        let error = list_of(item, &too_long, child).unwrap_err();
        assert_eq!(error.status, Status::Internal);
    }
}

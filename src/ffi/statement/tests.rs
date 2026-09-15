//! Tests for the statement entry points.
//!
//! A statement is live from the moment `AdbcStatementNew` builds it, and building one means
//! building a Spanner `DatabaseClient` — which opens a session with a real `CreateSession` RPC.
//! There is therefore no offline stand-in for a live statement, so what is exercised here is
//! everything that comes *before* one: argument validation, the released-handle refusals, the
//! partition export, and the Arrow C stream/array import a bound stream goes through.

use std::ffi::CString;
use std::sync::Arc;

use adbc_core::error::{AdbcStatusCode, Status};

use arrow_array::ffi::to_ffi;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatchIterator};

use super::*;
use crate::ffi::test_support::null;

const TARGET_TABLE: &str = "adbc.ingest.target_table";

/// The shape a caller sees after `AdbcStatementRelease`, and the shape of a zeroed struct that
/// was never passed to `AdbcStatementNew`.
fn released_handle() -> AdbcStatement {
    AdbcStatement {
        private_data: null(),
        private_driver: null(),
    }
}

fn key(name: &str) -> CString {
    CString::new(name).unwrap()
}

#[test]
fn rejects_a_null_statement_handle() {
    assert_eq!(
        unsafe { statement_new(null(), null(), null()) },
        AdbcStatusCode::from(Status::InvalidArguments)
    );
    assert_eq!(
        unsafe { statement_release(null(), null()) },
        AdbcStatusCode::from(Status::InvalidArguments)
    );
    // Everything that goes through `with_state` treats a null handle as an uninitialized one,
    // because that is exactly what it is.
    assert_eq!(
        unsafe { statement_prepare(null(), null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );
}

#[test]
fn every_entry_point_refuses_a_released_handle() {
    let mut handle = released_handle();
    let s = &raw mut handle;
    let name = key(TARGET_TABLE);
    let (mut length, mut integer, mut double) = (0_usize, 0_i64, 0.0_f64);

    for (label, status) in [
        ("prepare", unsafe { statement_prepare(s, null()) }),
        ("cancel", unsafe { statement_cancel(s, null()) }),
        ("set_sql_query", unsafe {
            statement_set_sql_query(s, name.as_ptr(), null())
        }),
        ("set_option", unsafe {
            statement_set_option(s, name.as_ptr(), name.as_ptr(), null())
        }),
        ("set_option_int", unsafe {
            statement_set_option_int(s, name.as_ptr(), 1, null())
        }),
        ("set_option_double", unsafe {
            statement_set_option_double(s, name.as_ptr(), 1.0, null())
        }),
        ("set_option_bytes", unsafe {
            statement_set_option_bytes(s, name.as_ptr(), b"x".as_ptr(), 1, null())
        }),
        ("set_substrait_plan", unsafe {
            statement_set_substrait_plan(s, b"x".as_ptr(), 1, null())
        }),
        ("get_option", unsafe {
            statement_get_option(s, name.as_ptr(), null(), &raw mut length, null())
        }),
        ("get_option_bytes", unsafe {
            statement_get_option_bytes(s, name.as_ptr(), null(), &raw mut length, null())
        }),
        ("get_option_int", unsafe {
            statement_get_option_int(s, name.as_ptr(), &raw mut integer, null())
        }),
        ("get_option_double", unsafe {
            statement_get_option_double(s, name.as_ptr(), &raw mut double, null())
        }),
        ("execute_query", unsafe {
            statement_execute_query(s, null(), null(), null())
        }),
        ("execute_schema", unsafe {
            statement_execute_schema(s, null(), null())
        }),
        ("execute_partitions", unsafe {
            statement_execute_partitions(s, null(), null(), null(), null())
        }),
        ("get_parameter_schema", unsafe {
            statement_get_parameter_schema(s, null(), null())
        }),
        ("bind", unsafe { statement_bind(s, null(), null(), null()) }),
        ("bind_stream", unsafe {
            statement_bind_stream(s, null(), null())
        }),
    ] {
        assert_eq!(
            status,
            AdbcStatusCode::from(Status::InvalidState),
            "{label}"
        );
    }
}

#[test]
fn bound_stream_schema_failures_preserve_diagnostics_and_release_ownership() {
    unsafe extern "C" fn failed_schema(
        _: *mut FFI_ArrowArrayStream,
        _: *mut FFI_ArrowSchema,
    ) -> std::ffi::c_int {
        libc::EIO
    }
    unsafe extern "C" fn description(_: *mut FFI_ArrowArrayStream) -> *const c_char {
        c"producer schema unavailable".as_ptr()
    }
    unsafe extern "C" fn no_description(_: *mut FFI_ArrowArrayStream) -> *const c_char {
        null()
    }
    unsafe extern "C" fn released_schema(
        _: *mut FFI_ArrowArrayStream,
        _: *mut FFI_ArrowSchema,
    ) -> std::ffi::c_int {
        0
    }

    for (get_schema, get_last_error, expected) in [
        (
            failed_schema as unsafe extern "C" fn(_, _) -> _,
            description as unsafe extern "C" fn(_) -> _,
            "producer schema unavailable",
        ),
        (failed_schema, no_description, "no error description"),
        (released_schema, no_description, "released schema"),
    ] {
        let lifetime = Arc::new(());
        let retained = lifetime.clone();
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema.clone());
        let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            std::iter::once_with(move || {
                drop(retained);
                Ok(batch)
            }),
            schema,
        )));
        stream.get_schema = Some(get_schema);
        stream.get_last_error = Some(get_last_error);
        assert_eq!(Arc::strong_count(&lifetime), 2);
        let failure = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap_err();
        assert_eq!(failure.status, Status::InvalidArguments);
        assert!(failure.message.contains(expected), "{}", failure.message);
        if expected != "released schema" {
            assert!(failure.message.contains(&libc::EIO.to_string()));
        }
        assert!(stream.release.is_none());
        assert_eq!(Arc::strong_count(&lifetime), 1);
    }
}

#[test]
fn imported_stream_batches_preserve_schema_rows_and_buffers_after_release() {
    use arrow_array::{RecordBatchOptions, RecordBatchReader};
    use arrow_schema::{DataType, Field};

    for fields in [vec![], vec![Field::new("value", DataType::Int64, true)]] {
        let schema = Arc::new(
            Schema::new(fields).with_metadata(std::collections::HashMap::from([(
                "source".to_owned(),
                "caller".to_owned(),
            )])),
        );
        let columns = if schema.fields().is_empty() {
            vec![]
        } else {
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef]
        };
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();
        let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            [
                Ok(RecordBatch::new_empty(schema.clone())),
                Ok(batch.clone()),
            ],
            schema.clone(),
        )));
        let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
        assert!(stream.release.is_none());
        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0);
        let found = reader.next().unwrap().unwrap();
        assert_eq!(found, batch);
        drop(batch);
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
        drop(reader);
        assert_eq!(found.num_rows(), 3);
        assert_eq!(found.schema(), schema);
        if !found.columns().is_empty() {
            let values = found
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values, &Int64Array::from(vec![Some(1), None, Some(3)]));
        }
    }
}

#[test]
fn imported_streams_reject_null_rows_and_mismatched_column_counts() {
    use arrow_schema::{DataType, Field};

    unsafe extern "C" fn null_row(
        _: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowArray,
    ) -> std::ffi::c_int {
        let array =
            StructArray::new_null(vec![Field::new("value", DataType::Int64, true)].into(), 1);
        let (array, _) = to_ffi(&array.to_data()).unwrap();
        unsafe { std::ptr::write(out, array) };
        0
    }

    for (fields, expected) in [
        (vec![], "returned 1 columns, but its schema declares 0"),
        (
            vec![Field::new("value", DataType::Int64, true)],
            "top-level null rows",
        ),
    ] {
        let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            std::iter::empty(),
            Arc::new(Schema::new(fields)),
        )));
        stream.get_next = Some(null_row);
        let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
        let failure = reader.next().unwrap().unwrap_err();
        assert!(failure.to_string().contains(expected), "{failure}");
        assert!(reader.next().is_none());
    }
}

/// A value long enough to live in a data buffer rather than inline in its view, holding bytes no
/// `str` may hold.
fn out_of_line_non_utf8_struct() -> arrow_data::ArrayData {
    use arrow_array::BinaryViewArray;
    use arrow_schema::{DataType, Field};

    let values = BinaryViewArray::from_iter_values([[0xff_u8; 16].as_slice()]);
    StructArray::new(
        vec![Field::new("payload", DataType::BinaryView, false)].into(),
        vec![Arc::new(values) as ArrayRef],
        None,
    )
    .to_data()
}

/// Arrow's C data interface importer ends in `ArrayData::new_unchecked`, so a producer's offsets,
/// view headers and UTF-8 are its own word until the driver checks them. Consumers cannot check
/// them for it: `GenericByteViewArray::value` resolves a view through `buffers.get_unchecked`, and
/// the byte-array families build a `&str` through `from_bytes_unchecked`, so a malformed array is
/// undefined behavior downstream rather than a panic that could be reported.
///
/// A view whose buffer index or offset addresses bytes the array never held cannot be built
/// through any safe arrow API, and `build_unchecked` would state a safety contract the data
/// breaks. Delivering a `BinaryView` array under a declared `Utf8View` schema reaches the same
/// guard safely: the two layouts are byte-identical, every buffer is genuinely present and
/// correctly sized, and only the one invariant `from_bytes_unchecked` rests on is false.
#[test]
fn imported_arrays_are_validated_and_name_the_column_that_fails() {
    use arrow_schema::{DataType, Field};

    unsafe extern "C" fn non_utf8_view(
        _: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowArray,
    ) -> std::ffi::c_int {
        let (array, _) = to_ffi(&out_of_line_non_utf8_struct()).unwrap();
        unsafe { std::ptr::write(out, array) };
        0
    }

    let declared = vec![Field::new("payload", DataType::Utf8View, false)];

    // The bound-stream path: every batch the producer yields is checked, not just the first.
    let mut stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
        std::iter::empty(),
        Arc::new(Schema::new(declared)),
    )));
    stream.get_next = Some(non_utf8_view);
    let mut reader = unsafe { BoundStreamReader::from_raw(&raw mut stream) }.unwrap();
    let failure = reader.next().unwrap().unwrap_err().to_string();
    assert!(
        failure.contains(r#"column "payload" is invalid"#),
        "{failure}"
    );
    assert!(failure.contains("non-UTF-8"), "{failure}");
    assert!(reader.next().is_none());
}

#[test]
fn exported_partitions_expose_their_payloads_and_release_idempotently() {
    let payloads = vec![b"first".to_vec(), Vec::new(), b"third-partition".to_vec()];
    let mut exported = export_partitions(payloads.clone());

    assert_eq!(exported.num_partitions, payloads.len());
    assert!(!exported.private_data.is_null());
    assert!(exported.release.is_some());
    for (index, expected) in payloads.iter().enumerate() {
        let pointer = unsafe { *exported.partitions.add(index) };
        let length = unsafe { *exported.partition_lengths.add(index) };
        assert_eq!(length, expected.len(), "partition {index}");
        // An empty `Vec` yields a dangling pointer that must not be dereferenced; the zero
        // length already says everything the caller needs.
        if expected.is_empty() {
            continue;
        }
        assert_eq!(
            unsafe { std::slice::from_raw_parts(pointer, length) },
            expected.as_slice(),
            "partition {index}"
        );
    }

    let release = exported.release.unwrap();
    unsafe { release(&raw mut exported) };
    assert!(exported.private_data.is_null());
    assert!(exported.partitions.is_null());
    assert!(exported.partition_lengths.is_null());
    assert_eq!(exported.num_partitions, 0);
    assert!(exported.release.is_none());

    // The header lets a caller release twice; the second call must be a no-op, not a
    // double free of the payload box.
    unsafe { release(&raw mut exported) };
    assert!(exported.private_data.is_null());
    assert_eq!(exported.num_partitions, 0);

    // A null argument is likewise tolerated.
    unsafe { release(null()) };
}

#[test]
fn exporting_no_partitions_still_releases_cleanly() {
    let mut exported = export_partitions(Vec::new());
    assert_eq!(exported.num_partitions, 0);
    assert!(!exported.private_data.is_null());
    // An empty `Vec`'s `as_mut_ptr` is a dangling non-null address, which a defensive or
    // sanitizer-instrumented driver manager reads as a real array of length zero.
    assert!(exported.partitions.is_null());
    assert!(exported.partition_lengths.is_null());
    let release = exported.release.unwrap();
    unsafe { release(&raw mut exported) };
    assert!(exported.private_data.is_null());
}

/// The pointer array must describe the payloads the box owns, not copies that were dropped on
/// the way out of `export_partitions`.
#[test]
fn exported_partitions_point_into_the_owned_payloads() {
    let payloads = vec![vec![9_u8; 4096]];
    let mut exported = export_partitions(payloads);
    let owner = unsafe { &*exported.private_data.cast::<PartitionsOwner>() };
    assert_eq!(owner.payloads.len(), 1);
    assert_eq!(
        unsafe { *exported.partitions },
        owner.payloads[0].as_ptr(),
        "the exported pointer must be the payload's own buffer"
    );
    assert_eq!(unsafe { *exported.partition_lengths }, 4096);
    let release = exported.release.unwrap();
    unsafe { release(&raw mut exported) };
}

//! Importing bound C streams, including producers that return an error without a description.

use std::ffi::{CStr, c_int};
use std::sync::Arc;

use adbc_core::error::Result;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi_and_data_type};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchOptions, RecordBatchReader, StructArray};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Fields, Schema, SchemaRef};

use crate::error::invalid_argument;

#[derive(Debug)]
pub(super) struct BoundStreamReader {
    stream: FFI_ArrowArrayStream,
    schema: SchemaRef,
    finished: bool,
}

impl BoundStreamReader {
    /// # Safety
    /// `stream` must be null or point to a valid Arrow C stream. Validated streams are moved
    /// into this reader; malformed callback tables retain caller ownership.
    pub(super) unsafe fn from_raw(stream: *mut FFI_ArrowArrayStream) -> Result<Self> {
        let Some(stream_ref) = (unsafe { stream.as_ref() }) else {
            return Err(invalid_argument("stream must not be null"));
        };
        if stream_ref.release.is_none() {
            return Err(invalid_argument("stream has already been released"));
        }
        let get_schema = stream_ref
            .get_schema
            .ok_or_else(|| invalid_argument("stream is missing its get_schema callback"))?;
        if stream_ref.get_next.is_none() {
            return Err(invalid_argument("stream is missing its get_next callback"));
        }
        if stream_ref.get_last_error.is_none() {
            return Err(invalid_argument(
                "stream is missing its get_last_error callback",
            ));
        }
        let mut stream = unsafe { FFI_ArrowArrayStream::from_raw(stream) };
        let mut schema = FFI_ArrowSchema::empty();
        let code = unsafe { get_schema(&raw mut stream, &raw mut schema) };
        if code != 0 {
            return Err(invalid_argument(stream_error(
                &mut stream,
                "get_schema",
                code,
            )));
        }
        if schema.release.is_none() {
            return Err(invalid_argument(
                "bound stream get_schema returned a released schema",
            ));
        }
        let schema = Schema::try_from(&schema).map_err(|failure| {
            invalid_argument(format!(
                "bound stream returned an invalid schema: {failure}"
            ))
        })?;
        Ok(Self {
            stream,
            schema: Arc::new(schema),
            finished: false,
        })
    }
}

impl Iterator for BoundStreamReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let mut array = FFI_ArrowArray::empty();
        let get_next = self
            .stream
            .get_next
            .expect("validated when the stream was bound");
        let code = unsafe { get_next(&raw mut self.stream, &raw mut array) };
        if code != 0 {
            self.finished = true;
            return Some(Err(ArrowError::CDataInterface(stream_error(
                &mut self.stream,
                "get_next",
                code,
            ))));
        }
        if array.release.is_none() {
            self.finished = true;
            return None;
        }
        if array.num_children() != self.schema.fields().len() {
            self.finished = true;
            return Some(Err(ArrowError::CDataInterface(format!(
                "bound stream returned {} columns, but its schema declares {}",
                array.num_children(),
                self.schema.fields().len(),
            ))));
        }
        let result = unsafe {
            from_ffi_and_data_type(array, DataType::Struct(self.schema.fields().clone()))
        }
        .and_then(|data| {
            validate_imported(&data, self.schema.fields()).map_err(|failure| {
                ArrowError::CDataInterface(format!(
                    "bound stream returned an invalid Arrow array: {failure}"
                ))
            })?;
            if data.null_count() != 0 {
                return Err(ArrowError::CDataInterface(
                    "bound stream must not contain top-level null rows; put null values in the individual columns instead".to_owned(),
                ));
            }
            let row_count = data.len();
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                StructArray::from(data).into_parts().1,
                &RecordBatchOptions::new().with_row_count(Some(row_count)),
            )
        });
        self.finished = result.is_err();
        Some(result)
    }
}

impl RecordBatchReader for BoundStreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Checks the invariants an array that arrived over the C data interface is only assumed to hold,
/// naming the column that violates one.
///
/// Arrow's importer ends in `ArrayData::new_unchecked`, so nothing a producer put in the buffers
/// has been looked at: offsets, view buffer indices, UTF-8 and declared null counts are all still
/// the producer's word. Everything downstream is ordinary safe Rust that trusts arrow-rs's
/// invariants instead, and two of those reads are unchecked by design --
/// `GenericByteViewArray::value` resolves a view through `buffers.get_unchecked`, and both byte
/// array families hand their bytes to `from_bytes_unchecked` to make a `&str`. A malformed array
/// is therefore undefined behavior in the consumer rather than a panic it could report, and no
/// consumer can defend itself against it: this driver's own `crate::bind::cell_value` reads every
/// bound value that way, for parameter binding and for bulk ingest alike. The boundary the arrays
/// enter through is the one place that covers all of them at once.
///
/// It covers an honest producer's bug, not a dishonest producer's lie, and nothing here can cover
/// the second. `FFI_ArrowArray` transmits buffer pointers and no buffer lengths, so for `Utf8`,
/// `Binary`, `LargeUtf8` and `LargeBinary` the values-buffer length *is* the last offset, which
/// `from_ffi` reads out of the producer's own offset buffer before this function is reached; a
/// last offset that lies yields a `Buffer` spanning memory the producer does not own, and the
/// validation below is then itself the out-of-bounds read. That is the `unsafe` contract
/// `from_ffi` documents rather than a defect in it, and it is not much of a security boundary
/// either way -- the producer shares this address space. What this buys is that a well-meaning
/// caller's bad batch is refused, by column name, instead of corrupting the driver silently.
///
/// The cost is one linear pass per buffer -- one offset or view header per row, a popcount over
/// each null bitmap, and a UTF-8 scan over the bytes of a string column -- paid once, at the
/// boundary. What follows it is per-row work of the same order and then a gRPC round trip: every
/// bound value is converted to a Spanner `Value` by `crate::bind::cell_value`, and an ingest's
/// rows become protobuf mutations that are serialized and TLS-encrypted on the way out. Only
/// arrays that arrive over the C data interface pay it at all -- the Rust trait API takes an
/// already-validated `RecordBatch`.
pub(super) fn validate_imported(
    data: &ArrayData,
    fields: &Fields,
) -> std::result::Result<(), String> {
    // The struct itself carries no values, so only its own layout and null buffer are checked
    // here; `validate_full` then covers each column and everything nested inside it.
    data.validate_data()
        .map_err(|failure| format!("the top-level struct is invalid: {failure}"))?;
    for (field, column) in fields.iter().zip(data.child_data()) {
        column
            .validate_full()
            .map_err(|failure| format!("column {:?} is invalid: {failure}", field.name()))?;
    }
    Ok(())
}

fn stream_error(stream: &mut FFI_ArrowArrayStream, callback: &str, code: c_int) -> String {
    let get_last_error = stream
        .get_last_error
        .expect("validated when the stream was bound");
    let message = unsafe { get_last_error(stream) };
    let detail = if message.is_null() {
        "producer supplied no error description".into()
    } else {
        unsafe { CStr::from_ptr(message) }.to_string_lossy()
    };
    format!("bound Arrow C stream {callback} failed with error code {code}: {detail}")
}

use std::ffi::CStr;
use std::sync::Arc;

use adbc_core::error::Error;
use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::*;
use crate::ffi::test_support::null;

/// A stream from a caller that populated no `private_driver`, which is what a directly linked
/// consumer does. The forwarding of a real one is covered by
/// `a_stream_error_carries_the_handles_driver_vtable`.
fn exported(reader: Box<dyn RecordBatchReader + Send>) -> FFI_ArrowArrayStream {
    export_reader(reader, null())
}

struct PanickingReader {
    schema: SchemaRef,
    panic_in_schema: bool,
}

impl Iterator for PanickingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        panic!("next contains a NUL: \0")
    }
}

impl RecordBatchReader for PanickingReader {
    fn schema(&self) -> SchemaRef {
        assert!(!self.panic_in_schema, "schema failed");
        self.schema.clone()
    }
}

#[test]
fn exports_an_ordinary_reader() {
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![7])) as ArrayRef)])
            .unwrap();
    let schema = batch.schema();
    let mut stream = exported(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch.clone())),
        schema,
    )));
    let mut reader = unsafe { ArrowArrayStreamReader::from_raw(&raw mut stream) }.unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), batch);
    assert!(reader.next().is_none());
}

#[test]
fn exports_adbc_statuses_as_errno() {
    for (status, expected) in [
        (Status::Ok, EIO),
        (Status::Unknown, EIO),
        (Status::NotImplemented, ENOTSUP),
        (Status::NotFound, ENOENT),
        (Status::AlreadyExists, EEXIST),
        (Status::InvalidArguments, EINVAL),
        (Status::InvalidState, EINVAL),
        (Status::InvalidData, EIO),
        (Status::Integrity, EIO),
        (Status::Internal, EIO),
        (Status::IO, EIO),
        (Status::Cancelled, ECANCELED),
        (Status::Timeout, ETIMEDOUT),
        (Status::Unauthenticated, EACCES),
        (Status::Unauthorized, EACCES),
    ] {
        let error = Error::with_message_and_status("ADBC stream failure", status);
        let mut stream = error_stream(ArrowError::ExternalError(Box::new(error)));
        let mut out = FFI_ArrowArray::empty();
        assert_eq!(
            unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut out) },
            expected,
            "{status:?}"
        );
        let message = unsafe { CStr::from_ptr(stream.get_last_error.unwrap()(&raw mut stream)) }
            .to_str()
            .unwrap();
        assert!(message.contains("ADBC stream failure"), "{message}");
    }

    // The ADBC status is found even when nested behind another external error.
    let error = Error::with_message_and_status("cancelled", Status::Cancelled);
    let error = ArrowError::ExternalError(Box::new(ArrowError::ExternalError(Box::new(error))));
    let mut stream = error_stream(error);
    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut out) },
        ECANCELED
    );

    // A plain Arrow error keeps arrow's own errno mapping.
    let mut stream = error_stream(ArrowError::NotYetImplemented("not supported".into()));
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut out) },
        ENOSYS
    );
    let message = unsafe { CStr::from_ptr(stream.get_last_error.unwrap()(&raw mut stream)) }
        .to_str()
        .unwrap();
    assert!(message.contains("not supported"), "{message}");
}

/// The reason this whole export layer exists, pinned end to end at the Rust level: the C++
/// `adbc_validation` case `SpannerStatementTest.SqlQueryCancel` requires the result stream's
/// `get_next` to return exactly `ECANCELED` (or 0) once the statement has been cancelled.
///
/// The error built here is byte for byte what a cancelled read hands this layer in production:
/// `runtime.rs` raises `("operation cancelled", Status::Cancelled)`, `SpannerBatchReader` wraps it
/// with `conversion.rs`'s `to_arrow_error` -- i.e. `ArrowError::ExternalError(Box::new(error))` --
/// and only this module's downcast back through the error chain recovers the ADBC status behind
/// it. Exporting the same reader through arrow-rs's own `FFI_ArrowArrayStream` exporter, which the
/// driver used before this layer, could not: it maps every `ArrowError` onto
/// `ENOSYS`/`ENOMEM`/`EIO`/`EINVAL`, so a cancel surfaced as `EINVAL` (22) and the case had to be
/// excluded from the validation gate.
#[test]
fn a_cancelled_read_reports_ecanceled_for_sql_query_cancel() {
    use super::super::abi::ADBC_STATUS_CANCELLED;

    let cancelled = Error::with_message_and_status("operation cancelled", Status::Cancelled);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(cancelled)));

    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut out) },
        ECANCELED,
        "SqlQueryCancel accepts only ECANCELED or 0 from get_next"
    );
    let message = unsafe { CStr::from_ptr(stream.get_last_error.unwrap()(&raw mut stream)) }
        .to_str()
        .unwrap();
    assert!(message.contains("operation cancelled"), "{message}");

    // The structured error a manager-mediated caller reads agrees with the errno.
    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert!(!exported.is_null());
    assert_eq!(status, ADBC_STATUS_CANCELLED);

    unsafe { stream.release.unwrap()(&raw mut stream) };
}

/// The timeout twin of `a_cancelled_read_reports_ecanceled_for_sql_query_cancel`: an RPC deadline
/// from `spanner.rpc.timeout_seconds.{query,fetch}` reaches the reader as the same boxed driver
/// error, only with `Status::Timeout`, and must come out as `ETIMEDOUT` rather than the `EINVAL`
/// arrow-rs's exporter gave every such failure.
#[test]
fn a_timed_out_read_reports_etimedout() {
    use super::super::abi::ADBC_STATUS_TIMEOUT;

    let timed_out = Error::with_message_and_status(
        "the query did not complete within spanner.rpc.timeout_seconds.query",
        Status::Timeout,
    );
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(timed_out)));

    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut out) },
        ETIMEDOUT
    );

    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert!(!exported.is_null());
    assert_eq!(status, ADBC_STATUS_TIMEOUT);

    unsafe { stream.release.unwrap()(&raw mut stream) };
}

/// A nonconforming host may keep the callback pointer and call it after release, when
/// `private_data` is already null; that must yield a null message, not undefined behavior.
#[test]
fn get_last_error_tolerates_a_released_or_null_stream() {
    let mut stream = error_stream(ArrowError::ComputeError("boom".to_owned()));
    let saved = stream.get_last_error.unwrap();
    unsafe { stream.release.unwrap()(&raw mut stream) };
    assert!(unsafe { saved(&raw mut stream) }.is_null());
    assert!(unsafe { saved(null()) }.is_null());
}

fn error_stream(error: ArrowError) -> FFI_ArrowArrayStream {
    exported(Box::new(RecordBatchIterator::new(
        std::iter::once(Err(error)),
        Arc::new(Schema::empty()),
    )))
}

#[test]
fn error_from_array_stream_exposes_the_pending_error() {
    use super::super::abi::ADBC_STATUS_CANCELLED;
    use super::super::error::{error_get_detail, error_get_detail_count};

    let mut error = Error::with_message_and_status("stream blew up", Status::Cancelled);
    // A detail in the shape `error::from_spanner` forwards: the lowercased proto type name of a
    // `google.rpc.Status` detail, keyed to its ProtoJSON bytes.
    error.details = Some(vec![(
        "google.rpc.retryinfo".to_string(),
        br#"{"retryDelay":"1s"}"#.to_vec(),
    )]);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(error)));
    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut out) },
        ECANCELED
    );

    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert!(!exported.is_null());
    assert_eq!(status, ADBC_STATUS_CANCELLED);
    // The structured error carries the driver's own message, not the Arrow chain prefix
    // that `get_last_error` reports.
    let message = unsafe { CStr::from_ptr((*exported).message) }
        .to_str()
        .unwrap();
    assert_eq!(message, "stream blew up");
    assert_eq!(unsafe { error_get_detail_count(exported) }, 1);
    let detail = unsafe { error_get_detail(exported, 0) };
    assert_eq!(
        unsafe { CStr::from_ptr(detail.key) }.to_str().unwrap(),
        "google.rpc.retryinfo"
    );
    assert_eq!(
        unsafe { std::slice::from_raw_parts(detail.value, detail.value_length) },
        br#"{"retryDelay":"1s"}"#
    );

    // Asking again re-exports the same error without leaking the previous export, and a
    // null status pointer must simply not be written.
    let exported = unsafe { error_from_array_stream(&raw mut stream, null()) };
    assert!(!exported.is_null());
    assert_eq!(unsafe { error_get_detail_count(exported) }, 1);

    // Releasing the stream invalidates the error and de-recognizes the stream.
    unsafe { stream.release.unwrap()(&raw mut stream) };
    let mut status: AdbcStatusCode = 0xEE;
    assert!(unsafe { error_from_array_stream(&raw mut stream, &raw mut status) }.is_null());
    assert_eq!(
        status, 0xEE,
        "status must stay untouched for unrecognized streams"
    );
}

#[test]
fn error_from_array_stream_reports_ok_when_nothing_failed() {
    use super::super::abi::ADBC_STATUS_OK;

    let mut stream = exported(Box::new(RecordBatchIterator::new(
        std::iter::empty(),
        Arc::new(Schema::empty()),
    )));
    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert!(!exported.is_null());
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(unsafe { (*exported).message.is_null() });
    unsafe { stream.release.unwrap()(&raw mut stream) };
}

#[test]
fn error_from_array_stream_rejects_foreign_and_null_streams() {
    unsafe extern "C" fn foreign_release(_stream: *mut FFI_ArrowArrayStream) {}

    let mut status: AdbcStatusCode = 0xEE;
    assert!(unsafe { error_from_array_stream(null(), &raw mut status) }.is_null());

    // A stream some other library produced: valid pointers, but not this driver's release.
    let mut foreign = FFI_ArrowArrayStream {
        get_schema: None,
        get_next: None,
        get_last_error: None,
        release: Some(foreign_release),
        private_data: (&raw mut status).cast::<c_void>(),
    };
    assert!(unsafe { error_from_array_stream(&raw mut foreign, &raw mut status) }.is_null());
    assert_eq!(
        status, 0xEE,
        "status must stay untouched for unrecognized streams"
    );
}

#[test]
fn contains_reader_panics_in_get_next() {
    let mut stream = exported(Box::new(PanickingReader {
        schema: Arc::new(Schema::empty()),
        panic_in_schema: false,
    }));
    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut out) },
        EINVAL
    );
    let message = unsafe { CStr::from_ptr(stream.get_last_error.unwrap()(&raw mut stream)) }
        .to_str()
        .unwrap();
    assert!(message.contains("next contains a NUL: ?"), "{message}");

    // Once a reader has panicked, do not enter its potentially inconsistent state again.
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut out) },
        EINVAL
    );
}

#[test]
fn contains_reader_panics_in_get_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let mut stream = exported(Box::new(PanickingReader {
        schema,
        panic_in_schema: true,
    }));
    let mut out = FFI_ArrowSchema::empty();
    assert_eq!(
        unsafe { stream.get_schema.unwrap()(&raw mut stream, &raw mut out) },
        EINVAL
    );
    let message = unsafe { CStr::from_ptr(stream.get_last_error.unwrap()(&raw mut stream)) }
        .to_str()
        .unwrap();
    assert!(message.contains("schema failed"), "{message}");
}

#[test]
fn contains_reader_panics_in_release() {
    struct DropPanic(PanickingReader);

    impl Iterator for DropPanic {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.0.next()
        }
    }

    impl RecordBatchReader for DropPanic {
        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }
    }

    impl Drop for DropPanic {
        fn drop(&mut self) {
            panic!("drop failed")
        }
    }

    let mut stream = exported(Box::new(DropPanic(PanickingReader {
        schema: Arc::new(Schema::empty()),
        panic_in_schema: false,
    })));
    unsafe { stream.release.unwrap()(&raw mut stream) };
    assert!(stream.release.is_none());
    assert!(stream.private_data.is_null());
}

/// `AdbcErrorFromArrayStream` hands back an `AdbcError` in the driver's own storage, so nothing
/// else will ever fill in its `private_driver` — and the driver manager reaches an error's
/// details only through that field. A stream that dropped the vtable of the handle it came from
/// therefore produced errors whose details no manager-mediated caller could read.
#[test]
fn a_stream_error_carries_the_handles_driver_vtable() {
    let vtable = 0x5eed_usize as *const AdbcDriver;
    let mut stream = export_reader(
        Box::new(RecordBatchIterator::new(
            std::iter::once(Err(ArrowError::MemoryError("no room".to_owned()))),
            Arc::new(Schema::empty()),
        )),
        vtable,
    );
    let mut out = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut out) },
        ENOMEM
    );

    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert!(!exported.is_null());
    assert_eq!(unsafe { (*exported).private_driver }, vtable);
    // Present on the empty export too, which is the same storage.
    unsafe { stream.release.unwrap()(&raw mut stream) };
}

/// A nonconforming host may release a stream twice, or keep the callback pointers it copied and
/// call them afterwards. The Arrow C Stream Interface says a released stream has null callbacks
/// and null `private_data`; the driver must survive being treated as though it did not.
#[test]
fn a_released_stream_survives_a_second_release_and_stale_callbacks() {
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let schema = batch.schema();
    let mut stream = exported(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch)),
        schema,
    )));
    let (get_next, get_schema, release) = (
        stream.get_next.unwrap(),
        stream.get_schema.unwrap(),
        stream.release.unwrap(),
    );

    unsafe { release(&raw mut stream) };
    assert!(stream.private_data.is_null());
    assert!(stream.get_next.is_none());
    assert!(stream.get_schema.is_none());
    assert!(stream.release.is_none());

    // A second release is a no-op, not a double free.
    unsafe { release(&raw mut stream) };
    assert!(stream.private_data.is_null());

    // Stale callbacks report EINVAL rather than dereferencing the null they were left with.
    let mut array = FFI_ArrowArray::empty();
    let mut out_schema = FFI_ArrowSchema::empty();
    assert_eq!(unsafe { get_next(&raw mut stream, &raw mut array) }, EINVAL);
    assert_eq!(
        unsafe { get_schema(&raw mut stream, &raw mut out_schema) },
        EINVAL
    );
    // And on a null stream pointer, which is what a zeroed struct amounts to.
    assert_eq!(unsafe { get_next(null(), &raw mut array) }, EINVAL);
    assert_eq!(unsafe { get_schema(null(), &raw mut out_schema) }, EINVAL);
    // Releasing a null stream must also be tolerated.
    unsafe { release(null()) };
}

/// A failed read followed by a successful one must not leave the old error standing:
/// `AdbcErrorFromArrayStream` reports "the ADBC status code, or ADBC_STATUS_OK if there is no
/// error", and `get_last_error`'s pointer is only promised to last until the next operation.
#[test]
fn a_successful_read_clears_the_previous_failure() {
    use super::super::abi::ADBC_STATUS_OK;

    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![7])) as ArrayRef)])
            .unwrap();
    let schema = batch.schema();
    let mut stream = exported(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch)),
        schema,
    )));

    // A null output is the one failure a caller can provoke without a failing reader.
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, null()) },
        EINVAL
    );
    assert!(!unsafe { stream.get_last_error.unwrap()(&raw mut stream) }.is_null());

    let mut array = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut array) },
        0
    );
    assert!(unsafe { stream.get_last_error.unwrap()(&raw mut stream) }.is_null());

    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(unsafe { (*exported).message.is_null() });

    unsafe { array.release.unwrap()(&raw mut array) };
    unsafe { stream.release.unwrap()(&raw mut stream) };
}

/// Reading the schema after a failed `get_next` must not make the failure disappear.
///
/// `get_schema` is callable at any point in a stream's life -- the Arrow C Stream Interface puts
/// no ordering on it -- and it consumes nothing, so its success is no evidence that the read
/// recovered. A caller that reaches for the schema while working out how to report the failure
/// would otherwise be told by `AdbcErrorFromArrayStream` that there was none.
#[test]
fn reading_the_schema_after_a_failed_read_keeps_the_failure() {
    use super::super::abi::ADBC_STATUS_CANCELLED;

    let error = Error::with_message_and_status("the query was cancelled", Status::Cancelled);
    let mut stream = error_stream(ArrowError::ExternalError(Box::new(error)));

    let mut array = FFI_ArrowArray::empty();
    assert_eq!(
        unsafe { stream.get_next.unwrap()(&raw mut stream, &raw mut array) },
        ECANCELED
    );

    let mut schema = FFI_ArrowSchema::empty();
    assert_eq!(
        unsafe { stream.get_schema.unwrap()(&raw mut stream, &raw mut schema) },
        0,
        "the schema is still readable after a failed read"
    );

    let pending = unsafe { stream.get_last_error.unwrap()(&raw mut stream) };
    assert!(
        !pending.is_null(),
        "the read failure was cleared by get_schema"
    );
    let message = unsafe { CStr::from_ptr(pending) }.to_str().unwrap();
    assert!(message.contains("the query was cancelled"), "{message}");

    let mut status: AdbcStatusCode = 0xEE;
    let exported = unsafe { error_from_array_stream(&raw mut stream, &raw mut status) };
    assert_eq!(
        status, ADBC_STATUS_CANCELLED,
        "the read failure was reported as ADBC_STATUS_OK"
    );
    assert!(!unsafe { (*exported).message.is_null() });

    unsafe { stream.release.unwrap()(&raw mut stream) };
}

//! Panic-contained Arrow C Stream export.
//!
//! Arrow-rs's exporter calls a [`RecordBatchReader`] directly from `extern "C"` callbacks. A
//! reader panic would therefore cross the C ABI and abort the host process. Keep the callback
//! implementation here so every stream produced by the driver has the same containment boundary.

use std::ffi::{CString, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};

use adbc_core::error::{Error, Status};
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, RecordBatchReader, StructArray};
use arrow_schema::ArrowError;

use libc::{EACCES, ECANCELED, EEXIST, EINVAL, EIO, ENOENT, ENOMEM, ENOSYS, ENOTSUP, ETIMEDOUT};

use super::abi::{
    ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA, ADBC_STATUS_OK, AdbcDriver, AdbcError, AdbcStatusCode,
};
use super::error::export_error;
use super::guard::{panic_message, sanitized_cstring};
use crate::error::chain;

/// A failed callback, kept in both the shapes the ABI can ask for it in: the C string
/// `get_last_error` returns, and the structured error `error_from_array_stream` exports.
struct LastError {
    message: CString,
    error: Error,
}

/// Tags [`PrivateData`] so [`error_from_array_stream`] can recognize this driver's own streams.
/// The release-callback address is the primary check; this guards the residual case of a linker
/// merging a byte-identical foreign release function onto the same address.
const STREAM_MAGIC: u64 = 0x5350_414e_5354_524d; // "SPANSTRM"

struct PrivateData {
    magic: u64,
    reader: Box<dyn RecordBatchReader + Send>,
    /// The driver vtable the handle that produced this stream carried, forwarded into the
    /// `AdbcError` [`error_from_array_stream`] hands back so a manager-mediated caller can read
    /// its details.
    private_driver: *const AdbcDriver,
    last_error: Option<LastError>,
    /// Backing store for the `AdbcError` handed out by [`error_from_array_stream`]. The header
    /// promises the caller that pointer stays valid until the stream's release callback runs, so
    /// the storage lives here and [`PrivateData::drop`] frees it.
    exported_error: Option<Box<AdbcError>>,
    panicked: bool,
}

impl Drop for PrivateData {
    fn drop(&mut self) {
        if let Some(mut exported) = self.exported_error.take()
            && let Some(release) = exported.release
        {
            unsafe { release(&raw mut *exported) };
        }
    }
}

pub(super) fn export_reader(
    reader: Box<dyn RecordBatchReader + Send>,
    private_driver: *const AdbcDriver,
) -> FFI_ArrowArrayStream {
    let private_data = Box::new(PrivateData {
        magic: STREAM_MAGIC,
        reader,
        private_driver,
        last_error: None,
        exported_error: None,
        panicked: false,
    });
    FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release),
        private_data: Box::into_raw(private_data).cast::<c_void>(),
    }
}

unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowSchema,
) -> c_int {
    unsafe {
        catch_callback(stream, |private| {
            if out.is_null() {
                return private.fail("Arrow C Stream schema output must not be null");
            }
            match FFI_ArrowSchema::try_from(private.reader.schema().as_ref()) {
                Ok(schema) => {
                    std::ptr::write_unaligned(out, schema);
                    0
                }
                Err(error) => private.arrow_error(&error),
            }
        })
    }
}

unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowArray,
) -> c_int {
    unsafe {
        catch_callback(stream, |private| {
            // Only this callback clears what a previous failure left behind, and it does so by
            // superseding it: `error_from_array_stream` promises "the ADBC status code, or
            // ADBC_STATUS_OK if there is no error" (adbc.h:398-399), so a `get_next` that
            // succeeded must not still be reporting the failure of an earlier one. Every exit
            // below that returns non-zero records its own error first, so this clear is only
            // observable on success.
            //
            // `get_schema` deliberately does not clear: it reads a schema this driver already
            // holds and consumes nothing, so its success says nothing about the read. Clearing
            // there would let a caller that asked for the schema after a failed `get_next` --
            // which the Arrow C Stream Interface permits, `get_schema` being callable at any
            // time -- see the query failure replaced by ADBC_STATUS_OK.
            private.last_error = None;
            if out.is_null() {
                return private.fail("Arrow C Stream array output must not be null");
            }
            match private.reader.next() {
                None => {
                    std::ptr::write_unaligned(out, FFI_ArrowArray::empty());
                    0
                }
                Some(Ok(batch)) => {
                    let array = StructArray::from(batch);
                    std::ptr::write_unaligned(out, FFI_ArrowArray::new(&array.to_data()));
                    0
                }
                Some(Err(error)) => private.arrow_error(&error),
            }
        })
    }
}

unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    // The null `private_data` check guards a released or zero-initialized stream, which
    // `catch_unwind` could not: dereferencing the null would be undefined behavior, not a panic.
    if stream.is_null() || unsafe { (*stream).private_data.is_null() } {
        return std::ptr::null();
    }
    catch_unwind(AssertUnwindSafe(|| unsafe {
        private_data(stream)
            .last_error
            .as_ref()
            .map_or(std::ptr::null(), |error| error.message.as_ptr())
    }))
    .unwrap_or(std::ptr::null())
}

/// The ADBC 1.1.0 `ErrorFromArrayStream` entry point: expose the structured error behind a
/// failed stream callback, which the Arrow C Stream Interface itself can only report as an
/// errno and a message.
///
/// Only streams produced by [`export_reader`] are recognized: the stream's `release` callback
/// must be this module's private [`release`] symbol and its private data must carry
/// [`STREAM_MAGIC`], so the check cannot misfire on a stream some other library produced, and a
/// released stream (whose callbacks are nulled) is rejected the same way. Unrecognized streams
/// get a null return with `status` untouched, exactly as the header specifies.
///
/// # Safety
/// `stream` must be null or point to a valid or released `ArrowArrayStream`; `status` must be
/// null or writable.
pub(super) unsafe extern "C" fn error_from_array_stream(
    stream: *mut FFI_ArrowArrayStream,
    status: *mut AdbcStatusCode,
) -> *const AdbcError {
    if stream.is_null() || unsafe { (*stream).private_data.is_null() } {
        return std::ptr::null();
    }
    let ours = unsafe { (*stream).release }.is_some_and(|callback| {
        std::ptr::fn_addr_eq(
            callback,
            release as unsafe extern "C" fn(*mut FFI_ArrowArrayStream),
        )
    });
    if !ours {
        return std::ptr::null();
    }
    catch_unwind(AssertUnwindSafe(|| {
        let private = unsafe { private_data(stream) };
        if private.magic != STREAM_MAGIC {
            return std::ptr::null();
        }
        // The storage must outlive this call, so it lives in `PrivateData`; the previous
        // export is released before it is overwritten, making repeated calls leak-free.
        let private_driver = private.private_driver;
        let exported = private.exported_error.get_or_insert_with(|| {
            Box::new(AdbcError {
                message: std::ptr::null_mut(),
                vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
                sqlstate: [0; 5],
                release: None,
                private_data: std::ptr::null_mut(),
                private_driver,
            })
        });
        match private.last_error.as_ref() {
            None => {
                // A recognized stream with nothing pending reports ADBC_STATUS_OK and an
                // empty error, mirroring the reference C++ drivers.
                if let Some(release) = exported.release {
                    unsafe { release(&raw mut **exported) };
                }
                if !status.is_null() {
                    unsafe { std::ptr::write_unaligned(status, ADBC_STATUS_OK) };
                }
            }
            Some(last) => {
                // `export_error` decides the layout from the sentinel, and this storage is
                // driver-owned, so it is always the full 1.1.0 struct.
                exported.vendor_code = ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
                unsafe { export_error(&raw mut **exported, last.error.clone()) };
                if !status.is_null() {
                    unsafe {
                        std::ptr::write_unaligned(status, AdbcStatusCode::from(last.error.status));
                    }
                }
            }
        }
        std::ptr::from_ref::<AdbcError>(&**exported)
    }))
    .unwrap_or(std::ptr::null())
}

unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    let stream = unsafe { &mut *stream };
    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;
    stream.release = None;
    let private_data = std::mem::replace(&mut stream.private_data, std::ptr::null_mut());
    if !private_data.is_null() {
        // A reader can own arbitrary client code. Its destructor must not unwind through `release`.
        let _ = catch_unwind(AssertUnwindSafe(|| {
            drop(unsafe { Box::from_raw(private_data.cast::<PrivateData>()) });
        }));
    }
}

unsafe fn catch_callback(
    stream: *mut FFI_ArrowArrayStream,
    callback: impl FnOnce(&mut PrivateData) -> c_int,
) -> c_int {
    if stream.is_null() || unsafe { (*stream).private_data.is_null() } {
        return EINVAL;
    }
    let private = unsafe { private_data(stream) };
    if private.panicked {
        return EINVAL;
    }
    match catch_unwind(AssertUnwindSafe(|| callback(private))) {
        Ok(code) => code,
        Err(cause) => {
            private.panicked = true;
            let message = format!(
                "Uncaught panic in the Spanner Arrow stream: {}",
                panic_message(cause.as_ref())
            );
            private.set_error(Error::with_message_and_status(message, Status::Internal));
            EINVAL
        }
    }
}

unsafe fn private_data<'a>(stream: *mut FFI_ArrowArrayStream) -> &'a mut PrivateData {
    unsafe { &mut *((*stream).private_data.cast::<PrivateData>()) }
}

impl PrivateData {
    fn set_error(&mut self, error: Error) {
        self.last_error = Some(LastError {
            message: sanitized_cstring(&error.message),
            error,
        });
    }

    fn fail(&mut self, message: impl Into<String>) -> c_int {
        self.set_error(Error::with_message_and_status(
            message.into(),
            Status::InvalidArguments,
        ));
        EINVAL
    }

    fn arrow_error(&mut self, error: &ArrowError) -> c_int {
        // The errno and the status are read off one table but are deliberately not the same
        // mapping: an Arrow variant the driver did not raise gets the errno the Arrow C Stream
        // Interface expects for it -- `NotYetImplemented` is ENOSYS, the callback not being
        // implemented at all, rather than the ENOTSUP `adbc_status_to_errno` gives
        // `Status::NotImplemented` for.
        let (errno, status) = match error {
            ArrowError::NotYetImplemented(_) => (ENOSYS, Status::NotImplemented),
            ArrowError::MemoryError(_) => (ENOMEM, Status::Internal),
            ArrowError::IoError(_, _) => (EIO, Status::IO),
            _ => (EINVAL, Status::Unknown),
        };
        let embedded = chain(error)
            .find_map(|source| source.downcast_ref::<Error>())
            .cloned();
        // A driver error embedded in the chain outranks the Arrow variant in both dimensions.
        let errno = embedded
            .as_ref()
            .map_or(errno, |embedded| adbc_status_to_errno(embedded.status));
        let structured =
            embedded.unwrap_or_else(|| Error::with_message_and_status(error.to_string(), status));
        // `get_last_error` reports the full Arrow error chain; the structured error keeps the
        // driver's own message, status, and details when one is embedded in that chain.
        self.last_error = Some(LastError {
            message: sanitized_cstring(&error.to_string()),
            error: structured,
        });
        errno
    }
}

fn adbc_status_to_errno(status: Status) -> c_int {
    match status {
        Status::Ok | Status::Unknown => EIO,
        Status::NotImplemented => ENOTSUP,
        Status::NotFound => ENOENT,
        Status::AlreadyExists => EEXIST,
        Status::InvalidArguments | Status::InvalidState => EINVAL,
        Status::InvalidData | Status::Integrity | Status::Internal | Status::IO => EIO,
        Status::Cancelled => ECANCELED,
        Status::Timeout => ETIMEDOUT,
        Status::Unauthenticated | Status::Unauthorized => EACCES,
    }
}

#[cfg(test)]
mod tests;

//! Writing a driver [`Error`] back through a caller-owned `AdbcError`.
//!
//! The caller allocates the struct, so its size depends on the ADBC revision it was built against.
//! A 1.1.0 caller signals the larger layout by presetting [`ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA`]
//! in `vendor_code`; anything else must be treated as the 1.0.0 prefix, where the trailing two
//! fields do not exist and writing them would run off the end of the caller's allocation.

use std::ffi::{CString, c_int, c_void};

use adbc_core::error::Error;

use super::abi::{ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA, AdbcError, AdbcErrorDetail, AdbcErrorV100};
use super::guard::sanitized_cstring;

/// Owned backing store for the key/value pairs handed out by [`error_get_detail`]. The header
/// promises the caller these borrow from the error and stay valid until it is released, so they
/// live here and are freed by [`release_error`].
type ErrorDetails = Vec<(CString, Vec<u8>)>;

/// Detail key carrying the numeric gRPC code that the 1.1.0 layout's `vendor_code` sentinel
/// displaces (see [`export_error`]).
///
/// It cannot collide with the keys `crate::error::details_for_adbc` emits: those are lowercased
/// fully-qualified protobuf type names, always of the form `<package>.<message>` with a real
/// package — `google.rpc.retryinfo` and friends — and no protobuf package is named `adbc`. The
/// `adbc.` prefix is the same namespace the standard option keys live in, narrowed by `spanner.`
/// to mark it as this driver's own rather than something the ADBC spec defines.
pub(crate) const VENDOR_CODE_DETAIL_KEY: &str = "adbc.spanner.vendor_code";

/// # Safety
/// `error` must be a pointer this driver previously wrote a 1.1.0 error into.
// Guard-exempt: must not unwind and cannot — freeing a `CString` and a details box does not
// panic, and the rest is plain pointer writes.
unsafe extern "C" fn release_error(error: *mut AdbcError) {
    let Some(error) = (unsafe { error.as_mut() }) else {
        return;
    };
    if !error.message.is_null() {
        drop(unsafe { CString::from_raw(error.message) });
        error.message = std::ptr::null_mut();
    }
    if !error.private_data.is_null() {
        drop(unsafe { Box::from_raw(error.private_data.cast::<ErrorDetails>()) });
        error.private_data = std::ptr::null_mut();
    }
    // Clearing this makes the callback idempotent, which the header requires.
    error.release = None;
}

/// # Safety
/// `error` must be a pointer this driver previously wrote a 1.0.0 error into.
// Guard-exempt: must not unwind and cannot — freeing a `CString` does not panic, and the rest is
// plain pointer writes.
unsafe extern "C" fn release_error_v100(error: *mut AdbcErrorV100) {
    let Some(error) = (unsafe { error.as_mut() }) else {
        return;
    };
    if !error.message.is_null() {
        drop(unsafe { CString::from_raw(error.message) });
        error.message = std::ptr::null_mut();
    }
    error.release = None;
}

/// Write `error` into the caller's out parameter, respecting whichever ABI revision it allocated.
///
/// # Safety
/// `out` must be null or point to a zero-initialized or previously-populated `AdbcError` whose
/// size matches the revision its `vendor_code` sentinel advertises.
pub(crate) unsafe fn export_error(out: *mut AdbcError, error: Error) {
    if out.is_null() {
        return;
    }

    // Both fields are read *before* any previous error is released: a stale release callback is
    // entitled to clobber them -- the reference C++ framework's memsets the whole 1.1.0 struct --
    // and a clobbered `vendor_code` would misreport the caller's ABI revision. `private_driver`
    // matters for the same reason: the driver manager writes it before the call (`INIT_ERROR`,
    // adbc_driver_manager_internal.h) and needs it back to route `AdbcErrorGetDetail*`
    // (adbc_driver_manager_api.cc:112), so losing it would cost the caller this error's details.
    // It exists in the caller's allocation only once the sentinel says so, hence the guard.
    let is_v110 = unsafe { (*out).vendor_code } == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
    let private_driver = if is_v110 {
        unsafe { (*out).private_driver }
    } else {
        std::ptr::null()
    };

    // Reusing an out parameter that still holds an error is legal; release it so it does not leak.
    if let Some(release) = unsafe { (*out).release } {
        unsafe { release(out) };
    }

    if is_v110 {
        // adbc.h:304 says of `private_data`: "If present, this field is NULLPTR iff the error is
        // uninitialized/freed." Leaving it null for a detail-free error would break that both
        // ways -- it is how a consumer distinguishes a written error from a zeroed one -- so the
        // box is always allocated, and `release_error` is what puts the field back to null.
        let mut details: ErrorDetails = error
            .details
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (sanitized_cstring(&key), value))
            .collect();
        // The `vendor_code` field is about to be overwritten with the sentinel (see below), so the
        // numeric gRPC code `crate::error::from_spanner` documents as recoverable there would be
        // lost to exactly the callers using the richer layout. Hand it back as a detail instead.
        // Appended *after* the forwarded `google.rpc.*` details so the indices a caller already
        // walks do not shift, and only when there is a code to report: zero is what `from_spanner`
        // stores for a failure that carried no gRPC status at all, and claiming code 0 (`OK`) for
        // one would be worse than saying nothing. The value is decimal ASCII, keeping the
        // all-details-are-UTF-8-text invariant the missing `-bin` key suffix advertises.
        if error.vendor_code != 0 {
            details.push((
                sanitized_cstring(VENDOR_CODE_DETAIL_KEY),
                error.vendor_code.to_string().into_bytes(),
            ));
        }
        let value = AdbcError {
            message: sanitized_cstring(&error.message).into_raw(),
            // Re-stamp the sentinel rather than the driver's vendor code: consumers read the
            // details in `private_data` only when they see it, so the sentinel is load-bearing.
            // The displaced code lives on as the `VENDOR_CODE_DETAIL_KEY` detail added above.
            vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
            sqlstate: error.sqlstate,
            release: Some(release_error),
            private_data: Box::into_raw(Box::new(details)).cast::<c_void>(),
            private_driver,
        };
        unsafe { std::ptr::write_unaligned(out, value) };
    } else {
        let value = AdbcErrorV100 {
            message: sanitized_cstring(&error.message).into_raw(),
            // A vendor code that happens to equal the sentinel must not be stored: the field is
            // the *only* record of how large this caller's allocation is, so the next export into
            // the same struct would read it back, believe the 1.1.0 tail exists, and write 48
            // bytes into 32. Zero is what adbc.h calls "not applicable"; a vendor code is
            // informational, so losing this one value is the cheap side of the trade.
            vendor_code: if error.vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA {
                0
            } else {
                error.vendor_code
            },
            sqlstate: error.sqlstate,
            release: Some(release_error_v100),
        };
        // Only the 1.0.0 prefix exists in the caller's allocation; the tail must stay untouched.
        unsafe { std::ptr::write_unaligned(out.cast::<AdbcErrorV100>(), value) };
    }
}

/// Borrow the detail box behind `error`, if the caller's struct is one that has the field at all.
///
/// The mirror image of the guard in [`export_error`]: adbc.h says `private_data` "may not be used
/// unless vendor_code is ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA", because a 1.0.0 caller allocated
/// only `ADBC_ERROR_1_0_0_SIZE` bytes and the field is past the end of it. So the sentinel decides
/// whether the pointer may even be read, and the read goes through the field's own address rather
/// than a reference to the whole struct.
///
/// # Safety
/// `error` must be null or point to at least the 1.0.0 prefix of a valid `AdbcError`.
unsafe fn details<'a>(error: *const AdbcError) -> Option<&'a ErrorDetails> {
    if error.is_null() {
        return None;
    }
    if unsafe { std::ptr::read_unaligned(&raw const (*error).vendor_code) }
        != ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA
    {
        return None;
    }
    let private_data = unsafe { std::ptr::read_unaligned(&raw const (*error).private_data) };
    unsafe { private_data.cast::<ErrorDetails>().as_ref() }
}

/// # Safety
/// `error` must be null or point to a valid `AdbcError`.
// Guard-exempt: pure reads of driver-owned allocations; the fallible conversion is saturating,
// so nothing here can panic.
pub(crate) unsafe extern "C" fn error_get_detail_count(error: *const AdbcError) -> c_int {
    let Some(details) = (unsafe { details(error) }) else {
        return 0;
    };
    c_int::try_from(details.len()).unwrap_or(c_int::MAX)
}

/// # Safety
/// `error` must be null or point to a valid `AdbcError`.
// Guard-exempt: pure reads of driver-owned allocations through checked lookups; nothing here can
// panic.
pub(crate) unsafe extern "C" fn error_get_detail(
    error: *const AdbcError,
    index: c_int,
) -> AdbcErrorDetail {
    let empty = AdbcErrorDetail {
        key: std::ptr::null(),
        value: std::ptr::null(),
        value_length: 0,
    };
    let Some(details) = (unsafe { details(error) }) else {
        return empty;
    };
    let Ok(index) = usize::try_from(index) else {
        return empty;
    };
    let Some((key, value)) = details.get(index) else {
        return empty;
    };
    AdbcErrorDetail {
        key: key.as_ptr(),
        value: value.as_ptr(),
        value_length: value.len(),
    }
}

#[cfg(test)]
mod tests;

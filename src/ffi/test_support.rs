//! Helpers shared by the test modules across this export layer.

use super::abi::{ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA, AdbcError};

/// An `AdbcError` as a 1.1.0 caller hands it in: zeroed, with the vendor-code sentinel that
/// advertises the larger layout.
pub(super) fn zeroed_error() -> AdbcError {
    AdbcError {
        message: std::ptr::null_mut(),
        vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
        sqlstate: [0; 5],
        release: None,
        private_data: std::ptr::null_mut(),
        private_driver: std::ptr::null(),
    }
}

/// Run the driver's release callback on an out parameter, when one was set.
pub(super) fn release_error(error: &mut AdbcError) {
    if let Some(release) = error.release {
        unsafe { release(&raw mut *error) };
    }
}

/// The message the driver exported into an out parameter, or `None` when it set none.
pub(super) fn error_message(error: &AdbcError) -> Option<String> {
    if error.message.is_null() {
        return None;
    }
    Some(
        unsafe { std::ffi::CStr::from_ptr(error.message) }
            .to_string_lossy()
            .into_owned(),
    )
}

/// A typed null for whichever pointer parameter a call leaves unset.
///
/// `*mut T` coerces to `*const T` at a call site, so this spells both.
pub(super) fn null<T>() -> *mut T {
    std::ptr::null_mut()
}

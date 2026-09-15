use adbc_core::error::Status;

use super::*;
use crate::ffi::test_support::{null, zeroed_error};

fn message_of(error: &AdbcError) -> String {
    unsafe { std::ffi::CStr::from_ptr(error.message) }
        .to_string_lossy()
        .into_owned()
}

#[test]
fn exports_a_message_and_releases_idempotently() {
    let mut out = zeroed_error();
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("boom", Status::IO),
        );
    }
    assert_eq!(message_of(&out), "boom");
    assert_eq!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);

    let release = out.release.expect("driver sets a release callback");
    unsafe { release(&raw mut out) };
    assert!(out.message.is_null());
    assert!(out.release.is_none());
    // A second release must be a no-op rather than a double free.
    unsafe { release(&raw mut out) };
}

#[test]
fn reusing_an_out_parameter_releases_the_previous_error() {
    let mut out = zeroed_error();
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("first", Status::IO),
        );
        export_error(
            &raw mut out,
            Error::with_message_and_status("second", Status::Internal),
        );
    }
    assert_eq!(message_of(&out), "second");
    unsafe { (out.release.unwrap())(&raw mut out) };
}

/// The 1.0.0 layout has no `private_data`/`private_driver`; a caller that allocated only the
/// prefix must get those bytes back untouched. This mirrors the C++ suite's
/// `StatementTest.ErrorCompatibility`.
#[test]
fn a_1_0_0_caller_keeps_its_trailing_bytes() {
    let sentinel_driver = 0x5eed_usize as *const super::super::abi::AdbcDriver;
    let sentinel_data = 0xf00d_usize as *mut c_void;
    let mut out = AdbcError {
        message: null(),
        vendor_code: 0, // not the sentinel, so: 1.0.0
        sqlstate: [0; 5],
        release: None,
        private_data: sentinel_data,
        private_driver: sentinel_driver,
    };
    let mut error = Error::with_message_and_status("legacy", Status::NotFound);
    error.vendor_code = 42;
    unsafe { export_error(&raw mut out, error) };

    assert_eq!(message_of(&out), "legacy");
    assert_eq!(out.vendor_code, 42);
    assert_eq!(out.private_data, sentinel_data);
    assert_eq!(out.private_driver, sentinel_driver);

    let release: unsafe extern "C" fn(*mut AdbcErrorV100) =
        unsafe { std::mem::transmute(out.release.unwrap()) };
    unsafe { release((&raw mut out).cast::<AdbcErrorV100>()) };
    assert_eq!(out.private_data, sentinel_data);
}

/// A driver error may legitimately carry `i32::MIN` as its vendor code, and storing it verbatim
/// in a 1.0.0 caller's struct would make the *next* export read it back as the 1.1.0 sentinel and
/// write the 48-byte struct into the caller's 32 bytes. The second export below is exactly that
/// call; what it must not do is touch the tail.
#[test]
fn a_1_0_0_caller_is_not_relabelled_by_a_sentinel_valued_vendor_code() {
    let sentinel_driver = 0x5eed_usize as *const super::super::abi::AdbcDriver;
    let sentinel_data = 0xf00d_usize as *mut c_void;
    let mut out = AdbcError {
        message: null(),
        vendor_code: 0, // not the sentinel, so: 1.0.0
        sqlstate: [0; 5],
        release: None,
        private_data: sentinel_data,
        private_driver: sentinel_driver,
    };

    let mut error = Error::with_message_and_status("first", Status::Internal);
    error.vendor_code = ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
    unsafe { export_error(&raw mut out, error) };
    assert_ne!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);

    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("second", Status::Internal),
        );
    }
    assert_eq!(message_of(&out), "second");
    assert_eq!(out.private_data, sentinel_data);
    assert_eq!(out.private_driver, sentinel_driver);

    let release: unsafe extern "C" fn(*mut AdbcErrorV100) =
        unsafe { std::mem::transmute(out.release.unwrap()) };
    unsafe { release((&raw mut out).cast::<AdbcErrorV100>()) };
}

#[test]
fn round_trips_error_details() {
    let mut error = Error::with_message_and_status("detailed", Status::Internal);
    error.details = Some(vec![
        ("k1".to_string(), b"v1".to_vec()),
        ("k2".to_string(), b"".to_vec()),
    ]);
    let mut out = zeroed_error();
    unsafe { export_error(&raw mut out, error) };

    assert_eq!(unsafe { error_get_detail_count(&raw const out) }, 2);
    let first = unsafe { error_get_detail(&raw const out, 0) };
    assert_eq!(
        unsafe { std::ffi::CStr::from_ptr(first.key) }
            .to_str()
            .unwrap(),
        "k1"
    );
    assert_eq!(
        unsafe { std::slice::from_raw_parts(first.value, first.value_length) },
        b"v1"
    );

    // Out-of-range indices, in both directions, must yield the documented empty detail.
    assert!(unsafe { error_get_detail(&raw const out, 2) }.key.is_null());
    assert!(
        unsafe { error_get_detail(&raw const out, -1) }
            .key
            .is_null()
    );

    unsafe { (out.release.unwrap())(&raw mut out) };
    assert_eq!(unsafe { error_get_detail_count(&raw const out) }, 0);
}

/// The detail getters honour the sentinel just as the export does: without it `private_data`
/// is not a field of the caller's allocation, and the bytes there hold a pointer that is not
/// the driver's.
#[test]
fn the_detail_getters_ignore_private_data_on_a_1_0_0_error() {
    let mut out = AdbcError {
        message: null(),
        vendor_code: 0, // not the sentinel, so: 1.0.0
        sqlstate: [0; 5],
        release: None,
        private_data: 0xf00d_usize as *mut c_void,
        private_driver: null(),
    };
    let mut error = Error::with_message_and_status("legacy", Status::Internal);
    error.details = Some(vec![("k1".to_string(), b"v1".to_vec())]);
    unsafe { export_error(&raw mut out, error) };

    // The export left the tail alone, and the getters must not go looking in it either.
    assert_eq!(out.private_data, 0xf00d_usize as *mut c_void);
    assert_eq!(unsafe { error_get_detail_count(&raw const out) }, 0);
    assert!(unsafe { error_get_detail(&raw const out, 0) }.key.is_null());

    let release: unsafe extern "C" fn(*mut AdbcErrorV100) =
        unsafe { std::mem::transmute(out.release.unwrap()) };
    unsafe { release((&raw mut out).cast::<AdbcErrorV100>()) };
}

#[test]
fn null_out_parameters_are_tolerated() {
    unsafe {
        export_error(
            null(),
            Error::with_message_and_status("dropped", Status::IO),
        );
    }
    assert_eq!(unsafe { error_get_detail_count(null()) }, 0);
    assert!(unsafe { error_get_detail(null(), 0) }.key.is_null());
}

#[test]
fn a_message_with_an_interior_nul_is_still_reported() {
    let mut out = zeroed_error();
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("before\0after", Status::IO),
        );
    }
    assert_eq!(message_of(&out), "before?after");
    unsafe { (out.release.unwrap())(&raw mut out) };
}

/// adbc.h:304: `private_data` "is NULLPTR iff the error is uninitialized/freed", so a populated
/// 1.1.0 error carries a detail box even when it has no details to put in it.
#[test]
fn a_detail_free_error_still_marks_itself_initialized() {
    let mut out = zeroed_error();
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("plain", Status::IO),
        );
    }
    assert!(!out.private_data.is_null());
    assert_eq!(unsafe { error_get_detail_count(&raw const out) }, 0);

    unsafe { (out.release.unwrap())(&raw mut out) };
    assert!(out.private_data.is_null());
}

/// The driver manager writes `private_driver` before the call and reads it back afterwards to
/// route the detail getters, so releasing a previous error -- here one whose callback clears the
/// whole struct, as the reference C++ framework's does -- must not take the field with it.
#[test]
fn a_foreign_release_cannot_take_private_driver_with_it() {
    unsafe extern "C" fn clearing_release(error: *mut AdbcError) {
        unsafe { std::ptr::write_bytes(error.cast::<u8>(), 0, size_of::<AdbcError>()) };
    }

    let sentinel_driver = 0x5eed_usize as *const super::super::abi::AdbcDriver;
    let mut out = AdbcError {
        message: null(),
        vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
        sqlstate: [0; 5],
        release: Some(clearing_release),
        private_data: null(),
        private_driver: sentinel_driver,
    };
    unsafe {
        export_error(
            &raw mut out,
            Error::with_message_and_status("boom", Status::IO),
        );
    }
    assert_eq!(out.private_driver, sentinel_driver);
    assert_eq!(out.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);
    unsafe { (out.release.unwrap())(&raw mut out) };
}

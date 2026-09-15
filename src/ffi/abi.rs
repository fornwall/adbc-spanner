//! The ADBC C ABI, transcribed from `arrow-adbc/c/include/arrow-adbc/adbc.h`.
//!
//! This module is declarations only: `#[repr(C)]` layouts, type aliases and constants. It holds no
//! logic, so it can be diffed against the upstream header line by line. Everything that decides
//! *what* to do lives in the sibling modules; this file only says what the wire format is.
//!
//! Field order in [`AdbcDriver`] is load-bearing: it is the vtable the driver manager indexes into.
//! Do not reorder, and append only at the end, mirroring the header.

#![allow(
    non_snake_case,
    reason = "field and function names mirror the C header verbatim"
)]

use std::os::raw::{c_char, c_int, c_void};

pub(crate) use adbc_core::constants::ADBC_STATUS_OK;
#[cfg(test)]
pub(crate) use adbc_core::constants::{
    ADBC_STATUS_ALREADY_EXISTS, ADBC_STATUS_CANCELLED, ADBC_STATUS_INTEGRITY, ADBC_STATUS_INTERNAL,
    ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_DATA, ADBC_STATUS_INVALID_STATE,
    ADBC_STATUS_IO, ADBC_STATUS_NOT_FOUND, ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_TIMEOUT,
    ADBC_STATUS_UNAUTHENTICATED, ADBC_STATUS_UNAUTHORIZED, ADBC_STATUS_UNKNOWN,
};
pub(crate) use adbc_core::error::AdbcStatusCode;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

pub(crate) const ADBC_VERSION_1_0_0: c_int = 1_000_000;
pub(crate) const ADBC_VERSION_1_1_0: c_int = 1_001_000;

/// Sentinel in [`AdbcError::vendor_code`] marking the struct as 1.1.0-sized, so the 1.1.0 fields
/// may be read and written. Callers that predate 1.1.0 leave a real vendor code (or zero) here.
pub(crate) const ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA: i32 = i32::MIN;

#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcError {
    pub(crate) message: *mut c_char,
    pub(crate) vendor_code: i32,
    pub(crate) sqlstate: [c_char; 5],
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self)>,
    /// Added in ADBC 1.1.0.
    pub(crate) private_data: *mut c_void,
    /// Added in ADBC 1.1.0.
    pub(crate) private_driver: *const AdbcDriver,
}

/// The 1.0.0 prefix of [`AdbcError`], for writing back into a caller-allocated struct that has no
/// room for the 1.1.0 tail.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcErrorV100 {
    pub(crate) message: *mut c_char,
    pub(crate) vendor_code: i32,
    pub(crate) sqlstate: [c_char; 5],
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self)>,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcErrorDetail {
    pub(crate) key: *const c_char,
    pub(crate) value: *const u8,
    pub(crate) value_length: usize,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcDatabase {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcConnection {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcStatement {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_driver: *const AdbcDriver,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct AdbcPartitions {
    pub(crate) num_partitions: usize,
    pub(crate) partitions: *mut *const u8,
    pub(crate) partition_lengths: *mut usize,
    pub(crate) private_data: *mut c_void,
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self)>,
}

/// The driver vtable. Fields up to (excluding) `ErrorGetDetailCount` are ADBC 1.0.0; the remainder
/// were added in 1.1.0. A `None` slot is a legal way to say "not implemented".
#[repr(C)]
pub(crate) struct AdbcDriver {
    pub(crate) private_data: *mut c_void,
    pub(crate) private_manager: *const c_void,
    pub(crate) release: Option<unsafe extern "C" fn(*mut Self, *mut AdbcError) -> AdbcStatusCode>,

    // --- ADBC 1.0.0 ---
    pub(crate) DatabaseInit:
        Option<unsafe extern "C" fn(*mut AdbcDatabase, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) DatabaseNew:
        Option<unsafe extern "C" fn(*mut AdbcDatabase, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) DatabaseSetOption: Option<
        unsafe extern "C" fn(
            *mut AdbcDatabase,
            *const c_char,
            *const c_char,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) DatabaseRelease:
        Option<unsafe extern "C" fn(*mut AdbcDatabase, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) ConnectionCommit:
        Option<unsafe extern "C" fn(*mut AdbcConnection, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) ConnectionGetInfo: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const u32,
            usize,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetObjects: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            c_int,
            *const c_char,
            *const c_char,
            *const c_char,
            *const *const c_char,
            *const c_char,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetTableSchema: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *const c_char,
            *const c_char,
            *mut FFI_ArrowSchema,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetTableTypes: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionInit: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *mut AdbcDatabase,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionNew:
        Option<unsafe extern "C" fn(*mut AdbcConnection, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) ConnectionSetOption: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *const c_char,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionReadPartition: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const u8,
            usize,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionRelease:
        Option<unsafe extern "C" fn(*mut AdbcConnection, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) ConnectionRollback:
        Option<unsafe extern "C" fn(*mut AdbcConnection, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) StatementBind: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *mut FFI_ArrowArray,
            *mut FFI_ArrowSchema,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementBindStream: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementExecuteQuery: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *mut FFI_ArrowArrayStream,
            *mut i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementExecutePartitions: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *mut FFI_ArrowSchema,
            *mut AdbcPartitions,
            *mut i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementGetParameterSchema: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *mut FFI_ArrowSchema,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementNew: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *mut AdbcStatement,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementPrepare:
        Option<unsafe extern "C" fn(*mut AdbcStatement, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) StatementRelease:
        Option<unsafe extern "C" fn(*mut AdbcStatement, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) StatementSetOption: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const c_char,
            *const c_char,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementSetSqlQuery: Option<
        unsafe extern "C" fn(*mut AdbcStatement, *const c_char, *mut AdbcError) -> AdbcStatusCode,
    >,
    pub(crate) StatementSetSubstraitPlan: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const u8,
            usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,

    // --- ADBC 1.1.0 ---
    pub(crate) ErrorGetDetailCount: Option<unsafe extern "C" fn(*const AdbcError) -> c_int>,
    pub(crate) ErrorGetDetail:
        Option<unsafe extern "C" fn(*const AdbcError, c_int) -> AdbcErrorDetail>,
    pub(crate) ErrorFromArrayStream: Option<
        unsafe extern "C" fn(*mut FFI_ArrowArrayStream, *mut AdbcStatusCode) -> *const AdbcError,
    >,
    pub(crate) DatabaseGetOption: Option<
        unsafe extern "C" fn(
            *mut AdbcDatabase,
            *const c_char,
            *mut c_char,
            *mut usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) DatabaseGetOptionBytes: Option<
        unsafe extern "C" fn(
            *mut AdbcDatabase,
            *const c_char,
            *mut u8,
            *mut usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) DatabaseGetOptionDouble: Option<
        unsafe extern "C" fn(
            *mut AdbcDatabase,
            *const c_char,
            *mut f64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) DatabaseGetOptionInt: Option<
        unsafe extern "C" fn(
            *mut AdbcDatabase,
            *const c_char,
            *mut i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) DatabaseSetOptionBytes: Option<
        unsafe extern "C" fn(
            *mut AdbcDatabase,
            *const c_char,
            *const u8,
            usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) DatabaseSetOptionDouble: Option<
        unsafe extern "C" fn(
            *mut AdbcDatabase,
            *const c_char,
            f64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) DatabaseSetOptionInt: Option<
        unsafe extern "C" fn(
            *mut AdbcDatabase,
            *const c_char,
            i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionCancel:
        Option<unsafe extern "C" fn(*mut AdbcConnection, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) ConnectionGetOption: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *mut c_char,
            *mut usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetOptionBytes: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *mut u8,
            *mut usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetOptionDouble: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *mut f64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetOptionInt: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *mut i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetStatistics: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *const c_char,
            *const c_char,
            c_char,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionGetStatisticNames: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *mut FFI_ArrowArrayStream,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionSetOptionBytes: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            *const u8,
            usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionSetOptionDouble: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            f64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) ConnectionSetOptionInt: Option<
        unsafe extern "C" fn(
            *mut AdbcConnection,
            *const c_char,
            i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementCancel:
        Option<unsafe extern "C" fn(*mut AdbcStatement, *mut AdbcError) -> AdbcStatusCode>,
    pub(crate) StatementExecuteSchema: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *mut FFI_ArrowSchema,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementGetOption: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const c_char,
            *mut c_char,
            *mut usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementGetOptionBytes: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const c_char,
            *mut u8,
            *mut usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementGetOptionDouble: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const c_char,
            *mut f64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementGetOptionInt: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const c_char,
            *mut i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementSetOptionBytes: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const c_char,
            *const u8,
            usize,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementSetOptionDouble: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const c_char,
            f64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub(crate) StatementSetOptionInt: Option<
        unsafe extern "C" fn(
            *mut AdbcStatement,
            *const c_char,
            i64,
            *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
}

/// `offsetof(struct AdbcDriver, ErrorGetDetailCount)` — the number of bytes a 1.0.0 caller
/// allocated, and therefore the most we may write when it asks for 1.0.0.
pub(crate) const ADBC_DRIVER_1_0_0_SIZE: usize =
    std::mem::offset_of!(AdbcDriver, ErrorGetDetailCount);

/// `offsetof(struct AdbcError, private_data)` — likewise for the error struct.
pub(crate) const ADBC_ERROR_1_0_0_SIZE: usize = std::mem::offset_of!(AdbcError, private_data);

/// `error.rs` writes the 1.0.0 prefix by casting to [`AdbcErrorV100`], which is only sound while
/// that struct is exactly the prefix. Enforce it at compile time rather than trusting the reader.
const _: () = assert!(ADBC_ERROR_1_0_0_SIZE == size_of::<AdbcErrorV100>());

#[cfg(test)]
mod tests {
    use adbc_core::error::Status;

    use super::*;

    /// The canonical `adbc_core` constants are what every exported function now returns. Pin
    /// them independently to the C header so an incompatible dependency change cannot silently
    /// alter this driver's ABI.
    #[test]
    fn status_codes_match_the_c_header() {
        assert_eq!(size_of::<AdbcStatusCode>(), 1);
        let header_codes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];
        assert_eq!(
            [
                ADBC_STATUS_OK,
                ADBC_STATUS_UNKNOWN,
                ADBC_STATUS_NOT_IMPLEMENTED,
                ADBC_STATUS_NOT_FOUND,
                ADBC_STATUS_ALREADY_EXISTS,
                ADBC_STATUS_INVALID_ARGUMENT,
                ADBC_STATUS_INVALID_STATE,
                ADBC_STATUS_INVALID_DATA,
                ADBC_STATUS_INTEGRITY,
                ADBC_STATUS_INTERNAL,
                ADBC_STATUS_IO,
                ADBC_STATUS_CANCELLED,
                ADBC_STATUS_TIMEOUT,
                ADBC_STATUS_UNAUTHENTICATED,
                ADBC_STATUS_UNAUTHORIZED,
            ],
            header_codes
        );
        assert_eq!(
            [
                Status::Ok,
                Status::Unknown,
                Status::NotImplemented,
                Status::NotFound,
                Status::AlreadyExists,
                Status::InvalidArguments,
                Status::InvalidState,
                Status::InvalidData,
                Status::Integrity,
                Status::Internal,
                Status::IO,
                Status::Cancelled,
                Status::Timeout,
                Status::Unauthenticated,
                Status::Unauthorized,
            ]
            .map(AdbcStatusCode::from),
            header_codes
        );
    }

    /// Layout drift against the C header is silent memory corruption, so these are pinned to the
    /// values the header itself produces on a 64-bit target, obtained by compiling
    /// `arrow-adbc/c/include/arrow-adbc/adbc.h` and printing the macros and offsets:
    ///
    /// ```text
    /// ADBC_DRIVER_1_0_0_SIZE=232   ADBC_DRIVER_1_1_0_SIZE=464
    /// ADBC_ERROR_1_0_0_SIZE=32     ADBC_ERROR_1_1_0_SIZE=48
    /// ```
    ///
    /// Sizes alone would not catch a pair of same-shaped fields being swapped, so interior offsets
    /// are pinned too: one early in the 1.0.0 block, the last 1.0.0 slot, and the last slot of all.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn driver_vtable_matches_the_c_header() {
        assert_eq!(ADBC_DRIVER_1_0_0_SIZE, 232);
        assert_eq!(size_of::<AdbcDriver>(), 464);

        assert_eq!(std::mem::offset_of!(AdbcDriver, ConnectionGetInfo), 64);
        assert_eq!(
            std::mem::offset_of!(AdbcDriver, StatementSetSubstraitPlan),
            224
        );
        assert_eq!(std::mem::offset_of!(AdbcDriver, StatementSetOptionInt), 456);
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn the_remaining_structs_match_the_c_header() {
        assert_eq!(ADBC_ERROR_1_0_0_SIZE, 32);
        assert_eq!(size_of::<AdbcError>(), 48);
        assert_eq!(size_of::<AdbcPartitions>(), 40);
        assert_eq!(size_of::<AdbcErrorDetail>(), 24);
    }
}

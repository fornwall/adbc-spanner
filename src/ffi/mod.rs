//! The ADBC C ABI export layer.
//!
//! This is the whole boundary between the driver and a C driver manager. It is organized so that
//! each concern can be reviewed on its own:
//!
//! * [`abi`] — the C header transcribed. Declarations only, no logic.
//! * [`error`] — writing a driver error back through the caller's `AdbcError`, at whichever ABI
//!   revision the caller allocated.
//! * [`guard`] — panic containment and the pointer/string conversions every entry point needs.
//! * [`handle`] — what lives behind `private_data`, and how a call reaches it safely.
//! * [`options`] — the handle prologues and option entry points every object repeats, written
//!   once and stamped out per object by a macro.
//! * [`database`], [`connection`], [`statement`] — the entry points themselves, one module per
//!   ADBC object, each a thin translation onto the driver's own types.
//!
//! Nothing outside this module is aware that the driver is exported over C.
//!
//! The shared library (`libadbc_spanner.so` / `.dylib` / `adbc_spanner.dll`) exports
//! [`AdbcSpannerInit`] — the driver-specific init symbol, named per the ADBC convention — and
//! [`AdbcDriverInit`], the fallback a driver manager tries when it was told no symbol name. Load
//! it from any ADBC driver manager by pointing at the library path, e.g. from Python:
//!
//! ```python
//! import adbc_driver_manager
//! db = adbc_driver_manager.AdbcDatabase(
//!     driver="/path/to/libadbc_spanner.so",
//!     entrypoint="AdbcSpannerInit",
//!     uri="spanner:///projects/p/instances/i/databases/d",
//! )
//! ```

mod abi;
mod connection;
mod database;
mod error;
mod guard;
mod handle;
mod import;
mod options;
#[cfg(test)]
mod roundtrip;
mod statement;
mod stream;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use std::ffi::{c_int, c_void};

use adbc_core::error::{Error, Status};

use abi::{
    ADBC_DRIVER_1_0_0_SIZE, ADBC_STATUS_OK, ADBC_VERSION_1_0_0, ADBC_VERSION_1_1_0, AdbcDriver,
    AdbcError, AdbcStatusCode,
};
use handle::finish;

/// Build the vtable. Slots the driver does not implement stay `None`, which the ABI reads as
/// "not implemented".
fn vtable() -> AdbcDriver {
    AdbcDriver {
        private_data: std::ptr::null_mut(),
        private_manager: std::ptr::null(),
        release: Some(release_driver),

        // --- ADBC 1.0.0 ---
        DatabaseInit: Some(database::database_init),
        DatabaseNew: Some(database::database_new),
        DatabaseSetOption: Some(database::database_set_option),
        DatabaseRelease: Some(database::database_release),
        ConnectionCommit: Some(connection::connection_commit),
        ConnectionGetInfo: Some(connection::connection_get_info),
        ConnectionGetObjects: Some(connection::connection_get_objects),
        ConnectionGetTableSchema: Some(connection::connection_get_table_schema),
        ConnectionGetTableTypes: Some(connection::connection_get_table_types),
        ConnectionInit: Some(connection::connection_init),
        ConnectionNew: Some(connection::connection_new),
        ConnectionSetOption: Some(connection::connection_set_option),
        ConnectionReadPartition: Some(connection::connection_read_partition),
        ConnectionRelease: Some(connection::connection_release),
        ConnectionRollback: Some(connection::connection_rollback),
        StatementBind: Some(statement::statement_bind),
        StatementBindStream: Some(statement::statement_bind_stream),
        StatementExecuteQuery: Some(statement::statement_execute_query),
        StatementExecutePartitions: Some(statement::statement_execute_partitions),
        StatementGetParameterSchema: Some(statement::statement_get_parameter_schema),
        StatementNew: Some(statement::statement_new),
        StatementPrepare: Some(statement::statement_prepare),
        StatementRelease: Some(statement::statement_release),
        StatementSetOption: Some(statement::statement_set_option),
        StatementSetSqlQuery: Some(statement::statement_set_sql_query),
        StatementSetSubstraitPlan: Some(statement::statement_set_substrait_plan),

        // --- ADBC 1.1.0 ---
        ErrorGetDetailCount: Some(error::error_get_detail_count),
        ErrorGetDetail: Some(error::error_get_detail),
        ErrorFromArrayStream: Some(stream::error_from_array_stream),
        DatabaseGetOption: Some(database::database_get_option),
        DatabaseGetOptionBytes: Some(database::database_get_option_bytes),
        DatabaseGetOptionDouble: Some(database::database_get_option_double),
        DatabaseGetOptionInt: Some(database::database_get_option_int),
        DatabaseSetOptionBytes: Some(database::database_set_option_bytes),
        DatabaseSetOptionDouble: Some(database::database_set_option_double),
        DatabaseSetOptionInt: Some(database::database_set_option_int),
        ConnectionCancel: Some(connection::connection_cancel),
        ConnectionGetOption: Some(connection::connection_get_option),
        ConnectionGetOptionBytes: Some(connection::connection_get_option_bytes),
        ConnectionGetOptionDouble: Some(connection::connection_get_option_double),
        ConnectionGetOptionInt: Some(connection::connection_get_option_int),
        ConnectionGetStatistics: Some(connection::connection_get_statistics),
        ConnectionGetStatisticNames: Some(connection::connection_get_statistic_names),
        ConnectionSetOptionBytes: Some(connection::connection_set_option_bytes),
        ConnectionSetOptionDouble: Some(connection::connection_set_option_double),
        ConnectionSetOptionInt: Some(connection::connection_set_option_int),
        StatementCancel: Some(statement::statement_cancel),
        StatementExecuteSchema: Some(statement::statement_execute_schema),
        StatementGetOption: Some(statement::statement_get_option),
        StatementGetOptionBytes: Some(statement::statement_get_option_bytes),
        StatementGetOptionDouble: Some(statement::statement_get_option_double),
        StatementGetOptionInt: Some(statement::statement_get_option_int),
        StatementSetOptionBytes: Some(statement::statement_set_option_bytes),
        StatementSetOptionDouble: Some(statement::statement_set_option_double),
        StatementSetOptionInt: Some(statement::statement_set_option_int),
    }
}

/// # Safety
/// `driver` must be null or point to a valid `AdbcDriver` this driver initialized.
// Guard-exempt: touches only the caller's own struct with non-panicking pointer operations; the
// error path is contained inside `finish`.
unsafe extern "C" fn release_driver(
    driver: *mut AdbcDriver,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let Some(driver) = (unsafe { driver.as_mut() }) else {
        return ADBC_STATUS_OK;
    };
    if driver.release.take().is_none() {
        return finish(
            Err(Error::with_message_and_status(
                "driver is already released",
                Status::InvalidState,
            )),
            error,
        );
    }
    ADBC_STATUS_OK
}

/// Populate the caller's vtable at the revision it asked for.
///
/// # Safety
/// `driver` must point to an `AdbcDriver` allocation of at least the size implied by `version`.
// Guard-exempt (also covers the two exported wrappers below): builds a vtable of plain function
// pointers and copies it with non-panicking pointer operations; the error paths are contained
// inside `finish`.
unsafe fn init(version: c_int, driver: *mut c_void, error: *mut AdbcError) -> AdbcStatusCode {
    if driver.is_null() {
        return finish(
            Err(crate::error::invalid_argument("driver must not be null")),
            error,
        );
    }

    let table = vtable();
    match version {
        ADBC_VERSION_1_1_0 => unsafe {
            std::ptr::write_unaligned(driver.cast::<AdbcDriver>(), table);
        },
        ADBC_VERSION_1_0_0 => {
            // The caller allocated only the 1.0.0 prefix, so copying the whole struct would run
            // off the end of its allocation. Copy exactly the bytes it owns and leak nothing: the
            // 1.1.0 tail holds only function pointers, which own no memory.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    (&raw const table).cast::<u8>(),
                    driver.cast::<u8>(),
                    ADBC_DRIVER_1_0_0_SIZE,
                );
            }
        }
        _ => {
            // The header tells callers to retry at a lower revision on NOT_IMPLEMENTED, so an
            // unsupported version must not be reported as a bad argument.
            return finish(
                Err(Error::with_message_and_status(
                    format!(
                        "unsupported ADBC version {version}; this driver implements \
                         {ADBC_VERSION_1_0_0} and {ADBC_VERSION_1_1_0}"
                    ),
                    Status::NotImplemented,
                )),
                error,
            );
        }
    }
    ADBC_STATUS_OK
}

/// The driver's entry point, named after the shared library.
///
/// # Safety
/// See [`init`].
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn AdbcSpannerInit(
    version: c_int,
    driver: *mut c_void,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { init(version, driver, error) }
}

/// The generic entry point, used by driver managers that were not told a symbol name.
///
/// # Safety
/// See [`init`].
#[unsafe(no_mangle)]
pub(crate) unsafe extern "C" fn AdbcDriverInit(
    version: c_int,
    driver: *mut c_void,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { init(version, driver, error) }
}

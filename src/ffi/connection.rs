//! `AdbcConnection*` entry points.

use std::collections::HashSet;
use std::ffi::{c_char, c_int};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection};
use adbc_core::{Connection, Database};
use arrow_array::RecordBatchReader;
use arrow_array::ffi::FFI_ArrowSchema;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use super::abi::{AdbcConnection, AdbcDatabase, AdbcError, AdbcStatusCode};
use super::guard::{
    byte_slice, optional_str, optional_str_list, required_output, required_str, write_out,
};
use super::handle::{Staged, install_cancel_handle};
use super::options::{driver_of, option_entry_points, slot_of, with_state};
use crate::connection::SpannerConnection;
use crate::error::invalid_argument;

pub(super) const KIND: &str = "connection";

pub(super) type State = Staged<OptionConnection, SpannerConnection>;

option_entry_points! {
    AdbcConnection {
        new: connection_new,
        release: connection_release,
        set_option: connection_set_option,
        set_option_bytes: connection_set_option_bytes,
        set_option_int: connection_set_option_int,
        set_option_double: connection_set_option_double,
        get_option: connection_get_option,
        get_option_bytes: connection_get_option_bytes,
        get_option_int: connection_get_option_int,
        get_option_double: connection_get_option_double,
    }
}

/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`, and `database` must be null or
/// point to a valid, initialized `AdbcDatabase` that outlives the connection.
pub(super) unsafe extern "C" fn connection_init(
    connection: *mut AdbcConnection,
    database: *mut AdbcDatabase,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Read once, up front: `install_cancel_handle` reaches the very object `with_state`
    // dispatches on, through its `cancel` lock rather than its state lock.
    let slot = unsafe { slot_of(connection) };
    unsafe {
        with_state(connection, error, |state| {
            let options = state.pending(KIND)?;
            // This call already holds the connection's dispatch turn; the database is locked
            // briefly on its own, and only read from here.
            let mut database = super::options::borrow(database)?;
            let database = database.ready(super::database::KIND)?;
            // Going through `new_connection_with_opts` rather than applying options one by one
            // keeps the driver's own ordering and validation rules in one place.
            let ready = database.new_connection_with_opts(options.entries.clone())?;
            // Cloned out so `connection_cancel` can reach the cancellation machinery without
            // taking this connection's dispatch turn -- and installed *before* the ready state
            // is published, because that is the instant the connection becomes usable and a
            // caller can start an operation to cancel. `statement_new` installs both handles at
            // construction for the same reason.
            install_cancel_handle::<State>(slot, ready.get_cancel_handle());
            *state = State::Ready(ready);
            Ok(())
        })
    }
}

/// Hand a driver-produced reader to the caller as an Arrow C stream.
///
/// The connection's `private_driver` travels with it: `AdbcErrorFromArrayStream` returns an
/// `AdbcError` in driver-owned storage, and the driver manager reads that error's details only
/// through the vtable this field names.
///
/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`, and `out` must be null or
/// point to a writable `FFI_ArrowArrayStream`.
unsafe fn export_stream(
    connection: *mut AdbcConnection,
    reader: Box<dyn RecordBatchReader + Send>,
    out: *mut FFI_ArrowArrayStream,
) -> Result<()> {
    let stream = super::stream::export_reader(reader, unsafe { driver_of(connection) });
    // On a null `out` the stream is dropped here rather than leaked: it has not been handed over,
    // so this side still owns it.
    unsafe { write_out(out, stream, "out") }
}

/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`.
pub(super) unsafe extern "C" fn connection_commit(
    connection: *mut AdbcConnection,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { with_state(connection, error, |state| state.ready(KIND)?.commit()) }
}

/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`.
pub(super) unsafe extern "C" fn connection_rollback(
    connection: *mut AdbcConnection,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { with_state(connection, error, |state| state.ready(KIND)?.rollback()) }
}

/// # Safety
/// `connection` must be null or point to a valid `AdbcConnection`.
pub(super) unsafe extern "C" fn connection_cancel(
    connection: *mut AdbcConnection,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Dispatch-free on purpose, through the handle installed at init; see `handle::cancel`.
    // With no operation in flight it reports `InvalidState`, which is exactly what ADBC asks for.
    unsafe { super::handle::cancel::<State>(slot_of(connection), KIND, error) }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call, and `info_codes` must be null or
/// point to `info_codes_length` initialized `u32`s.
pub(super) unsafe extern "C" fn connection_get_info(
    connection: *mut AdbcConnection,
    info_codes: *const u32,
    info_codes_length: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            // A null array means "everything the driver knows"; an empty non-null array is a
            // caller genuinely asking for nothing, so the two must not be collapsed.
            let codes = if info_codes.is_null() {
                None
            } else {
                Some(
                    std::slice::from_raw_parts(info_codes, info_codes_length)
                        .iter()
                        .copied()
                        .map(InfoCode::from)
                        .collect::<HashSet<_>>(),
                )
            };
            let reader = state.ready(KIND)?.get_info(codes)?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call, the string arguments must be null or
/// NUL-terminated, and `table_type` must be null or a NULL-terminated array of such strings.
#[allow(
    clippy::too_many_arguments,
    reason = "the signature is fixed by the ADBC C API"
)]
pub(super) unsafe extern "C" fn connection_get_objects(
    connection: *mut AdbcConnection,
    depth: c_int,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    table_type: *const *const c_char,
    column_name: *const c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            // `ObjectDepth::try_from` reports an unknown depth as `InvalidData`, which describes
            // bad data in a result set rather than a bad argument, and its message names neither
            // the entry point nor the depths that would have worked. The depth came straight from
            // the caller, so say so, and say what to pass instead.
            let depth = ObjectDepth::try_from(depth).map_err(|_| {
                invalid_argument(format!(
                    // There is no fifth depth to offer: adbc.h defines ADBC_OBJECT_DEPTH_COLUMNS
                    // as ADBC_OBJECT_DEPTH_ALL, so columns are what 0 already returns and 4 is
                    // not a value the header names at all.
                    "AdbcConnectionGetObjects depth {depth} is not a valid ADBC object depth; \
                     use 0 (all, including columns), 1 (catalogs), 2 (db_schemas), or 3 (tables)"
                ))
            })?;
            let catalog = optional_str(catalog, "catalog")?;
            let db_schema = optional_str(db_schema, "db_schema")?;
            let table_name = optional_str(table_name, "table_name")?;
            let table_type = optional_str_list(table_type, "table_type")?;
            let column_name = optional_str(column_name, "column_name")?;

            let reader = state.ready(KIND)?.get_objects(
                depth,
                catalog,
                db_schema,
                table_name,
                table_type,
                column_name,
            )?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `schema` must be null or valid for the call, and the string arguments must be
/// null or NUL-terminated.
pub(super) unsafe extern "C" fn connection_get_table_schema(
    connection: *mut AdbcConnection,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(schema, "schema")?;
            let catalog = optional_str(catalog, "catalog")?;
            let db_schema = optional_str(db_schema, "db_schema")?;
            // Unlike GetObjects, this looks up one specific table, so the name is not a filter and
            // cannot be omitted.
            let table_name = required_str(table_name, "table_name")?;

            let found = state
                .ready(KIND)?
                .get_table_schema(catalog, db_schema, table_name)?;
            let exported = FFI_ArrowSchema::try_from(&found).map_err(|failure| {
                Error::with_message_and_status(
                    format!(
                        "failed to export the table schema over the C data interface: {failure}"
                    ),
                    Status::Internal,
                )
            })?;
            write_out(schema, exported, "schema")
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call.
pub(super) unsafe extern "C" fn connection_get_table_types(
    connection: *mut AdbcConnection,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            let reader = state.ready(KIND)?.get_table_types()?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call, and the string arguments must be
/// null or NUL-terminated.
pub(super) unsafe extern "C" fn connection_get_statistics(
    connection: *mut AdbcConnection,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    approximate: c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            let catalog = optional_str(catalog, "catalog")?;
            let db_schema = optional_str(db_schema, "db_schema")?;
            let table_name = optional_str(table_name, "table_name")?;

            let reader = state.ready(KIND)?.get_statistics(
                catalog,
                db_schema,
                table_name,
                // The header declares a C boolean as `char`; anything non-zero is true.
                approximate != 0,
            )?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call.
pub(super) unsafe extern "C" fn connection_get_statistic_names(
    connection: *mut AdbcConnection,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            let reader = state.ready(KIND)?.get_statistic_names()?;
            export_stream(connection, reader, out)
        })
    }
}

/// # Safety
/// `connection` and `out` must be null or valid for the call, and `serialized_partition` must be
/// null or point to `serialized_length` initialized bytes.
pub(super) unsafe extern "C" fn connection_read_partition(
    connection: *mut AdbcConnection,
    serialized_partition: *const u8,
    serialized_length: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(connection, error, |state| {
            required_output(out, "out")?;
            let partition = byte_slice(
                serialized_partition,
                serialized_length,
                "serialized_partition",
            )?;
            let reader = state.ready(KIND)?.read_partition(partition)?;
            export_stream(connection, reader, out)
        })
    }
}

#[cfg(test)]
mod tests;

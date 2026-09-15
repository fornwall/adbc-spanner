//! `AdbcStatement*` entry points.
//!
//! Unlike a database or a connection, a statement has no `Init` step: `AdbcStatementNew` already
//! knows which connection it belongs to, so the driver object is built there and every option
//! applies to a live statement. That is why this module stores [`SpannerStatement`] directly
//! rather than a [`super::handle::Staged`] — there is nothing to buffer.

use std::ffi::{c_char, c_void};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Connection, Optionable, Statement};
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, StructArray};
use arrow_schema::{DataType, Schema};

use super::abi::{
    ADBC_STATUS_OK, AdbcConnection, AdbcDriver, AdbcError, AdbcPartitions, AdbcStatement,
    AdbcStatusCode,
};
use super::guard::{byte_slice, catch, required_output, required_str, write_out};
use super::handle::{Exported, finish};
use super::import::{BoundStreamReader, validate_imported};
use super::options::{FfiHandle, already_populated, driver_of, slot_of, with_state};
use crate::error::invalid_argument;
use crate::statement::SpannerStatement;

pub(super) const KIND: &str = "statement";

pub(super) type State = SpannerStatement;

impl FfiHandle for AdbcStatement {
    type State = State;
    const KIND: &'static str = KIND;

    fn private_data(&self) -> *mut c_void {
        self.private_data
    }

    fn private_data_mut(&mut self) -> &mut *mut c_void {
        &mut self.private_data
    }

    fn private_driver(&self) -> *const AdbcDriver {
        self.private_driver
    }
}

/// A statement is live from the moment it exists, so — unlike a database or a connection — options
/// apply directly and there is no pre-init phase for the getters to refuse.
impl super::options::OptionTarget for SpannerStatement {
    type Key = OptionStatement;
    type Options = Self;

    fn set(&mut self, key: OptionStatement, value: OptionValue) -> Result<()> {
        self.set_option(key, value)
    }

    fn options(&mut self, _kind: &str, _key: &OptionStatement) -> Result<&mut Self> {
        Ok(self)
    }
}

/// # Safety
/// `connection` must be null or point to a valid, initialized `AdbcConnection`, and `statement`
/// must be null or point to a valid `AdbcStatement`.
pub(super) unsafe extern "C" fn statement_new(
    connection: *mut AdbcConnection,
    statement: *mut AdbcStatement,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let Some(statement) = (unsafe { statement.as_mut() }) else {
        return finish(Err(invalid_argument("statement must not be null")), error);
    };
    // As `new_handle` does for the objects with a pre-init phase: a populated slot is a statement
    // the caller never released, and overwriting it would leak its clients and cancellation scope.
    if !statement.private_data.is_null() {
        return finish(Err(already_populated(KIND)), error);
    }
    // `new_statement` runs driver code — cloning clients, building a cancellation scope — so it is
    // contained like any other call even though there is no handle to poison yet.
    let created = catch(|| {
        let mut connection = unsafe { super::options::borrow(connection) }?;
        let state = connection.ready(super::connection::KIND)?.new_statement()?;
        // Cloned out up front so `statement_cancel` can reach the cancellation machinery without
        // taking the statement's dispatch turn.
        let cancel = state.get_cancel_handle();
        Ok((state, cancel))
    })
    .unwrap_or_else(Err);
    match created {
        Ok((state, cancel)) => {
            statement.private_data = Exported::into_private_data_with_cancel(state, Some(cancel));
            ADBC_STATUS_OK
        }
        Err(failure) => finish(Err(failure), error),
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `query` null or a
/// NUL-terminated string.
pub(super) unsafe extern "C" fn statement_set_sql_query(
    statement: *mut AdbcStatement,
    query: *const c_char,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            state.set_sql_query(required_str(query, "query")?)
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `plan` null or point to
/// `length` initialized bytes.
pub(super) unsafe extern "C" fn statement_set_substrait_plan(
    statement: *mut AdbcStatement,
    plan: *const u8,
    length: usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            state.set_substrait_plan(byte_slice(plan, length, "plan")?)
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`.
pub(super) unsafe extern "C" fn statement_prepare(
    statement: *mut AdbcStatement,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { with_state(statement, error, State::prepare) }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`.
pub(super) unsafe extern "C" fn statement_cancel(
    statement: *mut AdbcStatement,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // Dispatch-free on purpose, through the handle stored at `statement_new`; see
    // `handle::cancel`. With no operation in flight it reports `InvalidState`, which is exactly
    // what ADBC asks for.
    unsafe { super::handle::cancel::<State>(slot_of(statement), KIND, error) }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `values`/`schema` null or
/// point to a valid, non-released Arrow C data interface array and schema.
pub(super) unsafe extern "C" fn statement_bind(
    statement: *mut AdbcStatement,
    values: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            // Arrow's importer can panic on released inputs. Reject them before taking ownership
            // so a caller mistake neither consumes the other input nor poisons the statement.
            let Some(values_ref) = values.as_ref() else {
                return Err(invalid_argument("values must not be null"));
            };
            if values_ref.release.is_none() {
                return Err(invalid_argument("values has already been released"));
            }
            let Some(schema_ref) = schema.as_ref() else {
                return Err(invalid_argument("schema must not be null"));
            };
            if schema_ref.release.is_none() {
                return Err(invalid_argument("schema has already been released"));
            }
            // ADBC transfers both inputs. Moving only the array would leak the caller's schema.
            let values = FFI_ArrowArray::from_raw(values);
            let schema = FFI_ArrowSchema::from_raw(schema);
            let data = from_ffi(values, &schema).map_err(|failure| {
                invalid_argument(format!(
                    "bind values are not a valid Arrow array: {failure}"
                ))
            })?;
            let DataType::Struct(fields) = data.data_type() else {
                return Err(invalid_argument(format!(
                    "bind values must be a struct array, not {}",
                    data.data_type()
                )));
            };
            validate_imported(&data, fields).map_err(|failure| {
                invalid_argument(format!(
                    "bind values are not a valid Arrow array: {failure}"
                ))
            })?;
            if data.null_count() != 0 {
                return Err(invalid_argument(
                    "bind values must not contain top-level null rows; put null values in the \
                     individual columns instead",
                ));
            }
            state.bind(RecordBatch::from(StructArray::from(data)))
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `stream` null or point to a
/// valid, non-released Arrow C stream.
pub(super) unsafe extern "C" fn statement_bind_stream(
    statement: *mut AdbcStatement,
    stream: *mut FFI_ArrowArrayStream,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            let reader = BoundStreamReader::from_raw(stream)?;
            state.bind_stream(Box::new(reader))
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`; `out` must be null or point to
/// writable storage for an `FFI_ArrowArrayStream`, and `rows_affected` null or writable.
pub(super) unsafe extern "C" fn statement_execute_query(
    statement: *mut AdbcStatement,
    out: *mut FFI_ArrowArrayStream,
    rows_affected: *mut i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            // A null `out` is how the ABI says "run this, I do not want the rows" — the update
            // path, and the only one that can report a row count other than -1.
            if out.is_null() {
                let affected = state.execute_update()?.unwrap_or(-1);
                write_rows_affected(rows_affected, affected);
                return Ok(());
            }
            // A query's row count is not known until the stream has been drained, so the header's
            // spelling of "unknown" — -1 — is what the caller gets. Writing it is load-bearing:
            // adbc.h has the driver set this out parameter on every path, and a caller that reads
            // back whatever it happened to hold would see a stale count from an earlier execute.
            let reader = state.execute()?;
            std::ptr::write_unaligned(
                out,
                super::stream::export_reader(reader, driver_of(statement)),
            );
            write_rows_affected(rows_affected, -1);
            Ok(())
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `schema` null or point to
/// writable storage for an `FFI_ArrowSchema`.
pub(super) unsafe extern "C" fn statement_execute_schema(
    statement: *mut AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            required_output(schema, "schema")?;
            let found = state.execute_schema()?;
            write_out(schema, export_schema(&found)?, "schema")
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`, and `schema` null or point to
/// writable storage for an `FFI_ArrowSchema`.
pub(super) unsafe extern "C" fn statement_get_parameter_schema(
    statement: *mut AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            required_output(schema, "schema")?;
            let found = state.get_parameter_schema()?;
            write_out(schema, export_schema(&found)?, "schema")
        })
    }
}

/// # Safety
/// `statement` must be null or point to a valid `AdbcStatement`; `schema` and `partitions` must be
/// null or point to writable storage for their types, and `rows_affected` null or writable.
pub(super) unsafe extern "C" fn statement_execute_partitions(
    statement: *mut AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    partitions: *mut AdbcPartitions,
    rows_affected: *mut i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(statement, error, |state| {
            // Checked before executing: a missing out parameter is a caller bug, and discovering it
            // afterwards would mean having billed a query whose result nobody can collect.
            required_output(schema, "schema")?;
            required_output(partitions, "partitions")?;

            let result = state.execute_partitions()?;
            // Export the schema first: it is the only remaining fallible step, and failing after
            // writing the partitions would hand the caller a struct it must release to a call that
            // reported failure.
            let exported = export_schema(&result.schema)?;
            std::ptr::write_unaligned(schema, exported);
            std::ptr::write_unaligned(partitions, export_partitions(result.partitions));
            write_rows_affected(rows_affected, result.rows_affected);
            Ok(())
        })
    }
}

/// `rows_affected` is optional wherever it appears, so a null pointer means "not interested"
/// rather than a bad argument.
///
/// # Safety
/// `dst` must be null or point to a writable `i64`.
unsafe fn write_rows_affected(dst: *mut i64, value: i64) {
    if !dst.is_null() {
        unsafe { std::ptr::write_unaligned(dst, value) };
    }
}

/// Move a schema onto the C data interface.
///
/// A schema the driver itself produced but cannot export is a driver-side failure, not a caller
/// mistake, so the `ArrowError` becomes `Status::Internal`.
fn export_schema(schema: &Schema) -> Result<FFI_ArrowSchema> {
    FFI_ArrowSchema::try_from(schema).map_err(|failure| {
        Error::with_message_and_status(
            format!("failed to export the result schema over the C data interface: {failure}"),
            Status::Internal,
        )
    })
}

/// Everything an exported [`AdbcPartitions`] points at.
///
/// The ABI hands the caller two parallel arrays and a single release callback, so the payloads and
/// both arrays have to stay alive until that callback runs and then be freed together. Keeping
/// them in one box means the callback frees exactly one allocation and never has to reconstruct a
/// `Vec` capacity the C struct does not carry.
struct PartitionsOwner {
    /// Never read after construction: these exist so that what `pointers` points at stays alive
    /// for as long as the caller holds the struct.
    #[allow(
        dead_code,
        reason = "held only to keep the pointed-at payload bytes alive"
    )]
    payloads: Vec<Vec<u8>>,
    pointers: Vec<*const u8>,
    lengths: Vec<usize>,
}

fn export_partitions(payloads: Vec<Vec<u8>>) -> AdbcPartitions {
    let num_partitions = payloads.len();
    let pointers: Vec<*const u8> = payloads.iter().map(|payload| payload.as_ptr()).collect();
    let lengths: Vec<usize> = payloads.iter().map(Vec::len).collect();
    let mut owner = Box::new(PartitionsOwner {
        payloads,
        pointers,
        lengths,
    });
    // Taken off the boxed value rather than the locals: the heap buffers would survive the move
    // either way, but reading them here is one less invariant a later edit can break.
    //
    // An empty `Vec` has no allocation, so `as_mut_ptr` yields the element type's alignment as a
    // bare address -- non-null, dangling, and indistinguishable from a real array to anything
    // that inspects the struct. No caller may dereference it at `num_partitions == 0`, but a
    // sanitizer-instrumented or defensive driver manager is entitled to flag it, so the empty
    // case says null and means it.
    let (partitions, partition_lengths) = if num_partitions == 0 {
        (std::ptr::null_mut(), std::ptr::null_mut())
    } else {
        (owner.pointers.as_mut_ptr(), owner.lengths.as_mut_ptr())
    };
    AdbcPartitions {
        num_partitions,
        partitions,
        partition_lengths,
        private_data: Box::into_raw(owner).cast::<c_void>(),
        release: Some(release_partitions),
    }
}

/// # Safety
/// `partitions` must be null or point to an `AdbcPartitions` produced by [`export_partitions`].
// Guard-exempt: must not unwind and cannot — dropping the owner box frees plain memory, and the
// rest is non-panicking pointer writes.
unsafe extern "C" fn release_partitions(partitions: *mut AdbcPartitions) {
    let Some(partitions) = (unsafe { partitions.as_mut() }) else {
        return;
    };
    if !partitions.private_data.is_null() {
        drop(unsafe { Box::from_raw(partitions.private_data.cast::<PartitionsOwner>()) });
        partitions.private_data = std::ptr::null_mut();
    }
    // The arrays borrowed from the box that just went away, so they must not be left readable.
    partitions.num_partitions = 0;
    partitions.partitions = std::ptr::null_mut();
    partitions.partition_lengths = std::ptr::null_mut();
    // Clearing this makes the callback idempotent, which the header requires.
    partitions.release = None;
}

#[cfg(test)]
mod tests;

//! The entry points every exported object repeats, written once.
//!
//! A database, a connection and a statement all expose the same handle prologues (`New`,
//! `Release`) and the same eight option entry points (string/bytes/int/double setters and
//! getters). They live here as generic `extern "C"` functions, over [`FfiHandle`] — the link
//! between an ABI struct and the state behind its `private_data` — and over [`OptionTarget`] —
//! how options reach that state. The vtable names the instantiation each slot needs, so each
//! object module keeps only what is genuinely its own.

use std::ffi::{c_char, c_void};
use std::sync::MutexGuard;

use adbc_core::Optionable;
use adbc_core::error::Result;
use adbc_core::options::OptionValue;

use super::abi::{ADBC_STATUS_OK, AdbcDriver, AdbcError, AdbcStatusCode};
use super::guard::{
    byte_slice, missing_argument, required_str, write_bytes, write_out, write_string,
};
use super::handle::{Exported, Staged, dispatch};
use crate::error::{invalid_state, not_found};

/// How an entry point reaches the driver object behind one of the three exported ABI structs.
///
/// Implemented by `AdbcDatabase`, `AdbcConnection` and `AdbcStatement`, whose first field is the
/// `private_data` slot in every case.
pub(super) trait FfiHandle {
    /// What lives behind `private_data`.
    type State: OptionTarget + Send;
    /// The object's name in diagnostics: "database", "connection" or "statement".
    const KIND: &'static str;
    fn private_data(&self) -> *mut c_void;
    fn private_data_mut(&mut self) -> &mut *mut c_void;
    /// The driver vtable the caller's handle carries, which the driver manager populates and
    /// routes `AdbcErrorGetDetail*` back through.
    fn private_driver(&self) -> *const AdbcDriver;
}

/// How options reach the state behind a handle. A [`Staged`] object buffers sets until `Init` and
/// refuses gets before it; a statement is live from the moment it exists and does neither.
pub(super) trait OptionTarget {
    /// The typed option key, parsed from the caller's C string.
    type Key: for<'a> From<&'a str>;
    /// The live object the typed getters read from.
    type Options: Optionable<Option = Self::Key>;

    /// Apply an option, buffering it when the object has a pre-init phase it is still in.
    fn set(&mut self, key: Self::Key, value: OptionValue) -> Result<()>;

    /// The live optionable state the typed getters read `key` from, or the one status adbc.h
    /// gives those getters — `NOT_FOUND` — when there is no such state yet.
    fn options(&mut self, kind: &str, key: &Self::Key) -> Result<&mut Self::Options>;
}

impl<K, S> OptionTarget for Staged<K, S>
where
    K: PartialEq + for<'a> From<&'a str> + AsRef<str>,
    S: Optionable<Option = K>,
{
    type Key = K;
    type Options = S;

    /// A pre-`Init` set is buffered verbatim and validated when `Init` replays it through the
    /// driver's own constructor.
    ///
    /// adbc.h would rather have the key rejected here, but the driver's `set_option` is its own
    /// validator: a database option can rewrite several fields at once (`adbc.uri` expands into
    /// the individual keys), so there is no pure classifier to consult that would not be a second
    /// copy of the setter, free to drift from it. Buffering keeps one validator. The buffer is
    /// ordered and last-write-wins per key, which is what makes the replay mean the same thing as
    /// setting the options one by one on a live object: the driver's precedence rule -- a later
    /// option overrides what an earlier `adbc.uri` expanded to -- survives the `Init` boundary.
    fn set(&mut self, key: K, value: OptionValue) -> Result<()> {
        match self {
            Self::Pending(options) => {
                options.set(key, value);
                Ok(())
            }
            Self::Ready(state) => state.set_option(key, value),
        }
    }

    /// adbc.h documents exactly one failure return for `Get*Option*` -- "ADBC_STATUS_NOT_FOUND if
    /// the option is not recognized" -- and licenses no other, so a pre-`Init` object reports the
    /// key as absent rather than the handle as unusable. The refusal itself stands: what a caller
    /// buffered is not a value the driver has accepted, and reporting it back would claim a
    /// validation that has not happened yet.
    fn options(&mut self, kind: &str, key: &K) -> Result<&mut S> {
        match self {
            Self::Ready(state) => Ok(state),
            Self::Pending(_) => Err(not_found(format!(
                "Spanner {kind} option {} cannot be read before the {kind} is initialized; \
                 initialize it first",
                key.as_ref()
            ))),
        }
    }
}

type KeyOf<H> = <<H as FfiHandle>::State as OptionTarget>::Key;

/// The `private_data` slot behind a caller-supplied handle, with null standing in for a null or
/// never-populated handle.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
pub(super) unsafe fn slot_of<H: FfiHandle>(handle: *mut H) -> *mut c_void {
    unsafe { handle.as_ref() }.map_or(std::ptr::null_mut(), FfiHandle::private_data)
}

/// The driver vtable behind a caller-supplied handle, or null when the caller populated none.
///
/// It is carried into every exported Arrow stream, because `AdbcErrorFromArrayStream` hands the
/// caller an `AdbcError` in driver-owned storage and the driver manager reaches that error's
/// details through this field. A directly linked caller leaves it null and reads the details
/// through the driver's own `ErrorGetDetail*` instead.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
pub(super) unsafe fn driver_of<H: FfiHandle>(handle: *mut H) -> *const AdbcDriver {
    unsafe { handle.as_ref() }.map_or(std::ptr::null(), FfiHandle::private_driver)
}

/// Reject a null handle struct before anything behind it is touched. Distinct from a null
/// `private_data`: a null struct pointer is a bad argument, an unpopulated slot is an object used
/// out of order.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
unsafe fn checked<'a, H: FfiHandle>(handle: *mut H) -> Result<&'a mut H> {
    unsafe { handle.as_mut() }.ok_or_else(|| missing_argument(H::KIND))
}

/// Run `f` against the state behind a caller-supplied handle, dispatching through
/// [`super::handle::dispatch`] for panic containment and the poison/lock discipline.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
pub(super) unsafe fn with_state<H, F>(handle: *mut H, error: *mut AdbcError, f: F) -> AdbcStatusCode
where
    H: FfiHandle,
    F: FnOnce(&mut H::State) -> Result<()>,
{
    unsafe { dispatch::<H::State, _>(slot_of(handle), H::KIND, error, f) }
}

/// A `*New` on a handle that already carries an object.
///
/// adbc.h has the caller "pass in a zero-initialized" struct to every `New`, so a populated
/// `private_data` is caller error either way. Overwriting it would drop the caller's only pointer
/// to the previous object and leak it silently -- for a statement, live Spanner clients and a
/// cancellation scope. Refusing costs a null check and turns that into something a caller can see.
pub(super) fn already_populated(kind: &str) -> adbc_core::error::Error {
    invalid_state(format!(
        "{kind} already holds a driver object; release it before creating another, and pass a \
         zero-initialized struct to a first one"
    ))
}

/// `AdbcDatabaseNew`/`AdbcConnectionNew`: populate the caller's handle with a freshly built
/// pre-init state. A statement has no pre-`Init` phase, so it is built from its connection by
/// [`super::statement::statement_new`] instead.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct.
// Guard-exempt: no driver state exists yet and nothing here can unwind — the default state
// allocates nothing, the `Exported` box's allocation aborts rather than panics on failure, and
// the rest is plain pointer writes.
pub(super) unsafe extern "C" fn new_handle<H: FfiHandle>(
    handle: *mut H,
    error: *mut AdbcError,
) -> AdbcStatusCode
where
    H::State: Default,
{
    match unsafe { checked(handle) } {
        Ok(handle) if !handle.private_data().is_null() => {
            super::handle::finish(Err(already_populated(H::KIND)), error)
        }
        Ok(handle) => {
            *handle.private_data_mut() = Exported::into_private_data(H::State::default());
            ADBC_STATUS_OK
        }
        Err(failure) => super::handle::finish(Err(failure), error),
    }
}

/// Every `*Release`: drop the state and null the slot, so a second release is an error rather
/// than a double free.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct, and no other call on the object may be
/// in flight.
pub(super) unsafe extern "C" fn release_handle<H: FfiHandle>(
    handle: *mut H,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    match unsafe { checked(handle) } {
        Ok(handle) => unsafe {
            super::handle::release::<H::State>(handle.private_data_mut(), H::KIND, error)
        },
        Err(failure) => super::handle::finish(Err(failure), error),
    }
}

/// Lock the state behind a caller-supplied handle without dispatching a call on it, for the entry
/// points that need a second object beyond the one they dispatch on. The caller still has to
/// check readiness before using it.
///
/// # Safety
/// `handle` must be null or point to a valid ABI struct, and the guard must not outlive it.
pub(super) unsafe fn borrow<'a, H: FfiHandle>(handle: *mut H) -> Result<MutexGuard<'a, H::State>> {
    unsafe { super::handle::lock_state::<H::State>(slot_of(handle), H::KIND) }
}

/// # Safety
/// `key` must be null or a NUL-terminated string.
unsafe fn parse_key<H: FfiHandle>(key: *const c_char) -> Result<KeyOf<H>> {
    Ok(KeyOf::<H>::from(unsafe { required_str(key, "key") }?))
}

/// Apply an already-converted option value.
///
/// # Safety
/// `handle` and `key` must be null or valid for the call.
unsafe fn set_option<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: OptionValue,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            state.set(key, value)
        })
    }
}

/// # Safety
/// `handle`, `key` and `value` must be null or valid for the call.
pub(super) unsafe extern "C" fn set_option_string<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *const c_char,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    // The value is validated before the handle is even consulted, so a null value is a bad
    // argument on any handle.
    let value = match unsafe { required_str(value, "value") } {
        Ok(value) => OptionValue::String(value.to_string()),
        Err(failure) => return super::handle::finish(Err(failure), error),
    };
    unsafe { set_option(handle, key, value, error) }
}

/// # Safety
/// `handle` and `key` must be null or valid for the call, and `value` null or point to `length`
/// initialized bytes.
pub(super) unsafe extern "C" fn set_option_bytes<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let value = match unsafe { byte_slice(value, length, "value") } {
        Ok(value) => OptionValue::Bytes(value.to_vec()),
        Err(failure) => return super::handle::finish(Err(failure), error),
    };
    unsafe { set_option(handle, key, value, error) }
}

/// # Safety
/// `handle` and `key` must be null or valid for the call.
pub(super) unsafe extern "C" fn set_option_int<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { set_option(handle, key, OptionValue::Int(value), error) }
}

/// # Safety
/// `handle` and `key` must be null or valid for the call.
pub(super) unsafe extern "C" fn set_option_double<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: f64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe { set_option(handle, key, OptionValue::Double(value), error) }
}

/// # Safety
/// `handle`, `key`, `value` and `length` must be null or valid for the call.
pub(super) unsafe extern "C" fn get_option_string<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_string(key)?;
            write_string(&found, value, length)
        })
    }
}

/// # Safety
/// `handle`, `key`, `value` and `length` must be null or valid for the call.
pub(super) unsafe extern "C" fn get_option_bytes<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *mut u8,
    length: *mut usize,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_bytes(key)?;
            write_bytes(&found, value, length)
        })
    }
}

/// # Safety
/// `handle`, `key` and `value` must be null or valid for the call.
pub(super) unsafe extern "C" fn get_option_int<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *mut i64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_int(key)?;
            write_out(value, found, "value")
        })
    }
}

/// # Safety
/// `handle`, `key` and `value` must be null or valid for the call.
pub(super) unsafe extern "C" fn get_option_double<H: FfiHandle>(
    handle: *mut H,
    key: *const c_char,
    value: *mut f64,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(handle, error, |state| {
            let key = parse_key::<H>(key)?;
            let found = state.options(H::KIND, &key)?.get_option_double(key)?;
            write_out(value, found, "value")
        })
    }
}

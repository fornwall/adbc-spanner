//! How an exported object is represented behind `private_data`, and how entry points reach it.
//!
//! Every exported handle stores a reference-counted [`Exported<S>`], which centralizes what no
//! individual entry point should then have to remember: panic containment is per object rather
//! than per process, uninitialized and released handles are rejected uniformly, calls on one
//! object serialize behind a mutex, and [`cancel`] neither takes that mutex nor borrows the
//! handle's reference.
//!
//! What the reference count does *not* cover, and cannot: `private_data` is a field of the
//! caller's own `AdbcDatabase`/`AdbcConnection`/`AdbcStatement` struct. `Release` writes it while
//! `Cancel` reads it, so a host that lets the two overlap is racing on its own memory before the
//! driver is reached at all. What the count buys is narrower than "`Cancel` is safe against a
//! concurrent `Release`": from the moment [`cancel`] holds a reference of its own, a `Release` on
//! another thread can no longer free the object under it, and the last of the two frees it
//! instead. Reading the slot and taking that reference are two steps, though, and a `Release`
//! that lands between them drops the last reference to memory [`cancel`] is about to touch. No
//! driver reached through a raw `private_data` field can close that window — the upstream C++
//! framework has the same exposure — so the host owes `Release` the exclusion every other C API
//! demands of a destructor, against `Cancel` as much as against anything else.

use std::ffi::c_void;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use adbc_core::CancelHandle;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;

use super::abi::{ADBC_STATUS_OK, AdbcError, AdbcStatusCode};
use super::error::export_error;
use super::guard::catch;

/// Options accumulated before init, preserving insertion order with last-write-wins per key.
///
/// Order matters: the driver applies `adbc.uri` before other database options so that explicit
/// options win over URI-derived ones, and it rejects a *duplicated* URI. Collapsing repeats here
/// keeps a caller that simply sets the same option twice from tripping that check.
pub(crate) struct OptionBuffer<K> {
    /// Cloned rather than taken by the fallible constructors, so a failed `Init` keeps the
    /// options and the handle can be initialized again.
    pub(super) entries: Vec<(K, OptionValue)>,
}

impl<K: PartialEq> OptionBuffer<K> {
    pub(crate) fn set(&mut self, key: K, value: OptionValue) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|(existing, _)| *existing == key)
        {
            entry.1 = value;
        } else {
            self.entries.push((key, value));
        }
    }
}

/// An exported object is either still collecting options or fully initialized. ADBC splits
/// construction across `New`/`SetOption`/`Init`, and the driver's own constructors want all the
/// options at once, so the pre-init options are buffered until `Init`.
pub(crate) enum Staged<K, S> {
    Pending(OptionBuffer<K>),
    Ready(S),
}

/// A fresh handle is pending with an empty option buffer, which is what `New` installs.
impl<K: PartialEq, S> Default for Staged<K, S> {
    fn default() -> Self {
        Self::Pending(OptionBuffer {
            entries: Vec::new(),
        })
    }
}

impl<K: PartialEq, S> Staged<K, S> {
    /// The initialized state, or the uniform refusal for an object that has none.
    pub(crate) fn ready(&mut self, kind: &str) -> Result<&mut S> {
        match self {
            Self::Ready(state) => Ok(state),
            Self::Pending(_) => Err(uninitialized(kind)),
        }
    }

    /// The mirror of [`Self::ready`]: the options buffered before `Init`, for the entry point
    /// that performs it.
    pub(crate) fn pending(&mut self, kind: &str) -> Result<&mut OptionBuffer<K>> {
        match self {
            Self::Pending(options) => Ok(options),
            Self::Ready(_) => Err(Error::with_message_and_status(
                format!("{kind} is already initialized"),
                Status::InvalidState,
            )),
        }
    }
}

/// The contents of an exported handle's `private_data`.
///
/// The raw pointer the caller holds is shared freely between its threads, so everything here is
/// reached through `&Exported<S>` and interior mutability; forming a `&mut` to the whole struct
/// would alias when two calls overlap, which adbc.h explicitly permits for `Cancel` and the
/// option getters.
pub(crate) struct Exported<S> {
    /// Set when a call on this object panicked. The panic is stopped inside [`dispatch`], but the
    /// state it was mutating may hold torn invariants, so a poisoned object refuses all further
    /// dispatches. Atomic because entry points on other threads read it concurrently.
    poisoned: AtomicBool,
    /// The object's cancel handle, populated once the state is initialized. Kept beside — not
    /// inside — the state so [`cancel`] can reach the driver's thread-safe cancellation machinery
    /// while another call holds the state lock.
    cancel: Mutex<Option<Box<dyn CancelHandle>>>,
    /// The driver object itself. Every call that touches it takes this lock, so overlapping calls
    /// serialize instead of racing; [`cancel`] bypasses it.
    state: Mutex<S>,
}

// Every entry point reaches this struct through a raw `private_data` field, so no call site
// checks that sharing it between the caller's threads is sound -- the field types have to carry
// that, and the compiler is never asked. Ask it here, where the unsizing coercion is the proof
// and the function exists only to be type-checked: a field added later that is not `Sync` -- a
// `Cell`, an `Rc` -- would otherwise compile and race.
fn _exported_is_shareable<S: Send>(exported: &Exported<S>) -> &(dyn Send + Sync) {
    exported
}

// `S: Send` because the caller may create the object on one thread and use it from another; the
// mutex already prevents concurrent access, so `Sync` is not required of the state itself.
impl<S: Send> Exported<S> {
    pub(crate) fn into_private_data(state: S) -> *mut c_void {
        Self::into_private_data_with_cancel(state, None)
    }

    /// As [`Self::into_private_data`], also storing the handle the one lock-free entry point --
    /// [`cancel`] -- reaches without the state. Objects whose state is only initialized later
    /// install it via [`install_cancel_handle`] instead.
    pub(crate) fn into_private_data_with_cancel(
        state: S,
        cancel: Option<Box<dyn CancelHandle>>,
    ) -> *mut c_void {
        Arc::into_raw(Arc::new(Self {
            poisoned: AtomicBool::new(false),
            cancel: Mutex::new(cancel),
            state: Mutex::new(state),
        }))
        .cast::<c_void>()
        .cast_mut()
    }
}

impl<S> Exported<S> {
    /// Take the state lock, refusing a poisoned object.
    ///
    /// The poison flag is checked *after* acquiring the lock: the flag is set while the panicking
    /// call still holds the lock, so acquiring it is what guarantees the flag is visible here.
    /// The std mutex's own poisoning is deliberately ignored — a panic is stopped inside
    /// [`dispatch`] before it can unwind past the guard, so the std flag never carries
    /// information `poisoned` does not.
    fn lock_state(&self, kind: &str) -> Result<MutexGuard<'_, S>> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if self.poisoned.load(Ordering::Acquire) {
            return Err(poisoned(kind));
        }
        Ok(state)
    }
}

/// The one refusal for a handle that carries no usable driver object.
///
/// A null `private_data`, a pre-`Init` state and a missing cancel handle are the same thing to
/// the caller — a handle it may not use yet, or may not use any more — and the caller cannot
/// tell which side it is on from where the call landed. Naming the released case matters most:
/// it is the one that looks like a working handle from the outside.
#[cold]
#[inline(never)]
fn uninitialized(kind: &str) -> Error {
    Error::with_message_and_status(
        format!("{kind} is uninitialized or already released"),
        Status::InvalidState,
    )
}

#[cold]
#[inline(never)]
fn poisoned(kind: &str) -> Error {
    Error::with_message_and_status(
        format!("{kind} is unusable after an earlier panic in the driver"),
        Status::Internal,
    )
}

/// Borrow the object behind `slot` without taking a reference of one's own.
///
/// This is what every entry point but [`cancel`] uses: adbc.h leaves a `Release` that overlaps
/// another call on the same object undefined, so borrowing the handle's own reference is exactly
/// as strong a claim as those entry points are entitled to make.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released, and the reference must not outlive the object.
unsafe fn borrowed<'a, S>(slot: *mut c_void, kind: &str) -> Result<&'a Exported<S>> {
    unsafe { slot.cast::<Exported<S>>().as_ref() }.ok_or_else(|| uninitialized(kind))
}

/// Take a reference of one's own to the object behind `slot`, keeping it alive independently of
/// the handle.
///
/// [`cancel`] is the one entry point adbc.h says must always be thread-safe, so it is the one that
/// may legitimately still be running when [`release`] drops the handle's reference; cloning the
/// count here keeps that a completed cancel rather than a use-after-free.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released.
unsafe fn strong<S>(slot: *mut c_void, kind: &str) -> Result<Arc<Exported<S>>> {
    let pointer: *const Exported<S> = slot.cast_const().cast::<Exported<S>>();
    if pointer.is_null() {
        return Err(uninitialized(kind));
    }
    // The handle keeps its own reference; this reconstructs it only to clone from, so it must not
    // be dropped.
    let handle = ManuallyDrop::new(unsafe { Arc::from_raw(pointer) });
    Ok(Arc::clone(&handle))
}

/// Run `f` against the object behind `slot`, containing panics and reporting errors through
/// `error`.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released.
pub(crate) unsafe fn dispatch<S, F>(
    slot: *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
    f: F,
) -> AdbcStatusCode
where
    F: FnOnce(&mut S) -> Result<()>,
{
    let outcome = catch(|| {
        let exported = unsafe { borrowed::<S>(slot, kind) }?;
        let mut state = exported.lock_state(kind)?;
        // The driver call is contained on its own so a panic can mark this object. The store
        // happens while the state lock is still held, which is what makes it visible to every
        // later `lock_state`.
        catch(|| f(&mut state)).unwrap_or_else(|panicked| {
            exported.poisoned.store(true, Ordering::Release);
            Err(panicked)
        })
    });
    finish(outcome.unwrap_or_else(Err), error)
}

/// Cancel the object's in-flight operation without taking its dispatch turn.
///
/// adbc.h requires `Cancel` to stay callable while another thread is blocked inside an operation
/// on the same object — interrupting that operation is its main purpose — so this path must not
/// wait on the state lock that the blocked call holds. The handle stored beside the state reaches
/// the driver's own cancellation machinery, which is thread-safe throughout. The poison flag is
/// deliberately not consulted: cancelling whatever a panicked call left running is strictly more
/// useful than refusing.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released.
pub(crate) unsafe fn cancel<S>(
    slot: *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let outcome = catch(|| {
        // A reference of this call's own, not the handle's: see [`strong`].
        let exported = unsafe { strong::<S>(slot, kind) }?;
        let handle = exported
            .cancel
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match handle.as_ref() {
            Some(handle) => handle.try_cancel(),
            None => Err(uninitialized(kind)),
        }
    });
    finish(outcome.unwrap_or_else(Err), error)
}

/// Store the cancel handle for [`cancel`] to find. Called by an object whose state is only built
/// later, just *before* that state is published: [`cancel`] never takes the state lock, so the
/// instant the object becomes usable is the instant a caller may start an operation and cancel
/// it. Until the handle is installed [`cancel`] refuses the object as [`uninitialized`]. An
/// object that has its state at construction passes the handle to
/// [`Exported::into_private_data_with_handles`] instead and needs none of this.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released.
pub(crate) unsafe fn install_cancel_handle<S>(slot: *mut c_void, handle: Box<dyn CancelHandle>) {
    if let Some(exported) = unsafe { slot.cast::<Exported<S>>().as_ref() } {
        *exported
            .cancel
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(handle);
    }
}

/// Lock the state behind `slot` without dispatching a call on it.
///
/// This exists for the entry points that need a second object beyond the one they dispatch on —
/// `AdbcConnectionInit` reads the database while dispatching on the connection, and
/// `AdbcStatementNew` has no statement yet when it reaches into the connection. The lock makes
/// that access serialize with the other object's own calls.
///
/// # Safety
/// `slot` must be null or a pointer produced by [`Exported::<S>::into_private_data`] that has not
/// been released, and the guard must not outlive it.
pub(crate) unsafe fn lock_state<'a, S>(slot: *mut c_void, kind: &str) -> Result<MutexGuard<'a, S>> {
    unsafe { borrowed::<S>(slot, kind) }?.lock_state(kind)
}

/// Drop the handle's reference to the object and null the slot out, so a second release is an
/// error rather than a double free.
///
/// The object itself is freed here only if nothing else holds a reference — a [`cancel`] running
/// on another thread does, and then the last of the two frees it. Every other entry point merely
/// borrows, so adbc.h's rule still stands for them: no call on the object may be in flight.
///
/// # Safety
/// `slot` must point to a handle field holding null or a pointer produced by
/// [`Exported::<S>::into_private_data`], and no call other than [`cancel`] may be in flight.
pub(crate) unsafe fn release<S>(
    slot: &mut *mut c_void,
    kind: &'static str,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    let taken = std::mem::replace(slot, std::ptr::null_mut());
    if taken.is_null() {
        return finish(Err(uninitialized(kind)), error);
    }
    // Dropping driver state can run arbitrary code (closing clients, joining tasks), so it is
    // contained like any other call.
    let outcome = catch(|| {
        drop(unsafe { Arc::from_raw(taken.cast_const().cast::<Exported<S>>()) });
        Ok(())
    });
    finish(outcome.unwrap_or_else(Err), error)
}

/// Convert a driver result into a status code, exporting the error when there is one.
pub(crate) fn finish(result: Result<()>, error: *mut AdbcError) -> AdbcStatusCode {
    match result {
        Ok(()) => ADBC_STATUS_OK,
        Err(failure) => {
            let status = AdbcStatusCode::from(failure.status);
            // Exporting allocates and may run a stale release callback the caller left in the
            // struct, so it is contained like driver code. On a caught panic the caller still
            // gets the status code, with the error struct left as it was.
            let contained = catch(|| {
                unsafe { export_error(error, failure) };
                Ok(())
            });
            drop(contained);
            status
        }
    }
}

#[cfg(test)]
mod tests;

//! `AdbcDatabase*` entry points.

use std::ffi::c_void;
use std::sync::{Mutex, OnceLock};

use adbc_core::Driver;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionDatabase;

use super::abi::{AdbcDatabase, AdbcDriver, AdbcError, AdbcStatusCode};
use super::handle::Staged;
use super::options::{FfiHandle, with_state};
use crate::driver::{SpannerDatabase, SpannerDriver};

pub(super) const KIND: &str = "database";

pub(super) type State = Staged<OptionDatabase, SpannerDatabase>;

impl FfiHandle for AdbcDatabase {
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

/// One driver, and therefore one Tokio runtime, shared by every database in the process.
///
/// The exporter this replaces constructed a driver per `AdbcDatabaseInit` through `Default`, which
/// meant a fresh multi-threaded runtime for every database a host opened — and a *panic* rather
/// than an error when the runtime could not be built, since `Default` has nowhere to report one.
/// Owning the lifecycle here fixes both: the runtime is built once, and a failure to build it is
/// reported as `ADBC_STATUS_INTERNAL`.
fn shared_driver() -> Result<&'static Mutex<SpannerDriver>> {
    static DRIVER: OnceLock<Mutex<SpannerDriver>> = OnceLock::new();

    if let Some(driver) = DRIVER.get() {
        return Ok(driver);
    }
    // A lost race here drops a freshly built, taskless runtime, which is cheap. Constructing
    // eagerly keeps the fallible part outside `get_or_init`, which cannot report failure.
    let driver = SpannerDriver::try_new()?;
    Ok(DRIVER.get_or_init(|| Mutex::new(driver)))
}

/// # Safety
/// `database` must be null or point to a valid `AdbcDatabase`.
pub(super) unsafe extern "C" fn database_init(
    database: *mut AdbcDatabase,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    unsafe {
        with_state(database, error, |state| {
            let options = state.pending(KIND)?;
            let driver = shared_driver()?;
            let mut driver = driver.lock().map_err(|_| {
                Error::with_message_and_status(
                    "the shared Spanner driver is unusable after an earlier panic",
                    Status::Internal,
                )
            })?;
            // Replaying through `new_database_with_opts` rather than applying the options one by
            // one keeps the driver's ordering rule — `adbc.uri` expands eagerly and a later option
            // overrides what it wrote — in one place. The buffer is ordered, so the replay means
            // exactly what the caller's call sequence did.
            let database = driver.new_database_with_opts(options.entries.clone())?;
            *state = State::Ready(database);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;

//! End-to-end tests that drive this export layer through a real ADBC driver manager.
//!
//! Everything else in `src/ffi` is tested against the raw C structs. These tests instead load the
//! exported entry point the way a host would, so the vtable wiring and option plumbing are
//! exercised as one piece.
//!
//! They stop at the database, because that is as far as a Spanner driver gets without a server:
//! `AdbcConnectionInit` builds a `DatabaseClient`, which opens a session with a real
//! `CreateSession` RPC. Connection- and statement-level round trips (and query streaming) are
//! covered against the emulator by `tests/integration.rs` instead.

use std::ffi::{c_int, c_void};

use adbc_core::error::Status;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Database, Driver, Optionable};
use adbc_driver_manager::ManagedDriver;

use super::abi::{AdbcError, AdbcStatusCode};

fn load_driver() -> ManagedDriver {
    // `load_static` wants `adbc_ffi`'s spelling of the init function. Ours differs only in the
    // nominal type of the error pointer, and the two structs are the same `#[repr(C)]` layout, so
    // the pointers are interchangeable at the ABI level.
    type OurInit = unsafe extern "C" fn(c_int, *mut c_void, *mut AdbcError) -> AdbcStatusCode;
    let ours: OurInit = super::AdbcSpannerInit;
    let init: adbc_ffi::FFI_AdbcDriverInitFunc = unsafe { std::mem::transmute(ours) };
    ManagedDriver::load_static(&init, AdbcVersion::V110).unwrap()
}

/// A database in emulator mode pointed at an endpoint nothing listens on: enough to reach every
/// database option path through the manager without credentials or network access, because
/// `AdbcDatabaseInit` only replays the buffered options through the driver's own setters.
fn anonymous_database() -> impl Database {
    load_driver()
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                "spanner:///projects/a-project/instances/an-instance/databases/a-database".into(),
            ),
            (
                OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()),
                "http://127.0.0.1:1".into(),
            ),
            (
                OptionDatabase::Other(crate::OPTION_EMULATOR.to_owned()),
                "true".into(),
            ),
        ])
        .unwrap()
}

/// Per-option behavior is tested natively; through the manager the invariant is only that options
/// plumb through the C boundary, so one option set at construction and read back is enough. Every
/// Spanner database option is a string, so there is no integer one to pair it with.
#[test]
fn exported_driver_database_options_round_trip() {
    let mut database = anonymous_database();
    assert_eq!(
        database
            .get_option_string(OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()))
            .unwrap(),
        "http://127.0.0.1:1"
    );

    // A set after `Init` reaches the live database, and the new value comes back the same way.
    database
        .set_option(
            OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()),
            "http://127.0.0.1:2".into(),
        )
        .unwrap();
    assert_eq!(
        database
            .get_option_string(OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()))
            .unwrap(),
        "http://127.0.0.1:2"
    );
}

/// An unknown option must surface as a driver error through the C boundary rather than a panic or
/// a bare status code with no message.
#[test]
fn exported_driver_reports_unknown_options_with_a_message() {
    let mut driver = load_driver();
    let database = driver.new_database_with_opts([(
        OptionDatabase::Uri,
        "spanner:///projects/a-project/instances/an-instance/databases/a-database".into(),
    )]);
    let database = database.unwrap();
    let error = database
        .get_option_string(OptionDatabase::Other("spanner.nope".into()))
        .unwrap_err();
    assert!(!error.message.is_empty(), "the message must survive export");
}

/// Every Spanner option has one canonical string form, and the typed getters are reinterpretations
/// of it: `get_option_bytes` serves those very bytes, while `get_option_int`/`get_option_double`
/// parse them and report `InvalidArguments` when the stored value is not a number. Both the status
/// and the message have to survive the C boundary intact.
#[test]
fn exported_driver_option_getters_reinterpret_the_stored_string() {
    let database = anonymous_database();
    let endpoint = OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned());

    assert_eq!(
        database.get_option_bytes(endpoint.clone()).unwrap(),
        b"http://127.0.0.1:1".to_vec()
    );
    for (label, error) in [
        (
            "string option through get_option_int",
            database.get_option_int(endpoint.clone()).unwrap_err(),
        ),
        (
            "string option through get_option_double",
            database.get_option_double(endpoint.clone()).unwrap_err(),
        ),
    ] {
        assert_eq!(error.status, Status::InvalidArguments, "{label}");
        assert!(!error.message.is_empty(), "{label}");
    }

    // A key the driver has no value for is the one failure adbc.h documents for the getters.
    let unset = OptionDatabase::Other(crate::OPTION_KEYFILE.to_owned());
    assert_eq!(
        database.get_option_string(unset).unwrap_err().status,
        Status::NotFound
    );
}

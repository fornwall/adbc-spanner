use std::ffi::CString;

use super::*;
use crate::ffi::abi::{
    ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_NOT_FOUND,
    ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_OK,
};
use crate::ffi::options::{
    get_option_int, get_option_string, new_handle, release_handle, set_option_int,
    set_option_string,
};
use crate::ffi::test_support::{null, release_error, zeroed_error};

/// The database path every test below points at. Nothing here connects, so it need not exist.
const DATABASE: &str = "projects/a-project/instances/an-instance/databases/a-database";

fn pending() -> AdbcDatabase {
    let mut database = AdbcDatabase {
        private_data: null(),
        private_driver: null(),
    };
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { new_handle(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    database
}

fn set(database: &mut AdbcDatabase, key: &str, value: &str) -> AdbcStatusCode {
    let key = CString::new(key).unwrap();
    let value = CString::new(value).unwrap();
    let mut error = zeroed_error();
    let status = unsafe {
        set_option_string(
            &raw mut *database,
            key.as_ptr(),
            value.as_ptr(),
            &raw mut error,
        )
    };
    release_error(&mut error);
    status
}

fn get(database: &mut AdbcDatabase, key: &str) -> std::result::Result<String, AdbcStatusCode> {
    let key = CString::new(key).unwrap();
    let mut error = zeroed_error();
    let mut length = 0_usize;
    // First pass asks for the size, as the ADBC GetOption protocol requires.
    let status = unsafe {
        get_option_string(
            &raw mut *database,
            key.as_ptr(),
            null(),
            &raw mut length,
            &raw mut error,
        )
    };
    if status != ADBC_STATUS_OK {
        release_error(&mut error);
        return Err(status);
    }
    let mut buffer = vec![0_i8; length];
    let status = unsafe {
        get_option_string(
            &raw mut *database,
            key.as_ptr(),
            buffer.as_mut_ptr(),
            &raw mut length,
            &raw mut error,
        )
    };
    release_error(&mut error);
    if status != ADBC_STATUS_OK {
        return Err(status);
    }
    Ok(unsafe { std::ffi::CStr::from_ptr(buffer.as_ptr()) }
        .to_string_lossy()
        .into_owned())
}

/// Emulator mode against an endpoint nothing listens on: enough to build a real database without
/// credentials or network access. `Init` only replays the buffered options through the driver's
/// own setters — the Spanner client stack is built later, on the first connection — so nothing
/// here reaches the network.
fn initialized() -> AdbcDatabase {
    let mut database = pending();
    for (key, value) in [
        (
            adbc_core::constants::ADBC_OPTION_URI,
            &*format!("spanner:///{DATABASE}"),
        ),
        (crate::OPTION_ENDPOINT, "http://127.0.0.1:1"),
        (crate::OPTION_EMULATOR, "true"),
    ] {
        assert_eq!(set(&mut database, key, value), ADBC_STATUS_OK);
    }
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    database
}

fn release(database: &mut AdbcDatabase) -> AdbcStatusCode {
    let mut error = zeroed_error();
    let status = unsafe { release_handle(&raw mut *database, &raw mut error) };
    release_error(&mut error);
    status
}

/// adbc.h has the caller pass a zero-initialized struct to `AdbcDatabaseNew`, so a populated slot
/// is caller error. Overwriting it would drop the caller's only pointer to the first object and
/// leak it silently; the refusal leaves that object reachable, so it can still be released.
#[test]
fn a_second_new_on_a_populated_handle_is_refused() {
    let mut database = pending();
    let first = database.private_data;
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { new_handle(&raw mut database, &raw mut error) },
        ADBC_STATUS_INVALID_STATE
    );
    release_error(&mut error);
    assert_eq!(database.private_data, first);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// The buffer collapses repeats, so setting the URI twice must leave exactly the later one for
/// `Init` to replay.
#[test]
fn setting_the_same_option_twice_keeps_the_later_value() {
    let mut database = pending();
    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            "spanner:///projects/p/instances/i/databases/first"
        ),
        ADBC_STATUS_OK
    );
    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            "spanner:///projects/p/instances/i/databases/second"
        ),
        ADBC_STATUS_OK
    );
    assert_eq!(
        set(&mut database, crate::OPTION_EMULATOR, "true"),
        ADBC_STATUS_OK
    );
    // Before init there is nothing to read back yet, however many sets were buffered, and
    // adbc.h gives the getters only `NOT_FOUND` to say so with.
    assert_eq!(
        get(&mut database, adbc_core::constants::ADBC_OPTION_URI).unwrap_err(),
        ADBC_STATUS_NOT_FOUND
    );
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    assert_eq!(
        get(&mut database, adbc_core::constants::ADBC_OPTION_URI).unwrap(),
        "spanner:///projects/p/instances/i/databases/second"
    );
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// A failed `Init` must keep the buffered options so the offending one can be corrected and
/// `Init` retried, rather than silently reinitializing from an empty buffer.
#[test]
fn a_failed_init_keeps_the_buffered_options_for_a_retry() {
    let mut database = pending();
    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            &format!("spanner:///{DATABASE}")
        ),
        ADBC_STATUS_OK
    );
    assert_eq!(
        set(&mut database, crate::OPTION_ENDPOINT, "http://127.0.0.1:1"),
        ADBC_STATUS_OK
    );
    // Buffered without validation; rejected when Init applies it.
    assert_eq!(
        set(&mut database, crate::OPTION_EMULATOR, "maybe"),
        ADBC_STATUS_OK
    );
    let mut error = zeroed_error();
    assert_ne!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    release_error(&mut error);

    // The buffer is last-write-wins per key, so correcting the value replaces the bad one.
    assert_eq!(
        set(&mut database, crate::OPTION_EMULATOR, "true"),
        ADBC_STATUS_OK
    );
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    assert_eq!(
        get(&mut database, adbc_core::constants::ADBC_OPTION_URI).unwrap(),
        format!("spanner:///{DATABASE}")
    );
    assert_eq!(
        get(&mut database, crate::OPTION_ENDPOINT).unwrap(),
        "http://127.0.0.1:1"
    );
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

#[test]
fn initializing_twice_is_an_invalid_state() {
    let mut database = initialized();
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_INVALID_STATE
    );
    release_error(&mut error);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

#[test]
fn a_released_handle_rejects_every_call_including_a_second_release() {
    let mut database = initialized();
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
    assert!(database.private_data.is_null());
    assert_eq!(release(&mut database), ADBC_STATUS_INVALID_STATE);
    assert_eq!(
        set(&mut database, crate::OPTION_ENDPOINT, "http://127.0.0.1:2"),
        ADBC_STATUS_INVALID_STATE
    );
    assert_eq!(
        get(&mut database, crate::OPTION_ENDPOINT).unwrap_err(),
        ADBC_STATUS_INVALID_STATE
    );
}

#[test]
fn null_handles_and_keys_are_rejected() {
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { new_handle::<AdbcDatabase>(null(), &raw mut error) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    release_error(&mut error);

    let mut database = pending();
    let value = CString::new("x").unwrap();
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { set_option_string(&raw mut database, null(), value.as_ptr(), &raw mut error) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    release_error(&mut error);

    // A null value is rejected before the handle is even consulted.
    let key = CString::new(adbc_core::constants::ADBC_OPTION_URI).unwrap();
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { set_option_string(&raw mut database, key.as_ptr(), null(), &raw mut error) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    release_error(&mut error);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// Typed getters exist, but every database option this driver has is a string, so the typed
/// ones must report that rather than silently returning a default.
#[test]
fn typed_getters_reject_string_options() {
    let mut database = initialized();
    let key = CString::new(crate::OPTION_ENDPOINT).unwrap();
    let mut value = 0_i64;
    let mut error = zeroed_error();
    assert_ne!(
        unsafe {
            get_option_int(
                &raw mut database,
                key.as_ptr(),
                &raw mut value,
                &raw mut error,
            )
        },
        ADBC_STATUS_OK
    );
    release_error(&mut error);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// Nothing about a pre-`Init` set is validated — not the key, not the value's type. The typed
/// setter's payload is retained verbatim and the live database applies its ordinary type
/// validation when `Init` replays the buffer.
#[test]
fn a_typed_set_is_validated_by_the_live_database() {
    let mut database = pending();
    let key = CString::new(adbc_core::constants::ADBC_OPTION_URI).unwrap();
    assert_eq!(
        unsafe { set_option_int(&raw mut database, key.as_ptr(), 1, null()) },
        ADBC_STATUS_OK
    );

    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    release_error(&mut error);

    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            &format!("spanner:///{DATABASE}")
        ),
        ADBC_STATUS_OK
    );
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_OK
    );
    release_error(&mut error);
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// adbc.h would rather have `SetOption` answer `ADBC_STATUS_NOT_IMPLEMENTED` for a key it does
/// not recognize at the call that carried it. This driver cannot: a database option can rewrite
/// several fields at once (`adbc.uri` expands into the individual keys), so its setter is its own
/// validator and there is no pure classifier to consult before there is a database. A typo is
/// therefore buffered like any other key, and reported when `Init` replays it through
/// `new_database_with_opts` — naming the key, so the caller can still find the mistake.
#[test]
fn a_typo_before_init_is_reported_when_init_replays_it() {
    let mut database = pending();
    assert_eq!(
        set(&mut database, "spanner.endpoin", "http://127.0.0.1:1"),
        ADBC_STATUS_OK
    );
    assert_eq!(
        set(
            &mut database,
            adbc_core::constants::ADBC_OPTION_URI,
            &format!("spanner:///{DATABASE}")
        ),
        ADBC_STATUS_OK
    );
    let mut error = zeroed_error();
    assert_eq!(
        unsafe { database_init(&raw mut database, &raw mut error) },
        ADBC_STATUS_NOT_IMPLEMENTED
    );
    let message = crate::ffi::test_support::error_message(&error)
        .expect("the refusal must name the offending key");
    release_error(&mut error);
    assert!(message.contains("spanner.endpoin"), "{message}");
    assert_eq!(release(&mut database), ADBC_STATUS_OK);
}

/// The `Init` boundary moves *when* a key is classified, not *whether* it is: before `Init` every
/// key is accepted and buffered, and the live database is the one that knows which keys it has.
/// The live setter is the oracle here — the candidate list is not.
#[test]
fn a_pre_init_set_accepts_every_key_the_live_database_classifies() {
    let mut live = initialized();
    let keys = [
        OptionDatabase::Uri,
        OptionDatabase::Username,
        OptionDatabase::Password,
        OptionDatabase::Other("spanner.no.such.option".to_owned()),
        OptionDatabase::Other(crate::OPTION_ENDPOINT.to_owned()),
        OptionDatabase::Other(crate::OPTION_EMULATOR.to_owned()),
        OptionDatabase::Other(crate::OPTION_KEYFILE.to_owned()),
        OptionDatabase::Other(crate::OPTION_KEYFILE_JSON.to_owned()),
        OptionDatabase::Other(crate::OPTION_ACCESS_TOKEN.to_owned()),
        OptionDatabase::Other(crate::OPTION_QUOTA_PROJECT.to_owned()),
        OptionDatabase::Other(crate::OPTION_IMPERSONATE_TARGET_PRINCIPAL.to_owned()),
        OptionDatabase::Other(crate::OPTION_IMPERSONATE_DELEGATES.to_owned()),
        OptionDatabase::Other(crate::OPTION_IMPERSONATE_SCOPES.to_owned()),
        OptionDatabase::Other(crate::OPTION_IMPERSONATE_LIFETIME.to_owned()),
    ];
    let mut unknown = 0;
    for key in &keys {
        let mut database = pending();
        assert_eq!(
            set(&mut database, key.as_ref(), ""),
            ADBC_STATUS_OK,
            "{}: a pre-init set buffers every key",
            key.as_ref()
        );
        assert_eq!(release(&mut database), ADBC_STATUS_OK);
        // Only the live database refuses a key outright; a bad *value* (here, `""` for a key that
        // wants a URI or a boolean) is a different, non-`NOT_IMPLEMENTED` failure.
        if set(&mut live, key.as_ref(), "") == ADBC_STATUS_NOT_IMPLEMENTED {
            unknown += 1;
        }
    }
    assert_eq!(
        unknown, 3,
        "username, password and the made-up key are the ones the live database does not know"
    );
    assert_eq!(release(&mut live), ADBC_STATUS_OK);
}

/// The exporter this layer replaced built a fresh [`SpannerDriver`] — and therefore a fresh
/// multi-threaded Tokio runtime — on *every* `AdbcDatabaseInit`, and panicked outright when the
/// runtime could not be built. [`shared_driver`] builds one behind a `OnceLock` instead, so every
/// database a host opens in this process is created by the same driver and runs on its one
/// runtime.
///
/// Every `Init` test here executes that path, but none of them observes it, so the very bug this
/// layer exists to fix would pass the whole suite. This is the assertion that would not.
/// (`every_database_a_driver_creates_shares_the_drivers_runtime` in `src/driver/tests.rs` covers
/// the other half: that one driver means one runtime for all of its databases.)
#[test]
fn every_database_is_initialized_through_one_shared_driver() {
    let before = shared_driver().unwrap();
    // Two databases, initialized independently through the real entry point.
    let mut first = initialized();
    let mut second = initialized();
    let after = shared_driver().unwrap();

    assert!(
        std::ptr::eq(before, after),
        "AdbcDatabaseInit must not replace the process-wide driver"
    );
    // A separately built driver is a different object, so the identity above is a real check and
    // not one any two `&Mutex<SpannerDriver>` would satisfy.
    let fresh = Mutex::new(SpannerDriver::try_new().unwrap());
    assert!(!std::ptr::eq(before, &fresh));

    assert_eq!(release(&mut first), ADBC_STATUS_OK);
    assert_eq!(release(&mut second), ADBC_STATUS_OK);
    // Releasing every database leaves the shared driver in place for the next one.
    assert!(std::ptr::eq(before, shared_driver().unwrap()));
}

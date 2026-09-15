use std::ffi::{CString, c_void};

use adbc_core::options::OptionValue;

use super::super::abi::{
    ADBC_STATUS_INVALID_ARGUMENT, ADBC_STATUS_INVALID_STATE, ADBC_STATUS_NOT_FOUND, ADBC_STATUS_OK,
};
use super::super::handle::dispatch;
use super::super::options::{
    borrow, get_option_bytes, get_option_double, get_option_int, get_option_string, new_handle,
    release_handle, set_option_bytes, set_option_double, set_option_int, set_option_string,
};
use super::*;
use crate::ffi::test_support::{error_message, null, release_error, zeroed_error};

/// A handle in the state `AdbcConnectionNew` leaves it in: allocated, options buffered, not
/// yet initialized. Everything below can be exercised from here without touching the network,
/// because argument validation and the not-initialized check both come first.
fn pending() -> AdbcConnection {
    let mut connection = AdbcConnection {
        private_data: null(),
        private_driver: null(),
    };
    assert_eq!(
        unsafe { new_handle(&raw mut connection, null()) },
        ADBC_STATUS_OK
    );
    assert!(!connection.private_data.is_null());
    connection
}

fn drop_pending(connection: &mut AdbcConnection) {
    assert_eq!(
        unsafe { release_handle(&raw mut *connection, null()) },
        ADBC_STATUS_OK
    );
}

/// Copy the buffered options out of a not-yet-initialized handle, to assert on what the
/// setters recorded.
fn buffered(connection: &mut AdbcConnection) -> Vec<(OptionConnection, OptionValue)> {
    let mut state =
        unsafe { super::super::handle::lock_state::<State>(connection.private_data, KIND) }
            .unwrap();
    match &mut *state {
        State::Pending(options) => options.entries.clone(),
        State::Ready(_) => panic!("expected a pending connection"),
    }
}

#[test]
fn a_null_handle_is_a_bad_argument_not_a_crash() {
    assert_eq!(
        unsafe { new_handle::<AdbcConnection>(null(), null()) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { release_handle::<AdbcConnection>(null(), null()) },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    // A handle whose `private_data` was never populated is not a bad pointer, it is an object
    // used out of order, so it reports InvalidState instead.
    let mut connection = AdbcConnection {
        private_data: null(),
        private_driver: null(),
    };
    assert_eq!(
        unsafe { connection_commit(&raw mut connection, null()) },
        ADBC_STATUS_INVALID_STATE
    );
}

#[test]
fn releasing_twice_reports_invalid_state_rather_than_double_freeing() {
    let mut connection = pending();
    assert_eq!(
        unsafe { release_handle(&raw mut connection, null()) },
        ADBC_STATUS_OK
    );
    assert!(connection.private_data.is_null());
    assert_eq!(
        unsafe { release_handle(&raw mut connection, null()) },
        ADBC_STATUS_INVALID_STATE
    );
}

#[test]
fn options_set_before_init_are_buffered_in_order_with_last_write_winning() {
    let mut connection = pending();
    // Four keys, one per typed setter. Their *values* are deliberately mistyped: the buffer
    // records what it was handed, and all validation — of the key as well as the value — stays
    // deferred to `Init`, which replays the buffer through the driver's own setter.
    let first = CString::new(adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT).unwrap();
    let second = CString::new(crate::OPTION_RETRY_MAX_ATTEMPTS).unwrap();
    let third = CString::new(crate::OPTION_READ_STALENESS).unwrap();
    let fourth = CString::new(crate::OPTION_TRANSACTION_TAG).unwrap();
    let enabled = CString::new("true").unwrap();
    let disabled = CString::new("false").unwrap();

    for value in [&enabled, &disabled] {
        assert_eq!(
            unsafe {
                set_option_string(&raw mut connection, first.as_ptr(), value.as_ptr(), null())
            },
            ADBC_STATUS_OK
        );
    }
    assert_eq!(
        unsafe { set_option_int(&raw mut connection, second.as_ptr(), 4096, null()) },
        ADBC_STATUS_OK
    );
    assert_eq!(
        unsafe { set_option_double(&raw mut connection, third.as_ptr(), 0.5, null()) },
        ADBC_STATUS_OK
    );
    assert_eq!(
        unsafe {
            set_option_bytes(
                &raw mut connection,
                fourth.as_ptr(),
                b"xy".as_ptr(),
                2,
                null(),
            )
        },
        ADBC_STATUS_OK
    );

    let entries = buffered(&mut connection);
    assert_eq!(entries.len(), 4, "the repeated key must not add an entry");
    assert_eq!(entries[0].0, OptionConnection::AutoCommit);
    // `OptionValue` is not `PartialEq`, so the payloads are matched structurally.
    assert!(matches!(&entries[0].1, OptionValue::String(value) if value == "false"));
    assert!(matches!(entries[1].1, OptionValue::Int(4096)));
    assert!(matches!(entries[2].1, OptionValue::Double(value) if value == 0.5));
    assert!(matches!(&entries[3].1, OptionValue::Bytes(value) if value == b"xy"));

    drop_pending(&mut connection);
}

#[test]
fn setters_reject_a_null_or_non_utf8_key_or_value() {
    let mut connection = pending();
    let key = CString::new(adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT).unwrap();
    let value = CString::new("true").unwrap();
    // 0xFF cannot start a UTF-8 sequence, so this is a NUL-terminated non-UTF-8 string.
    let invalid = [0xFF_u8, 0x00];
    let invalid = invalid.as_ptr().cast::<c_char>();

    for status in [
        unsafe { set_option_string(&raw mut connection, null(), value.as_ptr(), null()) },
        unsafe { set_option_string(&raw mut connection, key.as_ptr(), null(), null()) },
        unsafe { set_option_string(&raw mut connection, invalid, value.as_ptr(), null()) },
        unsafe { set_option_int(&raw mut connection, invalid, 1, null()) },
        unsafe { set_option_bytes(&raw mut connection, key.as_ptr(), null(), 3, null()) },
    ] {
        assert_eq!(status, ADBC_STATUS_INVALID_ARGUMENT);
    }

    assert!(
        buffered(&mut connection).is_empty(),
        "a rejected setter must not record anything"
    );
    drop_pending(&mut connection);
}

/// adbc.h gives the four option getters one documented failure, `ADBC_STATUS_NOT_FOUND`, and no
/// other. A connection that has only buffered options has no value to report, so it reports the
/// key as absent rather than the connection as unusable.
#[test]
fn getters_report_a_pre_init_connection_option_as_not_found() {
    let mut connection = pending();
    let key = CString::new(adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT).unwrap();
    let mut length = 0_usize;
    let mut integer = 0_i64;
    let mut double = 0.0_f64;

    for status in [
        unsafe {
            get_option_string(
                &raw mut connection,
                key.as_ptr(),
                null(),
                &raw mut length,
                null(),
            )
        },
        unsafe {
            get_option_bytes(
                &raw mut connection,
                key.as_ptr(),
                null(),
                &raw mut length,
                null(),
            )
        },
        unsafe { get_option_int(&raw mut connection, key.as_ptr(), &raw mut integer, null()) },
        unsafe { get_option_double(&raw mut connection, key.as_ptr(), &raw mut double, null()) },
    ] {
        assert_eq!(status, ADBC_STATUS_NOT_FOUND);
    }
    // A bad key is rejected before the state is consulted, so it wins over the missing option.
    assert_eq!(
        unsafe { get_option_int(&raw mut connection, null(), &raw mut integer, null()) },
        ADBC_STATUS_INVALID_ARGUMENT
    );

    drop_pending(&mut connection);
}

#[test]
fn transaction_cancel_and_init_entry_points_check_their_state() {
    let mut connection = pending();
    for status in [
        unsafe { connection_commit(&raw mut connection, null()) },
        unsafe { connection_rollback(&raw mut connection, null()) },
        unsafe { connection_cancel(&raw mut connection, null()) },
        // Init reports an unusable (null) database rather than taking it on faith.
        unsafe { connection_init(&raw mut connection, null(), null()) },
    ] {
        assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    }
    drop_pending(&mut connection);
}

#[test]
fn metadata_streams_require_an_initialized_connection() {
    let mut connection = pending();
    let mut out = std::mem::MaybeUninit::<FFI_ArrowArrayStream>::zeroed();
    for status in [
        unsafe { connection_get_info(&raw mut connection, null(), 0, out.as_mut_ptr(), null()) },
        unsafe { connection_get_table_types(&raw mut connection, out.as_mut_ptr(), null()) },
        unsafe { connection_get_statistic_names(&raw mut connection, out.as_mut_ptr(), null()) },
        unsafe {
            connection_get_statistics(
                &raw mut connection,
                null(),
                null(),
                null(),
                1,
                out.as_mut_ptr(),
                null(),
            )
        },
        unsafe {
            connection_read_partition(
                &raw mut connection,
                b"plan".as_ptr(),
                4,
                out.as_mut_ptr(),
                null(),
            )
        },
    ] {
        assert_eq!(status, ADBC_STATUS_INVALID_STATE);
    }

    // A null partition with a length is a bad argument, caught before the state check.
    assert_eq!(
        unsafe {
            connection_read_partition(&raw mut connection, null(), 7, out.as_mut_ptr(), null())
        },
        ADBC_STATUS_INVALID_ARGUMENT
    );
    drop_pending(&mut connection);
}

#[test]
fn metadata_outputs_are_checked_before_initialization_or_network_work() {
    type MetadataCall = fn(*mut AdbcConnection, *mut AdbcError) -> AdbcStatusCode;

    let mut connection = pending();
    let mut error = zeroed_error();
    let connection_ptr = &raw mut connection;
    let error_ptr = &raw mut error;
    let calls: [(&str, MetadataCall); 7] = [
        ("out", |connection, error| unsafe {
            connection_get_info(connection, null(), 0, null(), error)
        }),
        ("out", |connection, error| unsafe {
            connection_get_objects(
                connection,
                0,
                null(),
                null(),
                null(),
                null(),
                null(),
                null(),
                error,
            )
        }),
        ("schema", |connection, error| unsafe {
            connection_get_table_schema(
                connection,
                null(),
                null(),
                c"Singers".as_ptr(),
                null(),
                error,
            )
        }),
        ("out", |connection, error| unsafe {
            connection_get_table_types(connection, null(), error)
        }),
        ("out", |connection, error| unsafe {
            connection_get_statistics(connection, null(), null(), null(), 1, null(), error)
        }),
        ("out", |connection, error| unsafe {
            connection_get_statistic_names(connection, null(), error)
        }),
        ("out", |connection, error| unsafe {
            connection_read_partition(connection, null(), 0, null(), error)
        }),
    ];
    for (output, call) in calls {
        assert_eq!(
            call(connection_ptr, error_ptr),
            ADBC_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            error_message(&error).unwrap(),
            format!("{output} must not be null")
        );
        release_error(&mut error);
    }

    // The rejected calls preserve the connection's pre-init state and option buffer.
    assert!(buffered(&mut connection).is_empty());
    drop_pending(&mut connection);
}

#[test]
fn get_objects_accepts_every_documented_depth_and_rejects_the_rest() {
    let mut connection = pending();
    let mut out = std::mem::MaybeUninit::<FFI_ArrowArrayStream>::zeroed();
    let types = [
        CString::new("TABLE").unwrap(),
        CString::new("VIEW").unwrap(),
    ];
    let type_list = [types[0].as_ptr(), types[1].as_ptr(), null()];

    let mut get_objects = |depth: c_int, error: *mut AdbcError| unsafe {
        connection_get_objects(
            &raw mut connection,
            depth,
            null(),
            null(),
            null(),
            type_list.as_ptr(),
            null(),
            out.as_mut_ptr(),
            error,
        )
    };

    // 0..=3 are ADBC_OBJECT_DEPTH_{ALL,CATALOGS,DB_SCHEMAS,TABLES}; COLUMNS is an alias of ALL,
    // so 4 is not a depth at all. Getting past the depth check lands on the state check.
    for depth in 0..=3 {
        assert_eq!(get_objects(depth, null()), ADBC_STATUS_INVALID_STATE);
    }
    for depth in [-1, 4, 99] {
        assert_eq!(get_objects(depth, null()), ADBC_STATUS_INVALID_ARGUMENT);
    }

    // The advice must not send the caller back with a depth that lands here again: 4 is one of
    // the values rejected just above, because `ADBC_OBJECT_DEPTH_COLUMNS` is `ADBC_OBJECT_DEPTH_ALL`.
    let mut error = zeroed_error();
    assert_eq!(get_objects(4, &raw mut error), ADBC_STATUS_INVALID_ARGUMENT);
    let message = error_message(&error).expect("the rejection carries a message");
    release_error(&mut error);
    assert!(!message.contains("4 (columns)"), "{message}");
    assert!(message.contains("0 (all, including columns)"), "{message}");

    drop_pending(&mut connection);
}

#[test]
fn get_table_schema_requires_a_table_name() {
    let mut connection = pending();
    let mut schema = std::mem::MaybeUninit::<FFI_ArrowSchema>::zeroed();
    let table = CString::new("Singers").unwrap();
    let invalid = [0xFF_u8, 0x00];

    let mut get_table_schema = |db_schema: *const c_char, table_name: *const c_char| unsafe {
        connection_get_table_schema(
            &raw mut connection,
            null(),
            db_schema,
            table_name,
            schema.as_mut_ptr(),
            null(),
        )
    };

    assert_eq!(
        get_table_schema(null(), null()),
        ADBC_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        get_table_schema(invalid.as_ptr().cast::<c_char>(), table.as_ptr()),
        ADBC_STATUS_INVALID_ARGUMENT,
        "a non-UTF-8 db_schema is still a bad argument even though the field is optional"
    );
    // With every argument acceptable, the uninitialized connection is what stops the call.
    assert_eq!(
        get_table_schema(null(), table.as_ptr()),
        ADBC_STATUS_INVALID_STATE
    );

    drop_pending(&mut connection);
}

#[test]
fn borrow_rejects_an_uninitialized_or_released_connection() {
    let mut connection = pending();
    // The lock is available, but the state behind it is still pending.
    let mut state = unsafe { borrow(&raw mut connection) }.unwrap();
    assert_eq!(state.ready(KIND).unwrap_err().status, Status::InvalidState);
    drop(state);

    drop_pending(&mut connection);
    let error = unsafe { borrow(&raw mut connection) }
        .map(|_| ())
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_eq!(
        unsafe { borrow::<AdbcConnection>(null()) }
            .map(|_| ())
            .unwrap_err()
            .status,
        Status::InvalidState
    );
}

/// Cancel must not queue behind the connection's dispatch turn: while a worker thread holds
/// it, `connection_cancel` still returns promptly (here `InvalidState`, since this pending
/// connection has no installed handle) instead of blocking or misreporting a panic.
#[test]
fn cancel_does_not_wait_for_the_connection_dispatch_turn() {
    let mut connection = pending();
    let (hold_sender, hold_receiver) = std::sync::mpsc::channel::<()>();
    let (started_sender, started_receiver) = std::sync::mpsc::channel::<()>();

    let address = connection.private_data as usize;
    let worker = std::thread::spawn(move || unsafe {
        dispatch::<State, _>(address as *mut c_void, KIND, null(), |_| {
            started_sender.send(()).expect("test channel");
            hold_receiver.recv().expect("test channel");
            Ok(())
        })
    });
    started_receiver.recv().unwrap();

    assert_eq!(
        unsafe { connection_cancel(&raw mut connection, null()) },
        ADBC_STATUS_INVALID_STATE
    );

    hold_sender.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), ADBC_STATUS_OK);
    drop_pending(&mut connection);
}

/// The pre-`Init` half of adbc.h's `SetOption` contract, as this driver answers it: there is no
/// classifier to consult before `Init`, so *every* key — standard, `spanner.*`, and outright
/// unknown — is accepted and buffered verbatim. The rejection an unknown key earns comes later,
/// from the driver's own setter, when `Init` replays the buffer through
/// `new_connection_with_opts`.
#[test]
fn every_key_is_buffered_before_init_whether_the_driver_knows_it_or_not() {
    let mut connection = pending();
    let keys = [
        OptionConnection::AutoCommit,
        OptionConnection::CurrentCatalog,
        OptionConnection::CurrentSchema,
        OptionConnection::ReadOnly,
        OptionConnection::IsolationLevel,
        OptionConnection::Other("adbc.connection.no_such_option".to_owned()),
        OptionConnection::Other("spanner.read.stalenes".to_owned()),
        OptionConnection::Other(crate::OPTION_READ_STALENESS.to_owned()),
        OptionConnection::Other(crate::OPTION_REQUEST_PRIORITY.to_owned()),
        OptionConnection::Other(crate::OPTION_TRANSACTION_TAG.to_owned()),
    ];
    for key in &keys {
        let ffi_key = CString::new(key.as_ref()).unwrap();
        let value = CString::new("true").unwrap();
        let status = unsafe {
            set_option_string(
                &raw mut connection,
                ffi_key.as_ptr(),
                value.as_ptr(),
                null(),
            )
        };
        assert_eq!(status, ADBC_STATUS_OK, "{key:?}: {status}");
    }

    assert_eq!(buffered(&mut connection).len(), keys.len());
    drop_pending(&mut connection);
}

/// A Spanner database has no switchable current catalog or schema — both are fixed at `""`, so
/// the live setter takes `""` as a conformant no-op and refuses anything else. Before `Init`
/// there is no live connection to ask, so both keys are buffered like every other key and the
/// refusal of a non-empty value waits for the replay at `Init`. A *get*, meanwhile, has no value
/// to report yet, and adbc.h licenses only `NOT_FOUND` to say so with.
#[test]
fn the_current_namespace_keys_buffer_before_init_and_read_back_as_not_found() {
    let mut connection = pending();
    for key in [
        OptionConnection::CurrentCatalog,
        OptionConnection::CurrentSchema,
    ] {
        let name = key.as_ref();
        let ffi_key = CString::new(name).unwrap();
        let value = CString::new("changed").unwrap();
        let mut error = zeroed_error();
        assert_eq!(
            unsafe {
                set_option_string(
                    &raw mut connection,
                    ffi_key.as_ptr(),
                    value.as_ptr(),
                    &raw mut error,
                )
            },
            ADBC_STATUS_OK,
            "{name}"
        );
        assert_eq!(error_message(&error), None, "{name}");
        release_error(&mut error);

        let mut length = 0_usize;
        let mut error = zeroed_error();
        assert_eq!(
            unsafe {
                get_option_string(
                    &raw mut connection,
                    ffi_key.as_ptr(),
                    null(),
                    &raw mut length,
                    &raw mut error,
                )
            },
            ADBC_STATUS_NOT_FOUND,
            "{name}"
        );
        assert_eq!(
            error_message(&error),
            Some(format!(
                "Spanner connection option {name} cannot be read before the connection is \
                 initialized; initialize it first"
            )),
            "{name}"
        );
        release_error(&mut error);
    }

    assert_eq!(
        buffered(&mut connection).len(),
        2,
        "both keys were buffered, to be validated when Init replays them"
    );
    drop_pending(&mut connection);
}

/// The connection counterpart of the statement's `every_entry_point_refuses_a_released_handle`.
/// A released handle is a null `private_data`, which every entry point must recognize before it
/// touches anything behind it; one that forgot would dereference null instead of reporting.
#[test]
fn every_entry_point_refuses_a_released_handle() {
    let mut handle = AdbcConnection {
        private_data: null(),
        private_driver: null(),
    };
    let c = &raw mut handle;
    let name = CString::new(adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT).unwrap();
    let (mut length, mut integer, mut double) = (0_usize, 0_i64, 0.0_f64);
    let mut database = AdbcDatabase {
        private_data: null(),
        private_driver: null(),
    };

    for (label, status) in [
        ("init", unsafe {
            connection_init(c, &raw mut database, null())
        }),
        ("commit", unsafe { connection_commit(c, null()) }),
        ("rollback", unsafe { connection_rollback(c, null()) }),
        ("cancel", unsafe { connection_cancel(c, null()) }),
        ("get_info", unsafe {
            connection_get_info(c, null(), 0, null(), null())
        }),
        ("get_objects", unsafe {
            connection_get_objects(c, 0, null(), null(), null(), null(), null(), null(), null())
        }),
        ("get_table_schema", unsafe {
            connection_get_table_schema(c, null(), null(), name.as_ptr(), null(), null())
        }),
        ("get_table_types", unsafe {
            connection_get_table_types(c, null(), null())
        }),
        ("get_statistics", unsafe {
            connection_get_statistics(c, null(), null(), null(), 0, null(), null())
        }),
        ("get_statistic_names", unsafe {
            connection_get_statistic_names(c, null(), null())
        }),
        ("read_partition", unsafe {
            connection_read_partition(c, b"x".as_ptr(), 1, null(), null())
        }),
        ("set_option", unsafe {
            set_option_string(c, name.as_ptr(), name.as_ptr(), null())
        }),
        ("set_option_int", unsafe {
            set_option_int(c, name.as_ptr(), 1, null())
        }),
        ("set_option_double", unsafe {
            set_option_double(c, name.as_ptr(), 1.0, null())
        }),
        ("set_option_bytes", unsafe {
            set_option_bytes(c, name.as_ptr(), b"x".as_ptr(), 1, null())
        }),
        ("get_option", unsafe {
            get_option_string(c, name.as_ptr(), null(), &raw mut length, null())
        }),
        ("get_option_bytes", unsafe {
            get_option_bytes(c, name.as_ptr(), null(), &raw mut length, null())
        }),
        ("get_option_int", unsafe {
            get_option_int(c, name.as_ptr(), &raw mut integer, null())
        }),
        ("get_option_double", unsafe {
            get_option_double(c, name.as_ptr(), &raw mut double, null())
        }),
        ("release", unsafe { release_handle(c, null()) }),
    ] {
        assert_eq!(status, ADBC_STATUS_INVALID_STATE, "{label}");
    }
}

/// `AdbcStatementNew` reaches into the connection without dispatching on a statement it does not
/// have yet, so it has its own state check to get wrong. A connection that exists but was never
/// initialized has no driver object to make a statement from.
#[test]
fn creating_a_statement_needs_an_initialized_connection() {
    let mut connection = pending();
    let mut statement = crate::ffi::abi::AdbcStatement {
        private_data: null(),
        private_driver: null(),
    };
    let mut error = zeroed_error();
    assert_eq!(
        unsafe {
            crate::ffi::statement::statement_new(
                &raw mut connection,
                &raw mut statement,
                &raw mut error,
            )
        },
        ADBC_STATUS_INVALID_STATE
    );
    let message = error_message(&error).expect("the refusal must say why");
    release_error(&mut error);
    assert!(message.contains("connection"), "{message}");
    assert!(statement.private_data.is_null(), "nothing was allocated");
    drop_pending(&mut connection);
}

/// `AdbcConnectionInit` locks the database it is given without dispatching on it, so the
/// database's own state has to be checked there. A database that was allocated but never
/// initialized is a different failure from a null one, which short-circuits earlier.
#[test]
fn init_against_an_allocated_but_uninitialized_database_is_refused() {
    let mut connection = pending();
    let mut database = AdbcDatabase {
        private_data: null(),
        private_driver: null(),
    };
    assert_eq!(
        unsafe { new_handle(&raw mut database, null()) },
        ADBC_STATUS_OK
    );

    let mut error = zeroed_error();
    assert_eq!(
        unsafe { connection_init(&raw mut connection, &raw mut database, &raw mut error) },
        ADBC_STATUS_INVALID_STATE
    );
    let message = error_message(&error).expect("the refusal must say why");
    release_error(&mut error);
    assert!(message.contains("database"), "{message}");

    // The connection kept its pre-init state, so a retry after the database is initialized is
    // still possible.
    assert!(matches!(
        &*unsafe { super::super::handle::lock_state::<State>(connection.private_data, KIND) }
            .unwrap(),
        State::Pending(_)
    ));

    assert_eq!(
        unsafe { release_handle(&raw mut database, null()) },
        ADBC_STATUS_OK
    );
    drop_pending(&mut connection);
}

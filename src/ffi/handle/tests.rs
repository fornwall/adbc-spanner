use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};

use super::*;
use crate::ffi::test_support::null;
use adbc_core::options::OptionDatabase;

/// A cancel handle that records whether it fired, standing in for the driver's
/// `OperationCancelHandle`.
struct RecordingCancel(Arc<AtomicBool>);

impl CancelHandle for RecordingCancel {
    fn try_cancel(&self) -> Result<()> {
        self.0.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn option_buffer_keeps_order_and_collapses_repeats() {
    let mut buffer = OptionBuffer {
        entries: Vec::new(),
    };
    buffer.set(OptionDatabase::Uri, "first".into());
    buffer.set(OptionDatabase::Other("x".into()), "y".into());
    buffer.set(OptionDatabase::Uri, "second".into());

    let entries = buffer.entries.clone();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, OptionDatabase::Uri);
    assert!(
        matches!(&entries[0].1, OptionValue::String(value) if value == "second"),
        "the later write must win"
    );
    assert_eq!(entries[1].0, OptionDatabase::Other("x".into()));
}

#[test]
fn a_pending_object_is_not_ready() {
    let mut staged: Staged<OptionDatabase, u8> = Staged::new();
    let error = staged.ready("database").unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    staged = Staged::Ready(3);
    assert_eq!(*staged.ready("database").unwrap(), 3);
}

#[test]
fn a_panicking_call_poisons_only_that_object() {
    let first = Exported::into_private_data(0_u32);
    let second = Exported::into_private_data(0_u32);

    let status =
        unsafe { dispatch::<u32, _>(first, "statement", null(), |_| panic!("inside the driver")) };
    assert_eq!(status, AdbcStatusCode::from(Status::Internal));

    // The panicking object refuses further work...
    let status = unsafe {
        dispatch::<u32, _>(first, "statement", null(), |state| {
            *state = 1;
            Ok(())
        })
    };
    assert_eq!(status, AdbcStatusCode::from(Status::Internal));

    // ...while an unrelated object is unaffected. Under the framework's global flag this
    // second call would also have failed.
    let status = unsafe {
        dispatch::<u32, _>(second, "statement", null(), |state| {
            *state = 1;
            Ok(())
        })
    };
    assert_eq!(status, ADBC_STATUS_OK);

    let mut first_slot = first;
    let mut second_slot = second;
    unsafe { release::<u32>(&mut first_slot, "statement", null()) };
    unsafe { release::<u32>(&mut second_slot, "statement", null()) };
}

/// A cancel handle that parks inside `try_cancel` until the test releases it, so a `Release` can
/// provably land while a cancel is still running.
struct BlockingCancel {
    entered: Sender<()>,
    resume: StdMutex<Receiver<()>>,
}

impl CancelHandle for BlockingCancel {
    fn try_cancel(&self) -> Result<()> {
        self.entered.send(()).expect("test channel");
        self.resume
            .lock()
            .expect("test mutex")
            .recv()
            .expect("test channel");
        Ok(())
    }
}

/// `Cancel` takes a reference of its own, so the object it is working on outlives a `Release`
/// that lands mid-cancel. With a plain box this freed the mutex the cancel was holding a guard on.
#[test]
fn a_release_during_a_cancel_does_not_free_the_object_under_it() {
    let (entered_sender, entered_receiver) = channel::<()>();
    let (resume_sender, resume_receiver) = channel::<()>();
    let slot = Exported::into_private_data_with_cancel(
        0_u32,
        Some(Box::new(BlockingCancel {
            entered: entered_sender,
            resume: StdMutex::new(resume_receiver),
        })),
    );

    let address = slot as usize;
    let canceller = std::thread::spawn(move || unsafe {
        cancel::<u32>(address as *mut c_void, "statement", null())
    });
    // The cancel is provably inside `try_cancel`, holding a guard on the object's cancel mutex.
    entered_receiver.recv().expect("test channel");

    let mut slot = slot;
    assert_eq!(
        unsafe { release::<u32>(&mut slot, "statement", null()) },
        ADBC_STATUS_OK
    );
    assert!(slot.is_null());

    // The in-flight cancel still owns a live object, and finishing it is what frees it.
    resume_sender.send(()).expect("test channel");
    assert_eq!(canceller.join().expect("cancel thread"), ADBC_STATUS_OK);

    // The handle is spent either way: the slot is null, so a later call reports rather than
    // reaching the object the cancel has since dropped.
    assert_eq!(
        unsafe { cancel::<u32>(slot, "statement", null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );
}

#[test]
fn returning_an_error_does_not_poison() {
    let slot = Exported::into_private_data(0_u32);
    let status = unsafe {
        dispatch::<u32, _>(slot, "statement", null(), |_| {
            Err(Error::with_message_and_status("nope", Status::NotFound))
        })
    };
    assert_eq!(status, AdbcStatusCode::from(Status::NotFound));

    let status = unsafe {
        dispatch::<u32, _>(slot, "statement", null(), |state| {
            *state = 5;
            Ok(())
        })
    };
    assert_eq!(status, ADBC_STATUS_OK);

    let mut slot = slot;
    unsafe { release::<u32>(&mut slot, "statement", null()) };
}

#[test]
fn dispatching_on_a_released_handle_is_an_invalid_state() {
    let mut slot = Exported::into_private_data(0_u32);
    assert_eq!(
        unsafe { release::<u32>(&mut slot, "database", null()) },
        ADBC_STATUS_OK
    );
    assert!(slot.is_null());
    // A second release must report, not double free.
    assert_eq!(
        unsafe { release::<u32>(&mut slot, "database", null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );
    assert_eq!(
        unsafe { dispatch::<u32, _>(slot, "database", null(), |_| Ok(())) },
        AdbcStatusCode::from(Status::InvalidState)
    );
}

/// The primary Cancel scenario from adbc.h: one thread is blocked inside an operation, and
/// another thread cancels it. Cancel must neither wait for the dispatch turn nor misreport the
/// busy object.
#[test]
fn cancel_does_not_wait_for_a_held_dispatch_turn() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let slot = Exported::into_private_data_with_cancel(
        0_u32,
        Some(Box::new(RecordingCancel(cancelled.clone()))),
    );
    let (hold_sender, hold_receiver) = channel::<()>();
    let (started_sender, started_receiver) = channel::<()>();

    let address = slot as usize;
    let worker = std::thread::spawn(move || unsafe {
        dispatch::<u32, _>(address as *mut c_void, "statement", null(), |_| {
            started_sender.send(()).expect("test channel");
            hold_receiver.recv().expect("test channel");
            Ok(())
        })
    });
    started_receiver.recv().unwrap();

    // The dispatch turn is provably held; cancel must still complete immediately.
    let status = unsafe { cancel::<u32>(slot, "statement", null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));

    hold_sender.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), ADBC_STATUS_OK);

    let mut slot = slot;
    unsafe { release::<u32>(&mut slot, "statement", null()) };
}

/// A call arriving while another holds the dispatch turn waits for it and then succeeds.
/// Before the state moved behind a lock, the second call either raced (undefined behavior) or
/// was refused with "unusable after an earlier panic".
#[test]
fn a_call_blocked_behind_a_dispatch_turn_completes_instead_of_failing() {
    let slot = Exported::into_private_data(7_u32);
    let (hold_sender, hold_receiver) = channel::<()>();
    let (started_sender, started_receiver) = channel::<()>();

    let address = slot as usize;
    let first = std::thread::spawn(move || unsafe {
        dispatch::<u32, _>(address as *mut c_void, "statement", null(), |_| {
            started_sender.send(()).expect("test channel");
            hold_receiver.recv().expect("test channel");
            Ok(())
        })
    });
    started_receiver.recv().unwrap();

    // Issued while the turn is definitely held, mimicking GetOption during a slow query.
    let second = std::thread::spawn(move || unsafe {
        dispatch::<u32, _>(address as *mut c_void, "statement", null(), |state| {
            assert_eq!(*state, 7);
            Ok(())
        })
    });

    hold_sender.send(()).unwrap();
    assert_eq!(first.join().unwrap(), ADBC_STATUS_OK);
    assert_eq!(second.join().unwrap(), ADBC_STATUS_OK);

    let mut slot = slot;
    unsafe { release::<u32>(&mut slot, "statement", null()) };
}

/// With the poison flag now atomic and the state behind a lock, overlapping ordinary calls
/// serialize; under the previous `&mut` scheme this loop was a data race.
#[test]
fn concurrent_dispatches_serialize_instead_of_racing() {
    const THREADS: usize = 8;
    const CALLS: u32 = 100;

    let slot = Exported::into_private_data(0_u32);
    let address = slot as usize;
    let workers: Vec<_> = (0..THREADS)
        .map(|_| {
            std::thread::spawn(move || {
                for _ in 0..CALLS {
                    let status = unsafe {
                        dispatch::<u32, _>(address as *mut c_void, "statement", null(), |state| {
                            *state += 1;
                            Ok(())
                        })
                    };
                    assert_eq!(status, ADBC_STATUS_OK);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }

    let total = *unsafe { lock_state::<u32>(slot, "statement") }.unwrap();
    assert_eq!(total, u32::try_from(THREADS).unwrap() * CALLS);

    let mut slot = slot;
    unsafe { release::<u32>(&mut slot, "statement", null()) };
}

#[test]
fn cancel_still_works_after_a_panic_poisoned_the_object() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let slot = Exported::into_private_data_with_cancel(
        0_u32,
        Some(Box::new(RecordingCancel(cancelled.clone()))),
    );
    let status =
        unsafe { dispatch::<u32, _>(slot, "statement", null(), |_| panic!("inside the driver")) };
    assert_eq!(status, AdbcStatusCode::from(Status::Internal));

    // Dispatches are refused, but cancelling what the panicked call left behind still works.
    let status = unsafe { cancel::<u32>(slot, "statement", null()) };
    assert_eq!(status, ADBC_STATUS_OK);
    assert!(cancelled.load(Ordering::SeqCst));

    let mut slot = slot;
    unsafe { release::<u32>(&mut slot, "statement", null()) };
}

#[test]
fn cancel_without_an_installed_handle_is_an_invalid_state() {
    // Null and released slots report as uninitialized...
    assert_eq!(
        unsafe { cancel::<u32>(null(), "connection", null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );
    // ...and a live object whose init has not installed a handle yet reports likewise.
    let slot = Exported::into_private_data(0_u32);
    assert_eq!(
        unsafe { cancel::<u32>(slot, "connection", null()) },
        AdbcStatusCode::from(Status::InvalidState)
    );

    let cancelled = Arc::new(AtomicBool::new(false));
    unsafe {
        install_cancel_handle::<u32>(slot, Box::new(RecordingCancel(cancelled.clone())));
    }
    assert_eq!(
        unsafe { cancel::<u32>(slot, "connection", null()) },
        ADBC_STATUS_OK
    );
    assert!(cancelled.load(Ordering::SeqCst));

    let mut slot = slot;
    unsafe { release::<u32>(&mut slot, "connection", null()) };
}

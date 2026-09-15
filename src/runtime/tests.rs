use super::*;
use std::time::Duration;

#[test]
fn uncancelled_operation_completes() {
    let runtime = new_runtime().unwrap();
    let cancel = CancelSignal::new();
    let result: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(42) });
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn signal_from_another_thread_cancels_the_operation() {
    let runtime = new_runtime().unwrap();
    let cancel = CancelSignal::new();
    let signaller = cancel.clone();
    // Fire the signal well after the operation has started and registered its waiter.
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        signaller.signal();
    });
    // Without a signal this future would block for far longer than the test.
    let result: Result<()> = block_on_cancellable(&runtime, &cancel, async {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(())
    });
    assert_eq!(result.unwrap_err().status, Status::Cancelled);
}

// The signal is sticky: a cancel that lands while *no* operation is parked (for a streamed
// result, between two chunk fetches) still cancels the next operation on the same signal.
#[test]
fn signal_between_operations_cancels_the_next_one() {
    let runtime = new_runtime().unwrap();
    let cancel = CancelSignal::new();
    let first: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(1) });
    assert_eq!(first.unwrap(), 1);
    cancel.signal(); // nothing in flight — must latch, not evaporate
    let second: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(2) });
    assert_eq!(second.unwrap_err().status, Status::Cancelled);
    // And it stays latched for every subsequent fetch of the cancelled stream.
    let third: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(3) });
    assert_eq!(third.unwrap_err().status, Status::Cancelled);
}

// A stale cancel does not leak into a new operation: entry points mint a fresh signal via
// `CancelSlot::begin_operation`, so the slot's current signal starts uncancelled.
#[test]
fn begin_operation_shields_a_new_operation_from_a_stale_cancel() {
    let runtime = new_runtime().unwrap();
    let slot = CancelSlot::new();
    slot.signal(); // cancel with nothing meaningful in flight
    let cancel = slot.begin_operation();
    let result: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(7) });
    assert_eq!(result.unwrap(), 7);
}

// The converse: beginning a new operation must not *un-cancel* an earlier operation's signal —
// a streamed reader holding it keeps failing with Cancelled. (A shared resettable signal would
// let a new operation silently revive a cancelled stream, or let it end cleanly truncated.)
#[test]
fn begin_operation_does_not_uncancel_an_earlier_operations_signal() {
    let runtime = new_runtime().unwrap();
    let slot = CancelSlot::new();
    let old = slot.begin_operation(); // what a streamed reader would hold on to
    slot.signal(); // cancel() aimed at that operation
    let fresh = slot.begin_operation(); // the owner's next operation
    let revived: Result<i32> = block_on_cancellable(&runtime, &old, async { Ok(1) });
    assert_eq!(revived.unwrap_err().status, Status::Cancelled);
    let new_op: Result<i32> = block_on_cancellable(&runtime, &fresh, async { Ok(2) });
    assert_eq!(new_op.unwrap(), 2);
    // A cancel now targets the current (fresh) signal, not the superseded one.
    slot.signal();
    let cancelled: Result<i32> = block_on_cancellable(&runtime, &slot.current(), async { Ok(3) });
    assert_eq!(cancelled.unwrap_err().status, Status::Cancelled);
}

// The ADBC cancel handle always reports success, even with nothing in flight — adbc.h asks for
// `InvalidState` there, but the driver cannot tell idle from a live streamed reader (see
// `SlotCancelHandle`). The latch such a cancel sets is superseded by the next operation.
#[test]
fn cancel_handle_reports_ok_when_nothing_is_in_flight() {
    let runtime = new_runtime().unwrap();
    let slot = Arc::new(CancelSlot::new());
    let handle = SlotCancelHandle::new(slot.clone());
    assert!(
        handle.try_cancel().is_ok(),
        "an idle cancel still reports Ok"
    );
    let cancel = slot.begin_operation();
    let result: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(5) });
    assert_eq!(
        result.unwrap(),
        5,
        "the idle cancel did not arm the next operation"
    );
}

/// One step of a [`ScriptedSource`]: a ready chunk (or error), or a fetch that never completes.
enum Step {
    Chunk(Result<Vec<i32>>),
    NeverCompletes,
}

/// A [`ChunkSource`] that replays a script, counting fetch calls, for driving [`prefetch_loop`]
/// offline. Past the end of the script it reports itself drained (empty chunks).
struct ScriptedSource {
    steps: std::collections::VecDeque<Step>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl ScriptedSource {
    fn new(steps: Vec<Step>) -> (Self, Arc<std::sync::atomic::AtomicUsize>) {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = Self {
            steps: steps.into(),
            calls: calls.clone(),
        };
        (source, calls)
    }
}

impl ChunkSource for ScriptedSource {
    type Row = i32;

    fn next_chunk(&mut self) -> impl Future<Output = Result<Vec<i32>>> + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let step = self.steps.pop_front();
        async move {
            match step {
                Some(Step::Chunk(chunk)) => chunk,
                Some(Step::NeverCompletes) => std::future::pending().await,
                None => Ok(Vec::new()),
            }
        }
    }
}

/// Wait (bounded) until `calls` reaches `at_least`, so assertions about the background task's
/// progress don't race its scheduling.
fn wait_for_calls(calls: &std::sync::atomic::AtomicUsize, at_least: usize) {
    for _ in 0..1000 {
        if calls.load(Ordering::SeqCst) >= at_least {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "background task never reached {at_least} fetches (got {})",
        calls.load(Ordering::SeqCst)
    );
}

// Chunks arrive in order and the channel closes on a drained source — and the fetch of the
// next chunk runs ahead of consumption (the whole point of the prefetch).
#[test]
fn prefetch_delivers_chunks_in_order_and_runs_ahead() {
    let runtime = new_runtime().unwrap();
    let (source, calls) = ScriptedSource::new(vec![
        Step::Chunk(Ok(vec![1])),
        Step::Chunk(Ok(vec![2, 3])),
        Step::Chunk(Ok(vec![4])),
    ]);
    let (mut rx, task) = spawn_prefetch(&runtime, CancelSignal::new(), source);
    // Before anything is consumed the task has already fetched chunk 1 (sent, buffered) and
    // started fetching chunk 2 — depth-1 prefetch.
    wait_for_calls(&calls, 2);
    assert_eq!(rx.blocking_recv().unwrap().unwrap(), vec![1]);
    assert_eq!(rx.blocking_recv().unwrap().unwrap(), vec![2, 3]);
    assert_eq!(rx.blocking_recv().unwrap().unwrap(), vec![4]);
    assert!(
        rx.blocking_recv().is_none(),
        "drained source closes the channel"
    );
    runtime.block_on(task).unwrap();
}

// A fetch error is delivered after the chunks that preceded it, then the channel closes.
#[test]
fn prefetch_surfaces_a_fetch_error_then_stops() {
    let runtime = new_runtime().unwrap();
    let (source, _calls) = ScriptedSource::new(vec![
        Step::Chunk(Ok(vec![1])),
        Step::Chunk(Err(err("stream broke", Status::IO))),
    ]);
    let (mut rx, task) = spawn_prefetch(&runtime, CancelSignal::new(), source);
    assert_eq!(rx.blocking_recv().unwrap().unwrap(), vec![1]);
    let error = rx.blocking_recv().unwrap().unwrap_err();
    assert_eq!(error.status, Status::IO);
    assert!(
        rx.blocking_recv().is_none(),
        "an errored source closes the channel"
    );
    runtime.block_on(task).unwrap();
}

// Cancelling aborts an in-flight fetch (here: one that would never complete), surfaces
// Status::Cancelled to the consumer, and ends the task.
#[test]
fn prefetch_cancel_aborts_an_in_flight_fetch() {
    let runtime = new_runtime().unwrap();
    let (source, calls) = ScriptedSource::new(vec![Step::NeverCompletes]);
    let cancel = CancelSignal::new();
    let (mut rx, task) = spawn_prefetch(&runtime, cancel.clone(), source);
    wait_for_calls(&calls, 1); // the doomed fetch is in flight
    cancel.signal();
    let error = rx.blocking_recv().unwrap().unwrap_err();
    assert_eq!(error.status, Status::Cancelled);
    assert!(rx.blocking_recv().is_none());
    // The task must have ended (not stay parked on the never-completing fetch).
    runtime.block_on(task).unwrap();
}

// Dropping the receiver stops the task at its next send instead of draining the whole source.
#[test]
fn prefetch_stops_when_the_receiver_is_dropped() {
    let runtime = new_runtime().unwrap();
    let (source, calls) = ScriptedSource::new(vec![
        Step::Chunk(Ok(vec![1])),
        Step::Chunk(Ok(vec![2])),
        Step::Chunk(Ok(vec![3])),
    ]);
    let (rx, task) = spawn_prefetch(&runtime, CancelSignal::new(), source);
    drop(rx);
    runtime.block_on(task).unwrap();
    // The task fetched at most the chunk whose send failed plus the one already in flight.
    assert!(calls.load(Ordering::SeqCst) <= 2);
}

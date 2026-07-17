//! A shared Tokio runtime used to drive the asynchronous Spanner client from the synchronous
//! ADBC trait methods.
//!
//! The runtime is created once by the [`SpannerDriver`](crate::SpannerDriver) and shared, via an
//! [`Arc`], with every database, connection and statement it spawns. Holding the [`Arc`] keeps the
//! runtime — and therefore any background tasks the Spanner client spawns (such as the session
//! maintainer) — alive for as long as any handle exists.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::error::{Error, Result, Status};
use tokio::runtime::Runtime;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::error::err;

/// A reference-counted handle to the driver's Tokio runtime.
pub(crate) type SharedRuntime = Arc<Runtime>;

/// A **sticky, per-operation** cancellation signal shared between one operation (and any streamed
/// reader it produces) and the `cancel()` call aimed at it.
///
/// ADBC's `cancel` must be thread-safe, so this is `Clone` + `Send`/`Sync` interior mutability.
/// [`CancelSignal::signal`] — invoked from another thread by a `cancel()` call — sets a latched
/// flag and wakes an operation currently waiting inside [`block_on_cancellable`], which then
/// returns [`Status::Cancelled`].
///
/// Once latched the flag stays set **forever** — there is deliberately no way to clear it. That is
/// what makes cancelling a streamed result reliable: a query's result is streamed lazily, so a
/// cancel that lands *between* two chunk fetches must still cancel the *next* fetch rather than
/// evaporate — `Notify` alone wakes only currently-registered waiters and would lose exactly that
/// signal. Scoping the signal to one operation is the other half: the owning statement/connection
/// mints a **fresh** signal per operation via [`CancelSlot::begin_operation`], so a stale cancel
/// cannot leak into the next operation, and — conversely — a new operation cannot un-cancel a
/// still-live streamed reader from an earlier one (the reader keeps its own signal).
#[derive(Clone)]
pub(crate) struct CancelSignal(Arc<CancelInner>);

impl std::fmt::Debug for CancelSignal {
    // `CancelInner` holds a `Notify`, which is not `Debug`; the latched flag is the only meaningful
    // state to surface.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelSignal")
            .field("cancelled", &self.0.cancelled.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

struct CancelInner {
    /// Latched cancellation state; `true` from `signal()` on, forever (never cleared).
    cancelled: AtomicBool,
    /// Wakes an operation currently parked in [`block_on_cancellable`].
    notify: Notify,
}

impl CancelSignal {
    pub(crate) fn new() -> Self {
        Self(Arc::new(CancelInner {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }))
    }

    /// Request cancellation: latch the flag and wake the in-flight operation, if any. The flag
    /// stays set forever, cancelling any subsequent [`block_on_cancellable`] on this signal —
    /// such as the next chunk fetch of a streamed result.
    pub(crate) fn signal(&self) {
        // Order matters: latch the flag before waking, so a woken waiter always observes it.
        self.0.cancelled.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }

    /// Wait until this signal is cancelled. Completes immediately if it already is.
    pub(crate) async fn cancelled(&self) {
        loop {
            if self.0.cancelled.load(Ordering::Acquire) {
                return;
            }
            let notified = self.0.notify.notified();
            tokio::pin!(notified);
            // Register the waiter, then re-check the flag: a `signal()` that lands between the
            // check above and this registration would otherwise be missed (`notify_waiters` only
            // wakes already-registered waiters).
            notified.as_mut().enable();
            if self.0.cancelled.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// The owner side of per-operation cancellation: a slot holding the [`CancelSignal`] of the
/// owning ADBC object's **current** operation.
///
/// Each entry point that begins a new operation mints a fresh, uncancelled signal via
/// [`CancelSlot::begin_operation`]; `cancel()` ([`CancelSlot::signal`]) always targets the
/// current one. A superseded signal is *replaced*, never cleared — once latched it stays latched —
/// which yields exactly the ADBC contract described on [`CancelSignal`]: a stale cancel cannot leak
/// into a new operation, and a new operation cannot revive an earlier one's cancelled stream, which
/// keeps failing with [`Status::Cancelled`] rather than presenting as cleanly complete.
#[derive(Debug)]
pub(crate) struct CancelSlot(std::sync::Mutex<CancelSignal>);

impl CancelSlot {
    pub(crate) fn new() -> Self {
        Self(std::sync::Mutex::new(CancelSignal::new()))
    }

    /// Begin a new operation: mint a fresh (uncancelled) signal and make it the target of
    /// subsequent `cancel()` calls. The previous operation's signal keeps its state — a latched
    /// cancel on it stays latched for any reader still holding it.
    pub(crate) fn begin_operation(&self) -> CancelSignal {
        let fresh = CancelSignal::new();
        *self.0.lock().unwrap() = fresh.clone();
        fresh
    }

    /// The current operation's signal (a clone sharing the same latch), for the operation's own
    /// [`block_on_cancellable`] waits and for handing to the streamed readers it produces.
    pub(crate) fn current(&self) -> CancelSignal {
        self.0.lock().unwrap().clone()
    }

    /// Forward a cancel to the current operation's signal, latching it forever.
    pub(crate) fn signal(&self) {
        self.0.lock().unwrap().signal();
    }
}

/// The error every cancelled operation surfaces, whether it was cancelled while parked in
/// [`block_on_cancellable`] or inside a background prefetch task ([`spawn_prefetch`]).
fn cancelled_err() -> Error {
    err("operation cancelled", Status::Cancelled)
}

/// Run `future` on `runtime`, returning [`Status::Cancelled`] if `cancel` is signalled before it
/// completes — or if it was already signalled (and not reset) when the call began.
pub(crate) fn block_on_cancellable<T>(
    runtime: &Runtime,
    cancel: &CancelSignal,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    // Box the operation future onto the heap. `block_on` polls it on the *calling* thread's stack
    // (in ADBC the application's own thread, whose stack size the driver cannot control), and these
    // operations compose deep client/timeout/retry/conversion futures whose debug-build state
    // machines sit right at the default 2 MiB stack — held inline in this frame they overflowed it
    // on some paths (driver-manager conformance, query/DML round-trips). The heap indirection keeps
    // the frame flat, at one allocation per bridged call — negligible against the RPC it wraps.
    let future = Box::pin(future);
    runtime.block_on(async move {
        tokio::select! {
            // Check/register the cancellation waiter before polling the operation.
            biased;
            _ = cancel.cancelled() => Err(cancelled_err()),
            result = future => result,
        }
    })
}

/// A pull-based source of row chunks that a background prefetch task ([`spawn_prefetch`]) can own
/// and drain. An **empty** chunk means the source is exhausted.
pub(crate) trait ChunkSource: Send + 'static {
    /// The row type carried by each chunk.
    type Row: Send + 'static;

    /// Pull the next chunk of rows; an empty chunk signals the end of the source.
    fn next_chunk(&mut self) -> impl Future<Output = Result<Vec<Self::Row>>> + Send;
}

/// The receiving end of a [`spawn_prefetch`] channel: each item is a prefetched chunk of rows, or
/// the error that ended the stream.
pub(crate) type ChunkReceiver<T> = mpsc::Receiver<Result<Vec<T>>>;

/// Spawn a background task on `runtime` that drains `source` chunk by chunk, sending each over the
/// returned channel — so the fetch of chunk N+1 overlaps the consumer's processing of chunk N.
///
/// Memory stays bounded at prefetch depth ~1: the channel holds one chunk and the task holds at
/// most one more (the fetch it is parked on / the send it is waiting to complete). The task ends —
/// closing the channel, which is how a clean end of stream is signalled — when the source is
/// drained, when a fetch errors (the error is sent first, to surface on the consumer's next
/// `recv`), when the receiver is dropped, or when `cancel` is signalled. On cancellation the
/// in-flight fetch is dropped immediately and a [`Status::Cancelled`] error is sent best-effort;
/// the consumer's own cancel-aware `recv` (see [`block_on_cancellable`]) observes the same latched
/// signal anyway, so a buffered-but-undelivered chunk never masks a cancel. Abort the returned
/// [`JoinHandle`] to stop the task promptly without cancelling (e.g. when the consumer is dropped
/// mid-stream).
pub(crate) fn spawn_prefetch<S: ChunkSource>(
    runtime: &Runtime,
    cancel: CancelSignal,
    source: S,
) -> (ChunkReceiver<S::Row>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(1);
    let task = runtime.spawn(prefetch_loop(source, tx, cancel));
    (rx, task)
}

/// The body of the [`spawn_prefetch`] task. See there for the termination conditions.
async fn prefetch_loop<S: ChunkSource>(
    mut source: S,
    tx: mpsc::Sender<Result<Vec<S::Row>>>,
    cancel: CancelSignal,
) {
    loop {
        let rows = tokio::select! {
            // Check the (sticky) signal before polling the fetch, mirroring `block_on_cancellable`.
            biased;
            _ = cancel.cancelled() => {
                // Cancelled mid-fetch: drop the in-flight pull and surface the cancellation.
                // Best-effort — if the channel is full the consumer hits the latched signal itself
                // on its next cancel-aware `recv`, before it would ever see the buffered chunk.
                let _ = tx.try_send(Err(cancelled_err()));
                return;
            }
            pulled = source.next_chunk() => match pulled {
                // An empty chunk means the source is drained; closing the channel signals the end.
                Ok(rows) if rows.is_empty() => return,
                Ok(rows) => rows,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            },
        };
        // Also watch the signal while parked on a full channel, so a cancel is not stalled behind
        // a consumer that has stopped draining.
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            sent = tx.send(Ok(rows)) => {
                if sent.is_err() {
                    return; // The consumer dropped the receiver; nothing left to fetch for.
                }
            }
        }
    }
}

/// Create a new multi-thread runtime for the driver.
pub(crate) fn new_runtime() -> Result<SharedRuntime> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("adbc-spanner")
        .build()
        .map_err(|e| {
            err(
                format!("failed to build Tokio runtime: {e}"),
                Status::Internal,
            )
        })?;
    Ok(Arc::new(runtime))
}

#[cfg(test)]
mod tests {
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
        let cancelled: Result<i32> =
            block_on_cancellable(&runtime, &slot.current(), async { Ok(3) });
        assert_eq!(cancelled.unwrap_err().status, Status::Cancelled);
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
}

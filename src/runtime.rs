//! A shared Tokio runtime used to drive the asynchronous Spanner client from the synchronous
//! ADBC trait methods.
//!
//! The runtime is created once by the [`SpannerDriver`](crate::SpannerDriver) and shared, via an
//! [`Arc`], with every database, connection and statement it spawns. Holding the [`Arc`] keeps the
//! runtime — and therefore any background tasks the Spanner client spawns (such as the session
//! maintainer) — alive for as long as any handle exists.
//!
//! # Being entered from an async context
//!
//! Because the bridge is `block_on`, every driver call **blocks the calling thread** until the
//! operation completes. Tokio permits that only on threads that are not currently *running* a
//! runtime, and *panics* otherwise — both when blocking ("Cannot block the current thread from
//! within a runtime") and when dropping a runtime ("Cannot drop a runtime in a context where
//! blocking is not allowed"). For a cdylib that is worse than it sounds: the panic unwinds across
//! the C FFI boundary and poisons the driver handle.
//!
//! Neither panic is predictable through Tokio's public API — [`Handle::try_current`] is `Ok` on a
//! runtime worker (blocking panics) *and* inside [`tokio::task::spawn_blocking`] (blocking is
//! legal, and is the sanctioned way to call a driver like this one). Guarding on it would
//! therefore reject the very workaround it would have to recommend. So instead of predicting,
//! each site uses a construction that is legal in every context it can be reached from:
//!
//! - **Calls** go through [`block_on_bridged`] (used by [`block_on_cancellable`] and by
//!   `SpannerDatabase::connect`'s plain `block_on`), which picks per runtime flavour between
//!   blocking directly, `block_in_place`, and a scoped thread.
//! - **Drops** go through [`DriverRuntime`], whose `Drop` falls back to
//!   [`Runtime::shutdown_background`] when a runtime context is detected — a *safe*
//!   over-approximation, unlike the call case: shutting down in the background where a blocking
//!   shutdown would also have worked costs nothing but the wait. This matters because a streamed
//!   [`SpannerBatchReader`](crate::conversion::SpannerBatchReader) can outlive every other handle
//!   and be dropped anywhere.
//!
//! The caller's thread still blocks — that is what a synchronous driver does, and what the caller
//! asked for. Prefer [`tokio::task::spawn_blocking`] so it is not an async worker that parks.

use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use adbc_core::error::{Error, Result, Status};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::error::err;

/// A reference-counted handle to the driver's Tokio runtime.
pub(crate) type SharedRuntime = Arc<DriverRuntime>;

/// The driver's Tokio runtime, wrapped so that dropping the last handle **from inside another
/// Tokio runtime does not panic**.
///
/// [`Runtime`]'s own `Drop` blocks the current thread while the runtime's worker threads wind
/// down, which Tokio refuses to do in an async context ("Cannot drop a runtime in a context where
/// blocking is not allowed"). The driver cannot prevent that placement: a streamed
/// [`SpannerBatchReader`](crate::conversion::SpannerBatchReader) holds a [`SharedRuntime`] and may
/// well be the last holder, dropped wherever the consumer happens to release it — including on a
/// Tokio worker thread. So `Drop` here detects a runtime context and shuts the runtime down in the
/// background (non-blocking) instead of waiting for it.
///
/// [`Handle::try_current`] over-approximates (it is also `Ok` inside `spawn_blocking`, where a
/// blocking drop would have been fine), but here that is harmless: the false positive only skips
/// the wait for already-idle worker threads. Contrast [`block_on_bridged`], where the same
/// over-approximation would have turned working calls into errors.
///
/// Deref-ing to [`Runtime`] keeps every call site (`block_on`, `spawn`) unchanged.
pub(crate) struct DriverRuntime(
    /// Always `Some` until `Drop`, which takes the runtime out to shut it down by value.
    Option<Runtime>,
);

impl DriverRuntime {
    fn new(runtime: Runtime) -> Self {
        Self(Some(runtime))
    }
}

impl Deref for DriverRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Runtime {
        // Only `Drop` ever takes the runtime out, and nothing can deref afterwards.
        self.0.as_ref().expect("runtime taken only by Drop")
    }
}

impl std::fmt::Debug for DriverRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DriverRuntime").field(&self.0).finish()
    }
}

impl Drop for DriverRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.0.take() else {
            return;
        };
        if Handle::try_current().is_ok() {
            // Dropping by value here would panic. Shut down without waiting instead: tasks are not
            // polled again and the worker threads are detached to exit on their own. Nothing is
            // lost that a blocking drop would have kept — the driver only ever runs *completed*
            // operations' futures on this runtime plus the prefetch tasks, whose readers are gone
            // by the time the last handle drops.
            runtime.shutdown_background();
        } else {
            drop(runtime);
        }
    }
}

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
/// cancel that lands *between* two chunk fetches (while rows are being converted to Arrow, or
/// while the consumer processes a batch) must still cancel the *next* fetch rather than evaporate
/// — `Notify` alone wakes only currently-registered waiters and would lose exactly that signal.
/// Scoping the signal to one operation is the other half: the owning statement/connection mints a
/// **fresh** signal per operation via [`CancelSlot::begin_operation`], so a stale cancel aimed at
/// a finished operation cannot leak into the next one, and — conversely — starting a new operation
/// cannot un-cancel a still-live streamed reader from an earlier one (the reader keeps its own
/// operation's signal, which nothing ever clears).
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
            // Woken: loop to re-read the flag (defensive against spurious wakeups).
        }
    }
}

/// The owner side of per-operation cancellation: a slot holding the [`CancelSignal`] of the
/// owning ADBC object's **current** operation.
///
/// Each entry point that begins a new operation mints a fresh, uncancelled signal via
/// [`CancelSlot::begin_operation`]; `cancel()` ([`CancelSlot::signal`]) always targets the
/// current one. A superseded signal is *replaced*, never cleared — once latched it stays latched
/// — which yields exactly the ADBC contract:
///
/// - a cancel aimed at a previous (or absent) operation does not leak into a new one (the new
///   operation runs on its own fresh signal);
/// - starting a new operation cannot **un-cancel** an earlier operation's still-live streamed
///   reader, and a cancelled stream can never present as cleanly complete: the reader keeps a
///   clone of its own operation's signal, whose latch nothing clears, so every subsequent fetch
///   keeps failing with [`Status::Cancelled`].
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

/// Block the calling thread on `future`, driving it on `runtime`, **from any thread context** —
/// including from inside somebody else's Tokio runtime, where a bare
/// [`Runtime::block_on`] would panic with "Cannot block the current thread from within a runtime".
///
/// Tokio permits blocking only on threads that are not currently *running* a runtime, and there is
/// no public API that reports that: [`Handle::try_current`] is `Ok` both on a runtime worker
/// (where blocking panics) *and* inside [`tokio::task::spawn_blocking`] / [`block_in_place`]
/// (where it is perfectly legal — those are the sanctioned ways to block). So this does not
/// *predict* whether blocking is allowed; it picks, per runtime flavour, a construction that is
/// legal in **every** context that flavour can present:
///
/// - **No runtime context** — an ordinary application thread, the overwhelmingly common case for a
///   synchronous ADBC driver. Block right here; nothing to work around.
/// - **A multi-threaded runtime** — [`block_in_place`] is exactly Tokio's answer to "this thread is
///   about to block": on a worker it hands the core to another thread first, so the caller's runtime
///   keeps its full capacity; on a `spawn_blocking` thread (not *running* the runtime) it is a plain
///   pass-through. Legal either way, and it never spawns a thread.
/// - **A current-thread runtime** — `block_in_place` panics there (there is no other worker to hand
///   the core to), and a `spawn_blocking` thread of a current-thread runtime is indistinguishable
///   from its one worker. So drive the future on a scoped thread, which carries no Tokio context at
///   all and may therefore always block. [`std::thread::scope`] borrows, so the future needs no
///   `'static`; the calling thread parks in `join` exactly as it would in `block_on`.
///
/// The caller's thread blocks in all three cases — that is inherent in bridging a synchronous API
/// onto an async client, and it is what the caller asked for by invoking a blocking driver. What is
/// avoided is the *panic*, which for a cdylib would unwind across the C FFI boundary and poison the
/// driver handle. A panic from `future` itself is still a genuine bug and is re-raised unchanged.
pub(crate) fn block_on_bridged<F>(runtime: &Runtime, future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    use tokio::runtime::RuntimeFlavor;
    use tokio::task::block_in_place;

    match Handle::try_current().map(|handle| handle.runtime_flavor()) {
        Err(_) => runtime.block_on(future),
        Ok(RuntimeFlavor::MultiThread) => block_in_place(|| runtime.block_on(future)),
        // `RuntimeFlavor` is `#[non_exhaustive]`; the scoped thread is legal for *any* flavour, so
        // it is also the right fallback for one that does not exist yet.
        Ok(_) => std::thread::scope(|scope| {
            match scope.spawn(|| runtime.block_on(future)).join() {
                Ok(output) => output,
                // Propagate a panic from the future itself unchanged, as a bare `block_on` would.
                Err(payload) => std::panic::resume_unwind(payload),
            }
        }),
    }
}

/// Run `future` on `runtime`, returning [`Status::Cancelled`] if `cancel` is signalled before it
/// completes — or if it was already signalled (and not reset) when the call began.
///
/// Safe to call from any thread, async context included — see [`block_on_bridged`].
pub(crate) fn block_on_cancellable<T: Send>(
    runtime: &Runtime,
    cancel: &CancelSignal,
    future: impl Future<Output = Result<T>> + Send,
) -> Result<T> {
    // Box the operation future onto the heap. `block_on` polls it on the *calling* thread's stack
    // (in ADBC that is the application's own thread, whose stack size the driver cannot control),
    // and the operations here compose deep client/timeout/retry/conversion futures whose debug-build
    // state machines are large enough to sit right at the default 2 MiB thread stack. Holding the
    // composite inline in this frame overflowed that stack on some paths (e.g. the driver-manager
    // conformance and query/DML round-trips); the heap indirection keeps the frame flat regardless
    // of the operation's size, at the cost of one allocation per bridged call — negligible against
    // the RPC it wraps.
    let future = Box::pin(future);
    block_on_bridged(runtime, async move {
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
    Ok(Arc::new(DriverRuntime::new(runtime)))
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

    /// Stand-ins for an application's own runtime, from inside which the driver is entered. Both
    /// flavours matter and take different paths through [`block_on_bridged`]: `#[tokio::main]`
    /// defaults to multi-thread, while `#[tokio::test]` and `flavor = "current_thread"` do not.
    fn multi_thread_caller() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn current_thread_caller() -> Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    // CON-1, the call half. Every context a driver call can be reached from must work — rather than
    // panic with "Cannot block the current thread from within a runtime", which for the cdylib
    // would unwind across the C FFI boundary and poison the driver handle.
    //
    // The `spawn_blocking` cases are the ones that make `Handle::try_current()` — the obvious
    // guard, and the one CON-1 suggested — unusable: it reports `Ok` there just as it does on a
    // worker thread, so a guard built on it would reject the very workaround the driver has to
    // recommend, in the default `#[tokio::main]` (multi-thread) setup. These cases pin that down.
    fn assert_call_works(runtime: &SharedRuntime, label: &str) {
        let result: Result<i32> =
            block_on_cancellable(runtime, &CancelSignal::new(), async { Ok(1) });
        assert_eq!(result.unwrap(), 1, "bridged call failed in {label}");
    }

    #[test]
    fn a_bridged_call_works_from_every_context() {
        let runtime = new_runtime().unwrap();

        // No runtime context: the ordinary synchronous ADBC caller.
        assert_call_works(&runtime, "a plain thread");

        // Multi-threaded runtime: worker threads (`block_on` body and a spawned task) go through
        // `block_in_place`; a `spawn_blocking` thread is not running the runtime, so it passes
        // straight through.
        multi_thread_caller().block_on({
            let runtime = runtime.clone();
            async move {
                assert_call_works(&runtime, "a multi-thread block_on body");
                let rt = runtime.clone();
                tokio::spawn(async move { assert_call_works(&rt, "a multi-thread task") })
                    .await
                    .unwrap();
                let rt = runtime.clone();
                tokio::task::spawn_blocking(move || {
                    assert_call_works(&rt, "a multi-thread spawn_blocking")
                })
                .await
                .unwrap();
            }
        });

        // Current-thread runtime: `block_in_place` would panic here, so these go via a scoped
        // thread — including `spawn_blocking`, which is indistinguishable from the one worker.
        current_thread_caller().block_on({
            let runtime = runtime.clone();
            async move {
                assert_call_works(&runtime, "a current-thread block_on body");
                let rt = runtime.clone();
                tokio::spawn(async move { assert_call_works(&rt, "a current-thread task") })
                    .await
                    .unwrap();
                let rt = runtime.clone();
                tokio::task::spawn_blocking(move || {
                    assert_call_works(&rt, "a current-thread spawn_blocking")
                })
                .await
                .unwrap();
            }
        });
    }

    // Cancellation still works when the call was bridged off the caller's thread (the scoped-thread
    // path), so the rescue does not quietly cost the driver its `cancel()` contract.
    #[test]
    fn a_bridged_call_is_still_cancellable_from_an_async_context() {
        let runtime = new_runtime().unwrap();
        let cancel = CancelSignal::new();
        let signaller = cancel.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            signaller.signal();
        });
        let error = current_thread_caller()
            .block_on(async {
                block_on_cancellable(&runtime, &cancel, async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok(())
                })
            })
            .unwrap_err();
        assert_eq!(error.status, Status::Cancelled);
    }

    // CON-1, the drop half. Dropping the last handle on a Tokio worker thread — what happens when a
    // streamed reader outlives every other handle and its consumer releases it from async code —
    // must not panic with "Cannot drop a runtime in a context where blocking is not allowed".
    #[test]
    fn dropping_the_last_handle_from_an_async_context_does_not_panic() {
        for (label, caller) in [
            ("multi-thread", multi_thread_caller()),
            ("current-thread", current_thread_caller()),
        ] {
            let runtime = new_runtime().unwrap();
            // Give the runtime a task to wind down, so the drop is not vacuously fine.
            runtime.spawn(async { std::future::pending::<()>().await });
            caller.block_on(async move {
                assert_eq!(
                    Arc::strong_count(&runtime),
                    1,
                    "{label}: must be the last handle"
                );
                drop(runtime);
            });
        }
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
    // result, between two chunk fetches) still cancels the next operation on the same signal —
    // previously it was silently lost and the stream ran to completion.
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
    // a streamed reader holding it keeps failing with Cancelled (previously a shared resettable
    // signal let a new operation silently revive a cancelled stream, or worse, let it end cleanly
    // truncated).
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

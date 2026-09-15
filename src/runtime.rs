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

use adbc_core::CancelHandle;
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

/// The ADBC [`CancelHandle`] over a [`CancelSlot`], returned by the connection's and statement's
/// `get_cancel_handle`.
///
/// `adbc_core` hands cancellation out as a *separate* handle (the borrow checker cannot express a
/// `&mut self` `cancel` racing an in-flight `&mut self` execution), and the FFI exporter takes one
/// **once**, when the connection/statement is created, then keeps it for that object's whole life.
/// So the handle must stay aimed at whatever operation is current at `try_cancel` time, not at the
/// one running when it was minted — which is exactly what sharing the owner's [`CancelSlot`]
/// through an [`Arc`] gives: `signal` always latches the slot's *current* signal, the same target
/// the deprecated `cancel()` method had.
#[derive(Debug)]
pub(crate) struct SlotCancelHandle(Arc<CancelSlot>);

impl SlotCancelHandle {
    pub(crate) fn new(slot: Arc<CancelSlot>) -> Self {
        Self(slot)
    }
}

impl CancelHandle for SlotCancelHandle {
    fn try_cancel(&self) -> Result<()> {
        self.0.signal();
        Ok(())
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
mod tests;

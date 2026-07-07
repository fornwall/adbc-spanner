//! A shared Tokio runtime used to drive the asynchronous Spanner client from the synchronous
//! ADBC trait methods.
//!
//! The runtime is created once by the [`SpannerDriver`](crate::SpannerDriver) and shared, via an
//! [`Arc`], with every database, connection and statement it spawns. Holding the [`Arc`] keeps the
//! runtime — and therefore any background tasks the Spanner client spawns (such as the session
//! maintainer) — alive for as long as any handle exists.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use tokio::runtime::Runtime;
use tokio::sync::Notify;

use crate::error::err;

/// A reference-counted handle to the driver's Tokio runtime.
pub(crate) type SharedRuntime = Arc<Runtime>;

/// A **sticky** cancellation signal shared between an ADBC object and its blocking operations.
///
/// ADBC's `cancel` must be thread-safe, so this is `Clone` + `Send`/`Sync` interior mutability.
/// [`CancelSignal::signal`] — invoked from another thread by a `cancel()` call — sets a latched
/// flag and wakes an operation currently waiting inside [`block_on_cancellable`], which then
/// returns [`Status::Cancelled`].
///
/// The flag *stays set* until [`CancelSignal::reset`] is called, which is what makes cancelling a
/// streamed result reliable: a query's result is streamed lazily, so a cancel that lands *between*
/// two chunk fetches (while rows are being converted to Arrow, or while the consumer processes a
/// batch) must still cancel the *next* fetch rather than evaporate — `Notify` alone wakes only
/// currently-registered waiters and would lose exactly that signal. Each ADBC entry point that
/// begins a **new** operation calls [`CancelSignal::reset`] first, so a stale cancel aimed at a
/// finished (or never-started) operation does not leak into the next one.
#[derive(Clone)]
pub(crate) struct CancelSignal(Arc<CancelInner>);

struct CancelInner {
    /// Latched cancellation state; `true` from `signal()` until the next `reset()`.
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
    /// stays set (cancelling any subsequent [`block_on_cancellable`] on this signal, such as the
    /// next chunk fetch of a streamed result) until [`CancelSignal::reset`].
    pub(crate) fn signal(&self) {
        // Order matters: latch the flag before waking, so a woken waiter always observes it.
        self.0.cancelled.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }

    /// Clear a latched cancellation. Called by ADBC entry points that begin a new operation, so a
    /// cancel aimed at a previous (or absent) operation does not cancel this one.
    pub(crate) fn reset(&self) {
        self.0.cancelled.store(false, Ordering::Release);
    }

    /// Wait until this signal is cancelled. Completes immediately if it already is.
    async fn cancelled(&self) {
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
            // Woken: loop to re-read the flag (a concurrent `reset()` may have cleared it).
        }
    }
}

/// Run `future` on `runtime`, returning [`Status::Cancelled`] if `cancel` is signalled before it
/// completes — or if it was already signalled (and not reset) when the call began.
pub(crate) fn block_on_cancellable<T>(
    runtime: &Runtime,
    cancel: &CancelSignal,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    runtime.block_on(async move {
        tokio::select! {
            // Check/register the cancellation waiter before polling the operation.
            biased;
            _ = cancel.cancelled() => Err(err("operation cancelled", Status::Cancelled)),
            result = future => result,
        }
    })
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

    // A stale cancel does not leak into a new operation: entry points reset the signal first.
    #[test]
    fn reset_clears_a_stale_signal() {
        let runtime = new_runtime().unwrap();
        let cancel = CancelSignal::new();
        cancel.signal();
        cancel.reset();
        let result: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(7) });
        assert_eq!(result.unwrap(), 7);
    }
}

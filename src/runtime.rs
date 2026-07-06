//! A shared Tokio runtime used to drive the asynchronous Spanner client from the synchronous
//! ADBC trait methods.
//!
//! The runtime is created once by the [`SpannerDriver`](crate::SpannerDriver) and shared, via an
//! [`Arc`], with every database, connection and statement it spawns. Holding the [`Arc`] keeps the
//! runtime — and therefore any background tasks the Spanner client spawns (such as the session
//! maintainer) — alive for as long as any handle exists.

use std::future::Future;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use tokio::runtime::Runtime;
use tokio::sync::Notify;

use crate::error::err;

/// A reference-counted handle to the driver's Tokio runtime.
pub(crate) type SharedRuntime = Arc<Runtime>;

/// A best-effort cancellation signal shared between an ADBC object and its in-flight blocking
/// operation.
///
/// ADBC's `cancel` must be thread-safe, so this is `Clone` + `Send`/`Sync` interior mutability
/// (an [`Arc<Notify>`]). [`CancelSignal::signal`] — invoked from another thread by a `cancel()`
/// call — wakes an operation currently waiting inside [`block_on_cancellable`], which then returns
/// [`Status::Cancelled`]. Signalling with no in-flight operation is a harmless no-op, and the
/// signal is reusable for the next operation. A query's result is streamed lazily, so the same
/// signal also interrupts an in-flight chunk fetch while a caller iterates the result reader, not
/// only the initial still-running query/DML.
#[derive(Clone)]
pub(crate) struct CancelSignal(Arc<Notify>);

impl CancelSignal {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Notify::new()))
    }

    /// Request cancellation of the in-flight operation, if any.
    pub(crate) fn signal(&self) {
        self.0.notify_waiters();
    }
}

/// Run `future` on `runtime`, returning [`Status::Cancelled`] if `cancel` is signalled before it
/// completes.
pub(crate) fn block_on_cancellable<T>(
    runtime: &Runtime,
    cancel: &CancelSignal,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    runtime.block_on(async move {
        tokio::select! {
            // Register the cancellation waiter before polling the operation.
            biased;
            _ = cancel.0.notified() => Err(err("operation cancelled", Status::Cancelled)),
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

    #[test]
    fn signal_with_no_operation_is_a_harmless_no_op() {
        let cancel = CancelSignal::new();
        cancel.signal();
        // A subsequent operation still runs normally.
        let runtime = new_runtime().unwrap();
        let result: Result<i32> = block_on_cancellable(&runtime, &cancel, async { Ok(7) });
        assert_eq!(result.unwrap(), 7);
    }
}

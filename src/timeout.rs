//! RPC timeout options (`spanner.rpc.timeout_seconds.{query,update,fetch}`).
//!
//! The ADBC traits are synchronous and every driver call bridges into the async Spanner client via
//! `block_on` (see [`crate::runtime`]), so without a deadline a hung RPC blocks the calling thread
//! indefinitely, with `cancel` as the only escape. These three options bound the driver's
//! Spanner-facing operations; which sites each one covers, and their value grammar and defaults,
//! are documented on the `OPTION_RPC_TIMEOUT_*` constants and in `docs/options.md`.
//!
//! Enforcement is an **overall deadline** per operation via [`tokio::time::timeout`]
//! ([`with_timeout`]), not a per-attempt gax timeout: the bound covers the whole driver-side
//! operation, including any retries the client performs inside it. An expired deadline surfaces as
//! [`Status::Timeout`]. Unlike the request tag/priority options — which deliberately leave the
//! driver-internal metadata queries untouched — these timeouts bound every driver-side network
//! path, DDL (an admin long-running operation) and the metadata queries included, so none can hang
//! unboundedly.

use std::future::Future;
use std::time::Duration;

use adbc_core::error::{Result, Status};

use crate::error::err;

/// The RPC timeout configuration held by a connection or statement
/// (`spanner.rpc.timeout_seconds.{query,update,fetch}`).
///
/// A connection's value is cloned into each statement it creates (which may then override any of
/// the three), mirroring how [`ReadStaleness`](crate::staleness::ReadStaleness) is inherited.
///
/// Values are stored as the `f64` seconds the caller set, so `get_option` /
/// `get_option_double` round-trip exactly what was configured; the `*_timeout()` accessors yield
/// the effective [`Duration`] (`None` when unset **or** set to `0`, both meaning "no timeout").
///
/// The three fields are set and read directly by
/// [`impl_shared_option_dispatch`](crate::options::impl_shared_option_dispatch), which parses each
/// with [`f64_option`](crate::options::f64_option) in the
/// [`NonNegativeSeconds`](crate::options::F64Range::NonNegativeSeconds) range.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RpcTimeouts {
    /// `spanner.rpc.timeout_seconds.query`, in seconds, when set.
    pub(crate) query: Option<f64>,
    /// `spanner.rpc.timeout_seconds.update`, in seconds, when set.
    pub(crate) update: Option<f64>,
    /// `spanner.rpc.timeout_seconds.fetch`, in seconds, when set.
    pub(crate) fetch: Option<f64>,
}

impl RpcTimeouts {
    /// The effective query timeout (`None` when unset or `0`).
    pub(crate) fn query_timeout(&self) -> Option<Duration> {
        as_duration(self.query)
    }

    /// The effective update timeout (`None` when unset or `0`).
    pub(crate) fn update_timeout(&self) -> Option<Duration> {
        as_duration(self.update)
    }

    /// The effective fetch timeout (`None` when unset or `0`).
    pub(crate) fn fetch_timeout(&self) -> Option<Duration> {
        as_duration(self.fetch)
    }
}

/// The effective [`Duration`] of a stored seconds value: `None` when unset or `0` (both meaning
/// "no timeout"). Conversion cannot fail — [`f64_option`](crate::options::f64_option) validated it
/// at set time.
fn as_duration(seconds: Option<f64>) -> Option<Duration> {
    let seconds = seconds?;
    if seconds > 0.0 {
        Duration::try_from_secs_f64(seconds).ok()
    } else {
        None
    }
}

/// Run `future` under an optional overall deadline, mapping expiry to [`Status::Timeout`].
///
/// `option` names the `spanner.rpc.timeout_seconds.*` option that imposed the deadline, so the
/// error tells the caller which knob fired. With `limit = None` the future runs unbounded.
pub(crate) async fn with_timeout<T>(
    limit: Option<Duration>,
    option: &'static str,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let Some(limit) = limit else {
        return future.await;
    };
    match tokio::time::timeout(limit, future).await {
        Ok(result) => result,
        Err(_) => Err(err(
            format!(
                "operation timed out after {}s ({option})",
                limit.as_secs_f64()
            ),
            Status::Timeout,
        )),
    }
}

#[cfg(test)]
mod tests;

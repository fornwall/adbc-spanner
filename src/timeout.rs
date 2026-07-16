//! RPC timeout options (`spanner.rpc.timeout_seconds.{query,update,fetch}`).
//!
//! The ADBC traits are synchronous and every driver call bridges into the async Spanner client via
//! `block_on` (see [`crate::runtime`]), so without a deadline a hung RPC blocks the calling thread
//! indefinitely, with `cancel` as the only escape. These three options bound the driver's
//! Spanner-facing operations — the naming parallels the Flight SQL ADBC driver's
//! `adbc.flight.sql.rpc.timeout_seconds.*` family:
//!
//! - [`OPTION_RPC_TIMEOUT_QUERY`](crate::OPTION_RPC_TIMEOUT_QUERY) — the **initial execution** of a
//!   query: the `ExecuteStreamingSql` call plus the first chunk of a streamed result (which is what
//!   settles the schema), the `execute_schema`/`execute_partitions` probes, and the initial fetch
//!   of `read_partition`. It also bounds the driver-internal metadata reads, each of which is a
//!   query execution: `get_objects`, `get_statistics` (both its discovery fetch and its per-table
//!   aggregate scans), `get_table_schema`, and the shared table-exists probe.
//! - [`OPTION_RPC_TIMEOUT_FETCH`](crate::OPTION_RPC_TIMEOUT_FETCH) — **each subsequent chunk
//!   fetch** of a streamed result, applied inside the background prefetch task
//!   ([`spawn_prefetch`](crate::runtime::spawn_prefetch)) so a stalled stream fails the consumer's
//!   next batch instead of hanging the prefetcher.
//! - [`OPTION_RPC_TIMEOUT_UPDATE`](crate::OPTION_RPC_TIMEOUT_UPDATE) — the **write paths**: DML /
//!   batch-DML read/write transactions (including the manual-mode commit), each bulk-ingest commit
//!   chunk, and DDL — the admin `UpdateDatabaseDdl` call **and** its long-running-operation poll
//!   loop, which otherwise polls without any bound.
//!
//! Each value is a number of **seconds**, parsed as `f64` (fractions allowed); it must be finite
//! and non-negative — `NaN`, the infinities and negatives are rejected with `InvalidArguments`,
//! matching Flight SQL's validation. `0` disables the timeout (the same behaviour as unset, but it
//! still round-trips through `get_option`); an empty string unsets. Like the read-staleness
//! options, a connection's values become the default for statements it creates (which may override
//! them), and every option round-trips through `get_option` and `get_option_double`.
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
use adbc_core::options::OptionValue;

use crate::error::err;
use crate::options::{F64Range, f64_option};

/// The RPC timeout configuration held by a connection or statement
/// (`spanner.rpc.timeout_seconds.{query,update,fetch}`).
///
/// A connection's value is cloned into each statement it creates (which may then override any of
/// the three), mirroring how [`ReadStaleness`](crate::staleness::ReadStaleness) is inherited.
///
/// Values are stored as the `f64` seconds the caller set, so `get_option` /
/// `get_option_double` round-trip exactly what was configured; the `*_timeout()` accessors yield
/// the effective [`Duration`] (`None` when unset **or** set to `0`, both meaning "no timeout").
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RpcTimeouts {
    /// `spanner.rpc.timeout_seconds.query`, in seconds, when set.
    query: Option<f64>,
    /// `spanner.rpc.timeout_seconds.update`, in seconds, when set.
    update: Option<f64>,
    /// `spanner.rpc.timeout_seconds.fetch`, in seconds, when set.
    fetch: Option<f64>,
}

impl RpcTimeouts {
    /// Handle a `set_option` for `spanner.rpc.timeout_seconds.query`. An empty string unsets it.
    pub(crate) fn set_query(&mut self, value: OptionValue) -> Result<()> {
        self.query = f64_option(
            value,
            crate::OPTION_RPC_TIMEOUT_QUERY,
            F64Range::NonNegativeSeconds,
        )?;
        Ok(())
    }

    /// Handle a `set_option` for `spanner.rpc.timeout_seconds.update`. An empty string unsets it.
    pub(crate) fn set_update(&mut self, value: OptionValue) -> Result<()> {
        self.update = f64_option(
            value,
            crate::OPTION_RPC_TIMEOUT_UPDATE,
            F64Range::NonNegativeSeconds,
        )?;
        Ok(())
    }

    /// Handle a `set_option` for `spanner.rpc.timeout_seconds.fetch`. An empty string unsets it.
    pub(crate) fn set_fetch(&mut self, value: OptionValue) -> Result<()> {
        self.fetch = f64_option(
            value,
            crate::OPTION_RPC_TIMEOUT_FETCH,
            F64Range::NonNegativeSeconds,
        )?;
        Ok(())
    }

    /// The canonical `spanner.rpc.timeout_seconds.query` value, for `get_option` round-trip.
    pub(crate) fn query_string(&self) -> Option<String> {
        self.query.map(|s| s.to_string())
    }

    /// The canonical `spanner.rpc.timeout_seconds.update` value, for `get_option` round-trip.
    pub(crate) fn update_string(&self) -> Option<String> {
        self.update.map(|s| s.to_string())
    }

    /// The canonical `spanner.rpc.timeout_seconds.fetch` value, for `get_option` round-trip.
    pub(crate) fn fetch_string(&self) -> Option<String> {
        self.fetch.map(|s| s.to_string())
    }

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
/// "no timeout"). Conversion cannot fail — [`f64_option`] validated it at set time.
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
mod tests {
    use super::*;
    use crate::runtime::{
        CancelSignal, ChunkSource, block_on_cancellable, new_runtime, spawn_prefetch,
    };

    fn s(v: &str) -> OptionValue {
        OptionValue::String(v.to_string())
    }

    #[test]
    fn parses_numeric_strings_ints_and_doubles() {
        let mut config = RpcTimeouts::default();
        // Numeric strings (trimmed, fractions allowed).
        config.set_query(s(" 2.5 ")).unwrap();
        assert_eq!(config.query_string().as_deref(), Some("2.5"));
        assert_eq!(config.query_timeout(), Some(Duration::from_millis(2500)));
        // Integers.
        config.set_update(OptionValue::Int(30)).unwrap();
        assert_eq!(config.update_string().as_deref(), Some("30"));
        assert_eq!(config.update_timeout(), Some(Duration::from_secs(30)));
        // Doubles (the `get_option_double` / `set_option_double` shape).
        config.set_fetch(OptionValue::Double(0.05)).unwrap();
        assert_eq!(config.fetch_string().as_deref(), Some("0.05"));
        assert_eq!(config.fetch_timeout(), Some(Duration::from_millis(50)));
    }

    #[test]
    fn zero_disables_but_still_round_trips() {
        let mut config = RpcTimeouts::default();
        for value in [s("0"), OptionValue::Int(0), OptionValue::Double(0.0)] {
            config.set_query(value).unwrap();
            // The stored value reports back...
            assert_eq!(config.query_string().as_deref(), Some("0"));
            // ...but no deadline is enforced.
            assert_eq!(config.query_timeout(), None);
        }
    }

    #[test]
    fn empty_string_unsets() {
        let mut config = RpcTimeouts::default();
        config.set_fetch(s("1.5")).unwrap();
        assert!(config.fetch_string().is_some());
        config.set_fetch(s("")).unwrap();
        assert_eq!(config.fetch_string(), None);
        assert_eq!(config.fetch_timeout(), None);
        // Whitespace-only counts as empty too (values are trimmed).
        config.set_fetch(s("2")).unwrap();
        config.set_fetch(s("  ")).unwrap();
        assert_eq!(config.fetch_string(), None);
    }

    #[test]
    fn rejects_nan_infinities_negatives_and_garbage() {
        let mut config = RpcTimeouts::default();
        config.set_query(s("5")).unwrap();
        let bad_values = [
            OptionValue::Double(f64::NAN),
            OptionValue::Double(f64::INFINITY),
            OptionValue::Double(f64::NEG_INFINITY),
            OptionValue::Double(-1.0),
            OptionValue::Int(-1),
            s("NaN"), /* parses as f64::NAN, still rejected */
            s("inf"),
            s("-2"),
            s("abc"),
            s("1s"),
            // Finite but too large for a Duration: rejected at set time, not at execution.
            OptionValue::Double(1e300),
            // Non-numeric value kinds.
            OptionValue::Bytes(vec![1, 2, 3]),
        ];
        for value in bad_values {
            let error = config.set_query(value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
            assert!(
                error.message.contains(crate::OPTION_RPC_TIMEOUT_QUERY),
                "{}",
                error.message
            );
            // A rejected value leaves the stored one untouched.
            assert_eq!(
                config.query_string().as_deref(),
                Some("5"),
                "value {value:?}"
            );
        }
    }

    #[test]
    fn the_three_timeouts_are_independent() {
        let mut config = RpcTimeouts::default();
        config.set_query(s("1")).unwrap();
        config.set_update(s("2")).unwrap();
        config.set_fetch(s("3")).unwrap();
        config.set_update(s("")).unwrap();
        assert_eq!(config.query_string().as_deref(), Some("1"));
        assert_eq!(config.update_string(), None);
        assert_eq!(config.fetch_string().as_deref(), Some("3"));
    }

    /// Statement inheritance is a plain clone of the connection's config (mirroring
    /// `ReadStaleness` / `RequestConfig`): the clone starts with the connection's values and
    /// overrides independently.
    #[test]
    fn cloned_config_inherits_then_overrides_independently() {
        let mut connection = RpcTimeouts::default();
        connection.set_query(s("10")).unwrap();
        connection.set_fetch(s("20")).unwrap();

        let mut statement = connection;
        assert_eq!(statement.query_string().as_deref(), Some("10"));
        assert_eq!(statement.fetch_string().as_deref(), Some("20"));

        statement.set_query(s("1.5")).unwrap();
        statement.set_fetch(s("")).unwrap();
        assert_eq!(statement.query_string().as_deref(), Some("1.5"));
        assert_eq!(statement.fetch_string(), None);
        // The connection is unaffected by statement-level overrides.
        assert_eq!(connection.query_string().as_deref(), Some("10"));
        assert_eq!(connection.fetch_string().as_deref(), Some("20"));
    }

    #[test]
    fn with_timeout_passes_a_completing_future_through() {
        let runtime = new_runtime().unwrap();
        let cancel = CancelSignal::new();
        // Bounded and unbounded alike.
        for limit in [None, Some(Duration::from_secs(30))] {
            let result: Result<i32> = block_on_cancellable(
                &runtime,
                &cancel,
                with_timeout(limit, crate::OPTION_RPC_TIMEOUT_QUERY, async { Ok(7) }),
            );
            assert_eq!(result.unwrap(), 7);
        }
    }

    /// The core bug being fixed: an operation that never resolves (a hung RPC) must fail with
    /// `Status::Timeout` — naming the responsible option — instead of blocking `block_on` forever.
    #[test]
    fn with_timeout_fails_a_hung_operation_with_timeout_status() {
        let runtime = new_runtime().unwrap();
        let cancel = CancelSignal::new();
        let result: Result<()> = block_on_cancellable(
            &runtime,
            &cancel,
            with_timeout(
                Some(Duration::from_millis(50)),
                crate::OPTION_RPC_TIMEOUT_QUERY,
                std::future::pending(),
            ),
        );
        let error = result.unwrap_err();
        assert_eq!(error.status, Status::Timeout);
        assert!(
            error.message.contains(crate::OPTION_RPC_TIMEOUT_QUERY),
            "{}",
            error.message
        );
        assert!(error.message.contains("0.05s"), "{}", error.message);
    }

    /// A [`ChunkSource`] whose every fetch hangs forever, bounded by the fetch timeout exactly the
    /// way `ResultSetChunks` bounds `pull_chunk` — for driving the prefetch task offline.
    struct StallingSource {
        timeout: Option<Duration>,
    }

    impl ChunkSource for StallingSource {
        type Row = i32;

        fn next_chunk(&mut self) -> impl std::future::Future<Output = Result<Vec<i32>>> + Send {
            with_timeout(
                self.timeout,
                crate::OPTION_RPC_TIMEOUT_FETCH,
                std::future::pending::<Result<Vec<i32>>>(),
            )
        }
    }

    /// The fetch timeout bounds each chunk fetch *inside* the background prefetch task: a stalled
    /// stream surfaces `Status::Timeout` on the consumer's next receive and ends the task, instead
    /// of leaving the prefetcher parked forever.
    #[test]
    fn fetch_timeout_fires_inside_the_prefetch_task() {
        let runtime = new_runtime().unwrap();
        let (mut rx, task) = spawn_prefetch(
            &runtime,
            CancelSignal::new(),
            StallingSource {
                timeout: Some(Duration::from_millis(50)),
            },
        );
        let error = rx.blocking_recv().unwrap().unwrap_err();
        assert_eq!(error.status, Status::Timeout);
        assert!(
            error.message.contains(crate::OPTION_RPC_TIMEOUT_FETCH),
            "{}",
            error.message
        );
        // The errored source closes the channel and the task ends.
        assert!(rx.blocking_recv().is_none());
        runtime.block_on(task).unwrap();
    }
}

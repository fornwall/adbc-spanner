use super::*;
use adbc_core::options::OptionValue;

use crate::options::{F64Range, f64_option};
use crate::runtime::{
    CancelSignal, ChunkSource, block_on_cancellable, new_runtime, spawn_prefetch,
};

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

/// The three arms of `impl_shared_option_dispatch` that set these fields, as functions the tests
/// can call: each parses the value with `f64_option` in the non-negative-seconds range and stores
/// it, leaving the previous value in place when the parse is rejected.
fn set_query(config: &mut RpcTimeouts, value: OptionValue) -> Result<()> {
    config.query = f64_option(
        value,
        crate::OPTION_RPC_TIMEOUT_QUERY,
        F64Range::NonNegativeSeconds,
    )?;
    Ok(())
}

fn set_update(config: &mut RpcTimeouts, value: OptionValue) -> Result<()> {
    config.update = f64_option(
        value,
        crate::OPTION_RPC_TIMEOUT_UPDATE,
        F64Range::NonNegativeSeconds,
    )?;
    Ok(())
}

fn set_fetch(config: &mut RpcTimeouts, value: OptionValue) -> Result<()> {
    config.fetch = f64_option(
        value,
        crate::OPTION_RPC_TIMEOUT_FETCH,
        F64Range::NonNegativeSeconds,
    )?;
    Ok(())
}

/// The canonical `get_option` string of a stored seconds value, as the dispatch's getter arms
/// render it.
fn string(value: Option<f64>) -> Option<String> {
    value.map(|s| s.to_string())
}

#[test]
fn parses_numeric_strings_ints_and_doubles() {
    let mut config = RpcTimeouts::default();
    // Numeric strings (trimmed, fractions allowed).
    set_query(&mut config, s(" 2.5 ")).unwrap();
    assert_eq!(string(config.query).as_deref(), Some("2.5"));
    assert_eq!(config.query_timeout(), Some(Duration::from_millis(2500)));
    // Integers.
    set_update(&mut config, OptionValue::Int(30)).unwrap();
    assert_eq!(string(config.update).as_deref(), Some("30"));
    assert_eq!(config.update_timeout(), Some(Duration::from_secs(30)));
    // Doubles (the `get_option_double` / `set_option_double` shape).
    set_fetch(&mut config, OptionValue::Double(0.05)).unwrap();
    assert_eq!(string(config.fetch).as_deref(), Some("0.05"));
    assert_eq!(config.fetch_timeout(), Some(Duration::from_millis(50)));
}

#[test]
fn zero_disables_but_still_round_trips() {
    let mut config = RpcTimeouts::default();
    for value in [s("0"), OptionValue::Int(0), OptionValue::Double(0.0)] {
        set_query(&mut config, value).unwrap();
        // The stored value reports back...
        assert_eq!(string(config.query).as_deref(), Some("0"));
        // ...but no deadline is enforced.
        assert_eq!(config.query_timeout(), None);
    }
}

#[test]
fn empty_string_unsets() {
    let mut config = RpcTimeouts::default();
    set_fetch(&mut config, s("1.5")).unwrap();
    assert!(string(config.fetch).is_some());
    set_fetch(&mut config, s("")).unwrap();
    assert_eq!(string(config.fetch), None);
    assert_eq!(config.fetch_timeout(), None);
    // Whitespace-only counts as empty too (values are trimmed).
    set_fetch(&mut config, s("2")).unwrap();
    set_fetch(&mut config, s("  ")).unwrap();
    assert_eq!(string(config.fetch), None);
}

#[test]
fn rejects_nan_infinities_negatives_and_garbage() {
    let mut config = RpcTimeouts::default();
    set_query(&mut config, s("5")).unwrap();
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
        let error = set_query(&mut config, value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        assert!(
            error.message.contains(crate::OPTION_RPC_TIMEOUT_QUERY),
            "{}",
            error.message
        );
        // A rejected value leaves the stored one untouched.
        assert_eq!(
            string(config.query).as_deref(),
            Some("5"),
            "value {value:?}"
        );
    }
}

#[test]
fn the_three_timeouts_are_independent() {
    let mut config = RpcTimeouts::default();
    set_query(&mut config, s("1")).unwrap();
    set_update(&mut config, s("2")).unwrap();
    set_fetch(&mut config, s("3")).unwrap();
    set_update(&mut config, s("")).unwrap();
    assert_eq!(string(config.query).as_deref(), Some("1"));
    assert_eq!(string(config.update), None);
    assert_eq!(string(config.fetch).as_deref(), Some("3"));
}

/// Statement inheritance is a plain clone of the connection's config (mirroring
/// `ReadStaleness` / `RequestConfig`): the clone starts with the connection's values and
/// overrides independently.
#[test]
fn cloned_config_inherits_then_overrides_independently() {
    let mut connection = RpcTimeouts::default();
    set_query(&mut connection, s("10")).unwrap();
    set_fetch(&mut connection, s("20")).unwrap();

    let mut statement = connection;
    assert_eq!(string(statement.query).as_deref(), Some("10"));
    assert_eq!(string(statement.fetch).as_deref(), Some("20"));

    set_query(&mut statement, s("1.5")).unwrap();
    set_fetch(&mut statement, s("")).unwrap();
    assert_eq!(string(statement.query).as_deref(), Some("1.5"));
    assert_eq!(string(statement.fetch), None);
    // The connection is unaffected by statement-level overrides.
    assert_eq!(string(connection.query).as_deref(), Some("10"));
    assert_eq!(string(connection.fetch).as_deref(), Some("20"));
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

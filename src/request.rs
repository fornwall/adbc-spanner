//! Request priority and request/transaction tag options.
//!
//! Spanner lets every request carry [`RequestOptions`](google_cloud_spanner::model::RequestOptions):
//! a **priority** (`PRIORITY_LOW` / `PRIORITY_MEDIUM` / `PRIORITY_HIGH`) that Spanner's scheduler
//! uses to arbitrate CPU between workloads, a free-form **request tag** for
//! [troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
//! (surfaced in query and transaction statistics), and a **transaction tag** attached to every
//! operation of a read/write transaction. This module parses the three driver options that expose
//! them and applies the stored values onto the client's builders:
//!
//! - [`OPTION_REQUEST_PRIORITY`](crate::OPTION_REQUEST_PRIORITY) (`spanner.request.priority`) —
//!   `low` / `medium` / `high` (case-insensitive). Applied to every query/DML statement the driver
//!   builds and, as the commit priority, to every read/write transaction runner. Connection and
//!   statement level.
//! - [`OPTION_REQUEST_TAG`](crate::OPTION_REQUEST_TAG) (`spanner.request.tag`) — a free-form
//!   per-request tag, applied to every statement and `ExecuteBatchDml` batch the driver builds.
//!   Connection and statement level.
//! - [`OPTION_TRANSACTION_TAG`](crate::OPTION_TRANSACTION_TAG) (`spanner.transaction.tag`) — a
//!   free-form per-transaction tag, applied wherever a read/write transaction runner is built
//!   (autocommit DML, the manual-mode commit, ingest commits). Connection level only.
//! - [`OPTION_MAX_COMMIT_DELAY`](crate::OPTION_MAX_COMMIT_DELAY) (`spanner.max_commit_delay`) — the
//!   maximum amount of time Spanner may delay a **commit** to batch it with others (a
//!   throughput-for-latency trade-off). A duration in `0..=500ms`, applied at every read/write
//!   commit site the runner / write-only builders cover (autocommit DML, the `ExecuteBatchDml`
//!   batch runner, the manual-mode commit, and the bulk-ingest write-only transaction).
//!   Connection and statement level.
//!
//! Like the read-staleness options, the connection's values become the default for statements it
//! creates (which may override them), setting an empty string unsets a value, and every option
//! round-trips through `get_option`. Driver-internal metadata queries (`get_objects`,
//! `get_table_schema` probes, …) are deliberately left untagged — the options cover the user's own
//! statements.

use std::time::Duration;

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_spanner::builder::{
    BatchDmlBuilder, TransactionRunnerBuilder, WriteOnlyTransactionBuilder,
};
use google_cloud_spanner::model::request_options::Priority;
use google_cloud_spanner::statement::StatementBuilder;
use google_cloud_wkt::Duration as WktDuration;

use crate::error::invalid_argument;
use crate::staleness::parse_duration;

/// Spanner caps `max_commit_delay` at 500 milliseconds (values above are rejected server-side); we
/// validate the same bound at set time for a clean `InvalidArguments` instead of a commit error.
const MAX_COMMIT_DELAY_CAP: Duration = Duration::from_millis(500);

/// A parsed `spanner.request.priority` value. A driver-owned enum (rather than the client's
/// non-exhaustive [`Priority`]) so the canonical option string can be recovered exactly for
/// `get_option` and the parsing is unit-testable offline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestPriority {
    Low,
    Medium,
    High,
}

impl RequestPriority {
    /// The canonical option string, for `get_option` round-trip.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RequestPriority::Low => "low",
            RequestPriority::Medium => "medium",
            RequestPriority::High => "high",
        }
    }

    /// The client's [`Priority`] for this value.
    fn to_client(self) -> Priority {
        match self {
            RequestPriority::Low => Priority::Low,
            RequestPriority::Medium => Priority::Medium,
            RequestPriority::High => Priority::High,
        }
    }
}

/// Parse a `spanner.request.priority` value: `low` / `medium` / `high`, case-insensitive.
pub(crate) fn parse_priority(value: &str) -> Result<RequestPriority> {
    match value.to_ascii_lowercase().as_str() {
        "low" => Ok(RequestPriority::Low),
        "medium" => Ok(RequestPriority::Medium),
        "high" => Ok(RequestPriority::High),
        other => Err(invalid_argument(format!(
            "unknown request priority {other:?}; expected \"low\", \"medium\" or \"high\" \
             (or an empty string to unset)"
        ))),
    }
}

/// The request priority / tag configuration held by a connection or statement.
///
/// A connection's value is cloned into each statement it creates (which may then override the
/// priority and request tag; the transaction tag stays connection-level), mirroring how
/// [`ReadStaleness`](crate::staleness::ReadStaleness) is inherited.
#[derive(Debug, Clone, Default)]
pub(crate) struct RequestConfig {
    /// Parsed `spanner.request.priority`, when set (`None` leaves the client/service default).
    priority: Option<RequestPriority>,
    /// Raw `spanner.request.tag` value, when set.
    request_tag: Option<String>,
    /// Raw `spanner.transaction.tag` value, when set (connection-level only).
    transaction_tag: Option<String>,
    /// Parsed `spanner.max_commit_delay`, with the raw option string kept for `get_option`
    /// round-trip. Applied as the commit delay wherever a read/write commit is built.
    max_commit_delay: Option<(String, Duration)>,
}

impl RequestConfig {
    /// Handle a `set_option` for `spanner.request.priority`. An empty value unsets it.
    pub(crate) fn set_priority(&mut self, value: OptionValue) -> Result<()> {
        let raw = as_string(value)?;
        let trimmed = raw.trim();
        self.priority = if trimmed.is_empty() {
            None
        } else {
            Some(parse_priority(trimmed)?)
        };
        Ok(())
    }

    /// Handle a `set_option` for `spanner.request.tag`. An empty value unsets it.
    pub(crate) fn set_request_tag(&mut self, value: OptionValue) -> Result<()> {
        self.request_tag = non_empty(as_string(value)?);
        Ok(())
    }

    /// Handle a `set_option` for `spanner.transaction.tag`. An empty value unsets it.
    pub(crate) fn set_transaction_tag(&mut self, value: OptionValue) -> Result<()> {
        self.transaction_tag = non_empty(as_string(value)?);
        Ok(())
    }

    /// Handle a `set_option` for `spanner.max_commit_delay`. An empty value unsets it; a malformed
    /// value or one outside `0..=500ms` is rejected with `InvalidArguments`.
    pub(crate) fn set_max_commit_delay(&mut self, value: OptionValue) -> Result<()> {
        let raw = as_string(value)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            self.max_commit_delay = None;
            return Ok(());
        }
        let duration = parse_max_commit_delay(trimmed)?;
        self.max_commit_delay = Some((trimmed.to_string(), duration));
        Ok(())
    }

    /// The canonical `spanner.request.priority` value, for `get_option` round-trip.
    pub(crate) fn priority_string(&self) -> Option<&'static str> {
        self.priority.map(RequestPriority::as_str)
    }

    /// The raw `spanner.request.tag` value, for `get_option` round-trip.
    pub(crate) fn request_tag_string(&self) -> Option<&str> {
        self.request_tag.as_deref()
    }

    /// The raw `spanner.transaction.tag` value, for `get_option` round-trip.
    pub(crate) fn transaction_tag_string(&self) -> Option<&str> {
        self.transaction_tag.as_deref()
    }

    /// The raw `spanner.max_commit_delay` value, for `get_option` round-trip.
    pub(crate) fn max_commit_delay_string(&self) -> Option<&str> {
        self.max_commit_delay.as_ref().map(|(raw, _)| raw.as_str())
    }

    /// The commit delay as the client's [`WktDuration`], when set. The conversion cannot fail — the
    /// stored value was validated to `0..=500ms` at set time.
    fn commit_delay(&self) -> Option<WktDuration> {
        self.max_commit_delay
            .as_ref()
            .and_then(|(_, d)| WktDuration::try_from(*d).ok())
    }

    /// Apply the priority and request tag to a statement builder (queries and DML alike).
    pub(crate) fn apply_to_statement(&self, mut builder: StatementBuilder) -> StatementBuilder {
        if let Some(priority) = self.priority {
            builder = builder.set_priority(priority.to_client());
        }
        if let Some(tag) = &self.request_tag {
            builder = builder.set_request_tag(tag.as_str());
        }
        builder
    }

    /// Apply the request tag to an `ExecuteBatchDml` batch builder. (The batch request carries a
    /// single request-level tag; the client exposes no batch-level priority setter — the runner's
    /// commit priority, from [`Self::apply_to_runner`], covers the transaction's commit instead.)
    pub(crate) fn apply_to_batch_dml(&self, mut builder: BatchDmlBuilder) -> BatchDmlBuilder {
        if let Some(tag) = &self.request_tag {
            builder = builder.set_request_tag(tag.as_str());
        }
        builder
    }

    /// Apply the commit priority, transaction tag and commit delay to a read/write transaction
    /// runner builder.
    pub(crate) fn apply_to_runner(
        &self,
        mut builder: TransactionRunnerBuilder,
    ) -> TransactionRunnerBuilder {
        if let Some(priority) = self.priority {
            builder = builder.set_commit_priority(priority.to_client());
        }
        if let Some(tag) = &self.transaction_tag {
            builder = builder.set_transaction_tag(tag.as_str());
        }
        if let Some(delay) = self.commit_delay() {
            builder = builder.set_max_commit_delay(delay);
        }
        builder
    }

    /// Apply the commit priority, transaction tag and commit delay to a write-only transaction
    /// builder (the mutation-based bulk-ingest commit path).
    pub(crate) fn apply_to_write_only(
        &self,
        mut builder: WriteOnlyTransactionBuilder,
    ) -> WriteOnlyTransactionBuilder {
        if let Some(priority) = self.priority {
            builder = builder.set_commit_priority(priority.to_client());
        }
        if let Some(tag) = &self.transaction_tag {
            builder = builder.set_transaction_tag(tag.as_str());
        }
        if let Some(delay) = self.commit_delay() {
            builder = builder.set_max_commit_delay(delay);
        }
        builder
    }
}

/// Parse a `spanner.max_commit_delay` value: a duration (the staleness duration grammar — `s`
/// default, `ms`, `us`/`µs`, `ns`, `m`, `h`) that must fall within Spanner's `0..=500ms` range.
pub(crate) fn parse_max_commit_delay(value: &str) -> Result<Duration> {
    let duration = parse_duration(value)?;
    if duration > MAX_COMMIT_DELAY_CAP {
        return Err(invalid_argument(format!(
            "spanner.max_commit_delay {value:?} exceeds Spanner's maximum of 500ms"
        )));
    }
    Ok(duration)
}

/// Extract a string from an option value, erroring on any other value kind.
fn as_string(value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(invalid_argument(
            "request priority/tag options require a string value",
        )),
    }
}

/// `None` for an empty string (the "unset" spelling), `Some` otherwise. Tags are free-form, so a
/// non-empty value is stored verbatim (no trimming).
fn non_empty(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    fn s(v: &str) -> OptionValue {
        OptionValue::String(v.to_string())
    }

    #[test]
    fn parses_priorities_case_insensitively() {
        for (input, expected) in [
            ("low", RequestPriority::Low),
            ("LOW", RequestPriority::Low),
            ("medium", RequestPriority::Medium),
            ("Medium", RequestPriority::Medium),
            ("high", RequestPriority::High),
            ("HIGH", RequestPriority::High),
        ] {
            assert_eq!(parse_priority(input).unwrap(), expected, "{input}");
        }
    }

    #[test]
    fn rejects_unknown_priorities() {
        for bad in ["urgent", "0", "priority_high", "hi gh", "médium"] {
            let error = parse_priority(bad).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{bad}");
        }
    }

    #[test]
    fn priority_round_trips_and_unsets() {
        let mut config = RequestConfig::default();
        assert_eq!(config.priority_string(), None);

        // Set (case-insensitive, surrounding whitespace tolerated), reported canonically.
        config.set_priority(s(" HIGH ")).unwrap();
        assert_eq!(config.priority_string(), Some("high"));
        config.set_priority(s("medium")).unwrap();
        assert_eq!(config.priority_string(), Some("medium"));

        // A bad value is rejected and leaves the stored value untouched.
        let error = config.set_priority(s("urgent")).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(config.priority_string(), Some("medium"));

        // An empty string unsets.
        config.set_priority(s("")).unwrap();
        assert_eq!(config.priority_string(), None);
    }

    #[test]
    fn tags_round_trip_verbatim_and_unset_on_empty() {
        let mut config = RequestConfig::default();
        assert_eq!(config.request_tag_string(), None);
        assert_eq!(config.transaction_tag_string(), None);

        // Free-form values are stored verbatim (no trimming or case folding).
        config.set_request_tag(s(" my-App/query=1 ")).unwrap();
        assert_eq!(config.request_tag_string(), Some(" my-App/query=1 "));
        config.set_transaction_tag(s("nightly-etl")).unwrap();
        assert_eq!(config.transaction_tag_string(), Some("nightly-etl"));

        // The two tags are independent.
        config.set_request_tag(s("")).unwrap();
        assert_eq!(config.request_tag_string(), None);
        assert_eq!(config.transaction_tag_string(), Some("nightly-etl"));
        config.set_transaction_tag(s("")).unwrap();
        assert_eq!(config.transaction_tag_string(), None);
    }

    #[test]
    fn non_string_values_are_rejected() {
        let mut config = RequestConfig::default();
        for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
            let error = config.set_priority(value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            let error = config.set_request_tag(value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            let error = config.set_transaction_tag(value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            let error = config.set_max_commit_delay(value).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
    }

    #[test]
    fn parses_max_commit_delay_with_units_and_enforces_the_500ms_cap() {
        // The staleness duration grammar (default seconds plus unit suffixes) applies.
        assert_eq!(
            parse_max_commit_delay("0.2s").unwrap(),
            Duration::from_millis(200)
        );
        assert_eq!(
            parse_max_commit_delay("200ms").unwrap(),
            Duration::from_millis(200)
        );
        assert_eq!(parse_max_commit_delay("0").unwrap(), Duration::ZERO);
        // The boundary is inclusive.
        assert_eq!(
            parse_max_commit_delay("500ms").unwrap(),
            Duration::from_millis(500)
        );
        // Above 500ms, negative, and malformed values are all rejected.
        for bad in ["501ms", "1s", "0.6s", "-1ms", "abc", "1x"] {
            let error = parse_max_commit_delay(bad).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{bad}");
        }
    }

    #[test]
    fn max_commit_delay_round_trips_and_unsets() {
        let mut config = RequestConfig::default();
        assert_eq!(config.max_commit_delay_string(), None);
        assert!(config.commit_delay().is_none());

        // The raw value round-trips verbatim (surrounding whitespace trimmed), and a client
        // WktDuration is produced.
        config.set_max_commit_delay(s(" 100ms ")).unwrap();
        assert_eq!(config.max_commit_delay_string(), Some("100ms"));
        assert_eq!(
            config.commit_delay(),
            Some(WktDuration::try_from(Duration::from_millis(100)).unwrap())
        );

        // A rejected value leaves the stored one untouched.
        let error = config.set_max_commit_delay(s("2s")).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(config.max_commit_delay_string(), Some("100ms"));

        // An empty string unsets.
        config.set_max_commit_delay(s("")).unwrap();
        assert_eq!(config.max_commit_delay_string(), None);
        assert!(config.commit_delay().is_none());
    }

    /// Statement inheritance is a plain clone of the connection's config (mirroring
    /// `ReadStaleness`): the clone starts with the connection's values and overrides independently.
    #[test]
    fn cloned_config_inherits_then_overrides_independently() {
        let mut connection = RequestConfig::default();
        connection.set_priority(s("low")).unwrap();
        connection.set_request_tag(s("conn-tag")).unwrap();
        connection.set_transaction_tag(s("txn-tag")).unwrap();
        connection.set_max_commit_delay(s("100ms")).unwrap();

        let mut statement = connection.clone();
        assert_eq!(statement.priority_string(), Some("low"));
        assert_eq!(statement.request_tag_string(), Some("conn-tag"));
        assert_eq!(statement.transaction_tag_string(), Some("txn-tag"));
        assert_eq!(statement.max_commit_delay_string(), Some("100ms"));

        statement.set_priority(s("high")).unwrap();
        statement.set_request_tag(s("")).unwrap();
        statement.set_max_commit_delay(s("250ms")).unwrap();
        assert_eq!(statement.priority_string(), Some("high"));
        assert_eq!(statement.request_tag_string(), None);
        assert_eq!(statement.max_commit_delay_string(), Some("250ms"));
        // The connection is unaffected by statement-level overrides.
        assert_eq!(connection.priority_string(), Some("low"));
        assert_eq!(connection.request_tag_string(), Some("conn-tag"));
        assert_eq!(connection.max_commit_delay_string(), Some("100ms"));
    }
}

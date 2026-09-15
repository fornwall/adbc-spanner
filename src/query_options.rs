//! Per-query optimizer options.
//!
//! Spanner lets every query carry
//! [`QueryOptions`](google_cloud_spanner::model::execute_sql_request::QueryOptions) that select the
//! query optimizer's behaviour: an **optimizer version** (a version string such as `"6"` or
//! `"latest"`) and an **optimizer statistics package** (a named statistics package to plan against).
//! This module parses the two driver options that expose them and applies the stored values onto the
//! query statement builder:
//!
//! - [`OPTION_QUERY_OPTIMIZER_VERSION`](crate::OPTION_QUERY_OPTIMIZER_VERSION)
//!   (`spanner.query.optimizer_version`) — the optimizer version. Connection and statement level.
//! - [`OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE`](crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE)
//!   (`spanner.query.optimizer_statistics_package`) — the optimizer statistics package. Connection
//!   and statement level.
//!
//! Like the read-staleness and request-tag options, the connection's values become the default for
//! statements it creates (which may override them), setting an empty string unsets a value, and
//! every option round-trips through `get_option`. The values are opaque strings passed through to
//! Spanner unchanged; the driver validates only that the option is a string.

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_spanner::model::execute_sql_request::QueryOptions;
use google_cloud_spanner::statement::StatementBuilder;

use crate::options::non_empty_string_option;

/// The query optimizer options held by a connection or statement.
///
/// A connection's value is cloned into each statement it creates (which may then override either
/// field independently), mirroring how [`RequestConfig`](crate::request::RequestConfig) and
/// [`ReadStaleness`](crate::staleness::ReadStaleness) are inherited.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryOptionsConfig {
    /// Raw `spanner.query.optimizer_version` value, when set.
    optimizer_version: Option<String>,
    /// Raw `spanner.query.optimizer_statistics_package` value, when set.
    optimizer_statistics_package: Option<String>,
}

impl QueryOptionsConfig {
    /// Handle a `set_option` for `spanner.query.optimizer_version`. An empty value unsets it.
    pub(crate) fn set_optimizer_version(&mut self, value: OptionValue) -> Result<()> {
        self.optimizer_version =
            non_empty_string_option(value, crate::OPTION_QUERY_OPTIMIZER_VERSION)?;
        Ok(())
    }

    /// Handle a `set_option` for `spanner.query.optimizer_statistics_package`. An empty value unsets
    /// it.
    pub(crate) fn set_optimizer_statistics_package(&mut self, value: OptionValue) -> Result<()> {
        self.optimizer_statistics_package =
            non_empty_string_option(value, crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE)?;
        Ok(())
    }

    /// The raw `spanner.query.optimizer_version` value, for `get_option` round-trip.
    pub(crate) fn optimizer_version_string(&self) -> Option<&str> {
        self.optimizer_version.as_deref()
    }

    /// The raw `spanner.query.optimizer_statistics_package` value, for `get_option` round-trip.
    pub(crate) fn optimizer_statistics_package_string(&self) -> Option<&str> {
        self.optimizer_statistics_package.as_deref()
    }

    /// Apply the optimizer options to a query statement builder. A no-op when neither is set, so an
    /// unset config leaves the request's query options empty (the service default optimizer).
    #[must_use]
    pub(crate) fn apply_to_statement(&self, builder: StatementBuilder) -> StatementBuilder {
        if self.optimizer_version.is_none() && self.optimizer_statistics_package.is_none() {
            return builder;
        }
        let mut options = QueryOptions::default();
        if let Some(version) = &self.optimizer_version {
            options = options.set_optimizer_version(version.as_str());
        }
        if let Some(package) = &self.optimizer_statistics_package {
            options = options.set_optimizer_statistics_package(package.as_str());
        }
        builder.set_query_options(options)
    }
}

#[cfg(test)]
mod tests;

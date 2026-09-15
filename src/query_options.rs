//! Per-query optimizer options.
//!
//! Spanner lets every query carry
//! [`QueryOptions`](google_cloud_spanner::model::execute_sql_request::QueryOptions) selecting an
//! **optimizer version** and an **optimizer statistics package**. This module parses the two driver
//! options that expose them ([`OPTION_QUERY_OPTIMIZER_VERSION`](crate::OPTION_QUERY_OPTIMIZER_VERSION)
//! and
//! [`OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE`](crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE))
//! and applies them onto the query statement builder. Both values are opaque strings passed through
//! to Spanner unchanged; the driver validates only that the option is a string.

use google_cloud_spanner::model::execute_sql_request::QueryOptions;
use google_cloud_spanner::statement::StatementBuilder;

/// The query optimizer options held by a connection or statement.
///
/// A connection's value is cloned into each statement it creates (which may then override either
/// field independently), mirroring how [`RequestConfig`](crate::request::RequestConfig) and
/// [`ReadStaleness`](crate::staleness::ReadStaleness) are inherited.
///
/// Both fields are set and read directly by
/// [`impl_shared_option_dispatch`](crate::options::impl_shared_option_dispatch), which parses each
/// with [`non_empty_string_option`](crate::options::non_empty_string_option).
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryOptionsConfig {
    /// Raw `spanner.query.optimizer_version` value, when set.
    pub(crate) optimizer_version: Option<String>,
    /// Raw `spanner.query.optimizer_statistics_package` value, when set.
    pub(crate) optimizer_statistics_package: Option<String>,
}

impl QueryOptionsConfig {
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

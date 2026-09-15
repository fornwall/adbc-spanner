use super::*;
use adbc_core::error::{Result, Status};
use adbc_core::options::OptionValue;

use crate::options::non_empty_string_option;

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

/// The two arms of `impl_shared_option_dispatch` that set these fields, as functions the tests can
/// call: each parses the value with `non_empty_string_option` and stores it.
fn set_optimizer_version(config: &mut QueryOptionsConfig, value: OptionValue) -> Result<()> {
    config.optimizer_version =
        non_empty_string_option(value, crate::OPTION_QUERY_OPTIMIZER_VERSION)?;
    Ok(())
}

fn set_optimizer_statistics_package(
    config: &mut QueryOptionsConfig,
    value: OptionValue,
) -> Result<()> {
    config.optimizer_statistics_package =
        non_empty_string_option(value, crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE)?;
    Ok(())
}

#[test]
fn options_round_trip_verbatim_and_unset_on_empty() {
    let mut config = QueryOptionsConfig::default();
    assert_eq!(config.optimizer_version.as_deref(), None);
    assert_eq!(config.optimizer_statistics_package.as_deref(), None);

    // Opaque values are stored verbatim (no trimming or case folding).
    set_optimizer_version(&mut config, s("latest")).unwrap();
    assert_eq!(config.optimizer_version.as_deref(), Some("latest"));
    set_optimizer_statistics_package(&mut config, s("auto_20240101")).unwrap();
    assert_eq!(
        config.optimizer_statistics_package.as_deref(),
        Some("auto_20240101")
    );

    // The two fields are independent.
    set_optimizer_version(&mut config, s("")).unwrap();
    assert_eq!(config.optimizer_version.as_deref(), None);
    assert_eq!(
        config.optimizer_statistics_package.as_deref(),
        Some("auto_20240101")
    );
    set_optimizer_statistics_package(&mut config, s("")).unwrap();
    assert_eq!(config.optimizer_statistics_package.as_deref(), None);
}

#[test]
fn non_string_values_are_rejected() {
    let mut config = QueryOptionsConfig::default();
    for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
        let error = set_optimizer_version(&mut config, value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        let error = set_optimizer_statistics_package(&mut config, value).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

/// Statement inheritance is a plain clone of the connection's config (mirroring `RequestConfig`):
/// the clone starts with the connection's values and overrides each field independently.
#[test]
fn cloned_config_inherits_then_overrides_independently() {
    let mut connection = QueryOptionsConfig::default();
    set_optimizer_version(&mut connection, s("6")).unwrap();
    set_optimizer_statistics_package(&mut connection, s("pkg_conn")).unwrap();

    let mut statement = connection.clone();
    assert_eq!(statement.optimizer_version.as_deref(), Some("6"));
    assert_eq!(
        statement.optimizer_statistics_package.as_deref(),
        Some("pkg_conn")
    );

    set_optimizer_version(&mut statement, s("latest")).unwrap();
    set_optimizer_statistics_package(&mut statement, s("")).unwrap();
    assert_eq!(statement.optimizer_version.as_deref(), Some("latest"));
    assert_eq!(statement.optimizer_statistics_package.as_deref(), None);
    // The connection is unaffected by statement-level overrides.
    assert_eq!(connection.optimizer_version.as_deref(), Some("6"));
    assert_eq!(
        connection.optimizer_statistics_package.as_deref(),
        Some("pkg_conn")
    );
}

/// `apply_to_statement` leaves the builder alone when nothing is set, and is callable when set
/// (we can't inspect the built request offline, but exercising the setter path guards the client
/// API surface the driver relies on).
#[test]
fn apply_to_statement_is_a_noop_when_unset() {
    let config = QueryOptionsConfig::default();
    // Both an unset and a fully-set config build without panicking.
    let _ = config.apply_to_statement(google_cloud_spanner::statement::Statement::builder(
        "SELECT 1",
    ));
    let mut set = QueryOptionsConfig::default();
    set_optimizer_version(&mut set, s("latest")).unwrap();
    set_optimizer_statistics_package(&mut set, s("pkg")).unwrap();
    let _ = set.apply_to_statement(google_cloud_spanner::statement::Statement::builder(
        "SELECT 1",
    ));
}

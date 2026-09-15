use super::*;
use adbc_core::error::Status;

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

#[test]
fn options_round_trip_verbatim_and_unset_on_empty() {
    let mut config = QueryOptionsConfig::default();
    assert_eq!(config.optimizer_version_string(), None);
    assert_eq!(config.optimizer_statistics_package_string(), None);

    // Opaque values are stored verbatim (no trimming or case folding).
    config.set_optimizer_version(s("latest")).unwrap();
    assert_eq!(config.optimizer_version_string(), Some("latest"));
    config
        .set_optimizer_statistics_package(s("auto_20240101"))
        .unwrap();
    assert_eq!(
        config.optimizer_statistics_package_string(),
        Some("auto_20240101")
    );

    // The two fields are independent.
    config.set_optimizer_version(s("")).unwrap();
    assert_eq!(config.optimizer_version_string(), None);
    assert_eq!(
        config.optimizer_statistics_package_string(),
        Some("auto_20240101")
    );
    config.set_optimizer_statistics_package(s("")).unwrap();
    assert_eq!(config.optimizer_statistics_package_string(), None);
}

#[test]
fn non_string_values_are_rejected() {
    let mut config = QueryOptionsConfig::default();
    for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
        let error = config.set_optimizer_version(value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        let error = config.set_optimizer_statistics_package(value).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

/// Statement inheritance is a plain clone of the connection's config (mirroring `RequestConfig`):
/// the clone starts with the connection's values and overrides each field independently.
#[test]
fn cloned_config_inherits_then_overrides_independently() {
    let mut connection = QueryOptionsConfig::default();
    connection.set_optimizer_version(s("6")).unwrap();
    connection
        .set_optimizer_statistics_package(s("pkg_conn"))
        .unwrap();

    let mut statement = connection.clone();
    assert_eq!(statement.optimizer_version_string(), Some("6"));
    assert_eq!(
        statement.optimizer_statistics_package_string(),
        Some("pkg_conn")
    );

    statement.set_optimizer_version(s("latest")).unwrap();
    statement.set_optimizer_statistics_package(s("")).unwrap();
    assert_eq!(statement.optimizer_version_string(), Some("latest"));
    assert_eq!(statement.optimizer_statistics_package_string(), None);
    // The connection is unaffected by statement-level overrides.
    assert_eq!(connection.optimizer_version_string(), Some("6"));
    assert_eq!(
        connection.optimizer_statistics_package_string(),
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
    set.set_optimizer_version(s("latest")).unwrap();
    set.set_optimizer_statistics_package(s("pkg")).unwrap();
    let _ = set.apply_to_statement(google_cloud_spanner::statement::Statement::builder(
        "SELECT 1",
    ));
}

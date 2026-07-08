//! Detecting the SQL dialect of the connected Spanner database.
//!
//! This driver emits **GoogleSQL** (`GOOGLE_STANDARD_SQL`) exclusively. A Spanner database can
//! instead be created with the **PostgreSQL** dialect, which speaks a different SQL and a different
//! `INFORMATION_SCHEMA`; run against such a database the driver would silently misbehave. So each
//! connection probes the dialect once, up front (see [`crate::driver::SpannerDatabase::connect`]),
//! and rejects an unsupported one with a clear [`Status::NotImplemented`] error instead of letting
//! it fail obscurely later.
//!
//! [`Status::NotImplemented`]: adbc_core::error::Status::NotImplemented

use adbc_core::error::Result;

use crate::error::not_implemented;

/// Whether a Spanner database dialect — named by its canonical enum name, as returned by the admin
/// `GetDatabase` `database_dialect` field — is one this driver supports.
///
/// The driver emits GoogleSQL only, so `GOOGLE_STANDARD_SQL` is supported. The proto default,
/// `DATABASE_DIALECT_UNSPECIFIED`, also maps to GoogleSQL (a database created without an explicit
/// dialect is GoogleSQL), so it is treated as supported too. Every other dialect — notably
/// `POSTGRESQL` — is not.
pub(crate) fn is_supported_dialect(name: &str) -> bool {
    matches!(name, "GOOGLE_STANDARD_SQL" | "DATABASE_DIALECT_UNSPECIFIED")
}

/// Classify a reported database dialect: `Ok(())` if the driver supports it (see
/// [`is_supported_dialect`]), otherwise a clear [`Status::NotImplemented`] error naming the dialect.
///
/// `dialect` is the dialect's display name (the admin enum's `Display`), used both to classify and,
/// on rejection, to name the offending dialect in the error message.
///
/// [`Status::NotImplemented`]: adbc_core::error::Status::NotImplemented
pub(crate) fn check_supported(dialect: &str) -> Result<()> {
    if is_supported_dialect(dialect) {
        Ok(())
    } else {
        Err(not_implemented(&format!(
            "the {dialect} database dialect (the Spanner ADBC driver emits GoogleSQL / \
             GOOGLE_STANDARD_SQL only)"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;

    #[test]
    fn googlesql_and_unspecified_are_supported() {
        // The driver emits GoogleSQL; the proto default resolves to GoogleSQL, so both pass.
        assert!(is_supported_dialect("GOOGLE_STANDARD_SQL"));
        assert!(is_supported_dialect("DATABASE_DIALECT_UNSPECIFIED"));
        assert!(check_supported("GOOGLE_STANDARD_SQL").is_ok());
        assert!(check_supported("DATABASE_DIALECT_UNSPECIFIED").is_ok());
    }

    #[test]
    fn postgresql_is_rejected_with_a_clear_error() {
        assert!(!is_supported_dialect("POSTGRESQL"));
        let error = check_supported("POSTGRESQL").unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        // The message names the offending dialect and points at the supported one.
        assert!(error.message.contains("POSTGRESQL"), "{}", error.message);
        assert!(
            error.message.contains("GOOGLE_STANDARD_SQL"),
            "{}",
            error.message
        );
    }

    #[test]
    fn an_unknown_or_future_dialect_is_rejected() {
        // Anything the driver does not explicitly speak fails fast rather than misbehaving; the
        // (numeric, for an unknown enum value) name is still surfaced.
        assert!(!is_supported_dialect("SOME_FUTURE_DIALECT"));
        assert!(!is_supported_dialect("3"));
        let error = check_supported("3").unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
    }
}

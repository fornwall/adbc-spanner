//! # adbc-spanner
//!
//! An [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver for
//! [Google Cloud Spanner](https://cloud.google.com/spanner), built on top of the official
//! `google-cloud-spanner` preview client and the native Rust [`adbc_core`] traits.
//!
//! The driver exposes Spanner through the standard ADBC object hierarchy:
//!
//! ```text
//! SpannerDriver ──> SpannerDatabase ──> SpannerConnection ──> SpannerStatement
//! ```
//!
//! Query results are returned as Arrow [`RecordBatch`](arrow_array::RecordBatch)es, so they can be
//! consumed by any Arrow-native tool without an intermediate row-by-row copy.
//!
//! ## Configuration
//!
//! A database is configured through ADBC options. The Spanner database path is required and can be
//! supplied either through the standard [`OptionDatabase::Uri`](adbc_core::options::OptionDatabase::Uri)
//! option or the driver-specific [`OPTION_DATABASE`] key:
//!
//! ```text
//! projects/<project>/instances/<instance>/databases/<database>
//! ```
//!
//! To talk to a Spanner emulator, either set the `SPANNER_EMULATOR_HOST` environment variable (the
//! driver picks it up automatically and uses anonymous credentials) or set the [`OPTION_ENDPOINT`]
//! and [`OPTION_EMULATOR`] options explicitly.
//!
//! ## Example
//!
//! ```no_run
//! use adbc_core::{Driver, Database, Connection, Statement};
//! use adbc_core::options::{OptionDatabase, OptionValue};
//! use adbc_spanner::{SpannerDriver, OPTION_DATABASE};
//! use arrow_array::RecordBatchReader;
//!
//! # fn main() -> adbc_core::error::Result<()> {
//! let mut driver = SpannerDriver::try_new()?;
//! let database = driver.new_database_with_opts([(
//!     OptionDatabase::Other(OPTION_DATABASE.into()),
//!     OptionValue::String("projects/p/instances/i/databases/d".into()),
//! )])?;
//! let mut connection = database.new_connection()?;
//! let mut statement = connection.new_statement()?;
//! statement.set_sql_query("SELECT 1 AS one")?;
//! let reader = statement.execute()?;
//! for batch in reader {
//!     let batch = batch?;
//!     println!("got {} rows", batch.num_rows());
//! }
//! # Ok(())
//! # }
//! ```

mod bind;
mod connection;
mod conversion;
mod ddl;
mod driver;
mod error;
#[cfg(feature = "ffi")]
mod ffi;
mod runtime;
mod statement;

pub use connection::SpannerConnection;
pub use driver::{SpannerDatabase, SpannerDriver};
pub use statement::SpannerStatement;

/// Driver-specific database option: the fully-qualified Spanner database path,
/// `projects/<project>/instances/<instance>/databases/<database>`.
///
/// Equivalent to setting [`OptionDatabase::Uri`](adbc_core::options::OptionDatabase::Uri).
pub const OPTION_DATABASE: &str = "adbc.spanner.database";

/// Driver-specific database option: an explicit gRPC endpoint (for example the address of a
/// Spanner emulator, `http://localhost:9010`). When unset the client connects to the production
/// Spanner service.
pub const OPTION_ENDPOINT: &str = "adbc.spanner.endpoint";

/// Driver-specific database option: when set to `true`, connect with anonymous credentials
/// (the mode used by the Spanner emulator). Automatically enabled when `SPANNER_EMULATOR_HOST`
/// is present in the environment.
pub const OPTION_EMULATOR: &str = "adbc.spanner.emulator";

/// Driver-specific database option: path to a service-account JSON key file to authenticate with
/// (dbt's `keyfile`). Overridden by [`OPTION_KEYFILE_JSON`] if both are set.
pub const OPTION_KEYFILE: &str = "adbc.spanner.keyfile";

/// Driver-specific database option: an inline service-account JSON key (dbt's `keyfile_json`).
///
/// When neither this nor [`OPTION_KEYFILE`] is set (and not connecting to an emulator), the driver
/// falls back to Application Default Credentials.
pub const OPTION_KEYFILE_JSON: &str = "adbc.spanner.keyfile_json";

/// The vendor name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const VENDOR_NAME: &str = "Google Cloud Spanner";

/// The driver name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const DRIVER_NAME: &str = "adbc-spanner";

/// The version of this driver.
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

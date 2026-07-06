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
mod info;
mod objects;
mod runtime;
mod statement;
mod statistics;

pub use connection::SpannerConnection;
pub use driver::{SpannerDatabase, SpannerDriver};
pub use statement::SpannerStatement;

/// Internal parsing helpers exposed for fuzz targets only (enable the `fuzzing` feature).
///
/// **Not** part of the public API — no stability guarantees.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing {
    /// Split a `;`-separated SQL batch into individual statements (quote/comment aware).
    pub fn split_statements(sql: &str) -> Vec<String> {
        crate::ddl::split_statements(sql)
    }
    /// Whether the SQL begins with a DDL statement.
    pub fn is_ddl(sql: &str) -> bool {
        crate::ddl::is_ddl(sql)
    }
    /// Parse a Spanner `DATE` string into Arrow `Date32` days.
    pub fn parse_date_days(s: &str) -> Option<i32> {
        crate::conversion::parse_date_days(s)
    }
    /// Parse a Spanner `TIMESTAMP` string into epoch microseconds.
    pub fn parse_timestamp_micros(s: &str) -> Option<i64> {
        crate::conversion::parse_timestamp_micros(s)
    }
    /// Parse a Spanner `NUMERIC` string into an unscaled `i128` (scale 9).
    pub fn parse_numeric_i128(s: &str) -> Option<i128> {
        crate::conversion::parse_numeric_i128(s)
    }
    /// Match an ADBC `LIKE` pattern against a value.
    pub fn like_match(pattern: &str, value: &str) -> bool {
        crate::connection::like_match(pattern, value)
    }
    /// Normalize an emulator endpoint by adding an `http://` scheme when absent.
    pub fn ensure_scheme(host: &str) -> String {
        crate::driver::ensure_scheme(host)
    }

    /// An arbitrary ADBC option value, mirroring the variants of
    /// [`OptionValue`](adbc_core::options::OptionValue) the driver accepts.
    #[derive(arbitrary::Arbitrary, Debug)]
    pub enum OptValue {
        Str(String),
        Int(i64),
        Double(f64),
        Bytes(Vec<u8>),
    }

    /// Drive the database option-handling code (`set_option` / `get_option_string`) with arbitrary
    /// key/value pairs, exactly as the C ABI would after a driver manager forwards untrusted option
    /// strings. Exercises the string/bool/int coercions and the unknown-key error path; must never
    /// panic. No network I/O — this stops well before `connect()`.
    ///
    /// The `SpannerDriver` (and its shared Tokio runtime) is built once and reused across calls so
    /// fuzzing throughput is not dominated by runtime construction.
    pub fn exercise_database_options(ops: Vec<(String, OptValue)>) {
        use adbc_core::options::{OptionDatabase, OptionValue};
        use adbc_core::{Driver, Optionable};
        use std::sync::{Mutex, OnceLock};

        static DRIVER: OnceLock<Mutex<crate::SpannerDriver>> = OnceLock::new();
        let driver = DRIVER.get_or_init(|| {
            Mutex::new(crate::SpannerDriver::try_new().expect("driver construction is infallible"))
        });

        let mut database = {
            let mut guard = driver.lock().unwrap();
            match guard.new_database() {
                Ok(db) => db,
                Err(_) => return,
            }
        };

        for (key, value) in ops {
            let value = match value {
                OptValue::Str(s) => OptionValue::String(s),
                OptValue::Int(i) => OptionValue::Int(i),
                OptValue::Double(d) => OptionValue::Double(d),
                OptValue::Bytes(b) => OptionValue::Bytes(b),
            };
            // Both known driver options and arbitrary unknown keys go through `Other`; errors
            // (unsupported key, wrong value type, non-boolean text) are expected, not panics.
            let _ = database.set_option(OptionDatabase::Other(key.clone()), value);
            let _ = database.get_option_string(OptionDatabase::Other(key));
        }
    }
}

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

/// Driver-specific statement option: the number of rows converted into each Arrow
/// [`RecordBatch`](arrow_array::RecordBatch) streamed by
/// [`Statement::execute`](adbc_core::Statement::execute). Larger batches trade memory for fewer
/// per-batch conversions; smaller batches lower first-batch latency and peak memory. Accepts a
/// positive integer (via `set_option`/`set_option_int`); defaults to 8192.
pub const OPTION_ROWS_PER_BATCH: &str = "adbc.spanner.rows_per_batch";

/// The vendor name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const VENDOR_NAME: &str = "Google Cloud Spanner";

/// The driver name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const DRIVER_NAME: &str = "adbc-spanner";

/// The version of this driver.
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

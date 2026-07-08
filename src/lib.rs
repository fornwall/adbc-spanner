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
//! The standard `adbc.connection.readonly`
//! ([`OptionConnection::ReadOnly`](adbc_core::options::OptionConnection::ReadOnly)) connection
//! option opens a **read-only** connection: when set to `true` the connection rejects all writes —
//! DML, DDL and bulk ingest fail with [`Status::InvalidState`](adbc_core::error::Status::InvalidState),
//! while read-only queries still run. It defaults to `false`, accepts `true`/`false`, and
//! round-trips through `get_option`. The flag is live: statements check it at execution time, so
//! toggling it takes effect immediately for existing statements as well as new ones.
//!
//! ## Transactions
//!
//! Connections are in **autocommit** mode by default. Setting `adbc.connection.autocommit` to
//! `false` enters manual transaction mode, in which DML is **buffered** and applied atomically in
//! one read/write transaction on `commit` (the Spanner client exposes read/write transactions only
//! through a closure-based runner, so there is no true open transaction to run statements in).
//! Two consequences: a manual transaction has **no read-your-writes** — queries run immediately in
//! a fresh read-only snapshot and do not see buffered DML, so an `INSERT` followed by a
//! `SELECT COUNT(*)` returns the pre-insert count — and DDL issued after buffered DML **executes
//! before it** (DDL runs immediately; it is never transactional in Spanner). See
//! [`SpannerConnection`] for the full model.
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

#![warn(missing_docs)]

mod bind;
mod connection;
mod conversion;
mod ddl;
mod driver;
mod error;
#[cfg(feature = "ffi")]
mod ffi;
mod info;
mod nested;
mod objects;
mod options;
mod runtime;
mod staleness;
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
    /// Whether the SQL contains a top-level `THEN RETURN` clause.
    pub fn is_dml_returning(sql: &str) -> bool {
        crate::ddl::is_dml_returning(sql)
    }
    /// Parse a Spanner `DATE` string into Arrow `Date32` days.
    pub fn parse_date_days(s: &str) -> Option<i32> {
        crate::conversion::parse_date_days(s)
    }
    /// Parse a Spanner `TIMESTAMP` string into epoch nanoseconds.
    pub fn parse_timestamp_nanos(s: &str) -> Option<i64> {
        crate::conversion::parse_timestamp_nanos(s)
    }
    /// Parse a Spanner `NUMERIC` string into an unscaled `i128` (scale 9).
    pub fn parse_numeric_i128(s: &str) -> Option<i128> {
        crate::conversion::parse_numeric_i128(s)
    }
    /// Match an ADBC `LIKE` pattern against a value.
    pub fn like_match(pattern: &str, value: &str) -> bool {
        crate::connection::like_match(pattern, value)
    }
    /// The first SQL keyword, uppercased — skipping whitespace, comments, and `@{…}` statement
    /// hints.
    pub fn first_keyword(sql: &str) -> Option<String> {
        crate::ddl::first_keyword(sql)
    }
    /// Whether the SQL begins with a DML statement (`INSERT`/`UPDATE`/`DELETE`).
    pub fn is_dml(sql: &str) -> bool {
        crate::ddl::is_dml(sql)
    }
    /// Strip trailing top-level `;` terminators from a single-statement query.
    pub fn strip_trailing_terminators(sql: &str) -> String {
        crate::ddl::strip_trailing_terminators(sql)
    }
    /// The distinct `@name` bind parameters of a query, in first-appearance order.
    pub fn named_parameters(sql: &str) -> Vec<String> {
        crate::bind::named_parameters(sql)
    }
    /// Backtick-quote a Spanner identifier (GoogleSQL backslash escaping).
    pub fn quote_ident(ident: &str) -> String {
        crate::bind::quote_ident(ident)
    }
    /// Resolve the column→parameter pairing for `sql` against a batch whose columns are named
    /// `column_names` (built here as nullable `Int64`; the pairing never looks at types).
    ///
    /// Returns the resolved names, or `None` on the documented count-mismatch rejection — after
    /// asserting the error is `InvalidArguments` (any other status, like any panic, is a bug).
    pub fn resolve_parameter_names(sql: &str, column_names: &[String]) -> Option<Vec<String>> {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema};
        let fields: Vec<Field> = column_names
            .iter()
            .map(|name| Field::new(name, DataType::Int64, true))
            .collect();
        let batch = RecordBatch::new_empty(std::sync::Arc::new(Schema::new(fields)));
        match crate::bind::resolve_parameter_names(sql, &batch) {
            Ok(names) => Some(names),
            Err(e) => {
                assert_eq!(
                    e.status,
                    adbc_core::error::Status::InvalidArguments,
                    "resolve_parameter_names must reject with InvalidArguments: {e:?}"
                );
                None
            }
        }
    }
    /// Decode an opaque partition descriptor, returning whether it decoded.
    ///
    /// The oracles live here (where the client's `Partition` type is in scope): a rejected
    /// descriptor must be a clean `InvalidArguments` error — never a panic — and an accepted one
    /// must survive a serialize → decode → serialize round trip unchanged (compared as
    /// `serde_json::Value`, so map key order is irrelevant).
    pub fn decode_partition(descriptor: &[u8]) -> bool {
        match crate::connection::decode_partition(descriptor) {
            Ok(partition) => {
                let value =
                    serde_json::to_value(&partition).expect("a decoded partition re-serializes");
                let bytes =
                    serde_json::to_vec(&value).expect("a serde_json::Value always serializes");
                let again = crate::connection::decode_partition(&bytes)
                    .expect("a re-serialized partition descriptor decodes");
                assert_eq!(
                    value,
                    serde_json::to_value(&again).expect("a decoded partition re-serializes"),
                    "partition descriptor round-trip changed the value"
                );
                true
            }
            Err(e) => {
                assert_eq!(
                    e.status,
                    adbc_core::error::Status::InvalidArguments,
                    "decode_partition must reject with InvalidArguments: {e:?}"
                );
                assert!(
                    e.message.contains("invalid partition descriptor"),
                    "unexpected rejection message: {}",
                    e.message
                );
                false
            }
        }
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

/// Internal conversion entry point exposed only for the criterion benchmarks in `benches/`
/// (a bench target can only reach the crate's public API).
///
/// **Not** part of the public API — no stability guarantees.
#[doc(hidden)]
pub mod bench_support {
    use arrow_array::ArrayRef;
    use arrow_schema::DataType;
    use google_cloud_spanner::value::Value;

    /// Build an Arrow array of `data_type` from one Spanner wire [`Value`] per row — the core of
    /// the row→Arrow conversion that `execute` performs for every streamed chunk.
    pub fn build_array(
        data_type: &DataType,
        values: &[Option<&Value>],
    ) -> adbc_core::error::Result<ArrayRef> {
        crate::conversion::build_array(data_type, values)
    }
}

/// Driver-specific database option: the fully-qualified Spanner database path,
/// `projects/<project>/instances/<instance>/databases/<database>`.
///
/// Equivalent to setting [`OptionDatabase::Uri`](adbc_core::options::OptionDatabase::Uri).
pub const OPTION_DATABASE: &str = "spanner.database";

/// Driver-specific database option: an explicit gRPC endpoint (for example the address of a
/// Spanner emulator, `http://localhost:9010`). When unset the client connects to the production
/// Spanner service.
pub const OPTION_ENDPOINT: &str = "spanner.endpoint";

/// Driver-specific database option: when set to `true`, connect with anonymous credentials
/// (the mode used by the Spanner emulator). Automatically enabled when `SPANNER_EMULATOR_HOST`
/// is present in the environment. Combining emulator mode with explicitly configured credentials
/// ([`OPTION_KEYFILE`], [`OPTION_KEYFILE_JSON`], or [`OPTION_IMPERSONATE_TARGET_PRINCIPAL`]) is
/// refused at connect time instead of silently ignoring them.
pub const OPTION_EMULATOR: &str = "spanner.emulator";

/// Driver-specific database option: path to a service-account JSON key file to authenticate with
/// (dbt's `keyfile`). Overridden by [`OPTION_KEYFILE_JSON`] if both are set.
pub const OPTION_KEYFILE: &str = "spanner.keyfile";

/// Driver-specific database option: an inline service-account JSON key (dbt's `keyfile_json`).
///
/// When neither this nor [`OPTION_KEYFILE`] is set (and not connecting to an emulator), the driver
/// falls back to Application Default Credentials.
pub const OPTION_KEYFILE_JSON: &str = "spanner.keyfile_json";

/// Driver-specific database option: the service-account email to impersonate. Setting this **enables
/// service-account impersonation** — the base credentials (ADC, keyfile, …) are used to mint a
/// short-lived access token for this target principal via the IAM Credentials
/// `generateAccessToken` API, and the driver authenticates as the target. When unset, no
/// impersonation happens and authentication is unchanged.
///
/// Mirrors the BigQuery ADBC driver's `bigquery.impersonate.target_principal` option.
pub const OPTION_IMPERSONATE_TARGET_PRINCIPAL: &str = "spanner.impersonate.target_principal";

/// Driver-specific database option: an optional delegation chain for impersonation — a
/// comma-separated list of service-account emails, each of which must have the *Token Creator* role
/// on the next, with the last granting it on [`OPTION_IMPERSONATE_TARGET_PRINCIPAL`]. Only used when
/// a target principal is set. Mirrors BigQuery's `bigquery.impersonate.delegates`.
pub const OPTION_IMPERSONATE_DELEGATES: &str = "spanner.impersonate.delegates";

/// Driver-specific database option: optional OAuth 2.0 scopes for the impersonated token, as a
/// comma-separated list. Defaults to the `cloud-platform` scope when unset. Only used when a target
/// principal is set. Mirrors BigQuery's `bigquery.impersonate.scopes`.
pub const OPTION_IMPERSONATE_SCOPES: &str = "spanner.impersonate.scopes";

/// Driver-specific database option: the lifetime (in seconds) of the impersonated access token.
/// Defaults to 3600 (one hour) when unset. Only used when a target principal is set. Mirrors
/// BigQuery's `bigquery.impersonate.lifetime`.
pub const OPTION_IMPERSONATE_LIFETIME: &str = "spanner.impersonate.lifetime";

/// Driver-specific statement option: the number of rows converted into each Arrow
/// [`RecordBatch`](arrow_array::RecordBatch) streamed by
/// [`Statement::execute`](adbc_core::Statement::execute). Larger batches trade memory for fewer
/// per-batch conversions; smaller batches lower first-batch latency and peak memory. Accepts a
/// positive integer (via `set_option`/`set_option_int`); defaults to 8192.
pub const OPTION_ROWS_PER_BATCH: &str = "spanner.rows_per_batch";

/// Driver-specific statement option: enable **Data Boost** for
/// [`Statement::execute_partitions`](adbc_core::Statement::execute_partitions). When `true`, each
/// partition executes on Spanner's serverless, workload-isolated compute (independent of the
/// provisioned instance). The flag is baked into every partition descriptor, so a partition read
/// back with [`Connection::read_partition`](adbc_core::Connection::read_partition) — on any
/// connection or worker — honours it. Accepts a boolean; defaults to `false`.
pub const OPTION_DATA_BOOST: &str = "spanner.data_boost_enabled";

/// Driver-specific statement option: the maximum number of partitions to request from
/// [`Statement::execute_partitions`](adbc_core::Statement::execute_partitions). This is a hint —
/// Spanner may return fewer. Accepts a positive integer; unset lets Spanner choose.
pub const OPTION_MAX_PARTITIONS: &str = "spanner.max_partitions";

/// Driver-specific connection **and** statement option: the **read staleness** for read-only
/// queries, as `"exact:<duration>"` or `"max:<duration>"`.
///
/// - `exact:<duration>` reads exactly `<duration>` in the past
///   ([`TimestampBound::exact_staleness`](https://docs.cloud.google.com/spanner/docs/timestamp-bounds#exact_staleness)) —
///   a single, repeatable timestamp, cheaper and lock-free.
/// - `max:<duration>` reads at any timestamp within `<duration>` of now (bounded staleness; the
///   server picks — single-use reads only).
///
/// `<duration>` is a non-negative number with an optional unit suffix: `s` (seconds, the default),
/// `ms`, `us`/`µs`, `ns`, `m` (minutes) or `h` (hours). Examples: `exact:10`, `exact:2.5s`,
/// `max:500ms`, `max:1m`.
///
/// Mutually exclusive with [`OPTION_READ_TIMESTAMP`]; set the other to an empty string to unset it.
/// Set on a connection it becomes the default for statements it creates; a statement may override
/// it. Unset (the default) means a **strong** read.
pub const OPTION_READ_STALENESS: &str = "spanner.read.staleness";

/// Driver-specific connection **and** statement option: an **absolute read timestamp** for
/// read-only queries — an RFC 3339 timestamp, optionally prefixed to select the mode:
///
/// - `read:<rfc3339>` (or a bare `<rfc3339>`) reads exactly as of that timestamp
///   ([`TimestampBound::read_timestamp`](https://docs.cloud.google.com/spanner/docs/timestamp-bounds#exact_staleness)).
/// - `min:<rfc3339>` reads at that timestamp or later (bounded staleness; single-use reads only).
///
/// Examples: `2026-07-07T00:00:00Z`, `read:2026-07-07T00:00:00Z`, `min:2026-07-07T00:00:00+02:00`.
///
/// Mutually exclusive with [`OPTION_READ_STALENESS`]; set the other to an empty string to unset it.
/// Set on a connection it becomes the default for statements it creates; a statement may override
/// it. Unset (the default) means a **strong** read.
pub const OPTION_READ_TIMESTAMP: &str = "spanner.read.timestamp";

/// The vendor name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const VENDOR_NAME: &str = "Google Cloud Spanner";

/// The driver name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const DRIVER_NAME: &str = "adbc-spanner";

/// The version of this driver.
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

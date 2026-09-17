//! # adbc-spanner
//!
//! An [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver for
//! [Google Cloud Spanner](https://cloud.google.com/spanner), built on top of the official
//! `google-cloud-spanner` preview client and the native Rust [`adbc_core`] traits. Query results
//! are returned as Arrow [`RecordBatch`](arrow_array::RecordBatch)es, without an intermediate
//! row-by-row copy.
//!
//! The driver exposes Spanner through the standard ADBC object hierarchy:
//!
//! ```text
//! SpannerDriver ──> SpannerDatabase ──> SpannerConnection ──> SpannerStatement
//! ```
//!
//! ## Configuration
//!
//! A database is configured through ADBC options; the authoritative reference for every option at
//! every level — types, defaults and `get_option` round-trip behaviour — is
//! [docs/options.md](https://github.com/fornwall/adbc-spanner/blob/main/docs/options.md). An option
//! documented below as a *connection **and** statement* option is inherited by every statement the
//! connection creates, and may then be overridden on that statement.
//!
//! The database path is required, and is supplied through the standard
//! [`OptionDatabase::Uri`](adbc_core::options::OptionDatabase::Uri) option as a **connection URI**
//! with the `spanner://` scheme (required — a bare database path is not accepted): the path is the
//! database path, the query parameters are database-level driver options, and the optional
//! `//host:port` authority becomes [`OPTION_ENDPOINT`].
//!
//! ```text
//! spanner:///projects/<p>/instances/<i>/databases/<d>?spanner.emulator=true
//! ```
//!
//! A URI is expanded into the individual options at the moment it is set, so an option set after
//! the URI wins. The two secret-holding options, [`OPTION_KEYFILE_JSON`] and [`OPTION_ACCESS_TOKEN`],
//! are **refused** as query parameters — a URI is routinely logged — and must be set as options
//! directly; [`OPTION_KEYFILE`], a path, is fine in a URI.
//!
//! ## Transactions
//!
//! Connections are in **autocommit** mode by default. Setting `adbc.connection.autocommit` to
//! `false` enters manual mode, where a transaction is exactly one of two kinds — **queries** (one
//! shared read-only snapshot) or **DML** — fixed by its *first* statement; a statement of the other
//! kind is rejected with `InvalidState` until `commit`/`rollback`. DML and bulk-ingest mutations
//! **buffer** until `commit`, so a DML transaction has no read-your-writes and reports unknown
//! (`None`) row counts until then. DDL is not transaction-aware: it always executes immediately.
//!
//! See [`SpannerConnection`] and
//! [docs/transactions.md](https://github.com/fornwall/adbc-spanner/blob/main/docs/transactions.md)
//! for the full model.
//!
//! ## Example
//!
//! ```no_run
//! use adbc_core::{Driver, Database, Connection, Statement};
//! use adbc_core::options::{OptionDatabase, OptionValue};
//! use adbc_spanner::SpannerDriver;
//! use arrow_array::RecordBatchReader;
//!
//! # fn main() -> adbc_core::error::Result<()> {
//! let mut driver = SpannerDriver::try_new()?;
//! let database = driver.new_database_with_opts([(
//!     OptionDatabase::Uri,
//!     OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
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

// `missing_docs`, `missing_debug_implementations`, `unreachable_pub` and `unsafe_op_in_unsafe_fn`
// are configured in `[lints.rust]` in Cargo.toml (they apply uniformly to all targets). The two
// `unsafe_code` lints below stay here because they are feature-conditional.
//
// All of this crate's `unsafe` lives in the `ffi` module — the hand-written ADBC C ABI export
// layer, which is nothing but pointer handling across that boundary — under a single audited
// `#[allow(unsafe_code)]`. Deny elsewhere so new unsafe can't creep in, and hard-`forbid` it
// entirely in the pure-Rust build (no `ffi` feature), where no unsafe exists at all.
#![deny(unsafe_code)]
#![cfg_attr(not(feature = "ffi"), forbid(unsafe_code))]

// Test-only ASan tripwire, compiled ONLY under `--cfg asan_canary` (the validation suite's
// `rust-asan` leg). Absent from every normal build — see the module docs.
#[cfg(asan_canary)]
mod asan_canary;

mod bind;
mod connection;
mod conversion;
mod directed_read;
mod driver;
mod error;
#[cfg(feature = "ffi")]
#[allow(unsafe_code)] // The hand-written C ABI export layer; see the module docs.
mod ffi;
mod info;
mod metadata;
mod nested;
mod objects;
mod options;
mod query_options;
mod request;
mod retry;
mod runtime;
mod sql;
mod staleness;
mod statement;
mod statistics;
mod timeout;

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
        crate::sql::split_statements(sql)
    }
    /// Whether the SQL begins with a DDL statement.
    pub fn is_ddl(sql: &str) -> bool {
        crate::sql::is_ddl(sql)
    }
    /// Whether the SQL contains a top-level `THEN RETURN` clause.
    pub fn is_dml_returning(sql: &str) -> bool {
        crate::sql::is_dml_returning(sql)
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
        crate::metadata::like_match(pattern, value)
    }
    /// The first SQL keyword, uppercased — skipping whitespace, comments, and `@{…}` statement
    /// hints.
    pub fn first_keyword(sql: &str) -> Option<String> {
        crate::sql::first_keyword(sql).map(str::to_ascii_uppercase)
    }
    /// Whether the SQL begins with a DML statement (`INSERT`/`UPDATE`/`DELETE`).
    pub fn is_dml(sql: &str) -> bool {
        crate::sql::is_dml(sql)
    }
    /// Strip trailing top-level `;` terminators from a single-statement query.
    pub fn strip_trailing_terminators(sql: &str) -> String {
        crate::sql::strip_trailing_terminators(sql)
    }
    /// The distinct `@name` bind parameters of a query, in first-appearance order.
    pub fn named_parameters(sql: &str) -> Vec<String> {
        crate::sql::named_parameters(sql)
    }
    /// Backtick-quote a Spanner identifier, or reject a name backticks cannot safely contain.
    pub fn quote_ident(ident: &str) -> Option<String> {
        crate::sql::quote_ident(ident).ok()
    }
    /// Resolve the column→parameter pairing for `sql` against a batch whose columns are named
    /// `column_names` (built here as nullable `Int64`; the pairing never looks at types), under the
    /// given `bind_by_name` mode. Returns the resolved names, or `None` after asserting the
    /// rejection is `InvalidArguments` (any other status, like any panic, is a bug).
    pub fn resolve_parameter_names(
        sql: &str,
        column_names: &[String],
        bind_by_name: bool,
    ) -> Option<Vec<String>> {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema};
        let fields: Vec<Field> = column_names
            .iter()
            .map(|name| Field::new(name, DataType::Int64, true))
            .collect();
        let batch = RecordBatch::new_empty(std::sync::Arc::new(Schema::new(fields)));
        match crate::bind::resolve_parameter_names(sql, &batch, bind_by_name) {
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
    /// Oracles: a rejected descriptor must be a clean `InvalidArguments` error — never a panic —
    /// and an accepted one must reach a byte-stable fixed point under the driver's own encoder.
    ///
    /// The fixed point is asserted from `encode_partition`'s *own* output, not from the arbitrary
    /// input's first encode: a hand-crafted descriptor can carry a value whose lexical form serde
    /// does not preserve (a huge integer literal overflows `i64`/`u64` and is parsed to `f64`
    /// imprecisely, so its first re-encode decodes back to a *different* `f64`). One normalization
    /// pass reaches the fixed point, and every descriptor `encode_partition` produces is already
    /// there.
    pub fn decode_partition(descriptor: &[u8]) -> bool {
        match crate::connection::decode_partition(descriptor) {
            Ok(partition) => {
                use crate::connection::{decode_partition, encode_partition};
                // Normalize once so the comparison starts from a canonical encoder output, then
                // assert the round-trip from there is byte-stable.
                let first = encode_partition(&partition).expect("a decoded partition re-encodes");
                let normalized =
                    decode_partition(&first).expect("a re-encoded partition descriptor decodes");
                let bytes = encode_partition(&normalized).expect("a decoded partition re-encodes");
                let again =
                    decode_partition(&bytes).expect("a re-encoded partition descriptor decodes");
                assert_eq!(
                    bytes,
                    encode_partition(&again).expect("a decoded partition re-encodes"),
                    "enveloped partition descriptor round-trip changed the bytes"
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
                    e.message.contains("invalid partition descriptor")
                        || e.message.contains("not supported by this driver"),
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

    /// Parse a `spanner.read.staleness` option value, exercising the read-bound grammar
    /// (`parse_read_bound` → `parse_duration` / RFC 3339) and the client `TimestampBound` mapping.
    /// Pure/offline; must never panic.
    pub fn parse_read_staleness(value: &str) {
        use adbc_core::options::OptionValue;
        let mut staleness = crate::staleness::ReadStaleness::default();
        if staleness
            .set_staleness(OptionValue::String(value.to_string()))
            .is_ok()
        {
            let _ = staleness.timestamp_bound();
            let _ = staleness.multi_use_timestamp_bound();
        }
    }

    /// Parse a duration option value with the shared grammar (`spanner.read.staleness` durations,
    /// also used by `spanner.commit.max_delay`). Pure/offline; must never panic.
    pub fn parse_duration(value: &str) {
        let _ = crate::staleness::parse_duration(value);
    }

    /// Parse a `spanner.directed_read` option value with the module's hand-written grammar parser
    /// (the most complex hand parser in the driver). Pure/offline; must never panic.
    pub fn parse_directed_read(value: &str) {
        let _ = crate::directed_read::parse(value);
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
    /// key/value pairs, as the C ABI would after a driver manager forwards untrusted option
    /// strings. No network I/O — this stops well before `connect()` — and must never panic. The
    /// `SpannerDriver` (and its shared runtime) is built once and reused so fuzzing throughput is
    /// not dominated by construction.
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

    /// Expand a `spanner:` connection URI through the database option boundary
    /// (`OptionDatabase::Uri`): scheme detection, `parse_connection_uri`, percent-decoding, and the
    /// eager expansion of query parameters into option fields — the surface a driver manager
    /// reaches by setting the standard `uri` option, unreachable from
    /// [`exercise_database_options`], which only forwards `Other(key)`. No network I/O; must never
    /// panic.
    pub fn expand_connection_uri(uri: &str) {
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

        // Both a well-formed `spanner://…` URI and arbitrary garbage go through the same option;
        // a bad scheme, path, query parameter, or value is an error, not a panic.
        let _ = database.set_option(OptionDatabase::Uri, OptionValue::String(uri.to_string()));
        let _ = database.get_option_string(OptionDatabase::Uri);
    }

    #[cfg(test)]
    mod tests {
        /// Every declared fuzz target must have a harness file, reach CI's matrix, and be
        /// documented. `fuzz.yml` derives its matrix from `fuzz/Cargo.toml` (a hardcoded list once
        /// left three targets unfuzzed for several releases); this test pins the links that
        /// derivation does not cover: harness file ↔ `[[bin]]` declaration, the workflow still
        /// deriving rather than hardcoding, and the docs naming every target.
        #[test]
        fn every_fuzz_target_is_wired_and_documented() {
            let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

            // `[[bin]]` is always immediately followed by its `name = "..."` — the same shape
            // fuzz.yml's `discover` job parses.
            let manifest = std::fs::read_to_string(root.join("fuzz/Cargo.toml")).unwrap();
            let mut lines = manifest.lines();
            let mut declared = std::collections::BTreeSet::new();
            while let Some(line) = lines.next() {
                if line.trim() != "[[bin]]" {
                    continue;
                }
                let name = lines
                    .next()
                    .expect("a [[bin]] section is followed by its name")
                    .trim()
                    .strip_prefix("name = \"")
                    .and_then(|rest| rest.strip_suffix('"'))
                    .expect("a [[bin]] section starts with `name = \"...\"`");
                declared.insert(name.to_string());
            }

            let harnesses: std::collections::BTreeSet<String> =
                std::fs::read_dir(root.join("fuzz/fuzz_targets"))
                    .expect("fuzz_targets directory exists")
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
                    .map(|path| path.file_stem().unwrap().to_str().unwrap().to_string())
                    .collect();
            assert_eq!(
                declared, harnesses,
                "every fuzz_targets/<name>.rs needs a [[bin]] in fuzz/Cargo.toml and vice versa \
                 (cargo-fuzz only runs declared targets, and fuzz.yml's matrix derives from them)"
            );

            // The matrix must stay derived. A literal list here is exactly the regression that
            // left three targets unfuzzed.
            let workflow =
                std::fs::read_to_string(root.join(".github/workflows/fuzz.yml")).unwrap();
            assert!(
                workflow.contains("fromJson(needs.discover.outputs.targets)"),
                "fuzz.yml must derive its matrix from fuzz/Cargo.toml via the `discover` job; a \
                 hardcoded target list silently skips new targets"
            );

            for doc in ["docs/testing.md", "CLAUDE.md"] {
                let text = std::fs::read_to_string(root.join(doc)).unwrap();
                for target in &declared {
                    assert!(
                        text.contains(&format!("`{target}`")),
                        "fuzz target `{target}` is not named in {doc}"
                    );
                }
            }
        }

        /// Run the partition-descriptor oracle over the checked-in fuzz seed corpus
        /// (`fuzz/seeds/partition/`), so a corpus/oracle mismatch fails `cargo test
        /// --features fuzzing` locally instead of only surfacing in a fuzz run. Every seed is
        /// listed with its expected verdict, and each verdict runs the oracle's internal
        /// round-trip and clean-rejection assertions.
        #[test]
        fn partition_seed_corpus_satisfies_the_oracle() {
            let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/seeds/partition");
            let mut verdicts = std::collections::BTreeMap::new();
            for entry in std::fs::read_dir(dir).expect("seed corpus directory exists") {
                let path = entry.unwrap().path();
                let bytes = std::fs::read(&path).unwrap();
                let name = path.file_name().unwrap().to_str().unwrap().to_string();
                verdicts.insert(name, super::decode_partition(&bytes));
            }
            let expected: std::collections::BTreeMap<String, bool> = [
                ("bare-bignum-query-descriptor", false),
                ("enveloped-bad-version", false),
                ("enveloped-bignum-query-descriptor", true),
                ("enveloped-query-descriptor", true),
            ]
            .into_iter()
            .map(|(name, accepted)| (name.to_string(), accepted))
            .collect();
            assert_eq!(verdicts, expected);
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

/// Driver-specific database option: an explicit gRPC endpoint (for example a Spanner emulator's
/// address, `http://localhost:9010`). Unset, the client connects to the production service.
pub const OPTION_ENDPOINT: &str = "spanner.endpoint";

/// Driver-specific database option: when `true`, connect with anonymous credentials (the mode used
/// by the Spanner emulator). Automatically enabled when `SPANNER_EMULATOR_HOST` is set. Combining
/// it with an explicitly configured credential or [`OPTION_QUOTA_PROJECT`] is refused at connect
/// time rather than silently ignoring them.
pub const OPTION_EMULATOR: &str = "spanner.emulator";

/// Driver-specific database option: path to a service-account JSON key file (dbt's `keyfile`).
/// Overridden by [`OPTION_KEYFILE_JSON`] if both are set. Being a path rather than a secret, it is
/// readable through `get_option` and may appear in a connection URI.
pub const OPTION_KEYFILE: &str = "spanner.auth.keyfile";

/// Driver-specific database option: an inline service-account JSON key (dbt's `keyfile_json`).
/// With neither this nor [`OPTION_KEYFILE`] set (and no emulator), the driver falls back to
/// Application Default Credentials.
///
/// **Write-only, and not a URI query parameter.** The value is a live private key, so `get_option`
/// always fails with [`Status::NotFound`](adbc_core::error::Status::NotFound) — set or not — and a
/// `spanner://` URI carrying it is `InvalidArguments`, a URI being routinely logged.
pub const OPTION_KEYFILE_JSON: &str = "spanner.auth.keyfile_json";

/// Driver-specific database option: the service-account email to impersonate. Setting it **enables
/// service-account impersonation** — the base credentials (ADC, keyfile, …) mint a short-lived
/// access token for this principal via IAM Credentials `generateAccessToken`. Unset, authentication
/// is unchanged. Follows gcloud's `--impersonate-service-account`.
pub const OPTION_IMPERSONATE_TARGET_PRINCIPAL: &str = "spanner.auth.impersonate.target_principal";

/// Driver-specific database option: an optional delegation chain for impersonation — a
/// comma-separated list of service-account emails, each needing the *Token Creator* role on the
/// next and the last on [`OPTION_IMPERSONATE_TARGET_PRINCIPAL`]. Only used with a target principal.
pub const OPTION_IMPERSONATE_DELEGATES: &str = "spanner.auth.impersonate.delegates";

/// Driver-specific database option: OAuth 2.0 scopes for the impersonated token, comma-separated.
/// Defaults to the `cloud-platform` scope. Only used with a target principal.
pub const OPTION_IMPERSONATE_SCOPES: &str = "spanner.auth.impersonate.scopes";

/// Driver-specific database option: the lifetime in seconds of the impersonated access token.
/// Defaults to 3600 (one hour). Only used with a target principal.
pub const OPTION_IMPERSONATE_LIFETIME: &str = "spanner.auth.impersonate.lifetime";

/// Driver-specific database option: a caller-supplied OAuth 2.0 access token, sent verbatim as
/// `Authorization: Bearer <token>` with **no refresh** — the caller must keep it valid.
///
/// A complete credential in its own right, so it is **mutually exclusive** with [`OPTION_KEYFILE`],
/// [`OPTION_KEYFILE_JSON`] and [`OPTION_IMPERSONATE_TARGET_PRINCIPAL`] (combining them is refused
/// at connect time with [`Status::InvalidState`](adbc_core::error::Status::InvalidState)) and
/// conflicts with emulator mode. **Write-only, and not a URI query parameter** — as
/// [`OPTION_KEYFILE_JSON`], and for the same reason.
pub const OPTION_ACCESS_TOKEN: &str = "spanner.auth.access_token";

/// Driver-specific database option: the **quota / billing project** charged for Spanner API usage
/// (the `x-goog-user-project` header), decoupled from the project owning the data. The caller must
/// hold `serviceusage.services.use` on it. Mirrors gcloud's `--billing-project`. Composes with
/// every non-emulator credential source and is refused in emulator mode. Not a secret, so it
/// round-trips through `get_option`; `""` unsets it. `GOOGLE_CLOUD_QUOTA_PROJECT` takes precedence
/// over it in the auth library.
pub const OPTION_QUOTA_PROJECT: &str = "spanner.auth.quota_project";

/// Driver-specific statement option: the number of rows converted into each Arrow
/// [`RecordBatch`](arrow_array::RecordBatch) streamed by
/// [`Statement::execute`](adbc_core::Statement::execute). Larger batches trade memory for fewer
/// conversions; smaller ones lower first-batch latency. A positive integer; defaults to 8192.
pub const OPTION_ROWS_PER_BATCH: &str = "spanner.rows_per_batch";

/// Driver-specific statement option: enable **Data Boost** for
/// [`Statement::execute_partitions`](adbc_core::Statement::execute_partitions), so each partition
/// executes on Spanner's serverless, workload-isolated compute. Baked into every partition
/// descriptor, so any connection or worker reading one back honours it. A boolean, default `false`.
pub const OPTION_DATA_BOOST: &str = "spanner.data_boost";

/// Statement option controlling how bound Arrow columns pair with the query's `@name` parameters,
/// following the ADBC SQLite reference driver's `bind_by_name` convention
/// ([apache/arrow-adbc#3362](https://github.com/apache/arrow-adbc/issues/3362)). A boolean,
/// default `false`, reported by `get_option` as `true`/`false`:
///
/// - **`false`**: strictly positional — the *i*-th bound column binds to the *i*-th distinct
///   `@name` parameter in query order, column names ignored. This is the ADBC ordinal contract
///   positional clients rely on.
/// - **`true`**: strict by-name — each column binds to `@<its own name>` (order-independent); a
///   column naming no query parameter fails with `InvalidArguments`.
pub const OPTION_BIND_BY_NAME: &str = "adbc.statement.bind_by_name";

/// Driver-specific **statement** option: route an autocommit bulk ingest's per-chunk mutations
/// through Spanner's **BatchWrite** RPC instead of a write-only transaction, for non-atomic,
/// high-throughput ("firehose") loads. A boolean, default `false`; `""` unsets it and `get_option`
/// round-trips the effective value.
///
/// BatchWrite applies each chunk's mutation groups **non-atomically** and independently; insert
/// semantics, chunking, the row count, the read-only guard and the append-mode
/// `NotFound`/`AlreadyExists` remap are preserved. Only affects **autocommit** ingests — a
/// manual-mode ingest buffers and commits atomically with its transaction, ignoring the flag.
/// `spanner.request.tag`, `spanner.commit.max_delay` and `spanner.commit_stats` do not reach this
/// path (`BatchWrite` accepts neither per-request tags nor commit options); priority and the
/// transaction tag do.
pub const OPTION_INGEST_BATCH_WRITE: &str = "spanner.ingest.batch_write";

/// Driver-specific **statement** option: run the statement as
/// [Partitioned DML](https://docs.cloud.google.com/spanner/docs/dml-partitioned), which applies
/// each partition independently and so never hits the per-commit mutation limit. A boolean,
/// default `false`; `""` unsets it and `get_option` round-trips the effective value.
///
/// It changes two guarantees, both on the caller:
///
/// - **Not atomic.** Each partition commits on its own and may be applied **more than once**, so
///   the statement must be **idempotent** (`SET active = true`, not `SET n = n + 1`).
/// - **The row count is a lower bound** — Spanner reports `row_count_lower_bound`, which
///   undercounts when a partition was retried; `execute_update` returns it as-is.
///
/// Spanner also restricts what it can run: exactly **one** statement per transaction, no
/// `THEN RETURN` (both `InvalidArguments`), and it cannot join a manual transaction
/// (`InvalidState`). Non-DML statements and bulk ingests ignore the flag. The commit options,
/// `spanner.transaction.tag` and the isolation level are inert here (there is no `Commit`).
pub const OPTION_DML_PARTITIONED: &str = "spanner.dml.partitioned";

/// Driver-specific connection **and** statement option: the **read bound** for read-only queries.
/// `""` unsets it; unset (the default) means a **strong** read.
///
/// The value is one of four prefixed forms — two *relative* (a duration in the past) and two
/// *absolute* ([RFC 3339](https://docs.cloud.google.com/spanner/docs/timestamp-bounds)):
///
/// - `exact:<duration>` reads exactly `<duration>` in the past — a single, repeatable timestamp,
///   cheaper and lock-free.
/// - `max:<duration>` reads at any timestamp within `<duration>` of now (bounded staleness; the
///   server picks — single-use reads only).
/// - `read:<rfc3339>` (or a bare `<rfc3339>`) reads exactly as of that timestamp.
/// - `min:<rfc3339>` reads at that timestamp or later (bounded staleness; single-use reads only).
///
/// `<duration>` is a non-negative number with an optional unit suffix: `s` (seconds, the default),
/// `ms`, `us`/`µs`, `ns`, `m` (minutes) or `h` (hours). Examples: `exact:10`, `exact:2.5s`,
/// `max:500ms`, `read:2026-07-07T00:00:00Z`, `min:2026-07-07T00:00:00+02:00`.
pub const OPTION_READ_STALENESS: &str = "spanner.read.staleness";

/// Driver-specific connection **and** statement option: the
/// [**request priority**](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/RequestOptions)
/// Spanner's scheduler arbitrates CPU with — exactly `low`, `medium` or `high`. Applied to every
/// query and DML statement and as the commit priority of every read/write transaction;
/// driver-internal metadata queries are unaffected. Unset (the default) leaves the service default
/// (high); `""` unsets.
pub const OPTION_REQUEST_PRIORITY: &str = "spanner.request.priority";

/// Driver-specific connection **and** statement option: a
/// [directed read](https://docs.cloud.google.com/spanner/docs/directed-reads) replica selection,
/// steering where a read is served. Honoured on **read-only query paths only** — Spanner rejects
/// directed reads on a read/write transaction — and ignored by DML/DDL. Unset by default (Spanner's
/// own routing); `""` unsets; malformed values fail with `InvalidArguments`. Round-trips through
/// `get_option` (raw and trimmed).
///
/// The value is a small grammar:
///
/// ```text
/// <mode> [ ":" <selection> ("," <selection>)* ] [ ";auto_failover_disabled" ]
/// ```
///
/// - `<mode>` is `include` (an ordered preference list Spanner tries in turn) or `exclude`
///   (replicas Spanner routes around), exact lowercase.
/// - Each `<selection>` is `<location>`, `<location>:<type>` or `:<type>` (at least one of the two),
///   where `<location>` is a region such as `us-east1` and `<type>` is `read_write`, `read_only` or
///   `any` (exact lowercase; `any`/omitted matches every replica type).
/// - The optional `;auto_failover_disabled` suffix (valid only with `include`) stops Spanner from
///   falling back to a replica outside the list when the listed ones are unavailable.
///
/// Examples: `include:us-east1`, `include:us-east1:read_only,us-east4:read_write`,
/// `exclude:us-central1`, `include:us-east1;auto_failover_disabled`.
pub const OPTION_DIRECTED_READ: &str = "spanner.directed_read";

/// Driver-specific connection **and** statement option: a free-form **request tag**, attached to
/// every query/DML statement (and `ExecuteBatchDml` batch) the driver builds and surfaced in
/// Spanner's query and transaction statistics for
/// [troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags).
/// Unset by default; `""` unsets.
pub const OPTION_REQUEST_TAG: &str = "spanner.request.tag";

/// Driver-specific connection **and** statement option: the query **optimizer version** Spanner
/// plans with — a version string such as `"6"` or `"latest"`, passed through unchanged as a
/// [`QueryOptions`](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/ExecuteSqlRequest#queryoptions)
/// on every query. Unset (the default) leaves the database/service default; `""` unsets.
pub const OPTION_QUERY_OPTIMIZER_VERSION: &str = "spanner.query.optimizer_version";

/// Driver-specific connection **and** statement option: the query **optimizer statistics package**
/// Spanner plans against — a named package, passed through unchanged as a `QueryOptions` on every
/// query. Unset (the default) leaves the database default; `""` unsets.
pub const OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE: &str =
    "spanner.query.optimizer_statistics_package";

/// Driver-specific connection **and** statement option: the **query timeout**, in seconds — an
/// overall deadline on the *initial execution* of a query (the `ExecuteStreamingSql` call plus the
/// first chunk, the `execute_schema` / `execute_partitions` probes, `read_partition`'s initial
/// fetch, and the driver-internal metadata reads). Later chunk fetches are bounded by
/// [`OPTION_RPC_TIMEOUT_FETCH`]. An expired deadline fails with
/// [`Status::Timeout`](adbc_core::error::Status::Timeout).
///
/// A finite, non-negative number of seconds (fractions allowed; `NaN`/infinities rejected),
/// accepted as a numeric string, an integer or a double and round-tripping through `get_option` /
/// `get_option_double`. `0` disables the timeout, `""` unsets it, and unset (the default) means no
/// deadline.
pub const OPTION_RPC_TIMEOUT_QUERY: &str = "spanner.rpc.timeout_seconds.query";

/// Driver-specific connection **and** statement option: the **update timeout**, in seconds — an
/// overall deadline on each write: an autocommit DML / batch-DML transaction, the manual-mode
/// commit (including re-enabling autocommit), each bulk-ingest commit chunk, and a DDL change —
/// the admin `UpdateDatabaseDdl` call **and** its long-running-operation poll loop, which is
/// otherwise unbounded. It covers the whole driver-side operation including client retries. An
/// expired deadline fails with [`Status::Timeout`](adbc_core::error::Status::Timeout) — note
/// Spanner may still have committed a transaction the driver stopped waiting for, the usual
/// ambiguity of any timed-out commit. Value syntax, `0`/`""` handling, round-trip and inheritance
/// are as [`OPTION_RPC_TIMEOUT_QUERY`].
pub const OPTION_RPC_TIMEOUT_UPDATE: &str = "spanner.rpc.timeout_seconds.update";

/// Driver-specific connection **and** statement option: the **fetch timeout**, in seconds — an
/// overall deadline on *each chunk fetch after the first* of a streamed result (the first being
/// [`OPTION_RPC_TIMEOUT_QUERY`]'s), enforced inside the background prefetch task so a stalled
/// stream fails the consumer's next batch with
/// [`Status::Timeout`](adbc_core::error::Status::Timeout) instead of hanging. For a bound
/// (parameterized) query it also covers executing each per-row statement as the stream advances.
/// Value syntax, `0`/`""` handling, round-trip and inheritance are as [`OPTION_RPC_TIMEOUT_QUERY`].
pub const OPTION_RPC_TIMEOUT_FETCH: &str = "spanner.rpc.timeout_seconds.fetch";

/// Driver-specific connection **and** statement option: the **maximum number of attempts** the
/// Spanner client makes for a retryable RPC — the first try plus retries — as a positive integer.
/// `1` disables retrying; unset (the default) leaves the client's own policy, uncapped on the unary
/// RPC paths and capped at 10 attempts on the streaming query path. Exact on both.
///
/// Accepted as an integer, a whole-valued double or a numeric string, round-tripping through
/// `get_option` / `get_option_int`; `""` unsets it. Setting it and/or
/// [`OPTION_RETRY_MAX_ELAPSED_SECONDS`] bounds the client's default retry policy while preserving
/// its transport-error-on-idempotent retrying, the loop stopping at whichever limit comes first. This tunes the
/// *per-attempt* retry loop; the [`OPTION_RPC_TIMEOUT_QUERY`] family bounds the *overall*
/// per-operation wall time.
pub const OPTION_RETRY_MAX_ATTEMPTS: &str = "spanner.retry.max_attempts";

/// Driver-specific connection **and** statement option: the **maximum total wall-clock time**, in
/// seconds, the Spanner client spends retrying before the last error is surfaced as permanent. A
/// finite, strictly positive number of seconds; unset (the default) leaves the client's own policy,
/// which has no elapsed-time cap. Value handling, combination with [`OPTION_RETRY_MAX_ATTEMPTS`]
/// and inheritance are as that option; round-trips through `get_option` / `get_option_double`.
///
/// **Unary RPCs only.** It bounds the unary retry loops (DML, `ExecuteBatchDml`, begin, commit) but
/// is **inert on the streaming query path**, an upstream defect the driver cannot correct (see
/// `src/retry.rs`'s module docs). To bound a query's wall-clock time use the
/// [`OPTION_RPC_TIMEOUT_QUERY`] / [`OPTION_RPC_TIMEOUT_FETCH`] family, which does cover it.
pub const OPTION_RETRY_MAX_ELAPSED_SECONDS: &str = "spanner.retry.max_elapsed_seconds";

/// Driver-specific connection **and** statement option: the **initial delay**, in seconds, of the
/// Spanner client's exponential backoff between retry attempts. A finite, strictly positive number
/// of seconds, accepted as a numeric string, an integer or a double and round-tripping through
/// `get_option` / `get_option_double`; `""` unsets it. Unset (the default) leaves the client's own
/// initial delay of 1 second.
///
/// Setting this or either sibling ([`OPTION_RETRY_BACKOFF_MAX_SECONDS`],
/// [`OPTION_RETRY_BACKOFF_MULTIPLIER`]) replaces the client's default backoff with one whose unset
/// knobs take the client defaults (1s, 60s, ×2.0), clamped to the gax recommended ranges.
/// Independent of the [`OPTION_RETRY_MAX_ATTEMPTS`] / [`OPTION_RETRY_MAX_ELAPSED_SECONDS`] limits.
pub const OPTION_RETRY_BACKOFF_INITIAL_SECONDS: &str = "spanner.retry.backoff.initial_seconds";

/// Driver-specific connection **and** statement option: the **maximum delay**, in seconds, the
/// Spanner client's exponential backoff is truncated at. A finite, strictly positive number; unset
/// (the default) leaves the client's own maximum of 60 seconds. A value below the effective initial
/// delay is raised to it by the gax clamp. Value handling, combination with the other
/// `spanner.retry.backoff.*` knobs and inheritance are as
/// [`OPTION_RETRY_BACKOFF_INITIAL_SECONDS`].
pub const OPTION_RETRY_BACKOFF_MAX_SECONDS: &str = "spanner.retry.backoff.max_seconds";

/// Driver-specific connection **and** statement option: the **growth factor** the Spanner client's
/// exponential backoff multiplies the delay by after each attempt. A finite, strictly positive
/// number; unset (the default) leaves the client's own multiplier of `2.0`. A value below `1.0` (a
/// shrinking backoff) is floored to `1.0` — a constant delay — by the gax clamp. Value handling,
/// combination with the other `spanner.retry.backoff.*` knobs and inheritance are as
/// [`OPTION_RETRY_BACKOFF_INITIAL_SECONDS`].
pub const OPTION_RETRY_BACKOFF_MULTIPLIER: &str = "spanner.retry.backoff.multiplier";

/// Driver-specific **connection** option: a free-form **transaction tag**, applied wherever the
/// driver builds a read/write transaction and attached by Spanner to every operation of that
/// transaction. Unset by default; `""` unsets. Connection-level only — a transaction can span
/// several statements, so there is no per-statement override.
pub const OPTION_TRANSACTION_TAG: &str = "spanner.transaction.tag";

/// Driver-specific connection **and** statement option: the
/// [**maximum commit delay**](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions)
/// Spanner may add to a read/write commit so it can batch it with others — a
/// throughput-for-latency trade-off — applied at every read/write commit the driver builds.
///
/// A duration in the same grammar as [`OPTION_READ_STALENESS`] (e.g. `100ms`, `0.2s`), which must
/// fall within Spanner's `0..=500ms` range; values outside it, and malformed ones, are rejected
/// with [`Status::InvalidArguments`](adbc_core::error::Status::InvalidArguments). `0` means no
/// delay, `""` unsets it; round-trips through `get_option`.
pub const OPTION_MAX_COMMIT_DELAY: &str = "spanner.commit.max_delay";

/// Driver-specific connection **and** statement option: whether to request Spanner return
/// [**commit statistics**](https://docs.cloud.google.com/spanner/docs/commit-statistics) for the
/// read/write commits the driver builds. A boolean — exactly the string `true`/`false` — `false`
/// by default, `""` unsets it, and `get_option` round-trips the effective value.
///
/// When enabled, the **mutation count** of the most recent such commit is captured and read back
/// via [`OPTION_COMMIT_STATS_MUTATION_COUNT`] (autocommit DML / bulk ingest report on the
/// statement; the manual-mode commit reports on the connection).
pub const OPTION_COMMIT_STATS: &str = "spanner.commit_stats";

/// Driver-specific **read-only** connection and statement option: the **mutation count** from the
/// most recent commit run with [`OPTION_COMMIT_STATS`] enabled.
///
/// Readable via `get_option` / `get_option_int`, and
/// [`Status::NotFound`](adbc_core::error::Status::NotFound) until such a commit has run on this
/// object — so enable [`OPTION_COMMIT_STATS`] first, run a write, then read this back on the same
/// statement (autocommit DML / bulk ingest) or connection (a manual-mode commit). Setting it is
/// [`Status::NotImplemented`](adbc_core::error::Status::NotImplemented): it is a result, not a
/// knob. When several commits run (e.g. a chunked ingest) it reports the most recent one's count.
pub const OPTION_COMMIT_STATS_MUTATION_COUNT: &str = "spanner.commit_stats.mutation_count";

/// Driver-specific connection **and** statement option: whether to
/// [**exclude a transaction's writes from change-stream capture**](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions).
/// A boolean — exactly the string `true`/`false` — `false` by default, `""` unsets it, and
/// `get_option` round-trips the effective value.
///
/// When `true` it applies at every write the driver builds, including the
/// `spanner.ingest.batch_write` firehose path. Per Spanner it only takes effect for change streams
/// created with the DDL option `allow_txn_exclusion = true`; others record the writes regardless.
pub const OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS: &str =
    "spanner.transaction.exclude_from_change_streams";

/// Driver-specific connection **and** statement option: the maximum precision at which Spanner
/// `TIMESTAMP` columns are read into Arrow. `""` resets to the default; round-trips through
/// `get_option`.
///
/// - `nanoseconds_error_on_overflow` (the default) — `Timestamp(Nanosecond, "UTC")`, preserving the
///   wire value's full precision. Arrow's `i64` nanoseconds span only ~1677-09-21 to 2262-04-11,
///   narrower than Spanner's 0001–9999 range, so a well-formed instant outside that window is a
///   loud `InvalidArguments` error naming the column and the value.
/// - `microseconds` — `Timestamp(Microsecond, "UTC")`, covering Spanner's **entire** 0001–9999
///   range: the escape hatch for tables the nanosecond representation cannot hold (mirroring the
///   Snowflake driver's `adbc.snowflake.sql.client_option.max_timestamp_precision`).
///   Sub-microsecond digits are **truncated toward negative infinity**.
///
/// There is deliberately no silently-wrapping nanosecond mode: a wrapped out-of-range timestamp is
/// indistinguishable from real data. The mode applies to every surface producing timestamps or
/// timestamp-typed schemas, `read_partition` using the **reading connection's** setting.
pub const OPTION_MAX_TIMESTAMP_PRECISION: &str = "spanner.max_timestamp_precision";

/// The vendor name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const VENDOR_NAME: &str = "Google Cloud Spanner";

/// The driver name reported by [`Connection::get_info`](adbc_core::Connection::get_info).
pub const DRIVER_NAME: &str = "adbc-spanner";

/// The version of this driver.
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Drift guard: every option key the driver handles must be documented in `docs/options.md`.
#[cfg(test)]
mod options_doc_tests {
    #[test]
    fn every_handled_option_key_is_documented() {
        use adbc_core::constants;

        let doc = include_str!("../docs/options.md");
        let keys: &[&str] = &[
            // Driver-specific options (this crate's OPTION_* constants).
            crate::OPTION_ENDPOINT,
            crate::OPTION_EMULATOR,
            crate::OPTION_KEYFILE,
            crate::OPTION_KEYFILE_JSON,
            crate::OPTION_IMPERSONATE_TARGET_PRINCIPAL,
            crate::OPTION_IMPERSONATE_DELEGATES,
            crate::OPTION_IMPERSONATE_SCOPES,
            crate::OPTION_IMPERSONATE_LIFETIME,
            crate::OPTION_ACCESS_TOKEN,
            crate::OPTION_QUOTA_PROJECT,
            crate::OPTION_ROWS_PER_BATCH,
            crate::OPTION_DATA_BOOST,
            crate::OPTION_DML_PARTITIONED,
            crate::OPTION_READ_STALENESS,
            crate::OPTION_REQUEST_PRIORITY,
            crate::OPTION_REQUEST_TAG,
            crate::OPTION_DIRECTED_READ,
            crate::OPTION_QUERY_OPTIMIZER_VERSION,
            crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE,
            crate::OPTION_TRANSACTION_TAG,
            crate::OPTION_MAX_COMMIT_DELAY,
            crate::OPTION_COMMIT_STATS,
            crate::OPTION_COMMIT_STATS_MUTATION_COUNT,
            crate::OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS,
            crate::OPTION_MAX_TIMESTAMP_PRECISION,
            crate::OPTION_RPC_TIMEOUT_QUERY,
            crate::OPTION_RPC_TIMEOUT_UPDATE,
            crate::OPTION_RPC_TIMEOUT_FETCH,
            crate::OPTION_RETRY_MAX_ATTEMPTS,
            crate::OPTION_RETRY_MAX_ELAPSED_SECONDS,
            crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS,
            crate::OPTION_RETRY_BACKOFF_MAX_SECONDS,
            crate::OPTION_RETRY_BACKOFF_MULTIPLIER,
            // Standard ADBC (spec) options the driver handles.
            constants::ADBC_OPTION_URI,
            constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT,
            constants::ADBC_CONNECTION_OPTION_READ_ONLY,
            constants::ADBC_CONNECTION_OPTION_ISOLATION_LEVEL,
            constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
            constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
            constants::ADBC_INGEST_OPTION_TARGET_TABLE,
            constants::ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
            constants::ADBC_INGEST_OPTION_TARGET_CATALOG,
            constants::ADBC_INGEST_OPTION_TEMPORARY,
            constants::ADBC_INGEST_OPTION_MODE,
        ];
        for key in keys {
            // Require the backticked form so a bare substring (e.g. "uri" inside another
            // word) cannot satisfy the check by accident.
            assert!(
                doc.contains(&format!("`{key}`")),
                "option key `{key}` is handled by the driver but missing from docs/options.md"
            );
        }
    }
}

# adbc-spanner

[![Crates.io](https://img.shields.io/crates/v/adbc-spanner.svg)](https://crates.io/crates/adbc-spanner)
[![Docs.rs](https://img.shields.io/docsrs/adbc-spanner)](https://docs.rs/adbc-spanner)
[![CI](https://github.com/fornwall/adbc-spanner/actions/workflows/ci.yml/badge.svg)](https://github.com/fornwall/adbc-spanner/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver for
[Google Cloud Spanner](https://cloud.google.com/spanner), written in Rust.

It implements the native Rust [`adbc_core`](https://crates.io/crates/adbc_core) driver traits on top
of the official [`google-cloud-spanner`](https://crates.io/crates/google-cloud-spanner) preview
client from [googleapis/google-cloud-rust](https://github.com/googleapis/google-cloud-rust). Query
results come back as Apache Arrow record batches, so Spanner data flows into the Arrow ecosystem
(DataFusion, Polars, Flight, …) without a row-by-row copy.

```text
SpannerDriver ──▶ SpannerDatabase ──▶ SpannerConnection ──▶ SpannerStatement
```

## Status

Early but working and tested end-to-end against the Spanner emulator. Supported today:

- Connecting to production Spanner or a Spanner emulator.
- SQL queries (`execute`), streamed back as typed Arrow `RecordBatch`es: rows are pulled from Spanner
  and converted to Arrow in bounded chunks as the reader is iterated, so a large result set is never
  fully materialised in memory. The chunk size is tunable via the `spanner.rows_per_batch`
  statement option (default 8192).
- DML (`execute_update`), returning the affected-row count. A `;`-separated batch (e.g. dbt's
  `DELETE; INSERT`) runs atomically in one read/write transaction via `ExecuteBatchDml`.
- DDL (`CREATE`/`ALTER`/`DROP`/`RENAME`/…), routed to the Database Admin `UpdateDatabaseDdl` API. A
  `;`-separated batch is submitted as a single schema change, so multi-step changes (e.g. dbt's
  intermediate-table build then rename swap) are near-atomic.
- Transactions: autocommit by default, or manual multi-statement transactions (set
  `adbc.connection.autocommit` to `false`, then `commit`/`rollback`). In manual mode DML is buffered
  and applied atomically in one read/write transaction on commit.
- Parameter binding: `bind`/`bind_stream` an Arrow batch whose columns become Spanner named
  parameters (a column `id` binds `@id`); each bound row runs the statement once.
- Bulk ingest: set `adbc.ingest.target_table`, bind an Arrow batch, and `execute_update` inserts the
  rows into that table in one transaction.
- Metadata: `get_table_types()`, `get_table_schema()`, and `get_objects()` (catalog/schema/table/
  column introspection from `INFORMATION_SCHEMA`).
- Statistics: `get_statistics()` computes exact table/column counts with one aggregate scan per table
  — `ROW_COUNT`, and per column `NULL_COUNT` (plus `DISTINCT_COUNT` for groupable types). Spanner has
  no cheap pre-computed statistics, so an `approximate` request returns nothing rather than scanning;
  `get_statistic_names` is empty (Spanner has no custom named statistics).
- `execute_schema()`: a query's result schema without running it (via `QueryMode::Plan`), so tools
  can introspect output columns — including a top-level `WITH` — with no data scan.

Not supported (return `NotImplemented`, by nature of Spanner): **Substrait** — Spanner has no
Substrait support; and **partitioned execution** (`execute_partitions`/`read_partition`) — Spanner's
Partition APIs are session-bound (they don't map onto ADBC's opaque-token model) and the emulator
doesn't implement them.

## Shared library (loadable driver)

Besides the Rust crate, this builds a C-ABI **shared library** that any ADBC driver manager can load
(`libadbc_spanner.so` on Linux, `libadbc_spanner.dylib` on macOS, `adbc_spanner.dll` on Windows). It
exports the standard `AdbcSpannerInit` entrypoint (plus an `AdbcDriverInit` fallback).

Prebuilt libraries for Linux, macOS and Windows are
attached to every CI run and to each tagged [release](https://github.com/fornwall/adbc-spanner/releases).
To build one yourself: `cargo build --release` → `target/release/libadbc_spanner.so`.

Example, loading it from the Python driver manager:

```python
import adbc_driver_manager
db = adbc_driver_manager.AdbcDatabase(
    driver="/path/to/libadbc_spanner.so",
    entrypoint="AdbcSpannerInit",
    uri="projects/my-project/instances/my-instance/databases/my-db",
)
```

## Usage

Add the dependency (this crate plus the Arrow crates you consume results with):

```toml
[dependencies]
adbc-spanner = "0.1"
adbc_core = "0.23"
arrow-array = "58"
```

```rust
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use adbc_spanner::SpannerDriver;

fn main() -> adbc_core::error::Result<()> {
    let mut driver = SpannerDriver::try_new()?;

    // The Spanner database path is supplied through the standard `uri` option.
    let database = driver.new_database_with_opts([(
        OptionDatabase::Uri,
        OptionValue::String("projects/my-project/instances/my-instance/databases/my-db".into()),
    )])?;

    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;

    statement.set_sql_query("SELECT SingerId FROM Singers ORDER BY SingerId")?;
    let reader = statement.execute()?;

    for batch in reader {
        let batch = batch?;
        let ids = batch.column(0).as_primitive::<Int64Type>();
        for id in ids.values() {
            println!("singer {id}");
        }
    }
    Ok(())
}
```

### Configuration options

Options are set on the database (via `new_database_with_opts` or `set_option`):

| Option                                                    | Meaning                                                                 |
| -------------------------------------------------------- | ----------------------------------------------------------------------- |
| `OptionDatabase::Uri` / `spanner.database`          | The Spanner database path `projects/<p>/instances/<i>/databases/<d>`. Required. |
| `spanner.endpoint`                                  | Explicit gRPC endpoint, e.g. `http://localhost:9010` for an emulator.   |
| `spanner.emulator`                                  | `true` to connect with anonymous credentials (emulator mode).           |
| `spanner.keyfile`                                   | Path to a service-account JSON key file (dbt's `keyfile`).              |
| `spanner.keyfile_json`                             | Inline service-account JSON key (dbt's `keyfile_json`); wins over `keyfile`. |

### Authentication

Credentials are resolved in this order:

1. **Emulator** — if `SPANNER_EMULATOR_HOST` is set (or `spanner.emulator` is `true`), anonymous
   credentials are used and the endpoint is taken from the environment.
2. **Service account** — a key supplied inline via `spanner.keyfile_json` or read from the path
   in `spanner.keyfile`.
3. **[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)**
   otherwise (e.g. `GOOGLE_APPLICATION_CREDENTIALS`, gcloud login, or the metadata server).

## Type mapping

| Spanner type                                | Arrow type                        |
| ------------------------------------------- | --------------------------------- |
| `BOOL`                                      | `Boolean`                         |
| `INT64`                                     | `Int64`                           |
| `FLOAT64`                                   | `Float64`                         |
| `FLOAT32`                                   | `Float32`                         |
| `DATE`                                      | `Date32`                          |
| `TIMESTAMP`                                 | `Timestamp(Nanosecond, "UTC")`    |
| `NUMERIC`                                   | `Decimal128(38, 9)`               |
| `BYTES`                                     | `Binary`                          |
| `STRING` / `JSON` / `UUID` / `INTERVAL` / `ENUM` | `Utf8`                       |
| `ARRAY<T>`                                  | `List<T>` (recursive)             |
| `STRUCT<..>`                                | `Struct<..>` (recursive)          |

`NULL`s are represented as null slots in the corresponding Arrow array. `ARRAY` and `STRUCT` map to
native Arrow `List`/`Struct` recursively, so nested shapes like `ARRAY<STRUCT<..>>` round-trip with
full type fidelity.

`TIMESTAMP` is read at full nanosecond precision (matching the bind/write path). Arrow stores
`Timestamp(Nanosecond)` as an `i64` count of nanoseconds since the Unix epoch, which spans only
~1677-09-21 to 2262-04-11 — a narrower window than Spanner's year 1–9999 range. A Spanner timestamp
outside that window cannot be represented, so reading one surfaces an `InvalidArguments` error naming
the offending value rather than silently truncating or wrapping it.

> **Note:** native `STRUCT` mapping needs `Type::struct_type()`, which is on `google-cloud-rust`
> `main` but not yet in a crates.io release. Until it ships, `Cargo.toml` pins the `google-cloud-*`
> crates to a git revision — so `adbc-spanner` cannot itself be published to crates.io in the
> meantime (see the note in `Cargo.toml`).

## Testing

Unit tests run with no external dependencies:

```sh
cargo test
```

The end-to-end integration test in [`tests/integration.rs`](tests/integration.rs) runs the driver
against Cloud Spanner. It is **skipped automatically** unless a target is configured, so the command
above stays green everywhere. Two targets are supported:

- `SPANNER_EMULATOR_HOST` — a local [Cloud Spanner emulator](https://cloud.google.com/spanner/docs/emulator).
- `SPANNER_GCP_DATABASE` — a real Cloud Spanner database, given as `project.instance.database`,
  reached with [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
  (e.g. after `gcloud auth application-default login`).

The helper script starts the emulator in Docker, points the tests at it, and tears it down again:

```sh
scripts/with-emulator.sh cargo test --test integration -- --nocapture

# Or against a real instance:
SPANNER_GCP_DATABASE=my-project.my-instance.my-db cargo test --test integration -- --nocapture
```

The integration suite also includes an **FFI smoke test** that loads the built shared library through
the ADBC [driver manager](https://crates.io/crates/adbc_driver_manager) (via the `AdbcSpannerInit`
C entrypoint) and runs a query — exercising the C ABI that the trait-level tests bypass.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `cargo fmt --check`, `clippy`,
`rustdoc -D warnings`, a `--no-default-features` build, the full test suite against an emulator
service container, and supply-chain checks (`cargo-deny` + `cargo-machete`).

### Fuzzing

The [`fuzz/`](fuzz/) crate has [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) targets over
the parts that parse untrusted strings — SQL statement splitting / DDL detection (`sql`), value
parsers for `DATE`/`TIMESTAMP`/`NUMERIC` (`values`), and the `LIKE` matcher (`like`) — asserting the
absence of panics (and, for `like`, no exponential blowup). Run one locally on nightly:

```sh
cargo +nightly fuzz run sql
```

Fuzzing runs weekly (and on demand) in CI ([`.github/workflows/fuzz.yml`](.github/workflows/fuzz.yml)).

## Releasing

Releases are cut with [`cargo-release`](https://github.com/crate-ci/cargo-release), configured under
`[package.metadata.release]` in `Cargo.toml`.

Prerequisites: `cargo install cargo-release`, a crates.io token (`cargo login`), and push access to
`main`.

Preview a release (dry run — this is the default, nothing is changed):

```sh
cargo release patch      # or: minor / major
```

Perform it:

```sh
cargo release patch --execute
```

That single command:

1. bumps the version in `Cargo.toml` and commits it (`Release X.Y.Z`),
2. publishes the crate to [crates.io](https://crates.io/crates/adbc-spanner),
3. creates the annotated tag `vX.Y.Z` and pushes the commit and tag to `origin`.

Pushing the `vX.Y.Z` tag triggers the [`Shared libraries`](.github/workflows/libraries.yml) workflow,
which builds the Linux (x86-64, aarch64), macOS (Apple Silicon) and Windows (x86-64) shared libraries
and attaches them to the [GitHub Release](https://github.com/fornwall/adbc-spanner/releases) for that
tag. So the flow is: `cargo release … --execute` → crates.io publish + tag → CI attaches the prebuilt
libraries to the release.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

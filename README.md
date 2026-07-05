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
- SQL queries (`execute`), returned as typed Arrow `RecordBatch`es.
- DML (`execute_update`), returning the affected-row count, run inside an automatically-retried
  read/write transaction.
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
- `get_table_types()` connection metadata.

Not yet supported (return `NotImplemented`): Substrait, partitioned execution, and the richer
catalog-metadata calls (`get_objects`, `get_table_schema`, `get_statistics`, …).

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
| `OptionDatabase::Uri` / `adbc.spanner.database`          | The Spanner database path `projects/<p>/instances/<i>/databases/<d>`. Required. |
| `adbc.spanner.endpoint`                                  | Explicit gRPC endpoint, e.g. `http://localhost:9010` for an emulator.   |
| `adbc.spanner.emulator`                                  | `true` to connect with anonymous credentials (emulator mode).           |

The driver also honours the `SPANNER_EMULATOR_HOST` environment variable: when set it is used as the
endpoint and anonymous credentials are selected automatically. Against production Spanner,
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
are used.

## Type mapping

| Spanner type                                | Arrow type                        |
| ------------------------------------------- | --------------------------------- |
| `BOOL`                                      | `Boolean`                         |
| `INT64`                                     | `Int64`                           |
| `FLOAT64`                                   | `Float64`                         |
| `FLOAT32`                                   | `Float32`                         |
| `DATE`                                      | `Date32`                          |
| `TIMESTAMP`                                 | `Timestamp(Microsecond, "UTC")`   |
| `NUMERIC`                                   | `Decimal128(38, 9)`               |
| `BYTES`                                     | `Binary`                          |
| `STRING` / `JSON` / `UUID` / `INTERVAL` / `ENUM` | `Utf8`                       |
| `ARRAY` / `STRUCT`                          | `Utf8` (JSON-encoded)             |

`NULL`s are represented as null slots in the corresponding Arrow array. `ARRAY`/`STRUCT` are still
rendered as JSON text; mapping them to Arrow `List`/`Struct` is a planned improvement.

## Testing

Unit tests run with no external dependencies:

```sh
cargo test
```

The end-to-end integration test in [`tests/emulator.rs`](tests/emulator.rs) runs the driver against
the [Cloud Spanner emulator](https://cloud.google.com/spanner/docs/emulator). It is **skipped
automatically** unless `SPANNER_EMULATOR_HOST` is set, so the command above stays green everywhere.

The helper script starts the emulator in Docker, points the tests at it, and tears it down again:

```sh
scripts/with-emulator.sh cargo test --test emulator -- --nocapture
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `cargo fmt --check`, `clippy`, and
the full test suite with the emulator as a service container, so the integration test runs on every
push.

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

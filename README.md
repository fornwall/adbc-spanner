> [!WARNING]
> **Experimental.** Project is AI-generated and has not seen real world usage.

# spanner-adbc

[![CI](https://github.com/fornwall/spanner-adbc/actions/workflows/ci.yml/badge.svg)](https://github.com/fornwall/spanner-adbc/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver for
[Google Cloud Spanner](https://cloud.google.com/spanner), available as a
[Python package](https://pypi.org/project/adbc-driver-spanner/), a Rust crate from git, and a
[loadable shared library](#shared-library-loadable-driver). CI tests against the Spanner emulator;
real-database and authentication tests run locally.

For Python installation and examples, see [python/README.md](python/README.md).

## Supported functionality

- **Queries:** lazy Arrow `RecordBatchReader` results, with bounded batches and background
  prefetching (`spanner.rows_per_batch`, default 8192).
- **DML and DDL:** single statements and homogeneous `;`-separated batches. Autocommit DML
  supports `THEN RETURN`; use `execute()` to read returned rows or `execute_update()` for the count.
- **Manual transactions:** `adbc.connection.autocommit=false`, `commit()` and `rollback()`.
  A transaction supports either snapshot queries or buffered writes; it cannot mix the two.
- **Binding:** Arrow batches or streams supply one execution per row. Binding is positional by
  default; `adbc.statement.bind_by_name=true` matches column names to `@name` parameters.
  Multi-row bound queries share one read-only snapshot.
- **Bulk ingest:** all four `adbc.ingest.mode` values, using insert mutations. Table creation
  leaves primary-key generation to Spanner's
  [hidden `rowid`](https://docs.cloud.google.com/spanner/docs/primary-key-default-value#tables-without-primary-keys).
- **Partitioned queries:** `execute_partitions()` and `read_partition()`, optionally with
  [Data Boost](https://docs.cloud.google.com/spanner/docs/databoost/databoost-overview).
- **Schema discovery:** `execute_schema()` and `get_parameter_schema()` use PLAN probes;
  undetermined parameter types are reported as Arrow `Null`.
- **Metadata:** `get_info()`, `get_objects()` (including foreign keys and native type names),
  `get_table_types()` and `get_table_schema()`.
- **Statistics:** exact row, null and distinct counts through aggregate scans, including when
  approximate results are requested. `get_statistic_names()` returns an empty typed result.
- **Cancellation:** connection and statement cancel handles interrupt calls and subsequent fetches
  of the same result. Each new operation gets a fresh cancellation signal.
- **Options:** read-only connections, write isolation, timestamp bounds, priorities, request and
  transaction tags, directed reads, commit statistics, retry policies, timeouts, commit delay,
  partitioned DML and BatchWrite ingest. See the [option reference](docs/options.md).

Change-stream DDL, metadata queries and `READ_<stream>` queries use the ordinary SQL paths;
returned nested records map to Arrow lists and structs.

## Transaction and API limits

- Ordinary DML batches run atomically in one read/write transaction. Mixed query/DML/DDL batches
  are rejected; `THEN RETURN` requires a single SQL statement and autocommit.
- DDL uses the Database Admin `UpdateDatabaseDdl` API. A batch is one
  [schema-update operation](https://docs.cloud.google.com/spanner/docs/schema-updates), **not an
  atomic transaction**. DDL runs immediately, before buffered writes, and rollback cannot undo it.
- Large autocommit ingests can span multiple commits. BatchWrite ingest is also non-atomic.
- Partition descriptors contain SQL and session/transaction identity. They are unauthenticated
  and execute with the receiving connection's credentials; exchange them only over trusted channels.
- Substrait plans, incremental partitioning and temporary ingest tables are unsupported.
- The current catalog is the connected database ID; only that same value can be set.
  `adbc.ingest.target_catalog` also accepts only that ID; leave it unset to use the default.
  The current schema is fixed at `""`; qualify named schemas in SQL or use `adbc.ingest.target_db_schema`.

See [transactions](docs/transactions.md) for transaction boundaries and RPC behavior, and
[ADBC concepts](docs/adbc.md) for handle ownership and errors. Spanner errors retain their gRPC
code and structured details; C ABI 1.1 callers retrieve the code through error details.

## Shared library (loadable driver)

The library exports `AdbcSpannerInit` and the `AdbcDriverInit` fallback:

| Platform | Library |
| --- | --- |
| Linux | `libspanner_adbc.so` |
| macOS | `libspanner_adbc.dylib` |
| Windows | `spanner_adbc.dll` |

Download artifacts from the
[Shared libraries workflow](https://github.com/fornwall/spanner-adbc/actions/workflows/libraries.yml)
or a tagged [release](https://github.com/fornwall/spanner-adbc/releases). To build locally, run
`cargo build --release`; the library is written under `target/release/`.

### Configuration options

Set options on the corresponding database, connection or statement. Driver options use
`spanner.*`; standard ADBC options use `adbc.*`. The [option reference](docs/options.md) lists
accepted types, defaults and getter behavior.

The database `uri` option accepts a `spanner://` connection URI, for example:

```text
spanner:///projects/p/instances/i/databases/d
spanner://localhost:9010/projects/p/instances/i/databases/d?spanner.emulator=true
```

The second form selects an emulator endpoint. See [connection URIs](docs/options.md#connection-uris)
for query options and precedence.

### Authentication

- **Emulator:** non-empty `SPANNER_EMULATOR_HOST` or `spanner.emulator=true` selects anonymous
  credentials. An explicit endpoint overrides the environment's endpoint. Explicit credentials
  and quota-project options conflict with emulator mode; ambient ADC does not.
- **Access token:** `spanner.auth.access_token` supplies a bearer token without refresh and cannot
  be combined with keyfile or impersonation options.
- **Credential JSON:** `spanner.auth.keyfile_json` takes precedence over `spanner.auth.keyfile`.
  Supported JSON types are `service_account`, `authorized_user`, `impersonated_service_account`
  and `external_account`.
- **Application Default Credentials:** used when no explicit credential source is set; see
  [Google's ADC guide](https://docs.cloud.google.com/docs/authentication/application-default-credentials).

`spanner.auth.impersonate.*` can wrap keyfile or ADC credentials. `spanner.auth.quota_project`
selects the quota project for authenticated requests. Inline JSON and access tokens are
write-only and prohibited in URI query parameters; keyfile paths remain readable and URI-compatible.
See [database options](docs/options.md#database-options) for details.

## Type mapping

| Spanner type | Arrow type |
| --- | --- |
| `BOOL` | `Boolean` |
| `INT64` | `Int64` |
| `FLOAT64` / `FLOAT32` | `Float64` / `Float32` |
| `DATE` | `Date32` |
| `TIMESTAMP` | `Timestamp(Nanosecond, "UTC")`, or microseconds by option |
| `NUMERIC` | `Decimal128(38, 9)` |
| `BYTES` | `Binary` |
| `STRING` / `UUID` / `INTERVAL` | `Utf8` |
| `JSON` | `Utf8` with the `arrow.json` extension |
| `ARRAY<T>` | `List<T>`, recursively |
| `STRUCT<..>` | `Struct<..>`, recursively |
| `ENUM` | `Int64` ordinal |
| `PROTO` | `Binary` serialized bytes |

SQL `NULL` becomes an Arrow null. Malformed non-null values produce errors. Struct fields are
matched by position, preserving duplicate or empty names.

JSON fields carry [`arrow.json`](https://arrow.apache.org/docs/format/CanonicalExtensions.html#json)
metadata, including on list child fields. Tagged strings bind and ingest as JSON; untagged string
parameters need `PARSE_JSON(@p)` when inserting JSON.

`ENUM`, `PROTO`, `INTERVAL` and `UUID` have no bind-side extension tag. Bound values use their
Arrow storage type, so cast parameters to the intended SQL type. Decode proto bytes with your
own descriptors, or use `CAST(col AS STRING)` for server-formatted enum/proto text. Ingest-created
tables use the Arrow storage types for these columns.

Binding and ingest widen `UInt8`/`UInt16`/`UInt32` to `INT64` and `Float16` to `FLOAT32`;
`UInt64` is unsupported. `FixedSizeBinary` maps to `BYTES`.

Nanosecond timestamps cover roughly 1677–2262; values outside that range fail with
`InvalidArguments`. Set `spanner.max_timestamp_precision=microseconds` to cover Spanner's full
year 1–9999 range, truncating sub-microsecond digits toward negative infinity. See
[timestamp precision](docs/options.md#timestamp-precision).

The crate uses git-pinned Google Cloud and ADBC dependencies and is not published to crates.io.
Downstream Rust users must use the same `adbc_core` revision as `Cargo.toml`. See
[dependency pins](CONTRIBUTING.md#dependency-pins) before changing them.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for checks and releases, and [docs/testing.md](docs/testing.md)
for test suites and CI coverage.

## License

[Apache-2.0](LICENSE)

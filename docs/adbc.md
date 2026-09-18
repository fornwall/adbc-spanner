# ADBC interface

`spanner-adbc` implements the [ADBC interfaces](https://arrow.apache.org/adbc/current/format/specification.html)
using Google Cloud Spanner and Arrow record batches. See the [README](../README.md) for usage,
[options](options.md) for configuration, and [transactions](transactions.md) for transaction rules.

## Objects and loading

| Object | Responsibility | Source |
| --- | --- | --- |
| `SpannerDriver` | Owns the Tokio runtime shared by its databases, connections and statements. | [driver.rs](../src/driver.rs) |
| `SpannerDatabase` | Stores endpoint, database and credential options; lazily caches the client stack. | [driver.rs](../src/driver.rs) |
| `SpannerConnection` | Owns independent transaction state and metadata operations. Connections from one database share its client stack. | [connection.rs](../src/connection.rs) |
| `SpannerStatement` | Configures SQL or bulk ingest, binds data and executes work. | [statement.rs](../src/statement.rs) |

Rust callers construct `SpannerDriver::try_new()`. The synchronous ADBC methods drive the async
Spanner client on the driver's runtime. The C ABI uses one process-wide driver/runtime.

Other languages load the shared library, built with `cargo build --release`, through an ADBC
driver manager. It exports `AdbcSpannerInit` and the fallback `AdbcDriverInit`; both populate
an ADBC 1.0.0 or 1.1.0 function table. [src/ffi/](../src/ffi) implements the C ABI and exchanges
Arrow data through the Arrow C Data and Stream interfaces. All crate `unsafe` code is confined
there; `--no-default-features` builds the Rust interface without it.

```python
import adbc_driver_manager

db = adbc_driver_manager.AdbcDatabase(
    driver="/path/to/libspanner_adbc.so",
    entrypoint="AdbcSpannerInit",
    uri="spanner:///projects/p/instances/i/databases/d",
)
```

The [Python package](../python/README.md) bundles the library and supplies a DBAPI interface.
The database object performs no network I/O until its first connection. That connection builds
the shared credentials, channels and multiplexed session; later connections reuse them. Setting
any database option invalidates the cache for future connections.

## Execution

```rust
let mut statement = connection.new_statement()?;
statement.set_sql_query("SELECT SingerId, FirstName FROM Singers")?;
for batch in statement.execute()? {
    println!("{} rows", batch?.num_rows());
}
```

Queries return a streaming `RecordBatchReader`. Execution fetches the initial chunk to obtain the
schema, then a background task prefetches bounded chunks as the reader advances. Batches contain
up to `spanner.rows_per_batch` rows (default 8192), subject to the conversion byte budget.
See [type mapping](../README.md#type-mapping), including the timestamp range/precision choice.

Both execution entry points accept queries, DML, DDL and bulk ingest:

| Work | `execute()` | `execute_update()` |
| --- | --- | --- |
| Query | Arrow result stream | Drains and discards rows; returns `None`. |
| Plain DML | Empty result stream | Affected-row count in autocommit; `None` when buffered. |
| DML with `THEN RETURN` | Returned rows, materialized before commit completes | Discards returned rows; reports affected count. |
| DDL | Empty result stream | `None`. |
| Bulk ingest | Empty result stream | Ingested-row count in autocommit; `None` when buffered. |

SQL classification and splitting live in [sql.rs](../src/sql.rs). A semicolon-separated DML
batch must contain only DML and executes atomically through `ExecuteBatchDml`. DDL uses the
Database Admin API. DML with `THEN RETURN` requires autocommit and cannot be combined with
other statements in a batch.

Autocommit is the default. In manual mode, the first query or write chooses the transaction kind:
queries share a read-only snapshot; DML and ingest mutations buffer until commit. Mixing reads
and writes fails with `InvalidState`, so there is no read-your-writes. DDL always executes
immediately and cannot be rolled back. See [transactions](transactions.md).

## Parameters and ingest

`bind` supplies an Arrow batch; `bind_stream` accepts a reader but currently collects its batches
in memory before execution.

- For SQL parameters, columns bind positionally to distinct `@name` parameters in SQL order.
  A parameter may be written `@name`, ``@`name` `` or with whitespace or a comment after the `@`,
  and names are matched case-insensitively, as Spanner resolves them.
  `adbc.statement.bind_by_name=true` matches column names instead. Multiple bound rows execute
  the query for each row, sharing one snapshot.
- For ingest, `adbc.ingest.target_table` selects a table and `adbc.ingest.mode` controls its
  creation or replacement. Table, schema and column names are validated before any RPC: a name
  containing a backtick, a backslash or a control character is rejected rather than escaped,
  because Spanner's DDL parser does not honour escapes inside backticks. Rows become insert
  mutations. Autocommit ingest commits chunks, so a
  whole ingest is not atomic; manual mode buffers all mutations for one commit. Any table DDL
  still executes immediately.

Setting SQL clears an ingest target, and setting an ingest target clears SQL. `prepare()` checks
that a destination is configured; Spanner plans SQL during execution.

## Metadata and planning

| Method | Result |
| --- | --- |
| `get_info` | Driver/vendor metadata; unsupported info codes are omitted. |
| `get_objects` | Catalogs, schemas, tables, columns and constraints from `INFORMATION_SCHEMA`. |
| `get_table_schema` | A table's columns mapped to Arrow fields. |
| `get_table_types` | `BASE TABLE` and `VIEW`. |
| `get_statistics` | Exact row, distinct and null counts from one aggregate scan per table, even when approximation is requested. |
| `get_statistic_names` | An empty, typed result: there are no driver-specific statistics. |
| `get_parameter_schema` | The bound schema, or parameter names/types from a best-effort PLAN probe; unresolved types are Arrow `Null`. |
| `execute_schema` | Query result schema from a PLAN probe; DML and DDL are rejected. |

### Catalogs

The catalog is the database id: `<d>` in `projects/<p>/instances/<i>/databases/<d>`. Metadata
results, `adbc.connection.catalog` and foreign-key usages report this same name. Catalog filters
must match it; an empty catalog filter matches nothing. `get_table_schema` and ingest catalog
options use an exact name, while `get_objects` and `get_statistics` accept catalog patterns.

`adbc.connection.db_schema` is `""`. Named schemas are supported through qualified names and
metadata, but there is no selectable session schema.

## Partitioning and cancellation

`execute_partitions` produces descriptors for a root-partitionable query; `read_partition`
executes one and streams its rows. Partitioning accepts at most one bound parameter row and
always uses its own batch read-only transaction. Descriptors contain SQL and session/transaction
identity and are not authenticated: only accept them from trusted sources. Set the reading
connection's timestamp precision to match the producing statement.

Cancellation interrupts the object's current blocking call and remains latched for that
operation, including subsequent result fetches. A new operation gets a fresh signal. Cancelling
an idle object succeeds; the driver does not track operation liveness to report `InvalidState`.

## Errors

[error.rs](../src/error.rs) maps gRPC failures to ADBC statuses and preserves structured details.
For example, `PERMISSION_DENIED` becomes `Unauthorized`, `FAILED_PRECONDITION` becomes
`InvalidState`, and `DEADLINE_EXCEEDED` becomes `Timeout`.

Rust callers and C ABI 1.0.0 callers receive the numeric gRPC status in `vendor_code`. C ABI
1.1.0 reserves that field for its details sentinel, so the driver supplies the original code as
the decimal-valued `adbc.spanner.vendor_code` detail instead.

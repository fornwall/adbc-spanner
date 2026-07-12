# Configuration options reference

The complete, authoritative list of every option the `adbc-spanner` driver supports, modeled on
the [BigQuery ADBC driver's options page](https://github.com/adbc-drivers/bigquery/blob/main/go/docs/bigquery.md).
Options exist at three levels — **database**, **connection** and **statement** — matching the ADBC
object they are set on. Driver-specific options use the `spanner.*` prefix; the standard `adbc.*`
(spec) options the driver honours are listed with their spec meaning. Anything not listed here is
rejected: setting an unknown option fails with `NotImplemented`, reading one fails with `NotFound`.

Options are set through the standard ADBC surfaces: `set_option` on the object,
`new_database_with_opts` / `new_connection_with_opts`, the C driver manager's
`AdbcDatabaseSetOption` / `AdbcConnectionSetOption` / `AdbcStatementSetOption`, or `db_kwargs` /
`conn_kwargs` in the Python `adbc_driver_manager` bindings.

## Value coercion

All levels parse option values with the same shared rules (`src/options.rs`):

- **boolean** — the strings `true` / `false` / `1` / `0` / `yes` / `no` (case-insensitive), or an
  integer value (`0` = false, any non-zero = true). Anything else fails with `InvalidArguments`.
- **positive integer** — an integer value or a numeric string; must be `> 0`.
- **non-negative seconds** — an integer value or a numeric string; must be `>= 0`.
- **string** — must be a string value; other value kinds fail with `InvalidArguments`.

The *Round-trips* column below says what `get_option` (the string form; `AdbcDatabaseGetOption`
etc.) returns for the option. Reading an option that is unset — and has no default to report —
fails with `NotFound`. The typed getters (`get_option_int` / `get_option_double`) reinterpret the
same stored string, so they serve any set option whose value parses as that type — e.g.
`spanner.rows_per_batch` and `spanner.partition.max_count` through `get_option_int`, and the
`spanner.rpc.timeout_seconds.*` options through `get_option_double` (integer-valued options are
served as doubles too) — and fail with `InvalidArguments` for options whose value does not.

## Database options

Set before connecting (a connection is established by `new_connection`; the database object only
holds configuration).

| Option | Type / allowed values | Default | Round-trips | Description |
| ------ | --------------------- | ------- | ----------- | ----------- |
| `uri` | string: `projects/<p>/instances/<i>/databases/<d>`, or a `spanner:` connection URI (see [Connection URIs](#connection-uris)) | — (required) | yes, when set (a connection URI reports the expanded database path, not the original URI) | **Standard ADBC.** The fully-qualified Spanner database path. Connecting without it fails with `InvalidState`. |
| `spanner.endpoint` | string: gRPC endpoint URL, e.g. `http://localhost:9010` | unset (production Spanner service) | yes, when set | Explicit gRPC endpoint, e.g. a Spanner emulator. Takes precedence over the endpoint derived from `SPANNER_EMULATOR_HOST` (see [Environment](#environment)). |
| `spanner.emulator` | boolean | `false` (forced `true` when `SPANNER_EMULATOR_HOST` is set non-empty) | yes, always (`true`/`false`) | Connect with **anonymous credentials** (emulator mode). Combining emulator mode with explicitly configured credentials (`spanner.auth.keyfile`, `spanner.auth.keyfile_json`, `spanner.auth.impersonate.target_principal`, or `spanner.auth.access_token`) is refused at connect time with `InvalidState` instead of silently ignoring them; ambient ADC does not conflict. |
| `spanner.auth.keyfile` | string: path to a credential JSON file | unset (Application Default Credentials) | yes, when set | Path to a Google credential JSON key file (dbt's `keyfile`). The credential flow is auto-detected from the JSON's `"type"` field: `service_account`, `authorized_user`, `impersonated_service_account`, or `external_account`. Overridden by `spanner.auth.keyfile_json` if both are set. See [README § Authentication](../README.md#authentication). |
| `spanner.auth.keyfile_json` | string: inline credential JSON | unset (Application Default Credentials) | yes, when set | Inline Google credential JSON (dbt's `keyfile_json`); same auto-detection as `spanner.auth.keyfile`, and wins over it when both are set. |
| `spanner.auth.impersonate.target_principal` | string: service-account email | unset (no impersonation) | yes, when set | Setting this **enables service-account impersonation**: the base credentials (keyfile or ADC) mint a short-lived token for this target via the IAM Credentials `generateAccessToken` API, and the driver authenticates as the target. Follows gcloud's `--impersonate-service-account` / `google-cloud-auth`'s `impersonated` builder. See [README § Service-account impersonation](../README.md#service-account-impersonation). |
| `spanner.auth.impersonate.delegates` | string: comma-separated service-account emails | unset (no delegation chain) | yes, when non-empty (normalised: entries trimmed, empties dropped, re-joined with `,`) | Delegation chain for impersonation; each account must hold the *Token Creator* role on the next, the last on the target principal. Only used when a target principal is set. |
| `spanner.auth.impersonate.scopes` | string: comma-separated OAuth 2.0 scopes | unset (the `cloud-platform` scope) | yes, when non-empty (normalised as above) | Scopes for the impersonated token. Only used when a target principal is set. |
| `spanner.auth.impersonate.lifetime` | non-negative seconds | `3600` (one hour) | yes, when explicitly set (the implicit default is **not** reported) | Lifetime of the impersonated access token, in seconds. Only used when a target principal is set. |
| `spanner.auth.access_token` | string: OAuth 2.0 bearer token | unset (Application Default Credentials) | yes, when set (returned verbatim, matching `spanner.auth.keyfile_json`) | Authenticate with a caller-supplied OAuth 2.0 access token, sent verbatim as `Authorization: Bearer <token>` with **no refresh** (the caller owns token validity). A complete credential in its own right, so it is **mutually exclusive** with `spanner.auth.keyfile`, `spanner.auth.keyfile_json`, and `spanner.auth.impersonate.target_principal` — combining it with any of them is refused at connect time with `InvalidState`. See [README § OAuth access token](../README.md#oauth-access-token). |
| `spanner.auth.quota_project` | string: GCP project id | unset (the credential's own project) | yes, when set (`""` unsets) | The **quota / billing project** charged for API usage, decoupled from the project that owns the data — sent as the `x-goog-user-project` header. Attached to whichever credentials are in effect (ADC, keyfile, impersonation, or the access token), so it composes with every non-emulator credential path; the caller must hold `serviceusage.services.use` on it. Refused in emulator mode (which ignores it), like the credential options. Mirrors BigQuery's `bigquery.auth.quota_project` / gcloud's `--billing-project`. If `GOOGLE_CLOUD_QUOTA_PROJECT` is set, the auth library gives it precedence. See [README § Quota / billing project](../README.md#quota--billing-project). |

### Connection URIs

Instead of a bare database path, `uri` also accepts a **connection URI** with
the `spanner:` scheme (matched ASCII case-insensitively; any other scheme, including
`cloudspanner:`, is not recognised and the value is stored verbatim as a database path):

```text
spanner:///projects/<p>/instances/<i>/databases/<d>?spanner.endpoint=http://localhost:9010&spanner.emulator=true
spanner://localhost:9010/projects/<p>/instances/<i>/databases/<d>
```

The URI path is the database path; an optional `//host:port` authority becomes `spanner.endpoint`
(write `spanner:///projects/…`, with three slashes, when no endpoint host is intended). Query
parameters must be database-option names from the table above (unknown keys are rejected with
`InvalidArguments`); values are percent-decoded per RFC 3986 (`+` is a literal plus, not a space).
The URI is expanded into the individual options at the moment it is set, so precedence is
last-writer-wins per option: a later `set_option` overrides what the URI carried, and the URI
overwrites only the options it actually names. `get_option("uri")` reports the stored database
path, not the original URI.

## Connection options

| Option | Type / allowed values | Default | Round-trips | Description |
| ------ | --------------------- | ------- | ----------- | ----------- |
| `adbc.connection.autocommit` | boolean | `true` | yes, always (`true`/`false`) | **Standard ADBC.** `false` enters manual transaction mode: DML is **buffered** and applied atomically in one read/write transaction on `commit` (`rollback` discards it); `execute_update` returns an unknown row count until then, and queries/DDL still run immediately, so there is **no read-your-writes** and DML/DDL can reorder — see [README § Status](../README.md#status) for the full caveats. Setting it back to `true` commits any buffered transaction (on failure the buffer is restored and the error returned). |
| `adbc.connection.readonly` | boolean | `false` | yes, always (`true`/`false`) | **Standard ADBC.** Reject all writes on this connection: DML, DDL and bulk ingest fail with `InvalidState`; queries still run. The flag is live — statements read it at execution time, so toggling it immediately affects statements that already exist. |
| `adbc.connection.transaction.isolation_level` | one of `adbc.connection.transaction.isolation.default`, `…isolation.serializable`, `…isolation.repeatable_read` | `adbc.connection.transaction.isolation.default` (the database default) | yes, always (the canonical spec string) | **Standard ADBC.** Isolation level for read/write (DML) transactions. `serializable` and `repeatable_read` map to Spanner's isolation levels; `default` leaves the database default. The other spec levels (`read_uncommitted`, `read_committed`, `snapshot`, `linearizable`) are rejected with `NotImplemented`; unknown strings with `InvalidArguments`. |
| `spanner.read.staleness` | `exact:<duration>`, `max:<duration>`, `read:<rfc3339>` or `min:<rfc3339>` (see [Stale reads](#stale-reads)); `""` unsets | unset (strong read) | yes, when set (the raw, trimmed value) | Read bound for read-only queries. Becomes the default for statements this connection creates (statements may override). One value at a time; setting a new value replaces the old. |
| `spanner.max_timestamp_precision` | `nanoseconds_error_on_overflow` or `microseconds` (see [Timestamp precision](#timestamp-precision)); `""` resets to the default | `nanoseconds_error_on_overflow` | yes, always (the effective mode) | How `TIMESTAMP` columns map to Arrow: `Timestamp(Nanosecond, "UTC")` with a loud error on instants outside ~1677–2262 (the default), or `Timestamp(Microsecond, "UTC")` covering Spanner's full 0001–9999 range (sub-microsecond digits truncate toward negative infinity). Becomes the default for statements this connection creates (statements may override); also governs `get_table_schema` and `read_partition`, which have no statement. |
| `spanner.request.priority` | `low`, `medium` or `high` (case-insensitive); `""` unsets | unset (service default, high) | yes, when set (the canonical lowercase form) | [Request priority](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/RequestOptions) applied to every query/DML statement the driver builds, and the commit priority of every read/write transaction. Becomes the default for statements this connection creates (statements may override). |
| `spanner.request.tag` | free-form string; `""` unsets | unset | yes, when set | [Request tag](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags) attached to every query/DML request the driver builds (surfaced in query/transaction statistics). Becomes the default for statements this connection creates (statements may override). Driver-internal metadata queries stay untagged. |
| `spanner.directed_read` | `include`/`exclude` replica selection (see [Directed reads](#directed-reads)); `""` unsets | unset (Spanner's own routing) | yes, when set (the raw, trimmed value) | [Directed read](https://docs.cloud.google.com/spanner/docs/directed-reads) replica selection applied to **read-only queries** (Spanner rejects it on writes). Becomes the default for statements this connection creates (statements may override). |
| `spanner.query.optimizer_version` | opaque version string, e.g. `"6"` or `"latest"`; `""` unsets | unset (database/service default) | yes, when set (the raw value) | [Query optimizer version](https://docs.cloud.google.com/spanner/docs/query-optimizer/manage-query-optimizer) applied as a `QueryOptions` on every query statement the driver builds. Becomes the default for statements this connection creates (statements may override). |
| `spanner.query.optimizer_statistics_package` | opaque statistics-package name; `""` unsets | unset (database default) | yes, when set (the raw value) | [Optimizer statistics package](https://docs.cloud.google.com/spanner/docs/query-optimizer/statistics-packages) applied as a `QueryOptions` on every query statement the driver builds. Same inheritance as `spanner.query.optimizer_version`. |
| `spanner.transaction.tag` | free-form string; `""` unsets | unset | yes, when set | Transaction tag attached to every read/write transaction the driver builds (autocommit DML, the manual-mode commit, ingest commits). Connection level only. |
| `spanner.commit.max_delay` | duration in `0..=500ms` (staleness grammar — default unit seconds, plus `s`/`ms`/`us`/`ns`/`m`/`h`; e.g. `100ms`, `0.2s`); `""` unsets | unset (no delay) | yes, when set (the raw, trimmed value) | [Maximum commit delay](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions) Spanner may add to a read/write commit so it can batch it with others (throughput-for-latency). Applied at every read/write commit the driver builds: autocommit DML, the `ExecuteBatchDml` batch runner, the manual-mode commit, and the bulk-ingest write-only transaction. Values above 500ms (and malformed ones) are rejected with `InvalidArguments`. Becomes the default for statements this connection creates (statements may override). |
| `spanner.commit_stats` | boolean (`true`/`false`/`1`/`0`/`yes`/`no`, or an integer); `""` unsets | `false` | yes, always (`true`/`false`) | Request [commit statistics](https://docs.cloud.google.com/spanner/docs/commit-statistics) on the read/write commits the driver builds (the same four sites as `spanner.commit.max_delay`). When enabled, the returned **mutation count** of the most recent commit is captured and read back via `spanner.commit_stats.mutation_count`. On a connection the count captured here is the **manual-mode commit's** (autocommit DML / bulk ingest report on the statement). Becomes the default for statements this connection creates (statements may override). |
| `spanner.commit_stats.mutation_count` | read-only (setting it is rejected with `NotImplemented`) | `NotFound` until a commit with stats has run | via `get_option` / `get_option_int` | The mutation count from the connection's most recent commit run with `spanner.commit_stats` enabled (the manual-mode commit). `NotFound` until such a commit has run. |
| `spanner.rpc.timeout_seconds.query` | finite, non-negative seconds (fractions allowed); `0` disables; `""` unsets (see [RPC timeouts](#rpc-timeouts)) | unset (no deadline) | yes, when set (also via `get_option_double`) | Overall deadline on a query's **initial execution**: the `ExecuteStreamingSql` call plus the first chunk of the streamed result, the `execute_schema` / `execute_partitions` probes, and `read_partition`'s initial fetch. Also bounds the driver-internal metadata **reads** (`get_objects`, `get_statistics`, `get_table_schema`, the ingest table-exists probe). Expiry fails with `Timeout`. Becomes the default for statements this connection creates (statements may override). |
| `spanner.rpc.timeout_seconds.update` | as `…query` | unset (no deadline) | yes, when set (also via `get_option_double`) | Overall deadline on each **write** operation: an autocommit DML / batch-DML transaction, the manual-mode commit, each bulk-ingest commit chunk, and a DDL change (the admin `UpdateDatabaseDdl` call **and** its long-running-operation poll loop) — covering any retries the client performs within it. A commit whose confirmation the driver stopped waiting for may still have landed server-side — the usual ambiguity of any timed-out commit (a timed-out DDL likewise may already have applied). Same inheritance as `…query`. |
| `spanner.rpc.timeout_seconds.fetch` | as `…query` | unset (no deadline) | yes, when set (also via `get_option_double`) | Overall deadline on **each subsequent chunk fetch** of a streamed result (after the first, which `…query` covers), enforced inside the background prefetch task so a stalled stream fails the consumer's next batch with `Timeout`. Same inheritance as `…query`. |
| `spanner.retry.max_attempts` | positive integer; `""` unsets (see [Retry tuning](#retry-tuning)) | unset (client default, no cap) | yes, when set (also via `get_option_int`) | Cap on the number of attempts (first try + retries) the client makes for a retryable RPC; `1` disables retrying. Bounds the client's default retry policy without dropping its transport-error-on-idempotent retrying. Becomes the default for statements this connection creates (statements may override). |
| `spanner.retry.max_elapsed_seconds` | finite, strictly positive seconds (fractions allowed); `""` unsets (see [Retry tuning](#retry-tuning)) | unset (client default, no cap) | yes, when set (also via `get_option_double`) | Cap on the total wall-clock time spent retrying a retryable RPC before the last error is surfaced. Combines with `spanner.retry.max_attempts` (whichever limit fires first wins). Same inheritance as `…max_attempts`. |
| `spanner.retry.backoff.initial_seconds` | finite, strictly positive seconds (fractions allowed); `""` unsets (see [Retry tuning](#retry-tuning)) | unset (client default, 1s) | yes, when set (also via `get_option_double`) | Initial delay of the client's exponential backoff between retry attempts. Setting any `spanner.retry.backoff.*` knob replaces the client's default backoff (unset knobs take the client defaults 1s / 60s / ×2, clamped to the gax recommended ranges). Independent of the attempt / elapsed-time caps. Same inheritance as `…max_attempts`. |
| `spanner.retry.backoff.max_seconds` | finite, strictly positive seconds (fractions allowed); `""` unsets (see [Retry tuning](#retry-tuning)) | unset (client default, 60s) | yes, when set (also via `get_option_double`) | Ceiling the growing backoff delay is truncated at. Raised to the effective initial delay if set below it. Same combination / inheritance as `…backoff.initial_seconds`. |
| `spanner.retry.backoff.multiplier` | finite, strictly positive number; `""` unsets (see [Retry tuning](#retry-tuning)) | unset (client default, `2.0`) | yes, when set (also via `get_option_double`) | Per-attempt growth factor for the backoff delay. A value below `1.0` is floored to `1.0` (constant delay). Same combination / inheritance as `…backoff.initial_seconds`. |

Two standard connection options are **read-only**: `adbc.connection.catalog` and
`adbc.connection.db_schema` (the "current" catalog/schema) both report `""` — a Spanner database
has a single, unnamed catalog and default schema — and cannot be set.

## Statement options

| Option | Type / allowed values | Default | Round-trips | Description |
| ------ | --------------------- | ------- | ----------- | ----------- |
| `spanner.rows_per_batch` | positive integer | `8192` | yes, always (also via `get_option_int`) | Number of rows converted into each Arrow `RecordBatch` streamed by `execute`. Larger batches trade memory for fewer per-batch conversions; smaller batches lower first-batch latency and peak memory. |
| `spanner.data_boost` | boolean | `false` | yes, always (`true`/`false`) | Run `execute_partitions` partitions on [Data Boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview) (Spanner's serverless, workload-isolated compute). Baked into every partition descriptor, so `read_partition` honours it on any connection. |
| `spanner.partition.max_count` | positive integer | unset (Spanner chooses) | yes, when set (also via `get_option_int`) | Hint for the maximum number of partitions returned by `execute_partitions`; Spanner may return fewer. |
| `spanner.read.staleness` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement read-bound override. Set `""` to clear a bound inherited from the connection (i.e. force a strong read). |
| `spanner.max_timestamp_precision` | as the connection option; `""` resets to the **driver** default | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement timestamp-precision override (see [Timestamp precision](#timestamp-precision)). Note `""` resets to the driver default (`nanoseconds_error_on_overflow`), not to the connection's value. |
| `spanner.request.priority` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement request-priority override. |
| `spanner.request.tag` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement request-tag override. |
| `spanner.directed_read` | as the connection option (see [Directed reads](#directed-reads)); `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement directed-read override. Applies to this statement's read-only queries only. |
| `spanner.commit.max_delay` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement max-commit-delay override. |
| `spanner.commit_stats` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes, always (`true`/`false`) | Per-statement commit-stats override. When enabled, `spanner.commit_stats.mutation_count` on **this statement** reports the mutation count from its most recent autocommit DML or bulk-ingest commit. |
| `spanner.commit_stats.mutation_count` | read-only (setting it is rejected with `NotImplemented`) | `NotFound` until a commit with stats has run | via `get_option` / `get_option_int` | The mutation count from this statement's most recent commit run with `spanner.commit_stats` enabled (autocommit DML or bulk ingest). For a chunked bulk ingest it reports the most recent chunk's count. `NotFound` until such a commit has run. |
| `spanner.query.optimizer_version` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement optimizer-version override. |
| `spanner.query.optimizer_statistics_package` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement optimizer-statistics-package override. |
| `spanner.rpc.timeout_seconds.query` | as the connection option; `0` disables; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement (also via `get_option_double`) | Per-statement query-timeout override (see [RPC timeouts](#rpc-timeouts)). |
| `spanner.rpc.timeout_seconds.update` | as the connection option; `0` disables; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement (also via `get_option_double`) | Per-statement update-timeout override. |
| `spanner.rpc.timeout_seconds.fetch` | as the connection option; `0` disables; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement (also via `get_option_double`) | Per-statement fetch-timeout override. |
| `spanner.retry.max_attempts` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement (also via `get_option_int`) | Per-statement retry-attempt-cap override (see [Retry tuning](#retry-tuning)). |
| `spanner.retry.max_elapsed_seconds` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement (also via `get_option_double`) | Per-statement retry-elapsed-cap override. |
| `spanner.retry.backoff.initial_seconds` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement (also via `get_option_double`) | Per-statement backoff-initial-delay override. |
| `spanner.retry.backoff.max_seconds` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement (also via `get_option_double`) | Per-statement backoff-maximum-delay override. |
| `spanner.retry.backoff.multiplier` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement (also via `get_option_double`) | Per-statement backoff-multiplier override. |
| `adbc.statement.bind_by_name` | boolean | `false` (positional) | yes (`true`/`false`) | How bound Arrow columns pair with the query's `@name` parameters, following the ADBC SQLite reference driver's `bind_by_name` convention ([apache/arrow-adbc#3362](https://github.com/apache/arrow-adbc/issues/3362)). **`false`** (the default): strictly positional — the *i*-th bound column binds to the *i*-th distinct parameter in query order, column names ignored (the ADBC ordinal contract positional clients and validation suites rely on). **`true`**: strict by-name — each column binds to `@<its own name>` (order-independent); a bound column that names no query parameter fails with `InvalidArguments` naming the missing parameter. See [README § Status](../README.md#status). |
| `adbc.ingest.target_table` | string: table name | unset | yes, when set | **Standard ADBC.** Bulk-ingest target table. Setting it clears any SQL query on the statement (query and ingest target are mutually exclusive on one handle). |
| `adbc.ingest.target_db_schema` | string: named schema (`""` = Spanner's default, unnamed schema) | unset (default schema) | yes, when set | **Standard ADBC.** Named schema qualifying the ingest target table. |
| `adbc.ingest.target_catalog` | `""` only | unset | yes, when set | **Standard ADBC.** Spanner has a single, unnamed catalog, so only the empty catalog is accepted; any other name fails with `NotImplemented`. |
| `adbc.ingest.temporary` | boolean; only `false` accepted | `false` | yes, always reports `false` | **Standard ADBC.** Spanner has no temporary tables. The spec default `false` is accepted as a no-op (so generic clients that always set it keep working); `true` fails with `NotImplemented`. |
| `adbc.ingest.mode` | `adbc.ingest.mode.append`, `adbc.ingest.mode.create`, `adbc.ingest.mode.create_append`, `adbc.ingest.mode.replace` (short forms `append` / `create` / `create_append` / `replace` also accepted) | append | yes, when set (always the canonical `adbc.ingest.mode.*` form) | **Standard ADBC.** Bulk-ingest mode. The create/replace modes build the table from the ingest data's Arrow schema with a synthetic `adbc_ingest_key` UUID primary key (Spanner requires one) — see [README § Status](../README.md#status). |
| `spanner.ingest.primary_key` | comma-separated existing column names; `""` unsets | unset (synthetic `adbc_ingest_key` UUID key) | yes, when set (the comma-joined column list) | Primary key for the create/`create_append`/`replace` ingest modes. Unset, they add a synthetic `adbc_ingest_key` UUID key. Set to one or more **existing** ingest columns (in key order — this drives Spanner's physical row layout) to key on them instead, adding no synthetic column. A named column absent from the ingest data fails with `InvalidArguments`; Spanner separately rejects key columns of unsupported types (e.g. `FLOAT64`, `JSON`, `ARRAY`). Ignored by `append` (the existing table's key governs). |
| `spanner.ingest.batch_write` | boolean; `""` unsets | `false` (write-only transaction) | yes (`true`/`false`) | Route an **autocommit** bulk ingest's per-chunk mutations through Spanner's **BatchWrite** RPC instead of a write-only transaction — a non-atomic, higher-throughput ("firehose") transport. Insert semantics, chunking, the ingested-row count, the read-only-connection guard and the append-mode `NotFound`/`AlreadyExists` remap are all preserved; BatchWrite applies its mutation groups **non-atomically** (the same "not atomic as a whole" guarantee the multi-chunk write-only path already has). Only affects autocommit ingests — **ignored** in manual-transaction mode (ingests buffer and commit atomically there). Because BatchWrite takes no per-request commit options, `spanner.request.priority` / `spanner.request.tag` / `spanner.commit.max_delay` / `spanner.commit_stats` do **not** apply on this path (and `spanner.commit_stats` reports no `mutation_count` for a BatchWrite ingest). |

## Stale reads

Read-only queries default to a **strong** bound. The single `spanner.read.staleness` option —
available at connection *and* statement level — requests a stale read instead (cheaper and
lock-free; ideal for analytics). Its value is one of four prefixed forms — two *relative* (a
duration) and two *absolute* (an RFC 3339 timestamp):

- `exact:<duration>` — read exactly `<duration>` in the past (a single, repeatable timestamp).
- `max:<duration>` — read at any timestamp within `<duration>` of now (bounded staleness; the
  server picks — single-use reads only).
- `read:<rfc3339>` (or a bare `<rfc3339>`) — read exactly as of that timestamp.
- `min:<rfc3339>` — read at that timestamp or later (bounded staleness; single-use reads only).

`<duration>` is a non-negative number with an optional unit suffix: `s` (seconds, the default),
`ms`, `us`/`µs`, `ns`, `m` (minutes) or `h` (hours). Examples: `exact:10`, `exact:2.5s`,
`max:500ms`, `max:1m`, `read:2026-07-07T00:00:00Z`, `min:2026-07-07T00:00:00+02:00`.

The four prefixes are distinct, so a value is unambiguous. The option holds one bound at a time;
setting a new value replaces the old, and `""` unsets it. Values are trimmed before parsing;
malformed values fail with `InvalidArguments`. A statement inherits the connection's bound at
creation and may override it (including clearing it with `""`). Because Spanner accepts the
bounded-staleness kinds (`max:` / `min:`) only on single-use transactions, contexts that need a
multi-use read-only transaction (a bound query over several parameter rows, `execute_partitions`)
pin them to their most-stale legal equivalent (`max:<d>` → exact staleness `<d>`, `min:<t>` → read
timestamp `<t>`).

## Directed reads

[Directed reads](https://docs.cloud.google.com/spanner/docs/directed-reads) steer where a read-only
query is served — to (or away from) replicas in a given region and/or of a given type.
`spanner.directed_read` — available at connection *and* statement level — carries the selection as a
small grammar:

```text
<mode> [ ":" <selection> ("," <selection>)* ] [ ";auto_failover_disabled" ]
```

- `<mode>` is `include` or `exclude` (case-insensitive):
  - `include` — an **ordered preference** list; Spanner tries the selections in turn.
  - `exclude` — replicas Spanner routes **around**.
- Each `<selection>` is `<location>`, `<location>:<type>`, or `:<type>` (at least one of the two):
  - `<location>` is a region such as `us-east1`.
  - `<type>` is `read_write`, `read_only` or `any` (case-insensitive). Omitted or `any` matches every
    replica type.
- The optional `;auto_failover_disabled` suffix — valid only with `include` — stops Spanner from
  falling back to a replica outside the list when the listed replicas are unavailable.

Examples:

- `include:us-east1` — prefer any replica in `us-east1`.
- `include:us-east1:read_only,us-east4:read_write` — prefer a read-only replica in `us-east1`, then a
  read-write replica in `us-east4`.
- `exclude:us-central1` — never route to replicas in `us-central1`.
- `include:us-east1;auto_failover_disabled` — prefer `us-east1` and do not fail over elsewhere.
- `include::read_only` — prefer any read-only replica, in any location.

Directed reads apply to **read-only queries only**: the driver attaches them to its query paths
(autocommit and manual mode, including parameterized/bound queries and `execute_partitions`) and
never to DML/DDL, which Spanner would reject. Values are trimmed before parsing; malformed values
fail with `InvalidArguments`. Unset (the default) leaves Spanner's own routing; set `""` to unset. A
statement inherits the connection's value at creation and may override it (including clearing it with
`""`).

## Timestamp precision

Spanner `TIMESTAMP` values span 0001-01-01 to 9999-12-31 at nanosecond precision. Arrow's
`Timestamp(Nanosecond)` stores an `i64` count of nanoseconds since the Unix epoch, which spans only
~1677-09-21 to 2262-04-11 — so the two ranges cannot both be honoured at nanosecond precision.
`spanner.max_timestamp_precision` — available at connection *and* statement level — picks how the
driver resolves the mismatch:

- **`nanoseconds_error_on_overflow`** (the default) — `TIMESTAMP` maps to
  `Timestamp(Nanosecond, "UTC")`, preserving the full nanosecond precision Spanner delivers on the
  wire. Reading a well-formed instant outside the representable ~1677–2262 window is a **loud
  `InvalidArguments` error** naming the column, the offending value, and this option as the escape
  hatch.
- **`microseconds`** — `TIMESTAMP` maps to `Timestamp(Microsecond, "UTC")`, whose `i64` covers
  Spanner's **entire** 0001–9999 range (and far beyond). Any sub-microsecond digits a value carries
  are **truncated toward negative infinity** (a floor on the timeline, also for pre-epoch instants:
  `…56.789012345Z` → `…56.789012`, and one nanosecond *before* the epoch becomes microsecond `-1`,
  not `0`). This is lossy for values with real nanosecond precision — that loss is the price of the
  full range.

Exactly these two values exist **by design**. A third mode that keeps nanoseconds and silently
wraps or clamps out-of-range values (as some drivers offer under a plain `nanoseconds` value) is
deliberately not offered: a wrapped timestamp is a plausible-looking, wrong instant —
indistinguishable from real data, i.e. silent corruption. Every supported mode is either lossless
or explicit about what it loses (documented microsecond truncation), and anything else fails
loudly. The option is modeled on the Snowflake ADBC driver's `max_timestamp_precision`
([apache/arrow-adbc#2917](https://github.com/apache/arrow-adbc/issues/2917)), minus its
silent-wraparound value.

The selected mode applies uniformly to **every** surface that produces timestamp data or
timestamp-typed schemas: `execute` (plain, parameterized and multi-row bound queries, including
the streamed batches after the first), DML `THEN RETURN` rows, `execute_schema` (the PLAN probe)
and the `execute_partitions` schema — so the advertised schema always carries the same unit as the
data — plus `get_table_schema` and `read_partition` at the connection level. `read_partition`
decodes under the **reading** connection's mode: set it to the same value as the producing
statement so the descriptor's schema matches what is streamed.

A statement inherits the connection's mode at creation and may override it; setting `""` resets to
the driver default (`nanoseconds_error_on_overflow`). `get_option` always reports the effective
mode. The **bind** (write) direction is unaffected: Arrow timestamp parameters of any unit
(`Second`/`Millisecond`/`Microsecond`/`Nanosecond`) are always accepted and bound at their full
source precision.

## RPC timeouts

Without a deadline, a hung RPC blocks the (synchronous) ADBC call indefinitely, with `cancel` as
the only escape. The `spanner.rpc.timeout_seconds.{query,update,fetch}` options — available at
connection *and* statement level, named in parallel with the Flight SQL ADBC driver's
`adbc.flight.sql.rpc.timeout_seconds.*` family — bound the driver's Spanner-facing operations:

- **`query`** — the *initial execution* of a query: the `ExecuteStreamingSql` call plus the first
  chunk of the streamed result (which settles the schema), the `execute_schema` /
  `execute_partitions` probes, and `read_partition`'s initial fetch. It also bounds the
  driver-internal metadata **read** queries — `get_objects`, `get_statistics` (both its
  INFORMATION_SCHEMA discovery fetch and its per-table aggregate scans), `get_table_schema`, and the
  bulk-ingest table-exists probe — since each is an execution of a query.
- **`fetch`** — *each subsequent chunk fetch* of a streamed result, enforced inside the background
  prefetch task, so a stream that stalls mid-result fails the consumer's next batch instead of
  hanging. For a bound (parameterized) query over several rows it also covers executing each
  per-row statement as the stream advances.
- **`update`** — each *write* operation: an autocommit DML / batch-DML read/write transaction, the
  manual-mode commit (including the commit performed when autocommit is re-enabled), each
  bulk-ingest commit chunk, and a DDL change — the admin `UpdateDatabaseDdl` call **and** its
  long-running-operation poll loop (the most dangerous unbounded path: an LRO poll with no cap).

Each value is a number of **seconds**, parsed as a double (fractions allowed) from a numeric
string, integer or double value; it must be finite and non-negative — `NaN`, the infinities and
negatives fail with `InvalidArguments`. `0` disables the timeout (same as unset, but it still
round-trips); an empty string (`""`) unsets. A statement inherits the connection's values at
creation and may override each independently. All three round-trip through `get_option` and
`get_option_double`.

Enforcement is an **overall deadline per operation** (a `tokio::time::timeout` around the whole
driver-side operation, including any retries the client performs inside it), not a per-attempt
gRPC timeout. An expired deadline fails with ADBC `Timeout` status. Note that a timed-out *commit*
may still have landed server-side — the usual ambiguity of any commit whose confirmation was not
awaited, and a timed-out DDL change may likewise have already applied (its poll simply stopped
being awaited). Unlike the request-tag/priority options, which leave the driver-internal metadata
queries untagged, these timeouts *do* bound them — `query` covers the metadata reads and `update`
covers DDL — so no driver-side network path is left able to hang unboundedly.

## Retry tuning

Every data-plane RPC the driver issues is retried by the Spanner client under a default policy —
strict [AIP-194](https://google.aip.dev/194), additionally retrying transport / IO errors on
idempotent requests (which all the driver's data-plane RPCs are). That default has **no** attempt or
elapsed-time cap, so a persistently `UNAVAILABLE` backend is retried until the operation-wide [RPC
timeout](#rpc-timeouts) (if any) fires. The two `spanner.retry.*` options — available at connection
*and* statement level — let you *bound* that retrying instead, mirroring the gax
`RetryPolicyExt::with_attempt_limit` / `with_time_limit` knobs:

- **`spanner.retry.max_attempts`** — the maximum number of attempts, the first try plus retries, as
  a positive integer (accepted as an integer, a whole-valued double, or a numeric string). `1`
  disables retrying. Round-trips through `get_option` and `get_option_int`.
- **`spanner.retry.max_elapsed_seconds`** — an upper bound, in seconds, on the total wall-clock time
  spent across attempts before the last error is surfaced as permanent. A finite, strictly positive
  number (fractions allowed), accepted from a numeric string, integer or double. Round-trips through
  `get_option` and `get_option_double`.

The two are independent and may be combined — the retry loop stops at whichever limit is reached
first. Zero, negative, non-finite and (for attempts) fractional or above-`u32::MAX` values fail with
`InvalidArguments`; an empty string (`""`) unsets. A statement inherits the connection's values at
creation and may override each independently.

Three further options tune the *delay between* attempts — the client's truncated exponential backoff
with jitter — mirroring the gax `ExponentialBackoffBuilder` knobs:

- **`spanner.retry.backoff.initial_seconds`** — the first inter-attempt delay, in seconds (client
  default 1s).
- **`spanner.retry.backoff.max_seconds`** — the ceiling the growing delay is truncated at, in seconds
  (client default 60s); raised to the effective initial delay if set below it.
- **`spanner.retry.backoff.multiplier`** — the per-attempt growth factor (client default `2.0`); a
  value below `1.0` is floored to `1.0` (a constant delay).

Setting **any** of them replaces the client's default backoff with an exponential backoff whose unset
knobs take the client defaults, with the whole combination clamped to the gax recommended ranges
(initial delay ≥ 1ms, maximum delay in `[1s, 24h]`, multiplier in `[1.0, 32.0]`) so it can never fail
to build. Each is a finite, strictly positive number accepted from a numeric string, integer or
double, round-trips through `get_option` / `get_option_double`, and an empty string unsets it. These
backoff knobs are **orthogonal** to the attempt / elapsed-time caps above — either family may be set
on its own — and inherit connection→statement the same way.

When **neither** is set the client keeps its default (unbounded) policy, so the feature is purely
opt-in and by default changes nothing. When either is set, the driver applies a bounded policy that
still retries transport / IO errors on idempotent requests exactly like the client's default — the
attempt / elapsed limits are layered on top rather than replacing that behaviour — to every user
query/DML statement, the read/write transaction runner's begin+commit RPCs, the bulk-ingest
write-only transaction, and the `ExecuteBatchDml` batch. This tunes the *per-attempt* retry loop;
the [RPC timeout](#rpc-timeouts) family bounds the *overall* per-operation wall time — the two are
complementary. The transaction-level abort retry (Spanner's optimistic-concurrency re-run on
`ABORTED`) is a separate policy and stays at the client default.

## Environment

- **`SPANNER_EMULATOR_HOST`** — read at connect time. When set non-empty, it supplies the gRPC
  endpoint (unless `spanner.endpoint` was set explicitly, which wins) and forces emulator mode —
  exactly as if `spanner.emulator=true`: anonymous credentials, and explicitly configured
  credential options are refused with `InvalidState` (a stray emulator variable must not silently
  redirect authenticated traffic). A bare `host:port` value gets an `http://` scheme prefixed.
  Note the emulator's gRPC port must be `9010`: the underlying client derives the admin REST
  endpoint by replacing `9010` with `9020` in the endpoint, and DDL goes through the admin API.
- **Application Default Credentials** — when no keyfile or `spanner.auth.access_token` option is set
  (and not in emulator mode), the driver falls back to standard
  [ADC](https://cloud.google.com/docs/authentication/application-default-credentials)
  resolution, which honours `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default
  login`, or the metadata server. See [README § Authentication](../README.md#authentication).

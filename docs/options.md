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
fails with `NotFound`. At the database and connection level only the string getter is implemented
(`get_option_int` / `get_option_double` always fail); the statement level additionally serves
`spanner.rows_per_batch` and `spanner.max_partitions` through `get_option_int`.

## Database options

Set before connecting (a connection is established by `new_connection`; the database object only
holds configuration).

| Option | Type / allowed values | Default | Round-trips | Description |
| ------ | --------------------- | ------- | ----------- | ----------- |
| `uri` | string: `projects/<p>/instances/<i>/databases/<d>` | — (required) | yes, when set | **Standard ADBC.** The fully-qualified Spanner database path. Equivalent to `spanner.database`; the two keys read and write the same value. Connecting without it fails with `InvalidState`. |
| `spanner.database` | string: `projects/<p>/instances/<i>/databases/<d>` | — (required) | yes, when set | Driver-specific alias for `uri`. |
| `spanner.endpoint` | string: gRPC endpoint URL, e.g. `http://localhost:9010` | unset (production Spanner service) | yes, when set | Explicit gRPC endpoint, e.g. a Spanner emulator. Takes precedence over the endpoint derived from `SPANNER_EMULATOR_HOST` (see [Environment](#environment)). |
| `spanner.emulator` | boolean | `false` (forced `true` when `SPANNER_EMULATOR_HOST` is set non-empty) | yes, always (`true`/`false`) | Connect with **anonymous credentials** (emulator mode). Combining emulator mode with explicitly configured credentials (`spanner.keyfile`, `spanner.keyfile_json`, or `spanner.impersonate.target_principal`) is refused at connect time with `InvalidState` instead of silently ignoring them; ambient ADC does not conflict. |
| `spanner.keyfile` | string: path to a credential JSON file | unset (Application Default Credentials) | yes, when set | Path to a Google credential JSON key file (dbt's `keyfile`). The credential flow is auto-detected from the JSON's `"type"` field: `service_account`, `authorized_user`, `impersonated_service_account`, or `external_account`. Overridden by `spanner.keyfile_json` if both are set. See [README § Authentication](../README.md#authentication). |
| `spanner.keyfile_json` | string: inline credential JSON | unset (Application Default Credentials) | yes, when set | Inline Google credential JSON (dbt's `keyfile_json`); same auto-detection as `spanner.keyfile`, and wins over it when both are set. |
| `spanner.impersonate.target_principal` | string: service-account email | unset (no impersonation) | yes, when set | Setting this **enables service-account impersonation**: the base credentials (keyfile or ADC) mint a short-lived token for this target via the IAM Credentials `generateAccessToken` API, and the driver authenticates as the target. Mirrors the BigQuery ADBC driver's `bigquery.impersonate.target_principal`. See [README § Service-account impersonation](../README.md#service-account-impersonation). |
| `spanner.impersonate.delegates` | string: comma-separated service-account emails | unset (no delegation chain) | yes, when non-empty (normalised: entries trimmed, empties dropped, re-joined with `,`) | Delegation chain for impersonation; each account must hold the *Token Creator* role on the next, the last on the target principal. Only used when a target principal is set. |
| `spanner.impersonate.scopes` | string: comma-separated OAuth 2.0 scopes | unset (the `cloud-platform` scope) | yes, when non-empty (normalised as above) | Scopes for the impersonated token. Only used when a target principal is set. |
| `spanner.impersonate.lifetime` | non-negative seconds | `3600` (one hour) | yes, when explicitly set (the implicit default is **not** reported) | Lifetime of the impersonated access token, in seconds. Only used when a target principal is set. |

## Connection options

| Option | Type / allowed values | Default | Round-trips | Description |
| ------ | --------------------- | ------- | ----------- | ----------- |
| `adbc.connection.autocommit` | boolean | `true` | yes, always (`true`/`false`) | **Standard ADBC.** `false` enters manual transaction mode: DML is **buffered** and applied atomically in one read/write transaction on `commit` (`rollback` discards it); `execute_update` returns an unknown row count until then, and queries/DDL still run immediately, so there is **no read-your-writes** and DML/DDL can reorder — see [README § Status](../README.md#status) for the full caveats. Setting it back to `true` commits any buffered transaction (on failure the buffer is restored and the error returned). |
| `adbc.connection.readonly` | boolean | `false` | yes, always (`true`/`false`) | **Standard ADBC.** Reject all writes on this connection: DML, DDL and bulk ingest fail with `InvalidState`; queries still run. The flag is live — statements read it at execution time, so toggling it immediately affects statements that already exist. |
| `adbc.connection.transaction.isolation_level` | one of `adbc.connection.transaction.isolation.default`, `…isolation.serializable`, `…isolation.repeatable_read` | `adbc.connection.transaction.isolation.default` (the database default) | yes, always (the canonical spec string) | **Standard ADBC.** Isolation level for read/write (DML) transactions. `serializable` and `repeatable_read` map to Spanner's isolation levels; `default` leaves the database default. The other spec levels (`read_uncommitted`, `read_committed`, `snapshot`, `linearizable`) are rejected with `NotImplemented`; unknown strings with `InvalidArguments`. |
| `spanner.read.staleness` | `exact:<duration>` or `max:<duration>` (see [Stale reads](#stale-reads)); `""` unsets | unset (strong read) | yes, when set (the raw, trimmed value) | Stale-read bound for read-only queries. Becomes the default for statements this connection creates (statements may override). Mutually exclusive with `spanner.read.timestamp`. |
| `spanner.read.timestamp` | RFC 3339 timestamp, optionally prefixed `read:` or `min:` (see [Stale reads](#stale-reads)); `""` unsets | unset (strong read) | yes, when set (the raw, trimmed value) | Absolute read timestamp for read-only queries. Same inheritance and mutual exclusion as `spanner.read.staleness`. |
| `spanner.request.priority` | `low`, `medium` or `high` (case-insensitive); `""` unsets | unset (service default, high) | yes, when set (the canonical lowercase form) | [Request priority](https://docs.cloud.google.com/spanner/docs/reference/rest/v1/RequestOptions) applied to every query/DML statement the driver builds, and the commit priority of every read/write transaction. Becomes the default for statements this connection creates (statements may override). |
| `spanner.request.tag` | free-form string; `""` unsets | unset | yes, when set | [Request tag](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags) attached to every query/DML request the driver builds (surfaced in query/transaction statistics). Becomes the default for statements this connection creates (statements may override). Driver-internal metadata queries stay untagged. |
| `spanner.transaction.tag` | free-form string; `""` unsets | unset | yes, when set | Transaction tag attached to every read/write transaction the driver builds (autocommit DML, the manual-mode commit, ingest commits). Connection level only. |

Two standard connection options are **read-only**: `adbc.connection.catalog` and
`adbc.connection.db_schema` (the "current" catalog/schema) both report `""` — a Spanner database
has a single, unnamed catalog and default schema — and cannot be set.

## Statement options

| Option | Type / allowed values | Default | Round-trips | Description |
| ------ | --------------------- | ------- | ----------- | ----------- |
| `spanner.rows_per_batch` | positive integer | `8192` | yes, always (also via `get_option_int`) | Number of rows converted into each Arrow `RecordBatch` streamed by `execute`. Larger batches trade memory for fewer per-batch conversions; smaller batches lower first-batch latency and peak memory. |
| `spanner.data_boost_enabled` | boolean | `false` | yes, always (`true`/`false`) | Run `execute_partitions` partitions on [Data Boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview) (Spanner's serverless, workload-isolated compute). Baked into every partition descriptor, so `read_partition` honours it on any connection. |
| `spanner.max_partitions` | positive integer | unset (Spanner chooses) | yes, when set (also via `get_option_int`) | Hint for the maximum number of partitions returned by `execute_partitions`; Spanner may return fewer. |
| `spanner.read.staleness` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement stale-read override. Set `""` to clear a bound inherited from the connection (i.e. force a strong read). Mutually exclusive with `spanner.read.timestamp`. |
| `spanner.read.timestamp` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement read-timestamp override; same semantics as above. |
| `spanner.request.priority` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement request-priority override. |
| `spanner.request.tag` | as the connection option; `""` unsets | inherited from the connection at statement creation | yes — reports the effective value, whether inherited or set on the statement | Per-statement request-tag override. |
| `adbc.ingest.target_table` | string: table name | unset | yes, when set | **Standard ADBC.** Bulk-ingest target table. Setting it clears any SQL query on the statement (query and ingest target are mutually exclusive on one handle). |
| `adbc.ingest.target_db_schema` | string: named schema (`""` = Spanner's default, unnamed schema) | unset (default schema) | yes, when set | **Standard ADBC.** Named schema qualifying the ingest target table. |
| `adbc.ingest.target_catalog` | `""` only | unset | yes, when set | **Standard ADBC.** Spanner has a single, unnamed catalog, so only the empty catalog is accepted; any other name fails with `NotImplemented`. |
| `adbc.ingest.temporary` | boolean; only `false` accepted | `false` | yes, always reports `false` | **Standard ADBC.** Spanner has no temporary tables. The spec default `false` is accepted as a no-op (so generic clients that always set it keep working); `true` fails with `NotImplemented`. |
| `adbc.ingest.mode` | `adbc.ingest.mode.append`, `adbc.ingest.mode.create`, `adbc.ingest.mode.create_append`, `adbc.ingest.mode.replace` (short forms `append` / `create` / `create_append` / `replace` also accepted) | append | yes, when set (always the canonical `adbc.ingest.mode.*` form) | **Standard ADBC.** Bulk-ingest mode. The create/replace modes build the table from the ingest data's Arrow schema with a synthetic `adbc_ingest_key` UUID primary key (Spanner requires one) — see [README § Status](../README.md#status). |

## Stale reads

Read-only queries default to a **strong** bound. `spanner.read.staleness` and
`spanner.read.timestamp` — available at connection *and* statement level — request a stale read
instead (cheaper and lock-free; ideal for analytics):

- `spanner.read.staleness` is a *relative* bound, `"<kind>:<duration>"`:
  - `exact:<duration>` — read exactly `<duration>` in the past (a single, repeatable timestamp).
  - `max:<duration>` — read at any timestamp within `<duration>` of now (bounded staleness; the
    server picks — single-use reads only).

  `<duration>` is a non-negative number with an optional unit suffix: `s` (seconds, the default),
  `ms`, `us`/`µs`, `ns`, `m` (minutes) or `h` (hours). Examples: `exact:10`, `exact:2.5s`,
  `max:500ms`, `max:1m`.

- `spanner.read.timestamp` is an *absolute* bound — an RFC 3339 timestamp, optionally prefixed to
  select the mode:
  - `read:<rfc3339>` (or a bare `<rfc3339>`) — read exactly as of that timestamp.
  - `min:<rfc3339>` — read at that timestamp or later (bounded staleness; single-use reads only).

  Examples: `2026-07-07T00:00:00Z`, `read:2026-07-07T00:00:00Z`, `min:2026-07-07T00:00:00+02:00`.

The two options are **mutually exclusive** — only one read bound can apply to a query. Setting one
while the other is set fails with `InvalidArguments`; set the other to an empty string (`""`) first
to unset it. Values are trimmed before parsing; malformed values fail with `InvalidArguments`. A
statement inherits the connection's bound at creation and may override it (including clearing it
with `""`). Because Spanner accepts the bounded-staleness kinds (`max:` / `min:`) only on
single-use transactions, contexts that need a multi-use read-only transaction (a bound query over
several parameter rows, `execute_partitions`) pin them to their most-stale legal equivalent
(`max:<d>` → exact staleness `<d>`, `min:<t>` → read timestamp `<t>`).

## Environment

- **`SPANNER_EMULATOR_HOST`** — read at connect time. When set non-empty, it supplies the gRPC
  endpoint (unless `spanner.endpoint` was set explicitly, which wins) and forces emulator mode —
  exactly as if `spanner.emulator=true`: anonymous credentials, and explicitly configured
  credential options are refused with `InvalidState` (a stray emulator variable must not silently
  redirect authenticated traffic). A bare `host:port` value gets an `http://` scheme prefixed.
  Note the emulator's gRPC port must be `9010`: the underlying client derives the admin REST
  endpoint by replacing `9010` with `9020` in the endpoint, and DDL goes through the admin API.
- **Application Default Credentials** — when no keyfile option is set (and not in emulator mode),
  the driver falls back to standard [ADC](https://cloud.google.com/docs/authentication/application-default-credentials)
  resolution, which honours `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default
  login`, or the metadata server. See [README § Authentication](../README.md#authentication).

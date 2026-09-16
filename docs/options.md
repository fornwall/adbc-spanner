# Configuration options

Set options with Rust `set_option` or constructor options, the C `Adbc*SetOption` functions,
or Python driver-manager `db_kwargs` / `conn_kwargs`. Unknown keys return `NotImplemented` on
set and `NotFound` on get.

## Option levels

| Level | Scope |
| --- | --- |
| [Database](#database-options) | Database path, endpoint and credentials. |
| [Connection](#connection-only-options) | Transaction mode, isolation and defaults for new statements. |
| [Statement](#statement-only-options) | SQL/ingest execution and overrides of shared options. |

### Inheritance: connection → statement

A new statement copies the connection's [shared options](#shared-options-connection-and-statement).
Later connection changes do not affect existing statements. Clearing a copied option resets the
statement to that option's unset/default state; it does not restore the connection's value.

`adbc.connection.readonly` is live: existing statements observe changes immediately. Commit
statistics are per object and are not inherited. The connection-only transaction tag and isolation
level are copied into new statements' execution configuration.

## Values and getters

- Booleans accept only string `true` or `false`, not integers or alternate spellings.
- Positive integers accept an integer or integer string; retry attempts also accept whole doubles.
- Fractional seconds accept numeric strings, integers or doubles, subject to the table's range.
  Values must be finite and representable as a duration.
- Other values must be strings. Enumerated values are case-sensitive. Tags are stored verbatim;
  structured parsers such as staleness trim surrounding whitespace.

Invalid values generally return `InvalidArguments`; unknown ingest modes return `NotImplemented`.
Unless a default is reported, an unset option returns
`NotFound`. The **Get** column describes the string getter: “set” means available only when set,
“always” includes the default, and “write-only” always returns `NotFound`. Typed getters parse
that same string as an integer/double (or return its UTF-8 bytes); failed numeric parsing returns
`InvalidArguments`.

At the C ABI, database/connection options set before `Init` are buffered, with the last value for
each key retained. Validation happens at `Init`, and every pre-`Init` get returns `NotFound`.
Statements have no pre-initialization phase.

## Database options

The first connection builds and caches the database's client stack. Later connections share it.
Setting any database option invalidates the cache for future connections; existing connections
keep their current stack. Environment-derived values are not reflected by option getters.

| Option | Values | Default | Get | Effect |
| --- | --- | --- | --- | --- |
| `uri` | `spanner://` URI | required | set, verbatim | Database path plus optional endpoint/query options; see [URIs](#connection-uris). Missing URI fails connection creation with `InvalidState`. |
| `spanner.endpoint` | gRPC endpoint URL | production endpoint | set | Overrides the endpoint from `SPANNER_EMULATOR_HOST`. |
| `spanner.emulator` | boolean | `false` | always | Uses anonymous credentials. A nonempty emulator environment variable also enables this mode. |
| `spanner.auth.keyfile` | credential JSON file path | ADC | set | Supports `service_account`, `authorized_user`, `impersonated_service_account` and `external_account` JSON. |
| `spanner.auth.keyfile_json` | inline credential JSON | ADC | write-only | Same credential formats; takes precedence over `keyfile`. Not allowed in URIs. |
| `spanner.auth.impersonate.target_principal` | service-account email | unset | set | Impersonates the target using keyfile/ADC base credentials. |
| `spanner.auth.impersonate.delegates` | comma-separated service-account emails | no delegates | nonempty | Delegation chain; entries are trimmed and empty entries removed. Used only with a target principal. |
| `spanner.auth.impersonate.scopes` | comma-separated OAuth scopes | `cloud-platform` | nonempty | Impersonated-token scopes; normalized like delegates. Used only with a target principal. |
| `spanner.auth.impersonate.lifetime` | non-negative integer seconds | `3600` | explicitly set only | Impersonated-token lifetime; accepts an integer or integer string. |
| `spanner.auth.access_token` | OAuth bearer token | ADC | write-only | Caller manages token validity; no refresh. Not allowed in URIs. |
| `spanner.auth.quota_project` | project id; `""` unsets | credential default | set | Sends `x-goog-user-project`; the caller needs `serviceusage.services.use`. With ADC, `GOOGLE_CLOUD_QUOTA_PROJECT` takes precedence. |

At connection-stack creation, emulator mode rejects explicit keyfile, inline JSON, access token,
impersonation target or quota project with `InvalidState`; ambient ADC does not conflict.
An access token is mutually exclusive with keyfile, inline JSON and an impersonation target.
See [authentication](../README.md#authentication) for setup.

### Connection URIs

The scheme is required and matched case-insensitively. Bare database paths and other schemes
return `InvalidArguments`.

```text
spanner:///projects/<p>/instances/<i>/databases/<d>?spanner.emulator=true
spanner://localhost:9010/projects/<p>/instances/<i>/databases/<d>
```

The path identifies the database. An authority (`host:port`) supplies `spanner.endpoint`; use
three slashes when there is no authority. An endpoint alone does not enable anonymous credentials.
Query keys must be database options above, except `uri`, `spanner.auth.keyfile_json` and
`spanner.auth.access_token`; unknown or forbidden keys return `InvalidArguments`. Values are
percent-decoded, with `+` kept as a literal plus. URI fragments are rejected.

Setting a URI expands its options immediately. The last writer wins per option, and the URI
changes only the options it contains. `get_option("uri")` returns the last accepted URI verbatim;
individual getters show the current expanded values, including any later overrides. Secret
values must be supplied separately; a keyfile path is allowed in the URI.

## Connection-only options

| Option | Values | Default | Get | Effect |
| --- | --- | --- | --- | --- |
| `adbc.connection.autocommit` | boolean | `true` | always | `false` enables manual transactions. Returning to `true` commits pending work; failure preserves it. |
| `adbc.connection.readonly` | boolean | `false` | always | Rejects DML, DDL, ingest and commits of buffered writes with `InvalidState`. Rollback and read-only/empty commits remain available. |
| `adbc.connection.transaction.isolation_level` | full ADBC isolation constant; see below | `adbc.connection.transaction.isolation.default` | always, effective level | Configures read/write transactions; copied into new statements. |
| `spanner.transaction.tag` | string; `""` unsets | unset | set | Tags read/write and write-only transactions and BatchWrite groups. Copied into new statements, which cannot set it. |
| `adbc.connection.catalog` | database id | current database id | always | Only the current value is accepted; another value returns `NotImplemented`. |
| `adbc.connection.db_schema` | `""` | `""` | always | Only `""` is accepted. Named schemas use qualified names; there is no selectable session schema. |

### Manual transactions

The first query or write determines the transaction kind. Queries share a read-only snapshot;
DML and ingest mutations buffer until commit. Mixing kinds fails with `InvalidState`, so there
is no read-your-writes. Buffered `execute_update` calls return `None`; commit returns no row count.
DDL applies immediately and cannot be rolled back. See [transactions](transactions.md).

### Isolation levels

Values require the prefix `adbc.connection.transaction.isolation.`. Getters report the effective
level; unknown values return `InvalidArguments`.

| Suffix | Effective level |
| --- | --- |
| `default` | Spanner default: `SERIALIZABLE`; getter retains `default`. |
| `serializable`, `linearizable` | `serializable` |
| `repeatable_read`, `snapshot`, `read_committed`, `read_uncommitted` | `repeatable_read` |

The option applies to DML read/write transactions. Queries use staleness instead; mutation-only
ingest commits and partitioned DML do not use this option. Spanner's repeatable read provides
snapshot isolation and permits write skew, even for a single autocommit statement that reads rows
it does not write. See [isolation and locking](transactions.md#isolation-and-locking).

## Shared options (connection and statement)

All settable options below accept `""` to clear/reset their value. Statement getters report their
own effective values after inheritance or overrides.

| Option | Values | Default | Get | Effect |
| --- | --- | --- | --- | --- |
| `spanner.read.staleness` | [staleness grammar](#stale-reads) | strong read | set, trimmed | Timestamp bound for read-only queries. Clearing an inherited bound restores strong reads. |
| `spanner.max_timestamp_precision` | `nanoseconds_error_on_overflow`, `microseconds` | `nanoseconds_error_on_overflow` | always | Arrow timestamp unit/range; also used by connection schema and partition reads. |
| `spanner.request.priority` | `low`, `medium`, `high` | service default: high | set | Query/DML, commit and BatchWrite priority; also applied to internal metadata reads. |
| `spanner.request.tag` | string | unset | set, verbatim | Tags user query/DML requests and batch DML. Internal metadata and BatchWrite requests remain untagged. |
| `spanner.directed_read` | [replica-selection grammar](#directed-reads) | service routing | set, trimmed | Replica selection for read-only queries, including metadata. |
| `spanner.query.optimizer_version` | version string or `latest` | database/service default | set, verbatim | Query optimizer version. |
| `spanner.query.optimizer_statistics_package` | package name | database default | set, verbatim | Query optimizer statistics package. |
| `spanner.commit.max_delay` | duration from `0` through `500ms` | unset | set, trimmed | Maximum extra commit delay for batching; same duration units as staleness. |
| `spanner.commit_stats` | boolean | `false` | always | Requests mutation statistics on read/write and write-only commits. |
| `spanner.commit_stats.mutation_count` | read-only | unavailable | after a stats-enabled commit | Most recent recorded mutation count for this object: manual commit on the connection, autocommit DML/ingest on the statement. For chunked ingest, the last chunk's count. Setting returns `NotImplemented`. |
| `spanner.transaction.exclude_from_change_streams` | boolean | `false` | always | Excludes writes from streams configured with `allow_txn_exclusion=true`; applies to ordinary writes, BatchWrite and partitioned DML. |
| `spanner.rpc.timeout_seconds.query` | non-negative fractional seconds | unset | set | Initial query/first chunk, PLAN and partition probes, initial partition fetch, and internal metadata reads. |
| `spanner.rpc.timeout_seconds.update` | non-negative fractional seconds | unset | set | DML, manual commit, each ingest chunk, BatchWrite, partitioned DML, and DDL plus operation polling. |
| `spanner.rpc.timeout_seconds.fetch` | non-negative fractional seconds | unset | set | Each later streamed chunk. |
| `spanner.retry.max_attempts` | integer `1..=4294967295` | client default | set | Attempt cap including the first try; `1` disables retries. See [retry tuning](#retry-tuning). |
| `spanner.retry.max_elapsed_seconds` | positive fractional seconds | unset | set | Retry elapsed-time limit; ineffective for streaming query resumption. |
| `spanner.retry.backoff.initial_seconds` | positive fractional seconds | `1` | explicitly set only | Initial backoff delay, clamped at execution. |
| `spanner.retry.backoff.max_seconds` | positive fractional seconds | `60` | explicitly set only | Maximum backoff delay, clamped at execution. |
| `spanner.retry.backoff.multiplier` | positive finite number | `2` | explicitly set only | Backoff growth factor, clamped at execution. |

Commit delay/stats apply to the read/write runner and write-only commits. They do not apply to
BatchWrite or partitioned DML. Internal metadata reads inherit priority, directed reads, retry
settings and timeouts, but no request/transaction tags.

## Statement-only options

| Option | Values | Default | Get | Effect |
| --- | --- | --- | --- | --- |
| `spanner.rows_per_batch` | positive integer | `8192` | always | Maximum rows per Arrow batch, also subject to the conversion byte budget. |
| `spanner.data_boost` | boolean | `false` | always | Enables [Data Boost](https://docs.cloud.google.com/spanner/docs/databoost/databoost-overview) for partitioned execution; carried in partition descriptors. |
| `adbc.statement.bind_by_name` | boolean | `false` | always | By default, columns bind to distinct parameters in SQL order. `true` matches column names; unknown names return `InvalidArguments`. |
| `adbc.statement.exec.incremental` | boolean, only `false` supported | `false` | always | `true` returns `NotImplemented`; incremental partition generation is unsupported. |
| `adbc.ingest.target_table` | table name | unset | set | Selects bulk ingest and clears SQL. Setting SQL clears this target. |
| `adbc.ingest.target_db_schema` | schema name; `""` for default schema | default schema | set | Qualifies the target table. |
| `adbc.ingest.target_catalog` | connection's database id | connection's catalog | set | Other values, including `""`, return `NotImplemented`. |
| `adbc.ingest.temporary` | boolean, only `false` supported | `false` | always | `true` returns `NotImplemented`; temporary tables are unsupported. |
| `adbc.ingest.mode` | `adbc.ingest.mode.append`, `create`, `create_append`, `replace` (prefix applies to each; bare suffixes also accepted) | `adbc.ingest.mode.create` | always, full constant | `append` requires a table; `create` requires absence; `create_append` creates if absent; `replace` drops and recreates. All insert rows. |
| `spanner.ingest.batch_write` | boolean; `""` resets | `false` | always | Uses non-atomic BatchWrite for autocommit ingest; ignored in manual mode. |
| `spanner.dml.partitioned` | boolean; `""` resets | `false` | always | Runs DML as partitioned DML; ignored for queries, DDL and ingest. |

Table-building ingest modes derive columns from Arrow and omit an explicit primary key; Spanner
supplies a hidden row id. Create the table yourself and use `append` to choose its keys. Table
DDL is immediate even in manual mode. See [primary keys](https://docs.cloud.google.com/spanner/docs/primary-key-default-value#tables-without-primary-keys).

BatchWrite uses one mutation group per row, so a chunk can partially succeed. Priority,
transaction tag, change-stream exclusion and the update timeout apply; request tags, commit
delay/stats and retry tuning do not. Ordinary autocommit ingest is atomic per chunk, while manual
ingest buffers mutations for one commit.

[Partitioned DML](https://docs.cloud.google.com/spanner/docs/dml-partitioned) requires an idempotent
`UPDATE`/`DELETE`: partitions commit independently and may execute more than once. The reported
row count is a lower bound. Manual mode returns `InvalidState`; multiple statements/parameter
rows or `THEN RETURN` return `InvalidArguments`. Priority/request tag, optimizer settings,
change-stream exclusion, statement retry tuning and the update timeout apply. Commit settings,
transaction tag and connection isolation do not.

## Stale reads

Queries default to strong reads. `spanner.read.staleness` accepts:

| Form | Meaning |
| --- | --- |
| `exact:<duration>` | Read exactly that far in the past. |
| `max:<duration>` | Server chooses a timestamp no older than the duration. |
| `read:<rfc3339>` or bare RFC 3339 | Read at the given timestamp. |
| `min:<rfc3339>` | Server chooses that timestamp or a later one. |

Durations are non-negative numbers with optional units: seconds by default, or `s`, `ms`,
`us`/`µs`, `ns`, `m`, `h`. Examples: `exact:2.5s`, `max:1m`,
`read:2026-07-07T00:00:00Z`. Prefixes are lowercase; surrounding whitespace is trimmed.

Spanner accepts `max:` and `min:` only for single-use reads. Manual query transactions and
multi-row bound queries pin them to their most-stale legal equivalent: `max:<d>` becomes exact
staleness `<d>`, and `min:<t>` becomes exact timestamp `<t>`. `execute_partitions` currently
passes the bound unchanged to a multi-use transaction; use strong, `exact:` or `read:` there.

## Directed reads

`spanner.directed_read` selects replicas for read-only queries:

```text
<mode> [ ":" <selection> ("," <selection>)* ] [ ";auto_failover_disabled" ]
```

- Mode is `include` (ordered preferences) or `exclude` (avoid these replicas).
- A selection is a location, `location:type`, or `:type`. Types are `read_write`, `read_only`,
  or `any`; omitted type means any. Each selection must constrain a location or a specific type.
- `;auto_failover_disabled` is valid only with `include` and prevents fallback outside the list.
  Only `include;auto_failover_disabled` may omit the selection list.

Examples: `include:us-east1`, `include:us-east1:read_only,us-east4:read_write`,
`exclude:us-central1`, `include::read_only`, `include:us-east1;auto_failover_disabled`.
Values are trimmed; invalid grammar returns `InvalidArguments`. See [directed reads](https://docs.cloud.google.com/spanner/docs/directed-reads).

## Timestamp precision

Spanner timestamps cover years 0001–9999. Arrow nanoseconds use an `i64`, covering roughly
1677–2262. `spanner.max_timestamp_precision` chooses:

- `nanoseconds_error_on_overflow`: `Timestamp(Nanosecond, "UTC")`; out-of-range values return
  `InvalidArguments` with the column and value.
- `microseconds`: `Timestamp(Microsecond, "UTC")`, covering Spanner's full range. Sub-microsecond
  digits round toward negative infinity; one nanosecond before the epoch becomes microsecond `-1`.

The mode applies to results and schemas, including returning DML, metadata schemas and partition
reads. `read_partition` uses the reading connection's mode; match it to the producing statement.
Clearing the option restores `nanoseconds_error_on_overflow`. Bound Arrow timestamps of any unit
retain their source precision; this option only changes the read direction.

## RPC timeouts

`spanner.rpc.timeout_seconds.{query,update,fetch}` sets an overall deadline for each covered
operation, including client retries. `0` disables the deadline; `""` unsets it. Expiry returns
`Timeout`. A timed-out commit or DDL operation may already have applied server-side.

The initial query/first chunk uses `query`; each later chunk uses `fetch`. For multi-row bound
queries, later per-row executions also fall under `fetch`. DDL submission and polling share
one `update` deadline. Client-stack creation/session maintenance is outside these wrappers.
See [timeout coverage](transactions.md#timeouts).

## Retry tuning

`spanner.retry.*` configures query/DML statement requests, batch DML and the begin/commit RPCs
of read/write and write-only transactions. It preserves the client's retryable-error policy.
It does not configure session creation, `PartitionQuery`, BatchWrite or admin DDL requests.
Whole-transaction retries on `ABORTED` use a separate, uncapped default policy; use an RPC timeout
to bound the enclosing operation.

### Attempt and elapsed limits

| Setting | Unary RPCs | Streaming query resumption |
| --- | --- | --- |
| `max_attempts=N` | At most N attempts | At most N attempts |
| `max_elapsed_seconds` | Bounds retry decisions | Ineffective: client resets the retry state's start time. |
| Neither limit set | No cap | Default cap of 10 attempts |

The query stream's initial open can fail without reaching its resumption retry loop. Setting
only `max_elapsed_seconds` replaces the default streaming policy, so set `max_attempts` too if
you need an attempt cap. Use `query`/`fetch` timeouts for streaming wall-clock bounds. These
behaviors are covered by `retry_max_*` tests in [mock_spanner.rs](../tests/mock_spanner.rs).

### Backoff

Setting any backoff option builds exponential backoff with jitter; unset components use
1 second / 60 seconds / factor 2. Effective values are clamped: maximum delay to 1 second–24 hours,
initial delay to 1 millisecond–the effective maximum, and multiplier to 1–32. In particular,
an initial delay above the maximum is lowered to the maximum. Getters retain configured values.
Backoff settings are independent of attempt/time caps.

## Environment

- `SPANNER_EMULATOR_HOST` is consulted when the shared client stack is built. A nonempty value
  forces emulator mode and supplies an endpoint unless `spanner.endpoint` overrides it. Bare
  `host:port` gains `http://`. Use gRPC port `9010`: the pinned client derives the admin REST
  endpoint by replacing it with `9020`.
- Without explicit credentials or emulator mode, the auth library resolves
  [Application Default Credentials](https://docs.cloud.google.com/docs/authentication/application-default-credentials),
  including `GOOGLE_APPLICATION_CREDENTIALS`, local gcloud ADC and the metadata server.
- `GOOGLE_CLOUD_QUOTA_PROJECT` overrides the quota-project option during ADC resolution.
  Explicit keyfile, impersonation and access-token credentials use the option on the final
  credential; an ADC source for impersonation can still use the environment value for its own requests.

"""Typed option-key constants for the Spanner ADBC driver.

These enums mirror the style of the BigQuery ADBC driver's ``DatabaseOptions`` /
``StatementOptions``: each member's ``.value`` is the raw option-key string the
driver understands. Every driver setting is passed as one of these keys — through
``db_kwargs=`` / ``conn_kwargs=`` on :func:`adbc_driver_spanner.dbapi.connect`, or
``adbc_stmt_kwargs=`` on ``conn.cursor(...)`` — and these enums make that
discoverable and typo-safe.

Example::

    import adbc_driver_spanner.dbapi as spanner
    from adbc_driver_spanner import ConnectionOptions, DatabaseOptions, StatementOptions

    with spanner.connect(
        db_kwargs={DatabaseOptions.URI.value: "spanner:///projects/p/instances/i/databases/d"},
        conn_kwargs={ConnectionOptions.READ_STALENESS.value: "max:10s"},
    ) as conn:
        cur = conn.cursor(
            adbc_stmt_kwargs={StatementOptions.ROWS_PER_BATCH.value: "1024"}
        )

Standard ADBC option keys (``adbc.*``) live in ``adbc_driver_manager``; a few that
the driver honours are included here for convenience. The authoritative reference
for every option, its type, default and behaviour is ``docs/options.md``.
"""

import enum

__all__ = ["DatabaseOptions", "ConnectionOptions", "StatementOptions"]


class DatabaseOptions(enum.Enum):
    """Database-level options (set before connecting, via ``db_kwargs``)."""

    #: **Standard ADBC.** A ``spanner://`` connection URI whose path is the fully-qualified
    #: database path, ``spanner:///projects/<p>/instances/<i>/databases/<d>``. The scheme is
    #: required; a bare path is rejected. Also settable as the ``uri`` kwarg on ``connect``.
    URI = "uri"
    #: Override the Spanner gRPC endpoint (e.g. an emulator ``host:port``).
    ENDPOINT = "spanner.endpoint"
    #: ``"true"`` to connect with anonymous credentials (emulator mode).
    EMULATOR = "spanner.emulator"
    #: Path to a service-account / credential JSON file.
    KEYFILE = "spanner.auth.keyfile"
    #: The same credential JSON passed inline as a string. Write-only: never
    #: readable back via ``get_option``, and not accepted as a ``URI`` query
    #: parameter (URIs get logged) — pass it here instead.
    KEYFILE_JSON = "spanner.auth.keyfile_json"
    #: A caller-supplied OAuth 2.0 bearer token, sent verbatim (no refresh).
    #: Write-only: never readable back via ``get_option``, and not accepted as a
    #: ``URI`` query parameter (URIs get logged) — pass it here instead.
    ACCESS_TOKEN = "spanner.auth.access_token"
    #: Service account to impersonate; setting it enables impersonation.
    IMPERSONATE_TARGET_PRINCIPAL = "spanner.auth.impersonate.target_principal"
    #: Optional impersonation delegation chain (comma-separated emails).
    IMPERSONATE_DELEGATES = "spanner.auth.impersonate.delegates"
    #: Optional OAuth scopes for the impersonated token (comma-separated).
    IMPERSONATE_SCOPES = "spanner.auth.impersonate.scopes"
    #: Optional impersonated-token lifetime, in seconds (default ``3600``).
    IMPERSONATE_LIFETIME = "spanner.auth.impersonate.lifetime"
    #: The GCP project charged for API quota (the ``x-goog-user-project`` header),
    #: decoupled from the project owning the data. ``""`` unsets.
    QUOTA_PROJECT = "spanner.auth.quota_project"


class ConnectionOptions(enum.Enum):
    """Connection-level options (set via ``conn_kwargs``)."""

    #: Standard ADBC. ``"false"`` enters buffer-and-commit manual-transaction mode.
    AUTOCOMMIT = "adbc.connection.autocommit"
    #: Standard ADBC. ``"true"`` rejects all writes on the connection.
    READONLY = "adbc.connection.readonly"
    #: Standard ADBC. Isolation level for read/write transactions.
    ISOLATION_LEVEL = "adbc.connection.transaction.isolation_level"
    #: Read bound: one of ``exact:<duration>`` / ``max:<duration>`` / ``read:<rfc3339>`` /
    #: ``min:<rfc3339>``.
    READ_STALENESS = "spanner.read.staleness"
    #: How ``TIMESTAMP`` maps to Arrow (``nanoseconds_error_on_overflow`` / ``microseconds``).
    MAX_TIMESTAMP_PRECISION = "spanner.max_timestamp_precision"
    #: Request priority (``low`` / ``medium`` / ``high``).
    REQUEST_PRIORITY = "spanner.request.priority"
    #: Request tag attached to every query/DML request.
    REQUEST_TAG = "spanner.request.tag"
    #: Transaction tag attached to every read/write transaction (connection-only).
    TRANSACTION_TAG = "spanner.transaction.tag"
    #: Directed-read replica selection for read-only queries.
    DIRECTED_READ = "spanner.directed_read"
    #: Query optimizer version.
    QUERY_OPTIMIZER_VERSION = "spanner.query.optimizer_version"
    #: Query optimizer statistics package.
    QUERY_OPTIMIZER_STATISTICS_PACKAGE = "spanner.query.optimizer_statistics_package"
    #: Maximum commit delay (``0..=500ms``) Spanner may add to batch commits.
    MAX_COMMIT_DELAY = "spanner.commit.max_delay"
    #: ``"true"`` requests commit statistics on read/write commits.
    COMMIT_STATS = "spanner.commit_stats"
    #: ``"true"`` excludes the transaction's writes from change-stream capture.
    EXCLUDE_TXN_FROM_CHANGE_STREAMS = "spanner.transaction.exclude_from_change_streams"
    #: Overall deadline (seconds) on a query's initial execution; ``0`` disables.
    RPC_TIMEOUT_QUERY = "spanner.rpc.timeout_seconds.query"
    #: Overall deadline (seconds) on each write operation; ``0`` disables.
    RPC_TIMEOUT_UPDATE = "spanner.rpc.timeout_seconds.update"
    #: Overall deadline (seconds) on each subsequent chunk fetch; ``0`` disables.
    RPC_TIMEOUT_FETCH = "spanner.rpc.timeout_seconds.fetch"
    #: Cap on retry attempts (first try + retries) for a retryable RPC.
    RETRY_MAX_ATTEMPTS = "spanner.retry.max_attempts"
    #: Cap (seconds) on total wall-clock time spent retrying a retryable RPC.
    RETRY_MAX_ELAPSED_SECONDS = "spanner.retry.max_elapsed_seconds"
    #: Initial delay (seconds) of the exponential backoff between retry attempts.
    RETRY_BACKOFF_INITIAL_SECONDS = "spanner.retry.backoff.initial_seconds"
    #: Ceiling (seconds) the growing retry backoff delay is truncated at.
    RETRY_BACKOFF_MAX_SECONDS = "spanner.retry.backoff.max_seconds"
    #: Per-attempt growth factor for the retry backoff delay.
    RETRY_BACKOFF_MULTIPLIER = "spanner.retry.backoff.multiplier"


class StatementOptions(enum.Enum):
    """Statement-level options (set via ``conn.cursor(adbc_stmt_kwargs=...)``).

    The options shared with :class:`ConnectionOptions` are per-statement overrides
    of the value inherited from the connection.
    """

    #: Rows converted into each streamed Arrow ``RecordBatch`` (default ``8192``).
    ROWS_PER_BATCH = "spanner.rows_per_batch"
    #: ``"true"`` runs ``execute_partitions`` partitions on Data Boost.
    DATA_BOOST = "spanner.data_boost"
    #: Per-statement override of :attr:`ConnectionOptions.READ_STALENESS`.
    READ_STALENESS = "spanner.read.staleness"
    #: Per-statement override of :attr:`ConnectionOptions.MAX_TIMESTAMP_PRECISION`.
    MAX_TIMESTAMP_PRECISION = "spanner.max_timestamp_precision"
    #: Per-statement override of :attr:`ConnectionOptions.REQUEST_PRIORITY`.
    REQUEST_PRIORITY = "spanner.request.priority"
    #: Per-statement override of :attr:`ConnectionOptions.REQUEST_TAG`.
    REQUEST_TAG = "spanner.request.tag"
    #: Per-statement override of :attr:`ConnectionOptions.DIRECTED_READ`.
    DIRECTED_READ = "spanner.directed_read"
    #: Per-statement override of :attr:`ConnectionOptions.MAX_COMMIT_DELAY`.
    MAX_COMMIT_DELAY = "spanner.commit.max_delay"
    #: Per-statement override of :attr:`ConnectionOptions.COMMIT_STATS`.
    COMMIT_STATS = "spanner.commit_stats"
    #: Per-statement override of
    #: :attr:`ConnectionOptions.EXCLUDE_TXN_FROM_CHANGE_STREAMS`.
    EXCLUDE_TXN_FROM_CHANGE_STREAMS = "spanner.transaction.exclude_from_change_streams"
    #: Per-statement override of :attr:`ConnectionOptions.QUERY_OPTIMIZER_VERSION`.
    QUERY_OPTIMIZER_VERSION = "spanner.query.optimizer_version"
    #: Per-statement override of :attr:`ConnectionOptions.QUERY_OPTIMIZER_STATISTICS_PACKAGE`.
    QUERY_OPTIMIZER_STATISTICS_PACKAGE = "spanner.query.optimizer_statistics_package"
    #: Per-statement override of :attr:`ConnectionOptions.RPC_TIMEOUT_QUERY`.
    RPC_TIMEOUT_QUERY = "spanner.rpc.timeout_seconds.query"
    #: Per-statement override of :attr:`ConnectionOptions.RPC_TIMEOUT_UPDATE`.
    RPC_TIMEOUT_UPDATE = "spanner.rpc.timeout_seconds.update"
    #: Per-statement override of :attr:`ConnectionOptions.RPC_TIMEOUT_FETCH`.
    RPC_TIMEOUT_FETCH = "spanner.rpc.timeout_seconds.fetch"
    #: Per-statement override of :attr:`ConnectionOptions.RETRY_MAX_ATTEMPTS`.
    RETRY_MAX_ATTEMPTS = "spanner.retry.max_attempts"
    #: Per-statement override of :attr:`ConnectionOptions.RETRY_MAX_ELAPSED_SECONDS`.
    RETRY_MAX_ELAPSED_SECONDS = "spanner.retry.max_elapsed_seconds"
    #: Per-statement override of :attr:`ConnectionOptions.RETRY_BACKOFF_INITIAL_SECONDS`.
    RETRY_BACKOFF_INITIAL_SECONDS = "spanner.retry.backoff.initial_seconds"
    #: Per-statement override of :attr:`ConnectionOptions.RETRY_BACKOFF_MAX_SECONDS`.
    RETRY_BACKOFF_MAX_SECONDS = "spanner.retry.backoff.max_seconds"
    #: Per-statement override of :attr:`ConnectionOptions.RETRY_BACKOFF_MULTIPLIER`.
    RETRY_BACKOFF_MULTIPLIER = "spanner.retry.backoff.multiplier"
    #: How bound Arrow columns pair with ``@name`` parameters (``"true"`` = by name).
    BIND_BY_NAME = "adbc.statement.bind_by_name"
    #: Primary key for the create/replace ingest modes (comma-separated columns).
    INGEST_PRIMARY_KEY = "spanner.ingest.primary_key"
    #: ``"true"`` routes an autocommit bulk ingest's chunks through Spanner's
    #: non-atomic **BatchWrite** RPC instead of a write-only transaction.
    #: Ignored in manual-transaction mode.
    INGEST_BATCH_WRITE = "spanner.ingest.batch_write"

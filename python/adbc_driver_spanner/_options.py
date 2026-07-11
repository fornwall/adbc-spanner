"""Typed option-key constants for the Spanner ADBC driver.

These enums mirror the style of the BigQuery ADBC driver's ``DatabaseOptions`` /
``StatementOptions``: each member's ``.value`` is the raw option-key string the
driver understands. They are a discoverability and typo-safety aid for the
lower-level escape hatches — ``db_kwargs=`` / ``conn_kwargs=`` on
:func:`adbc_driver_spanner.dbapi.connect`, and ``adbc_stmt_kwargs=`` on
``conn.cursor(...)`` — where you pass raw option strings instead of the friendly
keyword arguments.

Example::

    import adbc_driver_spanner.dbapi as spanner
    from adbc_driver_spanner import ConnectionOptions, StatementOptions

    with spanner.connect(
        database="projects/p/instances/i/databases/d",
        conn_kwargs={ConnectionOptions.READ_STALENESS.value: "max:10s"},
    ) as conn:
        cur = conn.cursor(
            adbc_stmt_kwargs={StatementOptions.ROWS_PER_BATCH.value: "1024"}
        )

Common credential settings (``keyfile``, ``access_token``, the ``impersonate_*``
group, …) also have dedicated keyword arguments on ``connect`` — prefer those.
Standard ADBC option keys (``adbc.*``) live in ``adbc_driver_manager``; a few that
the driver honours are included here for convenience. The authoritative reference
for every option, its type, default and behaviour is ``docs/options.md``.
"""

import enum

__all__ = ["DatabaseOptions", "ConnectionOptions", "StatementOptions"]


class DatabaseOptions(enum.Enum):
    """Database-level options (set before connecting, via ``db_kwargs``)."""

    #: Fully-qualified database path, ``projects/<p>/instances/<i>/databases/<d>``
    #: (or a ``spanner:`` connection URI). Alias of the standard ``uri``.
    DATABASE = "spanner.database"
    #: Standard ADBC alias of :attr:`DATABASE`.
    URI = "uri"
    #: Override the Spanner gRPC endpoint (e.g. an emulator ``host:port``).
    ENDPOINT = "spanner.endpoint"
    #: ``"true"`` to connect with anonymous credentials (emulator mode).
    EMULATOR = "spanner.emulator"
    #: Path to a service-account / credential JSON file.
    KEYFILE = "spanner.keyfile"
    #: The same credential JSON passed inline as a string.
    KEYFILE_JSON = "spanner.keyfile_json"
    #: A caller-supplied OAuth 2.0 bearer token, sent verbatim (no refresh).
    ACCESS_TOKEN = "spanner.access_token"
    #: Service account to impersonate; setting it enables impersonation.
    IMPERSONATE_TARGET_PRINCIPAL = "spanner.impersonate.target_principal"
    #: Optional impersonation delegation chain (comma-separated emails).
    IMPERSONATE_DELEGATES = "spanner.impersonate.delegates"
    #: Optional OAuth scopes for the impersonated token (comma-separated).
    IMPERSONATE_SCOPES = "spanner.impersonate.scopes"
    #: Optional impersonated-token lifetime, in seconds (default ``3600``).
    IMPERSONATE_LIFETIME = "spanner.impersonate.lifetime"


class ConnectionOptions(enum.Enum):
    """Connection-level options (set via ``conn_kwargs``)."""

    #: Standard ADBC. ``"false"`` enters buffer-and-commit manual-transaction mode.
    AUTOCOMMIT = "adbc.connection.autocommit"
    #: Standard ADBC. ``"true"`` rejects all writes on the connection.
    READONLY = "adbc.connection.readonly"
    #: Standard ADBC. Isolation level for read/write transactions.
    ISOLATION_LEVEL = "adbc.connection.transaction.isolation_level"
    #: Stale-read bound, ``exact:<duration>`` / ``max:<duration>``.
    READ_STALENESS = "spanner.read.staleness"
    #: Absolute read timestamp (RFC 3339, optional ``read:`` / ``min:`` prefix).
    READ_TIMESTAMP = "spanner.read.timestamp"
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
    MAX_COMMIT_DELAY = "spanner.max_commit_delay"
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


class StatementOptions(enum.Enum):
    """Statement-level options (set via ``conn.cursor(adbc_stmt_kwargs=...)``).

    The options shared with :class:`ConnectionOptions` are per-statement overrides
    of the value inherited from the connection.
    """

    #: Rows converted into each streamed Arrow ``RecordBatch`` (default ``8192``).
    ROWS_PER_BATCH = "spanner.rows_per_batch"
    #: ``"true"`` runs ``execute_partitions`` partitions on Data Boost.
    DATA_BOOST = "spanner.data_boost"
    #: Hint for the maximum number of partitions from ``execute_partitions``.
    MAX_PARTITIONS = "spanner.max_partitions"
    #: Per-statement override of :attr:`ConnectionOptions.READ_STALENESS`.
    READ_STALENESS = "spanner.read.staleness"
    #: Per-statement override of :attr:`ConnectionOptions.READ_TIMESTAMP`.
    READ_TIMESTAMP = "spanner.read.timestamp"
    #: Per-statement override of :attr:`ConnectionOptions.MAX_TIMESTAMP_PRECISION`.
    MAX_TIMESTAMP_PRECISION = "spanner.max_timestamp_precision"
    #: Per-statement override of :attr:`ConnectionOptions.REQUEST_PRIORITY`.
    REQUEST_PRIORITY = "spanner.request.priority"
    #: Per-statement override of :attr:`ConnectionOptions.REQUEST_TAG`.
    REQUEST_TAG = "spanner.request.tag"
    #: Per-statement override of :attr:`ConnectionOptions.DIRECTED_READ`.
    DIRECTED_READ = "spanner.directed_read"
    #: Per-statement override of :attr:`ConnectionOptions.MAX_COMMIT_DELAY`.
    MAX_COMMIT_DELAY = "spanner.max_commit_delay"
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
    #: How bound Arrow columns pair with ``@name`` parameters (``"true"`` = by name).
    BIND_BY_NAME = "adbc.statement.bind_by_name"
    #: Primary key for the create/replace ingest modes (comma-separated columns).
    INGEST_PRIMARY_KEY = "spanner.ingest.primary_key"

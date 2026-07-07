//! The [`SpannerConnection`] — an ADBC connection backed by a Spanner [`DatabaseClient`].
//!
//! ## Transactions
//!
//! By default the connection is in **autocommit** mode: every statement runs in its own Spanner
//! transaction (a single-use read-only transaction for queries, a read/write transaction for DML).
//!
//! Setting the `adbc.connection.autocommit` option to `false` begins **manual** transaction mode.
//! Because Spanner's client only exposes read/write transactions through a closure-based runner
//! (there is no public begin/commit handle), the driver implements manual transactions by
//! *buffering* DML statements and applying the whole batch atomically in a single read/write
//! transaction on [`Connection::commit`] — which also makes the retry-on-abort safe, since the
//! buffer is simply replayed. [`Connection::rollback`] discards the buffer.
//!
//! Consequences of this model, which callers should be aware of:
//! - In manual mode, `execute_update` on DML returns `None` (the affected-row count is not known
//!   until commit).
//! - DML with a `THEN RETURN` clause is rejected in manual mode: it must run via `ExecuteSql` to
//!   produce its rows, but buffered DML is applied through `ExecuteBatchDml` (which does not
//!   support `THEN RETURN`) — and the rows would be unobtainable at commit time anyway.
//! - Queries (`execute`) and DDL always run immediately (DDL is never transactional in Spanner), so
//!   a query does not observe DML buffered earlier in the same manual transaction.
//! - A **failed** commit keeps the buffer and the transaction open: the caller can retry
//!   [`Connection::commit`] (replaying the batch) or [`Connection::rollback`] to discard it. The
//!   same holds when re-enabling autocommit fails to commit the buffer: the connection stays in
//!   manual mode. On `ABORTED` (the retriable code preserved in `vendor_code`) the failed attempt
//!   is guaranteed not to have committed, so the replay is exact; after an *ambiguous* transport
//!   failure the usual Spanner caveat applies — the commit may have landed, so a replay can apply
//!   the batch twice unless the DML is idempotent.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::{
    Array, ArrayRef, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use google_cloud_spanner::batch::Partition;
use google_cloud_spanner::builder::{BatchDmlBuilder, TransactionRunnerBuilder};
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::transaction_options::IsolationLevel;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::ReadWriteTransaction;

use crate::bind::qualified_table;
use crate::conversion::{result_set_to_batch, stream_query};
use crate::driver::Connected;
use crate::error::{err, from_spanner, invalid_argument, invalid_state, not_implemented};
use crate::runtime::{block_on_cancellable, CancelSignal, SharedRuntime};
use crate::staleness::ReadStaleness;
use crate::statement::{SpannerStatement, DEFAULT_ROWS_PER_BATCH};

/// Transaction state shared between a connection and the statements it creates.
#[derive(Debug)]
pub(crate) struct TxnState {
    /// When false, the connection is in manual transaction mode and DML is buffered.
    autocommit: bool,
    /// DML statements buffered while in manual mode, applied atomically on commit. Built
    /// statements (not raw SQL) so that parameterized DML — which carries bound values — buffers
    /// just like a plain `;`-batch does.
    pending: Vec<SpannerSql>,
}

impl TxnState {
    fn new() -> Self {
        Self {
            autocommit: true,
            pending: Vec::new(),
        }
    }

    /// Whether the connection is currently in autocommit mode.
    pub(crate) fn autocommit(&self) -> bool {
        self.autocommit
    }

    /// Buffer a DML statement to be applied on the next commit.
    pub(crate) fn buffer(&mut self, statement: SpannerSql) {
        self.pending.push(statement);
    }
}

/// A handle to a connection's transaction state, shared with its statements.
pub(crate) type SharedTxn = Arc<Mutex<TxnState>>;

/// An ADBC connection to a Spanner database.
pub struct SpannerConnection {
    runtime: SharedRuntime,
    client: DatabaseClient,
    spanner: Spanner,
    database: String,
    read_only: bool,
    /// Isolation level applied to read/write transactions (autocommit DML and manual-mode commit),
    /// set via the standard ADBC `adbc.connection.transaction.isolation_level` option.
    /// [`IsolationLevel::Unspecified`] (the default) leaves the client/database default in place.
    isolation: IsolationLevel,
    /// Read staleness / timestamp bound for read-only queries (`spanner.read.staleness` /
    /// `spanner.read.timestamp`). The default is a strong read; this becomes the default for
    /// statements created on the connection, which may override it.
    read_staleness: ReadStaleness,
    txn: SharedTxn,
    /// Cancellation signal for this connection's in-flight metadata/commit operations
    /// (see [`Connection::cancel`]).
    cancel: CancelSignal,
}

impl SpannerConnection {
    pub(crate) fn new(runtime: SharedRuntime, connected: Connected) -> Self {
        Self {
            runtime,
            client: connected.client,
            spanner: connected.spanner,
            database: connected.database,
            read_only: false,
            isolation: IsolationLevel::Unspecified,
            read_staleness: ReadStaleness::default(),
            txn: Arc::new(Mutex::new(TxnState::new())),
            cancel: CancelSignal::new(),
        }
    }

    /// Apply the buffered DML statements atomically in one read/write transaction, discarding the
    /// affected-row count (a commit reports no count).
    fn apply_transaction(&self, statements: Vec<SpannerSql>) -> Result<()> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        run_batch_dml(
            &self.runtime,
            &self.client,
            &self.cancel,
            self.isolation.clone(),
            statements,
        )?;
        Ok(())
    }

    /// Query `INFORMATION_SCHEMA` and assemble the schema→table→column hierarchy for `get_objects`,
    /// applying the ADBC `LIKE`/type filters and the requested depth.
    fn collect_objects(
        &self,
        depth: ObjectDepth,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: &Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Vec<crate::objects::DbSchema>> {
        let populate_tables = matches!(
            depth,
            ObjectDepth::All | ObjectDepth::Tables | ObjectDepth::Columns
        );
        let populate_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);
        let client = self.client.clone();

        let (
            schema_batch,
            table_batch,
            column_batch,
            constraint_batch,
            key_column_batch,
            referential_batch,
        ) = block_on_cancellable(&self.runtime, &self.cancel, async move {
            let schemas = query_batch(
                &client,
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
            )
            .await?;
            let tables = if populate_tables {
                Some(
                    query_batch(
                        &client,
                        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES",
                    )
                    .await?,
                )
            } else {
                None
            };
            let columns = if populate_columns {
                Some(
                    query_batch(
                        &client,
                        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE \
                         FROM INFORMATION_SCHEMA.COLUMNS \
                         ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
                    )
                    .await?,
                )
            } else {
                None
            };
            // Constraints (primary/foreign/unique/check) and their key columns, populated at the
            // same depth as columns. KEY_COLUMN_USAGE covers the key-based constraints; its rows are
            // ordered so each constraint's columns come out in key order. For foreign keys,
            // REFERENTIAL_CONSTRAINTS maps the FK to the referenced unique/primary-key constraint,
            // whose own KEY_COLUMN_USAGE rows give the referenced (parent) columns in order — the
            // ordering CONSTRAINT_COLUMN_USAGE does not preserve.
            let (constraints, key_columns, referential) = if populate_columns {
                (
                    Some(
                        query_batch(
                            &client,
                            "SELECT TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE \
                             FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
                        )
                        .await?,
                    ),
                    Some(
                        query_batch(
                            &client,
                            "SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME, \
                             COLUMN_NAME, CAST(ORDINAL_POSITION AS STRING), \
                             CAST(POSITION_IN_UNIQUE_CONSTRAINT AS STRING) \
                             FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
                             ORDER BY CONSTRAINT_SCHEMA, CONSTRAINT_NAME, ORDINAL_POSITION",
                        )
                        .await?,
                    ),
                    Some(
                        query_batch(
                            &client,
                            "SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA, \
                             UNIQUE_CONSTRAINT_NAME \
                             FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
                        )
                        .await?,
                    ),
                )
            } else {
                (None, None, None)
            };
            Ok::<_, Error>((
                schemas,
                tables,
                columns,
                constraints,
                key_columns,
                referential,
            ))
        })?;

        let schema_names = str_col(&schema_batch, 0)?;

        // Group each INFORMATION_SCHEMA batch ONCE into lookup maps keyed by (schema, table) — and
        // the key/referential batches by (constraint_schema, constraint_name) — so the assembly
        // below is a series of hash lookups rather than a full rescan of each batch per table (which
        // was quadratic for large schemas). This mirrors `collect_statistics`. The per-group `Vec`s
        // keep batch (i.e. `ORDER BY`) order, so column and key-column ordering is preserved exactly.
        let tables_by_schema = match &table_batch {
            Some(batch) => group_tables(batch)?,
            None => HashMap::new(),
        };
        let columns_by_table = match &column_batch {
            Some(batch) => group_columns(batch)?,
            None => HashMap::new(),
        };
        let constraints_by_table = match &constraint_batch {
            Some(batch) => group_constraints(batch)?,
            None => HashMap::new(),
        };
        let key_columns_by_constraint = match &key_column_batch {
            Some(batch) => group_key_columns(batch)?,
            None => HashMap::new(),
        };
        let referential_by_constraint = match &referential_batch {
            Some(batch) => group_referential(batch)?,
            None => HashMap::new(),
        };

        let mut result = Vec::new();
        for i in 0..schema_batch.num_rows() {
            let schema_name = schema_names.value(i);
            if db_schema.is_some_and(|p| !like_match(p, schema_name)) {
                continue;
            }
            let mut tables = Vec::new();
            // `tables_by_schema` is empty unless tables were populated, so this yields no tables at
            // schema-only depth — matching the previous `if let Some(&table_batch)` guard.
            for table in tables_by_schema.get(schema_name).into_iter().flatten() {
                let name = table.name;
                if table_name.is_some_and(|p| !like_match(p, name)) {
                    continue;
                }
                let ttype = table.table_type.to_string();
                if table_type
                    .as_ref()
                    .is_some_and(|types| !types.iter().any(|t| *t == ttype))
                {
                    continue;
                }
                // Empty maps (columns/constraints not populated at this depth) yield empty lists,
                // matching the previous depth-gated `match` arms.
                let columns = collect_columns(&columns_by_table, schema_name, name, column_name);
                let constraints = collect_constraints(
                    &constraints_by_table,
                    &key_columns_by_constraint,
                    &referential_by_constraint,
                    schema_name,
                    name,
                );
                tables.push(crate::objects::Table {
                    name: name.to_string(),
                    table_type: ttype,
                    columns,
                    constraints,
                });
            }
            result.push(crate::objects::DbSchema {
                name: schema_name.to_string(),
                tables,
            });
        }
        Ok(result)
    }

    /// Whether a table exists, via a parameterized `INFORMATION_SCHEMA.TABLES` lookup. The default
    /// (unnamed) schema is the empty string in Spanner. Delegates to the shared [`table_exists`]
    /// probe so the same query serves the connection's introspection and the statement's ingest
    /// error path.
    fn table_exists(&self, db_schema: &str, table_name: &str) -> Result<bool> {
        table_exists(
            &self.runtime,
            &self.client,
            &self.cancel,
            db_schema,
            table_name,
        )
    }

    /// Compute exact statistics for the base tables matching the `LIKE` filters: `ROW_COUNT` per
    /// table, and `NULL_COUNT` (+ `DISTINCT_COUNT` for groupable types) per column.
    fn collect_statistics(
        &self,
        db_schema: Option<&str>,
        table_name: Option<&str>,
    ) -> Result<Vec<crate::statistics::SchemaStatistics>> {
        let client = self.client.clone();
        let (table_batch, column_batch) =
            block_on_cancellable(&self.runtime, &self.cancel, async move {
                let tables = query_batch(
                    &client,
                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
                     WHERE TABLE_TYPE = 'BASE TABLE'",
                )
                .await?;
                let columns = query_batch(
                    &client,
                    "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SPANNER_TYPE \
                     FROM INFORMATION_SCHEMA.COLUMNS \
                     ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
                )
                .await?;
                Ok::<_, Error>((tables, columns))
            })?;

        let (ts, tn) = (str_col(&table_batch, 0)?, str_col(&table_batch, 1)?);
        let (cts, ctn, ccn, ctype) = (
            str_col(&column_batch, 0)?,
            str_col(&column_batch, 1)?,
            str_col(&column_batch, 2)?,
            str_col(&column_batch, 3)?,
        );

        // Group the COLUMNS rows by (schema, table) in one pass: (column name, whether its type is
        // groupable → distinct-countable). The ORDER BY keeps each group in ordinal order.
        let mut columns_by_table: HashMap<(&str, &str), Vec<(String, bool)>> = HashMap::new();
        for c in 0..column_batch.num_rows() {
            columns_by_table
                .entry((cts.value(c), ctn.value(c)))
                .or_default()
                .push((ccn.value(c).to_string(), is_groupable(ctype.value(c))));
        }

        let mut schemas: Vec<crate::statistics::SchemaStatistics> = Vec::new();
        for r in 0..table_batch.num_rows() {
            let schema = ts.value(r);
            let table = tn.value(r);
            if db_schema.is_some_and(|p| !like_match(p, schema)) {
                continue;
            }
            if table_name.is_some_and(|p| !like_match(p, table)) {
                continue;
            }
            let columns = columns_by_table
                .remove(&(schema, table))
                .unwrap_or_default();
            let stats = self.table_statistics(schema, table, &columns)?;
            match schemas.iter_mut().find(|s| s.db_schema == schema) {
                Some(s) => s.statistics.extend(stats),
                None => schemas.push(crate::statistics::SchemaStatistics {
                    db_schema: schema.to_string(),
                    statistics: stats,
                }),
            }
        }
        Ok(schemas)
    }

    /// Run one aggregate scan over `table`, returning its `ROW_COUNT` and per-column `NULL_COUNT`
    /// (and `DISTINCT_COUNT` for groupable columns).
    fn table_statistics(
        &self,
        schema: &str,
        table: &str,
        columns: &[(String, bool)],
    ) -> Result<Vec<crate::statistics::Statistic>> {
        use adbc_core::constants::{
            ADBC_STATISTIC_DISTINCT_COUNT_KEY, ADBC_STATISTIC_NULL_COUNT_KEY,
            ADBC_STATISTIC_ROW_COUNT_KEY,
        };

        // Build one SELECT computing every count in a single scan; `plan` maps each result column
        // after the row count to its (column, statistic key).
        let mut exprs = vec!["COUNT(*)".to_string()];
        let mut plan: Vec<(String, i16)> = Vec::new();
        for (name, groupable) in columns {
            let quoted = crate::bind::quote_ident(name);
            exprs.push(format!("COUNTIF({quoted} IS NULL)"));
            plan.push((name.clone(), ADBC_STATISTIC_NULL_COUNT_KEY));
            if *groupable {
                exprs.push(format!("COUNT(DISTINCT {quoted})"));
                plan.push((name.clone(), ADBC_STATISTIC_DISTINCT_COUNT_KEY));
            }
        }
        let sql = format!(
            "SELECT {} FROM {}",
            exprs.join(", "),
            qualified_table(Some(schema), table)
        );
        let client = self.client.clone();
        // The aggregate scans the user table, so honour the connection's read staleness.
        let bound = self.read_staleness.timestamp_bound()?;
        let batch = block_on_cancellable(&self.runtime, &self.cancel, async move {
            let transaction = crate::staleness::single_use(&client, bound);
            let result_set = transaction
                .execute_query(SpannerSql::builder(sql).build())
                .await
                .map_err(from_spanner)?;
            let (_schema, batch) = result_set_to_batch(result_set).await?;
            Ok::<_, Error>(batch)
        })?;

        // The aggregate query always yields exactly one row of `Int64` counts.
        let value = |index: usize| -> Result<i64> {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .filter(|a| a.len() == 1)
                .map(|a| a.value(0))
                .ok_or_else(|| {
                    err(
                        "statistic aggregate is not a single integer",
                        Status::Internal,
                    )
                })
        };
        let mut out = vec![crate::statistics::Statistic {
            table: table.to_string(),
            column: None,
            key: ADBC_STATISTIC_ROW_COUNT_KEY,
            value: value(0)?,
        }];
        for (index, (column, key)) in plan.into_iter().enumerate() {
            out.push(crate::statistics::Statistic {
                table: table.to_string(),
                column: Some(column),
                key,
                value: value(index + 1)?,
            });
        }
        Ok(out)
    }
}

/// Whether a Spanner column type supports `COUNT(DISTINCT)`. `ARRAY`, `STRUCT` and `JSON` are not
/// groupable, so distinct counts are skipped for them.
fn is_groupable(spanner_type: &str) -> bool {
    let t = spanner_type.trim_start();
    !(t.starts_with("ARRAY") || t.starts_with("STRUCT") || t == "JSON")
}

/// Apply the connection's isolation level to a read/write transaction runner builder.
///
/// [`IsolationLevel::Unspecified`] leaves the builder untouched so the client/database default
/// (`SERIALIZABLE`) stands; a specific level is forwarded to
/// [`TransactionRunnerBuilder::set_isolation_level`].
pub(crate) fn apply_isolation(
    builder: TransactionRunnerBuilder,
    isolation: IsolationLevel,
) -> TransactionRunnerBuilder {
    match isolation {
        IsolationLevel::Unspecified => builder,
        level => builder.set_isolation_level(level),
    }
}

/// Map the standard ADBC `adbc.connection.transaction.isolation_level` value to the Spanner client's
/// [`IsolationLevel`]. Spanner supports `SERIALIZABLE` (the default) and `REPEATABLE_READ`; the
/// `default` value leaves the database default in place. Any other spec level (read uncommitted /
/// read committed / snapshot / linearizable) is rejected with `NotImplemented` rather than silently
/// ignored, so callers are not misled into thinking an unsupported guarantee is in effect.
fn parse_isolation_level(value: OptionValue) -> Result<IsolationLevel> {
    use adbc_core::constants::*;
    let s = match value {
        OptionValue::String(s) => s,
        _ => {
            return Err(invalid_argument(
                "expected a string isolation-level option value",
            ))
        }
    };
    match s.as_str() {
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT => Ok(IsolationLevel::Unspecified),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE => Ok(IsolationLevel::Serializable),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ => Ok(IsolationLevel::RepeatableRead),
        ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED
        | ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED
        | ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT
        | ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE => Err(not_implemented(&format!(
            "Spanner does not support isolation level {s:?}; supported: {ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE:?}, {ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ:?}, {ADBC_OPTION_ISOLATION_LEVEL_DEFAULT:?}"
        ))),
        other => Err(invalid_argument(format!(
            "unknown isolation level {other:?}"
        ))),
    }
}

/// The ADBC value string for the stored isolation level, so `get_option` round-trips what was set.
fn isolation_to_adbc_string(isolation: &IsolationLevel) -> &'static str {
    use adbc_core::constants::*;
    match isolation {
        IsolationLevel::Serializable => ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE,
        IsolationLevel::RepeatableRead => ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ,
        // Unspecified and any future variant report as the driver/database default.
        _ => ADBC_OPTION_ISOLATION_LEVEL_DEFAULT,
    }
}

/// Apply DML `statements` atomically in one read/write transaction via Spanner's `ExecuteBatchDml`
/// (a single RPC), returning the total affected-row count.
///
/// The runner may retry the closure on abort, so the (cloned) statement list is replayed on each
/// attempt. Shared by autocommit `execute_update` and the manual-mode commit path.
pub(crate) fn run_batch_dml(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    isolation: IsolationLevel,
    statements: Vec<SpannerSql>,
) -> Result<i64> {
    if statements.is_empty() {
        return Ok(0);
    }
    let client = client.clone();
    block_on_cancellable(runtime, cancel, async move {
        let runner = apply_isolation(client.read_write_transaction(), isolation)
            .build()
            .await
            .map_err(from_spanner)?;
        let outcome = runner
            .run(move |transaction: ReadWriteTransaction| {
                let statements = statements.clone();
                async move {
                    let mut batch = BatchDmlBuilder::new();
                    for statement in statements {
                        batch = batch.add_statement(statement);
                    }
                    let counts = transaction.execute_batch_update(batch.build()).await?;
                    Ok(counts.into_iter().sum::<i64>())
                }
            })
            .await
            .map_err(from_spanner)?;
        Ok::<i64, Error>(outcome.result)
    })
}

/// Whether a table exists, via a parameterized `INFORMATION_SCHEMA.TABLES` lookup. The default
/// (unnamed) schema is the empty string in Spanner.
///
/// A free function (rather than only a [`SpannerConnection`] method) so the statement's bulk-ingest
/// error path can reuse the exact same probe to remap a failed `append` to the spec-mandated status
/// (a missing table → `NotFound`, an existing-but-incompatible table → `AlreadyExists`).
pub(crate) fn table_exists(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    db_schema: &str,
    table_name: &str,
) -> Result<bool> {
    let client = client.clone();
    let (schema, table) = (db_schema.to_string(), table_name.to_string());
    block_on_cancellable(runtime, cancel, async move {
        let statement = SpannerSql::builder(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table",
        )
        .add_param("schema", &schema)
        .add_param("table", &table)
        .build();
        let transaction = client.single_use().build();
        let result_set = transaction
            .execute_query(statement)
            .await
            .map_err(from_spanner)?;
        let (_schema, batch) = result_set_to_batch(result_set).await?;
        Ok::<bool, Error>(batch.num_rows() > 0)
    })
}

/// Run a query and return its single materialised record batch.
async fn query_batch(client: &DatabaseClient, sql: &str) -> Result<RecordBatch> {
    let transaction = client.single_use().build();
    let result_set = transaction
        .execute_query(SpannerSql::builder(sql).build())
        .await
        .map_err(from_spanner)?;
    let (_schema, batch) = result_set_to_batch(result_set).await?;
    Ok(batch)
}

fn str_col(batch: &RecordBatch, index: usize) -> Result<&StringArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            err(
                format!("INFORMATION_SCHEMA column {index} is not a string"),
                Status::Internal,
            )
        })
}

/// One grouped `INFORMATION_SCHEMA.TABLES` row (per schema group).
struct TableRow<'a> {
    name: &'a str,
    table_type: &'a str,
}

/// Maps each foreign key's (constraint_schema, constraint_name) to the referenced constraint's
/// (unique_schema, unique_name), grouped once from `REFERENTIAL_CONSTRAINTS`.
type ReferentialMap<'a> = HashMap<(&'a str, &'a str), (&'a str, &'a str)>;

/// One grouped `INFORMATION_SCHEMA.COLUMNS` row (per (schema, table) group, in ordinal order).
struct ColumnRow<'a> {
    name: &'a str,
    ordinal: i32,
    nullable: bool,
}

/// One grouped `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` row (per (schema, table) group).
struct ConstraintRow<'a> {
    name: &'a str,
    constraint_type: &'a str,
}

/// One grouped `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` row (per (constraint_schema, constraint_name)
/// group, in `ORDINAL_POSITION` order). `position` is `POSITION_IN_UNIQUE_CONSTRAINT`, null except
/// for foreign-key columns.
struct KeyColumnRow<'a> {
    column: &'a str,
    table_schema: &'a str,
    table: &'a str,
    ordinal: &'a str,
    position: Option<&'a str>,
}

/// Group `INFORMATION_SCHEMA.TABLES` rows by `TABLE_SCHEMA`, preserving batch order within a schema.
fn group_tables(batch: &RecordBatch) -> Result<HashMap<&str, Vec<TableRow<'_>>>> {
    let (ts, tn, tt) = (str_col(batch, 0)?, str_col(batch, 1)?, str_col(batch, 2)?);
    let mut map: HashMap<&str, Vec<TableRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry(ts.value(r)).or_default().push(TableRow {
            name: tn.value(r),
            table_type: tt.value(r),
        });
    }
    Ok(map)
}

/// Group `INFORMATION_SCHEMA.COLUMNS` rows by (schema, table); each group keeps ordinal order (the
/// query's `ORDER BY`).
fn group_columns(batch: &RecordBatch) -> Result<HashMap<(&str, &str), Vec<ColumnRow<'_>>>> {
    let (ts, tn, cn, nul) = (
        str_col(batch, 0)?,
        str_col(batch, 1)?,
        str_col(batch, 2)?,
        str_col(batch, 4)?,
    );
    let ordinal = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| err("ORDINAL_POSITION is not an integer", Status::Internal))?;
    let mut map: HashMap<(&str, &str), Vec<ColumnRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((ts.value(r), tn.value(r)))
            .or_default()
            .push(ColumnRow {
                name: cn.value(r),
                ordinal: ordinal.value(r) as i32,
                nullable: nul.value(r).eq_ignore_ascii_case("YES"),
            });
    }
    Ok(map)
}

/// Group `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` rows by (schema, table).
fn group_constraints(batch: &RecordBatch) -> Result<HashMap<(&str, &str), Vec<ConstraintRow<'_>>>> {
    let (ts, tn, cn, ct) = (
        str_col(batch, 0)?,
        str_col(batch, 1)?,
        str_col(batch, 2)?,
        str_col(batch, 3)?,
    );
    let mut map: HashMap<(&str, &str), Vec<ConstraintRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((ts.value(r), tn.value(r)))
            .or_default()
            .push(ConstraintRow {
                name: cn.value(r),
                constraint_type: ct.value(r),
            });
    }
    Ok(map)
}

/// Group `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` rows by (constraint_schema, constraint_name); each
/// group keeps `ORDINAL_POSITION` order (the query's `ORDER BY`).
fn group_key_columns(batch: &RecordBatch) -> Result<HashMap<(&str, &str), Vec<KeyColumnRow<'_>>>> {
    // 0 CONSTRAINT_SCHEMA, 1 CONSTRAINT_NAME, 2 TABLE_SCHEMA, 3 TABLE_NAME, 4 COLUMN_NAME,
    // 5 ORDINAL_POSITION, 6 POSITION_IN_UNIQUE_CONSTRAINT.
    let (kcs, kcn, kts, ktn, kcol, kord, kpos) = (
        str_col(batch, 0)?,
        str_col(batch, 1)?,
        str_col(batch, 2)?,
        str_col(batch, 3)?,
        str_col(batch, 4)?,
        str_col(batch, 5)?,
        str_col(batch, 6)?,
    );
    let mut map: HashMap<(&str, &str), Vec<KeyColumnRow>> = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((kcs.value(r), kcn.value(r)))
            .or_default()
            .push(KeyColumnRow {
                column: kcol.value(r),
                table_schema: kts.value(r),
                table: ktn.value(r),
                ordinal: kord.value(r),
                position: if kpos.is_null(r) {
                    None
                } else {
                    Some(kpos.value(r))
                },
            });
    }
    Ok(map)
}

/// Group `INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS` by (constraint_schema, constraint_name),
/// mapping each foreign key to its referenced (unique_schema, unique_name). Keeps the first row per
/// key, matching the previous `find`.
fn group_referential(batch: &RecordBatch) -> Result<ReferentialMap<'_>> {
    // 0 CONSTRAINT_SCHEMA, 1 CONSTRAINT_NAME, 2 UNIQUE_CONSTRAINT_SCHEMA, 3 UNIQUE_CONSTRAINT_NAME.
    let (rcs, rcn, rus, run) = (
        str_col(batch, 0)?,
        str_col(batch, 1)?,
        str_col(batch, 2)?,
        str_col(batch, 3)?,
    );
    let mut map: ReferentialMap = HashMap::new();
    for r in 0..batch.num_rows() {
        map.entry((rcs.value(r), rcn.value(r)))
            .or_insert_with(|| (rus.value(r), run.value(r)));
    }
    Ok(map)
}

/// Collect the columns of one table from the pre-grouped `COLUMNS` map, applying the `LIKE` filter.
fn collect_columns<'a>(
    columns_by_table: &HashMap<(&'a str, &'a str), Vec<ColumnRow<'a>>>,
    schema: &'a str,
    table: &'a str,
    filter: Option<&str>,
) -> Vec<crate::objects::Column> {
    let mut columns = Vec::new();
    for column in columns_by_table.get(&(schema, table)).into_iter().flatten() {
        if filter.is_some_and(|p| !like_match(p, column.name)) {
            continue;
        }
        columns.push(crate::objects::Column {
            name: column.name.to_string(),
            ordinal: column.ordinal,
            nullable: column.nullable,
        });
    }
    columns
}

/// Assemble the constraints for one table from the pre-grouped `TABLE_CONSTRAINTS`,
/// `KEY_COLUMN_USAGE` and `REFERENTIAL_CONSTRAINTS` maps. Each constraint's key columns come out in
/// key order (the `KEY_COLUMN_USAGE` group keeps `ORDINAL_POSITION` order); check/... constraints
/// have no key columns and get an empty list.
fn collect_constraints<'a>(
    constraints_by_table: &HashMap<(&'a str, &'a str), Vec<ConstraintRow<'a>>>,
    key_columns_by_constraint: &HashMap<(&'a str, &'a str), Vec<KeyColumnRow<'a>>>,
    referential_by_constraint: &ReferentialMap<'a>,
    schema: &'a str,
    table: &'a str,
) -> Vec<crate::objects::Constraint> {
    let mut out = Vec::new();
    for constraint in constraints_by_table
        .get(&(schema, table))
        .into_iter()
        .flatten()
    {
        let name = constraint.name;
        // Columns for this constraint, in key order (the group keeps ORDINAL_POSITION order);
        // check/... constraints have no key columns and get an empty list.
        let columns = key_columns_by_constraint
            .get(&(schema, name))
            .into_iter()
            .flatten()
            .map(|k| k.column.to_string())
            .collect();
        let constraint_type = constraint.constraint_type.to_string();
        let usages = if constraint_type == "FOREIGN KEY" {
            foreign_key_usages(
                key_columns_by_constraint,
                referential_by_constraint,
                schema,
                name,
            )
        } else {
            Vec::new()
        };
        out.push(crate::objects::Constraint {
            name: Some(name.to_string()),
            constraint_type,
            columns,
            usages,
        });
    }
    out
}

/// The referenced (parent) columns of one foreign key, in the same order as its own key columns.
///
/// `CONSTRAINT_COLUMN_USAGE` lists the referenced columns but does not preserve order, so instead:
/// find the referenced unique constraint via `REFERENTIAL_CONSTRAINTS`, index its key columns by
/// ordinal, then walk the FK's own columns (ordered) mapping each through
/// `POSITION_IN_UNIQUE_CONSTRAINT` to the referenced column at that ordinal.
fn foreign_key_usages<'a>(
    key_columns_by_constraint: &HashMap<(&'a str, &'a str), Vec<KeyColumnRow<'a>>>,
    referential_by_constraint: &ReferentialMap<'a>,
    schema: &'a str,
    fk_name: &'a str,
) -> Vec<crate::objects::Usage> {
    // The referenced unique/primary-key constraint.
    let Some(&(unique_schema, unique_name)) = referential_by_constraint.get(&(schema, fk_name))
    else {
        return Vec::new();
    };

    // Index the referenced constraint's key columns by ordinal position.
    let referenced: HashMap<i64, &KeyColumnRow> = key_columns_by_constraint
        .get(&(unique_schema, unique_name))
        .into_iter()
        .flatten()
        .filter_map(|k| Some((k.ordinal.parse::<i64>().ok()?, k)))
        .collect();

    // Walk the FK's own columns in key order, mapping each to its referenced column.
    let mut fk_columns: Vec<(i64, i64)> = key_columns_by_constraint
        .get(&(schema, fk_name))
        .into_iter()
        .flatten()
        .filter_map(|k| Some((k.ordinal.parse().ok()?, k.position?.parse().ok()?)))
        .collect();
    fk_columns.sort_by_key(|&(ordinal, _)| ordinal);

    fk_columns
        .into_iter()
        .filter_map(|(_, position)| {
            referenced.get(&position).map(|k| crate::objects::Usage {
                db_schema: k.table_schema.to_string(),
                table: k.table.to_string(),
                column: k.column.to_string(),
            })
        })
        .collect()
}

/// Match an ADBC `LIKE` pattern (`%` = any run, `_` = one char) against a value, case-sensitively.
///
/// Iterative with backtrack pointers (O(pattern × value), no recursion) so adversarial patterns
/// like `%a%a%a…` cannot cause exponential blowup or stack overflow.
pub(crate) fn like_match(pattern: &str, value: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    let (mut pi, mut vi) = (0usize, 0usize);
    // Position in the pattern/value to backtrack to after the most recent `%`.
    let mut star: Option<(usize, usize)> = None;
    while vi < v.len() {
        // `%` must be tested before the literal/`_` branch: otherwise a `%` in the pattern that
        // happens to equal the current value char (e.g. both are `%`) would be consumed as a
        // literal instead of acting as a wildcard.
        if pi < p.len() && p[pi] == '%' {
            star = Some((pi, vi));
            pi += 1;
        } else if pi < p.len() && (p[pi] == '_' || p[pi] == v[vi]) {
            pi += 1;
            vi += 1;
        } else if let Some((sp, sv)) = star {
            // Let the last `%` consume one more character and retry.
            pi = sp + 1;
            vi = sv + 1;
            star = Some((sp, sv + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod like_tests {
    use super::like_match;

    #[test]
    fn like_matching() {
        assert!(like_match("", ""));
        assert!(like_match("%", ""));
        assert!(like_match("%", "anything"));
        assert!(like_match("Singers", "Singers"));
        assert!(!like_match("Singers", "singers")); // case-sensitive
        assert!(like_match("Sing%", "Singers"));
        assert!(like_match("%ers", "Singers"));
        assert!(like_match("S_ngers", "Singers"));
        assert!(like_match("%a%a%", "banana"));
        assert!(!like_match("%x%", "banana"));
        assert!(!like_match("", "x"));
        // A pattern `%` must stay a wildcard even when the value has a literal `%` where the
        // wildcard begins matching — the value starts with `%`, or a `%` follows matched literals.
        // The literal branch used to mis-consume it there, so these all failed. Found by the `like`
        // fuzz target's differential regex oracle.
        assert!(like_match("%", "%foo"));
        assert!(like_match("%", "%^%?"));
        assert!(like_match("a%", "a%b"));
    }
}

impl Optionable for SpannerConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::AutoCommit => {
                let enable = parse_bool(value)?;
                // Enabling autocommit commits any active manual transaction. Like `commit`, apply
                // from a clone and drain only on success: a failed apply must leave the buffer
                // (and manual mode — note the early return keeps `autocommit` false) intact so
                // the caller can retry or roll back, not silently lose the writes and flip mode.
                let pending = {
                    let st = self.txn.lock().unwrap();
                    (enable && !st.autocommit).then(|| st.pending.clone())
                };
                let applied = match pending {
                    Some(pending) => {
                        let applied = pending.len();
                        self.apply_transaction(pending)?;
                        applied
                    }
                    None => 0,
                };
                let mut st = self.txn.lock().unwrap();
                st.pending.drain(..applied);
                st.autocommit = enable;
            }
            OptionConnection::ReadOnly => self.read_only = parse_bool(value)?,
            OptionConnection::IsolationLevel => self.isolation = parse_isolation_level(value)?,
            OptionConnection::Other(k) if k == crate::OPTION_READ_STALENESS => {
                self.read_staleness.set_staleness(value)?;
            }
            OptionConnection::Other(k) if k == crate::OPTION_READ_TIMESTAMP => {
                self.read_staleness.set_timestamp(value)?;
            }
            other => {
                return Err(not_implemented(&format!(
                    "unsupported Spanner connection option: {}",
                    connection_option_name(other)
                )))
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionConnection::AutoCommit => Ok(self.txn.lock().unwrap().autocommit.to_string()),
            OptionConnection::ReadOnly => Ok(self.read_only.to_string()),
            OptionConnection::IsolationLevel => {
                Ok(isolation_to_adbc_string(&self.isolation).to_string())
            }
            OptionConnection::Other(k) if k == crate::OPTION_READ_STALENESS => self
                .read_staleness
                .staleness_string()
                .map(str::to_string)
                .ok_or_else(|| {
                    err(
                        format!("option {} is not set", crate::OPTION_READ_STALENESS),
                        Status::NotFound,
                    )
                }),
            OptionConnection::Other(k) if k == crate::OPTION_READ_TIMESTAMP => self
                .read_staleness
                .timestamp_string()
                .map(str::to_string)
                .ok_or_else(|| {
                    err(
                        format!("option {} is not set", crate::OPTION_READ_TIMESTAMP),
                        Status::NotFound,
                    )
                }),
            // A Spanner database has a single, unnamed catalog and (default) schema — both the empty
            // string in INFORMATION_SCHEMA, which is what `get_objects` reports — so the "current"
            // catalog/schema are reported as "". (They can't be switched; setting them is unsupported.)
            OptionConnection::CurrentCatalog | OptionConnection::CurrentSchema => Ok(String::new()),
            other => Err(err(
                format!("option {} is not set", connection_option_name(other)),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Ok(self.get_option_string(key)?.into_bytes())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(err(
            format!("option {} is not an integer", connection_option_name(&key)),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(err(
            format!("option {} is not a double", connection_option_name(&key)),
            Status::NotFound,
        ))
    }
}

impl Connection for SpannerConnection {
    type StatementType = SpannerStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(SpannerStatement::new(
            self.runtime.clone(),
            self.client.clone(),
            self.spanner.clone(),
            self.database.clone(),
            self.read_only,
            self.isolation.clone(),
            self.read_staleness.clone(),
            self.txn.clone(),
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        // Latch the (sticky) signal: an in-flight metadata/commit operation wakes and returns
        // Cancelled, and a cancel landing between two chunk fetches of a `read_partition` stream
        // still cancels the next fetch. The latch is cleared when the connection starts its next
        // operation. Statements have their own signal, so this does not affect a query running on
        // a statement from this connection.
        self.cancel.signal();
        Ok(())
    }

    /// Driver / vendor metadata, sourced entirely from static driver constants (no Spanner RPC).
    ///
    /// `codes = None` returns the set of codes the driver has a meaningful value for; an explicit
    /// set returns one row per requested code (a null value for codes it cannot answer).
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let batch = crate::info::build(codes)?;
        let schema = batch.schema();
        Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
    }

    /// Catalog/schema/table/column introspection, sourced from Spanner `INFORMATION_SCHEMA`.
    ///
    /// A Spanner database is a single, unnamed catalog (`""`). Name arguments are ADBC `LIKE`
    /// patterns (`%`/`_`); `depth` bounds how far the hierarchy is populated.
    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        let out_schema = adbc_core::schemas::GET_OBJECTS_SCHEMA.clone();
        // Spanner has a single catalog (""); a catalog filter that excludes it yields no rows.
        if catalog.is_some_and(|c| !like_match(c, "")) {
            return Ok(Box::new(RecordBatchIterator::new(Vec::new(), out_schema)));
        }
        let schemas =
            self.collect_objects(depth, db_schema, table_name, &table_type, column_name)?;
        let batch = crate::objects::build(depth, schemas)?;
        Ok(Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            out_schema,
        )))
    }

    /// Return the Arrow schema of a table.
    ///
    /// Implemented by running a zero-row `SELECT * FROM <table> LIMIT 0` and mapping the result-set
    /// column metadata to Arrow (the same mapping used for query results). Spanner has no catalog
    /// concept, so `catalog` is ignored.
    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        let table = qualified_table(db_schema, table_name);
        let sql = format!("SELECT * FROM {table} LIMIT 0");
        let client = self.client.clone();
        let bound = self.read_staleness.timestamp_bound()?;
        let result = block_on_cancellable(&self.runtime, &self.cancel, async move {
            let transaction = crate::staleness::single_use(&client, bound);
            let result_set = transaction
                .execute_query(SpannerSql::builder(sql).build())
                .await
                .map_err(from_spanner)?;
            result_set_to_batch(result_set).await
        });
        match result {
            Ok((schema, _batch)) => Ok((*schema).clone()),
            // A missing table surfaces from the query analyzer as `INVALID_ARGUMENT` ("Table not
            // found"), but ADBC wants `NotFound`. Only touch `INFORMATION_SCHEMA` on the error path
            // so the common (table exists) case stays a single query.
            Err(error) => {
                if self.table_exists(db_schema.unwrap_or(""), table_name)? {
                    Err(error)
                } else {
                    Err(err(
                        format!("table {table_name:?} not found"),
                        Status::NotFound,
                    ))
                }
            }
        }
    }

    /// Return the table types supported by Spanner as a single-column (`table_type: utf8`) batch,
    /// per the ADBC specification. The values are Spanner's own
    /// `INFORMATION_SCHEMA.TABLES.TABLE_TYPE` vocabulary (`BASE TABLE` / `VIEW`), which is what
    /// `get_objects` reports per table — so every value returned here round-trips as a
    /// `get_objects` `table_type` filter.
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let array = Arc::new(StringArray::from(vec!["BASE TABLE", "VIEW"])) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).map_err(|e| {
            err(
                format!("failed to build table types batch: {e}"),
                Status::Internal,
            )
        })?;
        Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
    }

    /// Spanner exposes no portable per-table statistics, so this returns an empty (but correctly
    /// typed) result set — i.e. "no statistic names".
    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(Box::new(RecordBatchIterator::new(
            Vec::new(),
            adbc_core::schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
        )))
    }

    /// Table/column statistics, computed exactly from aggregate scans (`ROW_COUNT`, and per column
    /// `NULL_COUNT` and `DISTINCT_COUNT`). Name arguments are ADBC `LIKE` patterns.
    ///
    /// Spanner keeps no cheap/pre-computed statistics, so an `approximate` request returns nothing
    /// rather than triggering the expensive exact scans; pass `approximate = false` to compute them.
    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        let out_schema = adbc_core::schemas::GET_STATISTICS_SCHEMA.clone();
        // Spanner is a single unnamed catalog (""); a catalog filter that excludes it yields nothing.
        if catalog.is_some_and(|c| !like_match(c, "")) {
            return Ok(Box::new(RecordBatchIterator::new(Vec::new(), out_schema)));
        }
        let schemas = if approximate {
            Vec::new()
        } else {
            self.collect_statistics(db_schema, table_name)?
        };
        let batch = crate::statistics::build(schemas, out_schema.clone())?;
        Ok(Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            out_schema,
        )))
    }

    fn commit(&mut self) -> Result<()> {
        // Apply from a *clone* of the buffer and drain it only after success. Taking the buffer
        // up front would lose the DML on a failed apply (e.g. ABORTED once the runner's retries
        // are exhausted — the very code `error.rs` preserves in `vendor_code` so callers can
        // retry) and, worse, a retried `commit()` would then see an empty list and report
        // success with nothing written. Keeping the buffer makes retry a genuine replay and
        // leaves `rollback()` available to discard instead (see the module doc for the replay
        // caveats).
        let pending = {
            let st = self.txn.lock().unwrap();
            if st.autocommit {
                return Err(invalid_state(
                    "commit invoked with autocommit enabled; no active transaction",
                ));
            }
            st.pending.clone()
        };
        let applied = pending.len();
        self.apply_transaction(pending)?;
        // Drain exactly the statements that were applied; anything buffered concurrently while
        // the commit RPC ran stays pending for the next commit.
        self.txn.lock().unwrap().pending.drain(..applied);
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let mut st = self.txn.lock().unwrap();
        if st.autocommit {
            return Err(invalid_state(
                "rollback invoked with autocommit enabled; no active transaction",
            ));
        }
        st.pending.clear();
        Ok(())
    }

    /// Execute a partition descriptor produced by `Statement::execute_partitions` and stream its
    /// rows as Arrow.
    ///
    /// # Security
    ///
    /// A partition descriptor is **opaque but executable**: it is serde-JSON of the client's
    /// `Partition`, whose inner `ExecuteSqlRequest` carries the SQL text itself along with the
    /// session and transaction identity. `read_partition` runs whatever that blob contains against
    /// this connection's `DatabaseClient`, with **this connection's credentials** — so a crafted
    /// descriptor executes arbitrary SQL as the connection's principal. This is inherent to ADBC's
    /// portable-descriptor design and the upstream serde format, and there is no in-band
    /// authentication of the blob. Treat a descriptor as an executable request, not as opaque data:
    /// transport it only over trusted channels and **never accept one from an untrusted source**.
    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // A new operation begins: clear any cancel aimed at a previous one (see `CancelSignal`).
        self.cancel.reset();
        // Decode the opaque descriptor produced by `Statement::execute_partitions`. It carries the
        // session, transaction id, partition token and Data Boost flag, so it executes on this
        // connection's client (which shares the same multiplexed session) with no further setup.
        let partition: Partition = serde_json::from_slice(partition.as_ref())
            .map_err(|e| invalid_argument(format!("invalid partition descriptor: {e}")))?;
        let client = self.client.clone();
        let runtime = self.runtime.clone();
        let cancel = self.cancel.clone();
        // Stream the partition's rows to Arrow exactly like `Statement::execute`. The connection has
        // no per-statement batch-size option, so the default chunk size is used.
        let reader = block_on_cancellable(&self.runtime, &self.cancel, async move {
            let result_set = partition.execute(&client).await.map_err(from_spanner)?;
            stream_query(runtime, cancel, result_set, DEFAULT_ROWS_PER_BATCH).await
        })?;
        Ok(Box::new(reader))
    }
}

fn parse_bool(value: OptionValue) -> Result<bool> {
    crate::options::bool_option(value, "option")
}

fn connection_option_name(key: &OptionConnection) -> String {
    key.as_ref().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_isolation_levels() {
        use adbc_core::constants::*;
        let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE).unwrap(),
            IsolationLevel::Serializable
        );
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ).unwrap(),
            IsolationLevel::RepeatableRead
        );
        // `default` maps to the client's unspecified level, leaving the database default in place.
        assert_eq!(
            parse(ADBC_OPTION_ISOLATION_LEVEL_DEFAULT).unwrap(),
            IsolationLevel::Unspecified
        );
    }

    #[test]
    fn rejects_unsupported_isolation_levels() {
        use adbc_core::constants::*;
        let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
        // Spec levels Spanner cannot honour are rejected (NotImplemented), not silently ignored.
        for level in [
            ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED,
            ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED,
            ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT,
            ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE,
        ] {
            let err = parse(level).unwrap_err();
            assert_eq!(err.status, Status::NotImplemented, "level {level}");
        }
        // A completely unknown value is an invalid argument.
        assert_eq!(
            parse("not-a-level").unwrap_err().status,
            Status::InvalidArguments
        );
        // A non-string option value is rejected.
        assert_eq!(
            parse_isolation_level(OptionValue::Int(1))
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
    }

    #[test]
    fn isolation_level_round_trips_to_adbc_string() {
        use adbc_core::constants::*;
        assert_eq!(
            isolation_to_adbc_string(&IsolationLevel::Serializable),
            ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
        );
        assert_eq!(
            isolation_to_adbc_string(&IsolationLevel::RepeatableRead),
            ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
        );
        assert_eq!(
            isolation_to_adbc_string(&IsolationLevel::Unspecified),
            ADBC_OPTION_ISOLATION_LEVEL_DEFAULT
        );
    }
}

//! Building the result of [`Connection::get_statistics`](adbc_core::Connection::get_statistics).
//!
//! The ADBC `get_statistics` result is nested `catalog → list<db_schema → list<statistic>>` (a
//! Spanner database is a single, unnamed catalog). Each statistic carries a dictionary key, a
//! dense-union value, and an is-approximate flag.
//!
//! Spanner has no statistics catalog, but the count-style statistics can be computed exactly with a
//! single aggregate scan per table: `ROW_COUNT` (table-level) plus per-column `NULL_COUNT` and
//! `DISTINCT_COUNT`. All are integers, so they use the union's `int64` branch and are exact — the
//! same exact scans serve both `approximate = false` (which *requires* exact values) and
//! `approximate = true` (which merely *allows* approximate ones; exact values always satisfy it),
//! and every row reports `statistic_is_approximate = false`. (The
//! `MIN_VALUE`/`MAX_VALUE` statistics are not reported: the value union only has int64/uint64/
//! float64/binary members, so they cannot represent Spanner's STRING/DATE/TIMESTAMP/NUMERIC types.)

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use arrow_array::{
    new_null_array, Array, ArrayRef, BooleanArray, Int16Array, Int64Array, RecordBatch,
    StringArray, StructArray,
};
use arrow_schema::{DataType, Fields, SchemaRef};
use futures_util::stream::{self, StreamExt};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement as SpannerSql;

use crate::bind::{qualified_table, quote_ident};
use crate::connection::{like_match, query_batch, str_col};
use crate::conversion::result_set_to_batch;
use crate::error::{err, from_spanner};
use crate::nested::{arrow_err, dense_union, field, list_item, list_of, struct_fields};
use crate::runtime::{block_on_cancellable, CancelSignal, SharedRuntime};
use crate::staleness::ReadStaleness;

/// The `int64` branch of the `statistic_value` union (see `STATISTIC_VALUE_SCHEMA`).
const INT64_BRANCH: i8 = 0;

/// How many per-table aggregate statistics scans to run concurrently. Each scan is one independent
/// read-only query, so running a small bounded batch of them at once (rather than strictly one after
/// another) cuts the wall-clock of `get_statistics` near-linearly on a many-table database without
/// unbounded fan-out against Spanner. They all share the driver's one Tokio runtime.
const STATISTICS_SCAN_CONCURRENCY: usize = 8;

/// One computed statistic for a table (or one of its columns).
pub(crate) struct Statistic {
    pub table: String,
    /// The column the statistic applies to, or `None` for a table-level statistic (e.g. `ROW_COUNT`).
    pub column: Option<String>,
    /// The ADBC statistic key (`ADBC_STATISTIC_*_KEY`).
    pub key: i16,
    /// The value; all reported statistics are counts, so they go in the union's `int64` branch.
    pub value: i64,
}

/// The statistics of all tables in one db schema.
pub(crate) struct SchemaStatistics {
    pub db_schema: String,
    pub statistics: Vec<Statistic>,
}

/// Compute exact statistics for the base tables matching the `LIKE` filters: `ROW_COUNT` per
/// table, and `NULL_COUNT` (+ `DISTINCT_COUNT` for groupable types) per column.
pub(crate) fn collect_statistics(
    runtime: &SharedRuntime,
    client: &DatabaseClient,
    cancel: &CancelSignal,
    read_staleness: &ReadStaleness,
    db_schema: Option<&str>,
    table_name: Option<&str>,
) -> Result<Vec<SchemaStatistics>> {
    let (table_batch, column_batch) = {
        let client = client.clone();
        block_on_cancellable(runtime, cancel, async move {
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
        })?
    };

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

    // Prepare each matching table's aggregate query up front, in deterministic `table_batch`
    // order. The scans themselves run concurrently below, so the order the *results* arrive in is
    // non-deterministic — we keep this prepared list as the canonical order and reassemble against
    // it, so the output (tables, schemas and their statistics) is identical to the old sequential
    // loop regardless of completion order.
    let bound = read_staleness.timestamp_bound()?;
    let mut prepared: Vec<PreparedTable> = Vec::new();
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
        let (sql, plan) = build_table_query(schema, table, &columns);
        prepared.push(PreparedTable {
            schema: schema.to_string(),
            table: table.to_string(),
            sql,
            plan,
        });
    }

    // Run the per-table aggregate scans with bounded concurrency on the one shared runtime.
    // `buffer_unordered` yields results as they finish (out of order), so tag each with its input
    // index and slot it back into deterministic order. The whole stream is driven inside a single
    // `block_on_cancellable`, so a `cancel` still interrupts an in-flight batch of scans, and any
    // scan error propagates out (via `?`) as an overall `Err`.
    let batches: Vec<RecordBatch> = block_on_cancellable(runtime, cancel, async {
        let mut slots: Vec<Option<RecordBatch>> = (0..prepared.len()).map(|_| None).collect();
        let mut scans = stream::iter(prepared.iter().enumerate().map(|(idx, p)| {
            let client = client.clone();
            let sql = p.sql.clone();
            // The aggregate scans the user table, so honour the connection's read staleness.
            let bound = bound.clone();
            async move {
                let transaction = crate::staleness::single_use(&client, bound);
                let result_set = transaction
                    .execute_query(SpannerSql::builder(sql).build())
                    .await
                    .map_err(from_spanner)?;
                let (_schema, batch) = result_set_to_batch(result_set).await?;
                Ok::<_, Error>((idx, batch))
            }
        }))
        .buffer_unordered(STATISTICS_SCAN_CONCURRENCY);
        while let Some(result) = scans.next().await {
            let (idx, batch) = result?;
            slots[idx] = Some(batch);
        }
        // Every prepared table produced exactly one batch (the loop consumed the whole stream).
        Ok::<_, Error>(slots.into_iter().map(|b| b.unwrap()).collect())
    })?;

    // Parse each batch and re-group by schema, in the original deterministic order.
    let mut schemas: Vec<SchemaStatistics> = Vec::new();
    for (p, batch) in prepared.into_iter().zip(batches) {
        let stats = parse_table_statistics(&batch, &p.table, p.plan)?;
        match schemas.iter_mut().find(|s| s.db_schema == p.schema) {
            Some(s) => s.statistics.extend(stats),
            None => schemas.push(SchemaStatistics {
                db_schema: p.schema,
                statistics: stats,
            }),
        }
    }
    Ok(schemas)
}

/// A per-table aggregate statistics query, prepared but not yet run.
struct PreparedTable {
    schema: String,
    table: String,
    /// The single-scan aggregate `SELECT`.
    sql: String,
    /// Maps each result column after the row count to its (column, statistic key).
    plan: Vec<(String, i16)>,
}

/// Build the single-scan aggregate query for one table: `COUNT(*)` plus per-column
/// `COUNTIF(... IS NULL)` and (for groupable columns) `COUNT(DISTINCT ...)`. Returns the SQL and a
/// `plan` mapping each result column after the row count to its (column name, statistic key).
fn build_table_query(
    schema: &str,
    table: &str,
    columns: &[(String, bool)],
) -> (String, Vec<(String, i16)>) {
    use adbc_core::constants::{ADBC_STATISTIC_DISTINCT_COUNT_KEY, ADBC_STATISTIC_NULL_COUNT_KEY};

    let mut exprs = vec!["COUNT(*)".to_string()];
    let mut plan: Vec<(String, i16)> = Vec::new();
    for (name, groupable) in columns {
        let quoted = quote_ident(name);
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
    (sql, plan)
}

/// Extract the `ROW_COUNT` and per-column `NULL_COUNT`/`DISTINCT_COUNT` statistics from a table's
/// single-row aggregate result, using the `plan` produced by [`build_table_query`].
fn parse_table_statistics(
    batch: &RecordBatch,
    table: &str,
    plan: Vec<(String, i16)>,
) -> Result<Vec<Statistic>> {
    use adbc_core::constants::ADBC_STATISTIC_ROW_COUNT_KEY;

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
    let mut out = vec![Statistic {
        table: table.to_string(),
        column: None,
        key: ADBC_STATISTIC_ROW_COUNT_KEY,
        value: value(0)?,
    }];
    for (index, (column, key)) in plan.into_iter().enumerate() {
        out.push(Statistic {
            table: table.to_string(),
            column: Some(column),
            key,
            value: value(index + 1)?,
        });
    }
    Ok(out)
}

/// Whether a Spanner column type supports `COUNT(DISTINCT)`. `ARRAY`, `STRUCT`, `JSON`, `TOKENLIST`
/// and `PROTO<...>` are not groupable, so distinct counts are skipped for them. The names are the
/// `SPANNER_TYPE` strings from `INFORMATION_SCHEMA.COLUMNS`; a non-groupable column left in the
/// aggregate `COUNT(DISTINCT)` scan would make the whole per-table query fail, so it is important
/// this list stays complete.
fn is_groupable(spanner_type: &str) -> bool {
    let t = spanner_type.trim_start();
    !(t.starts_with("ARRAY")
        || t.starts_with("STRUCT")
        || t.starts_with("PROTO")
        || t == "JSON"
        || t == "TOKENLIST")
}

/// Build the single-catalog `get_statistics` record batch from per-schema statistics.
pub(crate) fn build(schemas: Vec<SchemaStatistics>, out_schema: SchemaRef) -> Result<RecordBatch> {
    let top_fields = out_schema.fields();

    let db_schemas_field = field(top_fields, "catalog_db_schemas")?;
    let db_schema_item = list_item(&db_schemas_field)?;
    let db_schema_fields = struct_fields(&db_schema_item)?;

    // db_schema_statistics: list<statistic struct> per schema. Flatten the statistics across
    // schemas to build one struct array, then re-group by per-schema lengths.
    let stats_field = field(&db_schema_fields, "db_schema_statistics")?;
    let stat_item = list_item(&stats_field)?;
    let stat_fields = struct_fields(&stat_item)?;
    let flat: Vec<&Statistic> = schemas.iter().flat_map(|s| s.statistics.iter()).collect();
    let stat_struct = build_statistic_struct(&stat_fields, &flat)?;
    let lengths: Vec<usize> = schemas.iter().map(|s| s.statistics.len()).collect();
    let db_schema_statistics = list_of(stat_item, &lengths, stat_struct)?;

    let schema_names: ArrayRef = Arc::new(StringArray::from_iter(
        schemas.iter().map(|s| Some(s.db_schema.clone())),
    ));
    let db_schema_struct: ArrayRef = Arc::new(
        StructArray::try_new(
            db_schema_fields.clone(),
            vec![schema_names, db_schema_statistics],
            None,
        )
        .map_err(arrow_err)?,
    );
    let catalog_db_schemas = list_of(db_schema_item, &[schemas.len()], db_schema_struct)?;

    let catalog_name: ArrayRef = Arc::new(StringArray::from(vec![""]));
    RecordBatch::try_new(out_schema, vec![catalog_name, catalog_db_schemas]).map_err(arrow_err)
}

/// Build the statistic struct array (`table_name` / `column_name` / `statistic_key` /
/// `statistic_value` / `statistic_is_approximate`). All values are exact integers.
fn build_statistic_struct(stat_fields: &Fields, stats: &[&Statistic]) -> Result<ArrayRef> {
    let n = stats.len();

    // statistic_value: a dense union with every value in the `int64` branch.
    let value_field = field(stat_fields, "statistic_value")?;
    let union_fields = match value_field.data_type() {
        DataType::Union(fields, _) => fields.clone(),
        _ => {
            return Err(err(
                "unexpected ADBC result schema shape: expected `statistic_value` to be a union",
                Status::Internal,
            ))
        }
    };
    let int64_values: ArrayRef =
        Arc::new(Int64Array::from_iter(stats.iter().map(|s| Some(s.value))));
    // Every statistic value lives in the `int64` branch; `dense_union` fills the other branches with
    // empty children so the union type still matches the schema.
    let type_ids = vec![INT64_BRANCH; n];
    let offsets: Vec<i32> = (0..n as i32).collect();
    let statistic_value = dense_union(
        &union_fields,
        &[(INT64_BRANCH, int64_values)],
        type_ids,
        offsets,
    )?;

    let arrays: Vec<ArrayRef> = stat_fields
        .iter()
        .map(|f| match f.name().as_str() {
            "table_name" => Arc::new(StringArray::from_iter(
                stats.iter().map(|s| Some(s.table.clone())),
            )) as ArrayRef,
            "column_name" => Arc::new(StringArray::from_iter(
                stats.iter().map(|s| s.column.clone()),
            )) as ArrayRef,
            "statistic_key" => {
                Arc::new(Int16Array::from_iter(stats.iter().map(|s| Some(s.key)))) as ArrayRef
            }
            "statistic_value" => statistic_value.clone(),
            "statistic_is_approximate" => {
                Arc::new(BooleanArray::from_iter(stats.iter().map(|_| Some(false)))) as ArrayRef
            }
            _ => new_null_array(f.data_type(), n),
        })
        .collect();

    Ok(Arc::new(
        StructArray::try_new(stat_fields.clone(), arrays, None).map_err(arrow_err)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::constants::{ADBC_STATISTIC_NULL_COUNT_KEY, ADBC_STATISTIC_ROW_COUNT_KEY};
    use adbc_core::schemas::GET_STATISTICS_SCHEMA;

    #[test]
    fn build_matches_schema() {
        let schemas = vec![SchemaStatistics {
            db_schema: String::new(),
            statistics: vec![
                Statistic {
                    table: "Users".into(),
                    column: None,
                    key: ADBC_STATISTIC_ROW_COUNT_KEY,
                    value: 42,
                },
                Statistic {
                    table: "Users".into(),
                    column: Some("Name".into()),
                    key: ADBC_STATISTIC_NULL_COUNT_KEY,
                    value: 3,
                },
            ],
        }];
        let batch = build(schemas, GET_STATISTICS_SCHEMA.clone()).unwrap();
        assert_eq!(batch.schema(), GET_STATISTICS_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 1); // one catalog
    }

    #[test]
    fn groupable_types() {
        // Ordinary scalar types are distinct-countable.
        for t in [
            "INT64",
            "STRING(MAX)",
            "BOOL",
            "BYTES(10)",
            "NUMERIC",
            "TIMESTAMP",
            "DATE",
        ] {
            assert!(is_groupable(t), "{t} should be groupable");
        }
        // Non-groupable Spanner types: a COUNT(DISTINCT) over any of these fails the aggregate
        // scan, which would otherwise break get_statistics for the whole database.
        for t in [
            "ARRAY<INT64>",
            "STRUCT<a INT64>",
            "JSON",
            "TOKENLIST",
            "PROTO<examples.spanner.music.SingerInfo>",
        ] {
            assert!(!is_groupable(t), "{t} should not be groupable");
        }
    }

    #[test]
    fn empty_is_valid() {
        let batch = build(Vec::new(), GET_STATISTICS_SCHEMA.clone()).unwrap();
        assert_eq!(batch.schema(), GET_STATISTICS_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 1);
    }
}

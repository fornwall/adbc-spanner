//! Building the result of [`Connection::get_statistics`](adbc_core::Connection::get_statistics).
//!
//! The ADBC `get_statistics` result is nested `catalog → list<db_schema → list<statistic>>` (a
//! Spanner database is a single, unnamed catalog). Each statistic carries a dictionary key, a
//! dense-union value, and an is-approximate flag.
//!
//! Spanner has no statistics catalog, but the count-style statistics can be computed exactly with a
//! single aggregate scan per table: `ROW_COUNT` (table-level) plus per-column `NULL_COUNT` and
//! `DISTINCT_COUNT`. All are integers, so they use the union's `int64` branch and are exact. (The
//! `MIN_VALUE`/`MAX_VALUE` statistics are not reported: the value union only has int64/uint64/
//! float64/binary members, so they cannot represent Spanner's STRING/DATE/TIMESTAMP/NUMERIC types.)

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::{
    new_empty_array, new_null_array, ArrayRef, BooleanArray, Int16Array, Int64Array, ListArray,
    RecordBatch, StringArray, StructArray, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields, SchemaRef, UnionFields};

use crate::error::err;

/// The `int64` branch of the `statistic_value` union (see `STATISTIC_VALUE_SCHEMA`).
const INT64_BRANCH: i8 = 0;

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

fn arrow_err(e: ArrowError) -> adbc_core::error::Error {
    err(
        format!("failed to build get_statistics batch: {e}"),
        Status::Internal,
    )
}

/// The `Field` for a named field within `fields`.
fn field(fields: &Fields, name: &str) -> FieldRef {
    fields
        .find(name)
        .map(|(_, f)| f.clone())
        .expect("adbc_core get_statistics schema field")
}

/// Wrap `child` into a `ListArray` grouping its elements by `lengths`, one non-null list per parent.
fn list_of(item: FieldRef, lengths: &[usize], child: ArrayRef) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0i32);
    let mut acc = 0i32;
    for len in lengths {
        acc += *len as i32;
        offsets.push(acc);
    }
    let list = ListArray::try_new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        None,
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(list))
}

/// Build the single-catalog `get_statistics` record batch from per-schema statistics.
pub(crate) fn build(schemas: Vec<SchemaStatistics>, out_schema: SchemaRef) -> Result<RecordBatch> {
    let top_fields = out_schema.fields();

    let db_schemas_field = field(top_fields, "catalog_db_schemas");
    let db_schema_item = match db_schemas_field.data_type() {
        DataType::List(item) => item.clone(),
        _ => unreachable!("catalog_db_schemas is a list"),
    };
    let db_schema_fields = match db_schema_item.data_type() {
        DataType::Struct(fs) => fs.clone(),
        _ => unreachable!("db_schema is a struct"),
    };

    // db_schema_statistics: list<statistic struct> per schema. Flatten the statistics across
    // schemas to build one struct array, then re-group by per-schema lengths.
    let stats_field = field(&db_schema_fields, "db_schema_statistics");
    let stat_item = match stats_field.data_type() {
        DataType::List(item) => item.clone(),
        _ => unreachable!("db_schema_statistics is a list"),
    };
    let stat_fields = match stat_item.data_type() {
        DataType::Struct(fs) => fs.clone(),
        _ => unreachable!("statistic is a struct"),
    };
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
    let value_field = field(stat_fields, "statistic_value");
    let union_fields = match value_field.data_type() {
        DataType::Union(fields, _) => fields.clone(),
        _ => unreachable!("statistic_value is a union"),
    };
    let int64_values: ArrayRef =
        Arc::new(Int64Array::from_iter(stats.iter().map(|s| Some(s.value))));
    let statistic_value = build_value_union(&union_fields, int64_values, n)?;

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

/// Build the `statistic_value` dense union with all `n` values in the `int64` branch.
fn build_value_union(
    union_fields: &UnionFields,
    int64_values: ArrayRef,
    n: usize,
) -> Result<ArrayRef> {
    let type_ids = ScalarBuffer::from(vec![INT64_BRANCH; n]);
    let offsets = ScalarBuffer::from((0..n as i32).collect::<Vec<_>>());
    let children: Vec<ArrayRef> = union_fields
        .iter()
        .map(|(id, f)| {
            if id == INT64_BRANCH {
                int64_values.clone()
            } else {
                new_empty_array(f.data_type())
            }
        })
        .collect();
    let union = UnionArray::try_new(union_fields.clone(), type_ids, Some(offsets), children)
        .map_err(arrow_err)?;
    Ok(Arc::new(union))
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
    fn empty_is_valid() {
        let batch = build(Vec::new(), GET_STATISTICS_SCHEMA.clone()).unwrap();
        assert_eq!(batch.schema(), GET_STATISTICS_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 1);
    }
}

//! Binding Arrow parameter data to Spanner statements.
//!
//! ADBC supplies statement parameters (and bulk-ingest rows) as an Arrow [`RecordBatch`]: each
//! column is one parameter, and each row is one set of bindings. Spanner uses **named** query
//! parameters (`@name`), so a bind column named `id` binds to `@id` in the SQL.
//!
//! Supported Arrow parameter types are `Int64`, `Float64`, `Float32`, `Boolean`, `Utf8`, `Binary`,
//! and their nulls. Other Arrow types are rejected with an `InvalidArguments` error.

use adbc_core::error::Result;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use google_cloud_spanner::statement::StatementBuilder;

use crate::error::invalid_argument;

/// Bind every column of `batch` at `row` as a named parameter on `builder`.
pub(crate) fn bind_row(
    mut builder: StatementBuilder,
    batch: &RecordBatch,
    row: usize,
) -> Result<StatementBuilder> {
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let name = field.name().as_str();
        let column = batch.column(i);
        let is_null = column.is_null(row);
        builder = match field.data_type() {
            DataType::Int64 => {
                let a = column.as_primitive::<Int64Type>();
                bind_scalar(builder, name, is_null, || a.value(row))
            }
            DataType::Float64 => {
                let a = column.as_primitive::<Float64Type>();
                bind_scalar(builder, name, is_null, || a.value(row))
            }
            DataType::Float32 => {
                let a = column.as_primitive::<Float32Type>();
                bind_scalar(builder, name, is_null, || a.value(row))
            }
            DataType::Boolean => {
                let a = column.as_boolean();
                bind_scalar(builder, name, is_null, || a.value(row))
            }
            DataType::Utf8 => {
                let a = column.as_string::<i32>();
                if is_null {
                    builder.add_param(name, &None::<String>)
                } else {
                    builder.add_param(name, &a.value(row))
                }
            }
            DataType::Binary => {
                let a = column.as_binary::<i32>();
                if is_null {
                    builder.add_param(name, &None::<Vec<u8>>)
                } else {
                    builder.add_param(name, &a.value(row).to_vec())
                }
            }
            other => {
                return Err(invalid_argument(format!(
                    "cannot bind parameter {name:?}: unsupported Arrow type {other:?}"
                )))
            }
        };
    }
    Ok(builder)
}

/// Bind a `Copy` scalar (or a typed null) as parameter `name`.
fn bind_scalar<T>(
    builder: StatementBuilder,
    name: &str,
    is_null: bool,
    value: impl FnOnce() -> T,
) -> StatementBuilder
where
    T: google_cloud_spanner::value::ToValue,
    Option<T>: google_cloud_spanner::value::ToValue,
{
    if is_null {
        builder.add_param(name, &None::<T>)
    } else {
        builder.add_param(name, &value())
    }
}

/// Build an `INSERT INTO <table> (<cols>) VALUES (@<cols>)` statement for bulk ingest.
pub(crate) fn insert_sql(table: &str, columns: &[String]) -> String {
    let names = columns.join(", ");
    let params = columns
        .iter()
        .map(|c| format!("@{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} ({names}) VALUES ({params})")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_insert_sql() {
        assert_eq!(
            insert_sql("Users", &["Id".to_string(), "Name".to_string()]),
            "INSERT INTO Users (Id, Name) VALUES (@Id, @Name)"
        );
        assert_eq!(
            insert_sql("T", &["a".to_string()]),
            "INSERT INTO T (a) VALUES (@a)"
        );
    }
}

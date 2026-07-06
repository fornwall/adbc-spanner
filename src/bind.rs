//! Binding Arrow parameter data to Spanner statements.
//!
//! ADBC supplies statement parameters (and bulk-ingest rows) as an Arrow [`RecordBatch`]: each
//! column is one parameter, and each row is one set of bindings. Spanner uses **named** query
//! parameters (`@name`), so a bind column named `id` binds to `@id` in the SQL.
//!
//! Supported Arrow parameter types are `Int64`, `Float64`, `Float32`, `Boolean`, `Utf8`, `Binary`,
//! `Date32` (→ `DATE`), `Timestamp(Microsecond)` (→ `TIMESTAMP`), `Decimal128` (→ `NUMERIC`), and
//! their nulls. Other Arrow types are rejected with an `InvalidArguments` error.
//!
//! Spanner encodes `DATE` / `TIMESTAMP` / `NUMERIC` values on the wire as strings, and query
//! parameters are sent untyped (Spanner infers the type from the SQL). So these three are formatted
//! straight to their Spanner string forms — `YYYY-MM-DD`, RFC 3339, and a plain decimal — which
//! keeps the full `Decimal128` (`i128`) range rather than routing through a narrower decimal type.

use adbc_core::error::Result;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int64Type, TimestampMicrosecondType,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
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
            DataType::Date32 => {
                let a = column.as_primitive::<Date32Type>();
                if is_null {
                    builder.add_param(name, &None::<String>)
                } else {
                    // Arrow `Date32` is days since the Unix epoch; Spanner wants `YYYY-MM-DD`.
                    let days = a.value(row);
                    let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_signed(Duration::days(i64::from(days)))
                        .ok_or_else(|| {
                            invalid_argument(format!(
                                "cannot bind DATE parameter {name:?}: {days} is out of range"
                            ))
                        })?;
                    builder.add_param(name, &date.format("%Y-%m-%d").to_string())
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let a = column.as_primitive::<TimestampMicrosecondType>();
                if is_null {
                    builder.add_param(name, &None::<String>)
                } else {
                    let micros = a.value(row);
                    let ts = DateTime::<Utc>::from_timestamp_micros(micros).ok_or_else(|| {
                        invalid_argument(format!(
                            "cannot bind TIMESTAMP parameter {name:?}: {micros} is out of range"
                        ))
                    })?;
                    builder.add_param(name, &ts.to_rfc3339_opts(SecondsFormat::Micros, true))
                }
            }
            DataType::Decimal128(_precision, scale) => {
                let a = column.as_primitive::<Decimal128Type>();
                if is_null {
                    builder.add_param(name, &None::<String>)
                } else {
                    let scale =
                        u32::try_from(*scale)
                            .ok()
                            .filter(|s| *s <= 38)
                            .ok_or_else(|| {
                                invalid_argument(format!(
                            "cannot bind NUMERIC parameter {name:?}: unsupported scale {scale}"
                        ))
                            })?;
                    // Format the full i128 directly; no narrower decimal type in the way.
                    builder.add_param(name, &numeric_string(a.value(row), scale))
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

/// Format an unscaled `Decimal128` value at the given scale as a plain decimal string, exact across
/// the whole `i128` range (which covers Spanner's `NUMERIC`). `scale` must be `<= 38`.
fn numeric_string(unscaled: i128, scale: u32) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    let negative = unscaled < 0;
    let magnitude = unscaled.unsigned_abs();
    let divisor = 10u128.pow(scale); // scale <= 38, so this fits in u128
    format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        magnitude / divisor,
        magnitude % divisor,
        width = scale as usize
    )
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
    use std::sync::Arc;

    use arrow_array::{Date32Array, Decimal128Array, Int32Array, TimestampMicrosecondArray};
    use arrow_schema::{Field, Schema};
    use google_cloud_spanner::statement::Statement;

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

    fn batch(fields: Vec<Field>, columns: Vec<arrow_array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn binds_date_timestamp_and_numeric() {
        // DATE, TIMESTAMP(µs) and a NUMERIC within range all bind without error.
        let b = batch(
            vec![
                Field::new("d", DataType::Date32, false),
                Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
                Field::new("n", DataType::Decimal128(38, 9), false),
            ],
            vec![
                Arc::new(Date32Array::from(vec![19737])), // 2024-01-15
                Arc::new(TimestampMicrosecondArray::from(vec![1_705_322_096_789_012])),
                Arc::new(
                    Decimal128Array::from(vec![1_500_000_000i128]) // 1.5 at scale 9
                        .with_precision_and_scale(38, 9)
                        .unwrap(),
                ),
            ],
        );
        assert!(bind_row(Statement::builder("SELECT @d, @t, @n"), &b, 0).is_ok());
    }

    #[test]
    fn null_temporal_and_numeric_bind() {
        let b = batch(
            vec![
                Field::new("d", DataType::Date32, true),
                Field::new("n", DataType::Decimal128(38, 9), true),
            ],
            vec![
                Arc::new(Date32Array::from(vec![None::<i32>])),
                Arc::new(
                    Decimal128Array::from(vec![None::<i128>])
                        .with_precision_and_scale(38, 9)
                        .unwrap(),
                ),
            ],
        );
        assert!(bind_row(Statement::builder("SELECT @d, @n"), &b, 0).is_ok());
    }

    #[test]
    fn numeric_string_formats_full_i128_range() {
        assert_eq!(numeric_string(1_500_000_000, 9), "1.500000000");
        assert_eq!(numeric_string(-2_250_000_000, 9), "-2.250000000");
        assert_eq!(numeric_string(1, 9), "0.000000001");
        assert_eq!(numeric_string(-1, 9), "-0.000000001");
        assert_eq!(numeric_string(150, 2), "1.50");
        assert_eq!(numeric_string(-7, 0), "-7");
        // 10^30 overflows a 96-bit decimal mantissa but is a valid Spanner NUMERIC; it must format
        // exactly, which is the whole point of formatting the i128 rather than a narrower type.
        assert_eq!(
            numeric_string(10i128.pow(30), 9),
            "1000000000000000000000.000000000"
        );
    }

    #[test]
    fn binds_numeric_beyond_a_96_bit_decimal() {
        // 10^30 (unscaled) overflows a 96-bit decimal but binds fine: we format the i128 directly.
        let b = batch(
            vec![Field::new("n", DataType::Decimal128(38, 9), false)],
            vec![Arc::new(
                Decimal128Array::from(vec![10i128.pow(30)])
                    .with_precision_and_scale(38, 9)
                    .unwrap(),
            )],
        );
        assert!(bind_row(Statement::builder("SELECT @n"), &b, 0).is_ok());
    }

    #[test]
    fn rejects_unsupported_arrow_type() {
        // A type with no mapping (Int32 here) is still rejected, unchanged by the new arms.
        let b = batch(
            vec![Field::new("x", DataType::Int32, false)],
            vec![Arc::new(Int32Array::from(vec![1]))],
        );
        assert!(bind_row(Statement::builder("SELECT @x"), &b, 0).is_err());
    }
}

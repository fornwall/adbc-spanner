//! Conversion from Spanner result sets to Arrow record batches.
//!
//! Spanner returns values over the wire in a JSON-ish protobuf encoding (see
//! [`google_cloud_spanner::value::Value`]): integers, dates, timestamps and numerics all arrive as
//! strings, floats as numbers, and so on. The column *types* are carried separately in the result
//! set metadata. We use that metadata to pick an Arrow [`DataType`] per column and then decode each
//! value accordingly.
//!
//! The type mapping is:
//!
//! | Spanner type                                | Arrow type                        |
//! |---------------------------------------------|-----------------------------------|
//! | `BOOL`                                      | `Boolean`                         |
//! | `INT64`                                     | `Int64`                           |
//! | `FLOAT64`                                   | `Float64`                         |
//! | `FLOAT32`                                   | `Float32`                         |
//! | `DATE`                                      | `Date32`                          |
//! | `TIMESTAMP`                                 | `Timestamp(Microsecond, "UTC")`   |
//! | `NUMERIC`                                   | `Decimal128(38, 9)`               |
//! | `BYTES`                                     | `Binary`                          |
//! | `STRING`/`JSON`/`UUID`/`INTERVAL`/`ENUM`    | `Utf8`                            |
//! | `ARRAY`/`STRUCT`                            | `Utf8` (JSON-encoded)             |
//!
//! Arrays and structs are still rendered as JSON text; mapping them to Arrow `List`/`Struct` is a
//! future improvement.

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use base64::Engine;
use google_cloud_spanner::result::{ResultSet, ResultSetMetadata, Row};
use google_cloud_spanner::value::{Kind, Type, TypeCode, Value};

use crate::error::{err, from_spanner};

/// Precision and scale of Spanner's `NUMERIC` type (GoogleSQL `NUMERIC` is fixed at 38 / 9).
const NUMERIC_PRECISION: u8 = 38;
const NUMERIC_SCALE: i8 = 9;
/// Spanner `TIMESTAMP` values are absolute instants in UTC.
const TIMESTAMP_TZ: &str = "UTC";

/// Drain a Spanner [`ResultSet`] and materialise it as a single Arrow [`RecordBatch`] together with
/// its schema.
pub(crate) async fn result_set_to_batch(mut rs: ResultSet) -> Result<(SchemaRef, RecordBatch)> {
    let mut rows: Vec<Row> = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row.map_err(from_spanner)?);
    }
    // Metadata (including the row type) is delivered with the first partial result set and retained
    // by the ResultSet, so it is available here even for empty results.
    let schema = build_schema(rs.metadata(), rows.first());
    let batch = build_batch(schema.clone(), &rows)?;
    Ok((schema, batch))
}

/// Build the Arrow schema for a result set from Spanner's column metadata, falling back to
/// all-`Utf8` columns inferred from the first row's width when metadata is unavailable.
pub(crate) fn build_schema(
    metadata: Option<&ResultSetMetadata>,
    first_row: Option<&Row>,
) -> SchemaRef {
    if let Some(md) = metadata {
        let names = md.column_names();
        if !names.is_empty() {
            let types = md.column_types();
            let fields: Vec<Field> = names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let data_type = types.get(i).map(arrow_type).unwrap_or(DataType::Utf8);
                    Field::new(name, data_type, true)
                })
                .collect();
            return Arc::new(Schema::new(fields));
        }
    }

    let width = first_row.map(|r| r.raw_values().len()).unwrap_or(0);
    let fields: Vec<Field> = (0..width)
        .map(|i| Field::new(format!("col{i}"), DataType::Utf8, true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Map a Spanner column [`Type`] to an Arrow [`DataType`].
fn arrow_type(ty: &Type) -> DataType {
    match ty.code() {
        TypeCode::Bool => DataType::Boolean,
        TypeCode::Int64 => DataType::Int64,
        TypeCode::Float64 => DataType::Float64,
        TypeCode::Float32 => DataType::Float32,
        TypeCode::Bytes => DataType::Binary,
        TypeCode::Date => DataType::Date32,
        TypeCode::Timestamp => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into()))
        }
        TypeCode::Numeric => DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
        // STRING, JSON, UUID, INTERVAL, ENUM, PROTO, ARRAY, STRUCT and any future/unknown code are
        // represented as (possibly JSON-encoded) UTF-8 text.
        _ => DataType::Utf8,
    }
}

fn build_batch(schema: SchemaRef, rows: &[Row]) -> Result<RecordBatch> {
    let mut builders: Vec<ColumnBuilder> = schema
        .fields()
        .iter()
        .map(|f| ColumnBuilder::new(f.data_type()))
        .collect();

    for row in rows {
        let values = row.raw_values();
        for (i, builder) in builders.iter_mut().enumerate() {
            builder.append(values.get(i))?;
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(ColumnBuilder::finish).collect();
    RecordBatch::try_new(schema, arrays).map_err(|e| {
        err(
            format!("failed to build record batch: {e}"),
            Status::Internal,
        )
    })
}

/// A typed Arrow array builder, one per result column.
enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Float32(Float32Builder),
    Date32(Date32Builder),
    TimestampMicros(TimestampMicrosecondBuilder),
    Decimal128(Decimal128Builder),
    Binary(BinaryBuilder),
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => ColumnBuilder::Bool(BooleanBuilder::new()),
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::new()),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::new()),
            DataType::Float32 => ColumnBuilder::Float32(Float32Builder::new()),
            DataType::Date32 => ColumnBuilder::Date32(Date32Builder::new()),
            // Carry the exact tz / precision-scale from the schema onto the builder so the finished
            // array's data type matches the field.
            DataType::Timestamp(TimeUnit::Microsecond, _) => ColumnBuilder::TimestampMicros(
                TimestampMicrosecondBuilder::new().with_data_type(data_type.clone()),
            ),
            DataType::Decimal128(_, _) => ColumnBuilder::Decimal128(
                Decimal128Builder::new().with_data_type(data_type.clone()),
            ),
            DataType::Binary => ColumnBuilder::Binary(BinaryBuilder::new()),
            _ => ColumnBuilder::Utf8(StringBuilder::new()),
        }
    }

    fn append(&mut self, value: Option<&Value>) -> Result<()> {
        let value = match value {
            Some(v) if v.kind() != Kind::Null => v,
            // Missing column or explicit NULL.
            _ => {
                self.append_null();
                return Ok(());
            }
        };

        match self {
            ColumnBuilder::Bool(b) => match value.try_as_bool() {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            ColumnBuilder::Int64(b) => match parse_int64(value) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            ColumnBuilder::Float64(b) => match parse_f64(value) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            ColumnBuilder::Float32(b) => match parse_f64(value) {
                Some(v) => b.append_value(v as f32),
                None => b.append_null(),
            },
            ColumnBuilder::Date32(b) => match value.try_as_string().and_then(parse_date_days) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            ColumnBuilder::TimestampMicros(b) => {
                match value.try_as_string().and_then(parse_timestamp_micros) {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            ColumnBuilder::Decimal128(b) => {
                match value.try_as_string().and_then(parse_numeric_i128) {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            ColumnBuilder::Binary(b) => match value.try_as_string() {
                // Spanner encodes BYTES as base64.
                Some(s) => match base64::engine::general_purpose::STANDARD.decode(s) {
                    Ok(bytes) => b.append_value(bytes),
                    Err(e) => {
                        return Err(err(
                            format!("failed to base64-decode BYTES value: {e}"),
                            Status::InvalidData,
                        ))
                    }
                },
                None => b.append_null(),
            },
            ColumnBuilder::Utf8(b) => match value.try_as_string() {
                Some(s) => b.append_value(s),
                // Non-string values in a text column (numbers, bools, arrays, structs) are rendered
                // as JSON.
                None => b.append_value(value_to_json(value).to_string()),
            },
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            ColumnBuilder::Bool(b) => b.append_null(),
            ColumnBuilder::Int64(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::Float32(b) => b.append_null(),
            ColumnBuilder::Date32(b) => b.append_null(),
            ColumnBuilder::TimestampMicros(b) => b.append_null(),
            ColumnBuilder::Decimal128(b) => b.append_null(),
            ColumnBuilder::Binary(b) => b.append_null(),
            ColumnBuilder::Utf8(b) => b.append_null(),
        }
    }

    fn finish(mut self) -> ArrayRef {
        match &mut self {
            ColumnBuilder::Bool(b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(b) => Arc::new(b.finish()),
            ColumnBuilder::Date32(b) => Arc::new(b.finish()),
            ColumnBuilder::TimestampMicros(b) => Arc::new(b.finish()),
            ColumnBuilder::Decimal128(b) => Arc::new(b.finish()),
            ColumnBuilder::Binary(b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(b) => Arc::new(b.finish()),
        }
    }
}

/// Parse a Spanner `INT64` value. Integers arrive as strings; we also accept a numeric encoding for
/// robustness.
fn parse_int64(value: &Value) -> Option<i64> {
    if let Some(s) = value.try_as_string() {
        return s.parse::<i64>().ok();
    }
    value.try_as_f64().map(|f| f as i64)
}

/// Parse a Spanner floating-point value. Finite values arrive as numbers; `NaN` and the infinities
/// arrive as strings.
fn parse_f64(value: &Value) -> Option<f64> {
    if let Some(f) = value.try_as_f64() {
        return Some(f);
    }
    match value.try_as_string()? {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        s => s.parse::<f64>().ok(),
    }
}

/// Parse a Spanner `DATE` (`YYYY-MM-DD`) into days since the Unix epoch (Arrow `Date32`).
fn parse_date_days(s: &str) -> Option<i32> {
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from((date - epoch).num_days()).ok()
}

/// Parse a Spanner `TIMESTAMP` (RFC 3339, e.g. `2024-01-15T12:34:56.789Z`) into microseconds since
/// the Unix epoch (Arrow `Timestamp(Microsecond)`).
fn parse_timestamp_micros(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
}

/// Parse a Spanner `NUMERIC` (decimal string) into an unscaled `i128` at scale 9 (Arrow
/// `Decimal128(38, 9)`). Returns `None` on malformed input or i128 overflow.
fn parse_numeric_i128(s: &str) -> Option<i128> {
    let s = s.trim();
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    // Pad/truncate the fractional part to the fixed scale of 9.
    let mut frac = String::with_capacity(NUMERIC_SCALE as usize);
    frac.push_str(&frac_part[..frac_part.len().min(NUMERIC_SCALE as usize)]);
    while frac.len() < NUMERIC_SCALE as usize {
        frac.push('0');
    }
    let int_val: i128 = if int_part.is_empty() {
        0
    } else {
        int_part.parse().ok()?
    };
    let frac_val: i128 = frac.parse().ok()?;
    let unscaled = int_val.checked_mul(1_000_000_000)?.checked_add(frac_val)?;
    Some(if negative { -unscaled } else { unscaled })
}

/// Recursively convert a Spanner [`Value`] into a [`serde_json::Value`], used to render arrays and
/// structs as text.
fn value_to_json(value: &Value) -> serde_json::Value {
    match value.kind() {
        Kind::Null => serde_json::Value::Null,
        Kind::Bool => serde_json::Value::Bool(value.as_bool()),
        Kind::Number => serde_json::json!(value.as_f64()),
        Kind::String => serde_json::Value::String(value.as_string().to_string()),
        Kind::List => serde_json::Value::Array(value.as_list().iter().map(value_to_json).collect()),
        Kind::Struct => serde_json::Value::Object(
            value
                .as_struct()
                .fields()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_to_epoch_days() {
        assert_eq!(parse_date_days("1970-01-01"), Some(0));
        assert_eq!(parse_date_days("1970-01-02"), Some(1));
        assert_eq!(parse_date_days("1969-12-31"), Some(-1));
        assert_eq!(parse_date_days("2024-01-15"), Some(19737));
        assert_eq!(parse_date_days("not-a-date"), None);
    }

    #[test]
    fn timestamps_to_epoch_micros() {
        assert_eq!(parse_timestamp_micros("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_timestamp_micros("1970-01-01T00:00:00.000001Z"),
            Some(1)
        );
        assert_eq!(
            parse_timestamp_micros("2024-01-15T12:34:56.789012Z"),
            Some(1_705_322_096_789_012)
        );
        // Sub-microsecond precision is truncated.
        assert_eq!(
            parse_timestamp_micros("1970-01-01T00:00:00.000000999Z"),
            Some(0)
        );
        assert_eq!(parse_timestamp_micros("nope"), None);
    }

    #[test]
    fn numerics_to_unscaled_i128() {
        assert_eq!(parse_numeric_i128("0"), Some(0));
        assert_eq!(parse_numeric_i128("1"), Some(1_000_000_000));
        assert_eq!(parse_numeric_i128("1.5"), Some(1_500_000_000));
        assert_eq!(parse_numeric_i128("-2.25"), Some(-2_250_000_000));
        assert_eq!(parse_numeric_i128("0.000000001"), Some(1));
        assert_eq!(parse_numeric_i128("+3"), Some(3_000_000_000));
        // More than 9 fractional digits: extra precision is truncated.
        assert_eq!(parse_numeric_i128("0.0000000019"), Some(1));
        assert_eq!(parse_numeric_i128("abc"), None);
        assert_eq!(parse_numeric_i128(""), None);
    }
}

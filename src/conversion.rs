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
//! | `ARRAY<T>`                                  | `List<T>`                         |
//! | `STRUCT<..>`                                | `Struct<..>`                      |
//!
//! `ARRAY` and `STRUCT` map to native Arrow `List`/`Struct` recursively, so nested shapes like
//! `ARRAY<STRUCT<..>>` round-trip with full type fidelity. (Struct field metadata comes from
//! [`Type::struct_type`](google_cloud_spanner::value::Type::struct_type).)

use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int64Array, ListArray, RecordBatch, RecordBatchReader, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use base64::Engine;
use google_cloud_spanner::result::{ResultSet, ResultSetMetadata, Row};
use google_cloud_spanner::value::{Kind, Type, TypeCode, Value};

use crate::error::{err, from_spanner};
use crate::runtime::{block_on_cancellable, CancelSignal, SharedRuntime};

/// Field name used for the element of an Arrow `List` (the Arrow convention).
const LIST_ITEM: &str = "item";

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

/// Pull up to `max` rows from a Spanner result set, stopping early when the stream ends.
async fn pull_chunk(rs: &mut ResultSet, max: usize) -> Result<Vec<Row>> {
    let mut rows = Vec::with_capacity(max);
    while rows.len() < max {
        match rs.next().await {
            Some(row) => rows.push(row.map_err(from_spanner)?),
            None => break,
        }
    }
    Ok(rows)
}

/// Wrap a Spanner [`ResultSet`] as a streaming Arrow [`RecordBatchReader`].
///
/// The first chunk of rows is pulled here (Spanner delivers the column metadata with the first
/// partial result set, so this also settles the schema), and the reader yields the rest lazily,
/// converting one bounded chunk to Arrow per [`Iterator::next`] rather than materialising the whole
/// result up front. Each chunk fetch is cancellable via the shared [`CancelSignal`].
pub(crate) async fn stream_query(
    runtime: SharedRuntime,
    cancel: CancelSignal,
    mut rs: ResultSet,
    batch_size: usize,
) -> Result<SpannerBatchReader> {
    let first = pull_chunk(&mut rs, batch_size).await?;
    let schema = build_schema(rs.metadata(), first.first());
    Ok(SpannerBatchReader {
        runtime,
        cancel,
        schema,
        result_set: Some(rs),
        first: Some(first),
        batch_size,
    })
}

/// A streaming [`RecordBatchReader`] over a Spanner [`ResultSet`].
///
/// Rows are fetched from the server and converted to Arrow in bounded chunks of `batch_size`, so a
/// large result set is never fully held in memory — at most one chunk of [`Row`]s plus the Arrow
/// batch built from it. The ADBC traits are synchronous, so each `next` bridges to the async client
/// with a cancellable `block_on`.
pub(crate) struct SpannerBatchReader {
    runtime: SharedRuntime,
    cancel: CancelSignal,
    schema: SchemaRef,
    /// The live result set; `None` once the stream is exhausted or a chunk fetch errored.
    result_set: Option<ResultSet>,
    /// The first chunk of rows, fetched up front to settle the schema; emitted on the first `next`.
    first: Option<Vec<Row>>,
    batch_size: usize,
}

/// Surface a driver error to a `RecordBatchReader` consumer, whose only error channel is
/// [`ArrowError`] (this preserves the message and, via the source chain, the ADBC status).
fn to_arrow_error(e: adbc_core::error::Error) -> ArrowError {
    ArrowError::ExternalError(Box::new(e))
}

impl Iterator for SpannerBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Emit the prefetched first chunk (which settled the schema) before pulling any more. This
        // also yields the single (possibly empty) batch of a small or empty result, matching the
        // one-batch shape callers previously saw.
        if let Some(rows) = self.first.take() {
            return Some(build_batch(self.schema.clone(), &rows).map_err(to_arrow_error));
        }
        let rs = self.result_set.as_mut()?;
        match block_on_cancellable(&self.runtime, &self.cancel, pull_chunk(rs, self.batch_size)) {
            // An empty chunk means the stream is drained; drop the result set and stop.
            Ok(rows) if rows.is_empty() => {
                self.result_set = None;
                None
            }
            Ok(rows) => Some(build_batch(self.schema.clone(), &rows).map_err(to_arrow_error)),
            Err(e) => {
                self.result_set = None;
                Some(Err(to_arrow_error(e)))
            }
        }
    }
}

impl RecordBatchReader for SpannerBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
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
        TypeCode::Struct => struct_arrow_type(ty),
        TypeCode::Array => match ty.array_element_type() {
            // ARRAY<T> → Arrow List<T> (recursively; T may itself be a STRUCT). Spanner does not
            // allow arrays of arrays; fall back to JSON text for anything unexpected.
            Some(element) if !matches!(element.code(), TypeCode::Array | TypeCode::Unspecified) => {
                DataType::List(Arc::new(Field::new(LIST_ITEM, arrow_type(&element), true)))
            }
            _ => DataType::Utf8,
        },
        // STRING, JSON, UUID, INTERVAL, ENUM, PROTO and any future/unknown code are UTF-8 text.
        _ => DataType::Utf8,
    }
}

/// Map a Spanner `STRUCT` type to an Arrow `Struct`, using the field names and types from the
/// result metadata. Falls back to `Utf8` if the struct type is somehow unavailable.
fn struct_arrow_type(ty: &Type) -> DataType {
    match ty.struct_type() {
        Some(st) => DataType::Struct(struct_fields(st)),
        None => DataType::Utf8,
    }
}

/// Build the Arrow child fields for a Spanner struct type (names verbatim, including empties/dups).
fn struct_fields(st: &google_cloud_spanner::model::StructType) -> Fields {
    st.fields
        .iter()
        .map(|f| {
            let field_type = f
                .r#type
                .as_deref()
                .cloned()
                .map(Type::from)
                .unwrap_or_default();
            Field::new(&f.name, arrow_type(&field_type), true)
        })
        .collect()
}

fn build_batch(schema: SchemaRef, rows: &[Row]) -> Result<RecordBatch> {
    // Collect values column-major, then build each column (recursively, for nested lists).
    let mut columns: Vec<Vec<Option<&Value>>> =
        vec![Vec::with_capacity(rows.len()); schema.fields().len()];
    for row in rows {
        let values = row.raw_values();
        for (i, column) in columns.iter_mut().enumerate() {
            column.push(values.get(i));
        }
    }

    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .zip(&columns)
        .map(|(field, values)| build_array(field.data_type(), values))
        .collect::<Result<_>>()?;

    RecordBatch::try_new(schema, arrays).map_err(|e| {
        err(
            format!("failed to build record batch: {e}"),
            Status::Internal,
        )
    })
}

/// Return the value unless it is a SQL `NULL` (or absent).
fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|v| v.kind() != Kind::Null)
}

fn arrow_err(e: ArrowError) -> adbc_core::error::Error {
    err(
        format!("failed to build Arrow array: {e}"),
        Status::Internal,
    )
}

/// Build an Arrow array of the given `data_type` from one Spanner value per row.
fn build_array(data_type: &DataType, values: &[Option<&Value>]) -> Result<ArrayRef> {
    Ok(match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from_iter(
            values
                .iter()
                .map(|&v| present(v).and_then(Value::try_as_bool)),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter(
            values.iter().map(|&v| present(v).and_then(parse_int64)),
        )),
        DataType::Float64 => Arc::new(Float64Array::from_iter(
            values.iter().map(|&v| present(v).and_then(parse_f64)),
        )),
        DataType::Float32 => Arc::new(Float32Array::from_iter(
            values
                .iter()
                .map(|&v| present(v).and_then(parse_f64).map(|f| f as f32)),
        )),
        DataType::Date32 => Arc::new(Date32Array::from_iter(values.iter().map(|&v| {
            present(v)
                .and_then(Value::try_as_string)
                .and_then(parse_date_days)
        }))),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let array = TimestampMicrosecondArray::from_iter(values.iter().map(|&v| {
                present(v)
                    .and_then(Value::try_as_string)
                    .and_then(parse_timestamp_micros)
            }));
            Arc::new(array.with_timezone_opt(tz.clone()))
        }
        DataType::Decimal128(precision, scale) => {
            let array = Decimal128Array::from_iter(values.iter().map(|&v| {
                present(v)
                    .and_then(Value::try_as_string)
                    .and_then(parse_numeric_i128)
            }));
            Arc::new(
                array
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(arrow_err)?,
            )
        }
        // Spanner encodes BYTES as base64.
        DataType::Binary => Arc::new(BinaryArray::from_iter(values.iter().map(|&v| {
            present(v)
                .and_then(Value::try_as_string)
                .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
        }))),
        DataType::List(field) => build_list(field, values)?,
        DataType::Struct(fields) => build_struct(fields, values)?,
        // Utf8 and every fallback (JSON, …): keep strings verbatim, render anything else (numbers,
        // bools, nested values) as JSON text.
        _ => Arc::new(StringArray::from_iter(values.iter().map(|&v| {
            present(v).map(|x| {
                x.try_as_string()
                    .map(str::to_string)
                    .unwrap_or_else(|| value_to_json(x).to_string())
            })
        }))),
    })
}

/// Build an Arrow `List` array: each Spanner value is a list (or null) of the element type.
fn build_list(field: &FieldRef, values: &[Option<&Value>]) -> Result<ArrayRef> {
    let mut children: Vec<Option<&Value>> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(values.len());
    for value in values {
        match value {
            Some(v) if v.kind() == Kind::List => {
                children.extend(v.as_list().iter().map(Some));
                validity.push(true);
            }
            _ => validity.push(false),
        }
        offsets.push(children.len() as i32);
    }
    let child = build_array(field.data_type(), &children)?;
    let list = ListArray::try_new(
        field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        Some(NullBuffer::from(validity)),
    )
    .map_err(arrow_err)?;
    Ok(Arc::new(list))
}

/// Build an Arrow `Struct` array. Spanner encodes struct values positionally (a `ListValue` whose
/// elements match the struct's field order); a value delivered as a keyed struct is handled too.
fn build_struct(fields: &Fields, values: &[Option<&Value>]) -> Result<ArrayRef> {
    let mut children: Vec<Vec<Option<&Value>>> =
        vec![Vec::with_capacity(values.len()); fields.len()];
    let mut validity: Vec<bool> = Vec::with_capacity(values.len());
    for value in values {
        match value {
            Some(v) if v.kind() == Kind::List => {
                let list = v.as_list();
                for (i, child) in children.iter_mut().enumerate() {
                    child.push(list.get(i));
                }
                validity.push(true);
            }
            Some(v) if v.kind() == Kind::Struct => {
                let s = v.as_struct();
                for (field, child) in fields.iter().zip(children.iter_mut()) {
                    child.push(s.get(field.name()));
                }
                validity.push(true);
            }
            _ => {
                children.iter_mut().for_each(|child| child.push(None));
                validity.push(false);
            }
        }
    }
    let arrays = fields
        .iter()
        .zip(&children)
        .map(|(field, vals)| build_array(field.data_type(), vals))
        .collect::<Result<Vec<_>>>()?;
    let array = StructArray::try_new(fields.clone(), arrays, Some(NullBuffer::from(validity)))
        .map_err(arrow_err)?;
    Ok(Arc::new(array))
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
pub(crate) fn parse_date_days(s: &str) -> Option<i32> {
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from((date - epoch).num_days()).ok()
}

/// Parse a Spanner `TIMESTAMP` (RFC 3339, e.g. `2024-01-15T12:34:56.789Z`) into microseconds since
/// the Unix epoch (Arrow `Timestamp(Microsecond)`).
pub(crate) fn parse_timestamp_micros(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
}

/// Parse a Spanner `NUMERIC` (decimal string) into an unscaled `i128` at scale 9 (Arrow
/// `Decimal128(38, 9)`). Returns `None` on malformed input or i128 overflow.
pub(crate) fn parse_numeric_i128(s: &str) -> Option<i128> {
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

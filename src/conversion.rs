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
//! | `TIMESTAMP`                                 | `Timestamp(Nanosecond, "UTC")`    |
//! | `NUMERIC`                                   | `Decimal128(38, 9)`               |
//! | `BYTES`                                     | `Binary`                          |
//! | `STRING`/`UUID`/`INTERVAL`/`ENUM`/`PROTO`   | `Utf8`                            |
//! | `JSON`                                      | `Utf8` + `arrow.json` extension   |
//! | `ARRAY<T>`                                  | `List<T>`                         |
//! | `STRUCT<..>`                                | `Struct<..>`                      |
//!
//! `ARRAY` and `STRUCT` map to native Arrow `List`/`Struct` recursively, so nested shapes like
//! `ARRAY<STRUCT<..>>` round-trip with full type fidelity. (Struct field metadata comes from
//! [`Type::struct_type`](google_cloud_spanner::value::Type::struct_type).)
//!
//! `JSON` columns keep `Utf8` storage (the value bytes are the JSON text) but carry the canonical
//! `arrow.json` extension type as field metadata (`ARROW:extension:name` = `arrow.json`), so Arrow
//! consumers that understand the extension recognize the logical JSON type. The extension lives on
//! the [`Field`], not the [`DataType`]; for `ARRAY<JSON>` it sits on the list's child (`item`)
//! field. The other Utf8-backed codes stay plain, untagged `Utf8`.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::{Result, Status};
use arrow_array::builder::{BinaryBuilder, BooleanBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int64Type,
};
use arrow_array::{
    ArrayRef, ListArray, PrimitiveArray, RecordBatch, RecordBatchReader, StructArray,
    TimestampNanosecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use base64::Engine;
use google_cloud_spanner::result::{ResultSet, ResultSetMetadata, Row};
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::MultiUseReadOnlyTransaction;
use google_cloud_spanner::value::{Kind, Type, TypeCode, Value};

use crate::error::{err, from_spanner, invalid_argument};
use crate::runtime::{block_on_cancellable, CancelSignal, SharedRuntime};

/// Field name used for the element of an Arrow `List` (the Arrow convention).
const LIST_ITEM: &str = "item";

/// Precision and scale of Spanner's `NUMERIC` type (GoogleSQL `NUMERIC` is fixed at 38 / 9).
const NUMERIC_PRECISION: u8 = 38;
const NUMERIC_SCALE: i8 = 9;
/// Spanner `TIMESTAMP` values are absolute instants in UTC.
const TIMESTAMP_TZ: &str = "UTC";

/// Arrow field-metadata key naming a canonical extension type.
const ARROW_EXTENSION_NAME: &str = "ARROW:extension:name";
/// Arrow field-metadata key carrying an extension type's serialized parameters.
const ARROW_EXTENSION_METADATA: &str = "ARROW:extension:metadata";
/// Canonical Arrow extension name for JSON stored as a Utf8 (string) column.
const ARROW_JSON_EXTENSION: &str = "arrow.json";

/// Drain a Spanner [`ResultSet`] and materialise it as a single Arrow [`RecordBatch`] together with
/// its schema.
pub(crate) async fn result_set_to_batch(mut rs: ResultSet) -> Result<(SchemaRef, RecordBatch)> {
    let mut rows: Vec<Row> = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row.map_err(from_spanner)?);
    }
    // Metadata (including the row type) is delivered with the first partial result set and retained
    // by the ResultSet, so it is available here even for empty results.
    rows_to_batch(rs.metadata(), &rows)
}

/// Materialise already-drained Spanner rows (plus their result-set metadata) as a single Arrow
/// [`RecordBatch`] together with its schema. Used where the rows had to be drained inside a
/// read/write transaction runner (whose closure must keep the client's error type for abort/retry
/// detection) and are converted afterwards — e.g. DML with `THEN RETURN`.
pub(crate) fn rows_to_batch(
    metadata: Option<&ResultSetMetadata>,
    rows: &[Row],
) -> Result<(SchemaRef, RecordBatch)> {
    let schema = build_schema(metadata, rows.first());
    let batch = build_batch(schema.clone(), rows)?;
    Ok((schema, batch))
}

/// Additional per-chunk byte budget for [`pull_chunk`], on top of the `max` (row-count) cap.
///
/// The row cap alone bounds rows, not bytes: 8192 rows of `STRING(MAX)`/`BYTES(MAX)` (up to ~10 MB
/// each) would be tens of GB per chunk, and a chunk is held roughly twice — the [`Row`]s plus the
/// Arrow batch built from them — during conversion. So `pull_chunk` also cuts a chunk once its
/// accumulated (approximate) wire size crosses this budget. 32 MiB sits in the middle of the
/// 16–64 MB range: large enough that ordinary rows still batch efficiently, small enough to cap
/// peak memory. A single row larger than the whole budget still forms its own one-row chunk (the
/// check runs *after* the row is buffered), so streaming never stalls or emits an empty chunk.
const CHUNK_BYTE_BUDGET: usize = 32 * 1024 * 1024;

/// Pull up to `max` rows from a Spanner result set, stopping early when the stream ends — or, as an
/// additional cap, once the accumulated rows exceed [`CHUNK_BYTE_BUDGET`] approximate bytes.
async fn pull_chunk(rs: &mut ResultSet, max: usize) -> Result<Vec<Row>> {
    let mut rows = Vec::with_capacity(max);
    let mut bytes: usize = 0;
    while rows.len() < max {
        match rs.next().await {
            Some(row) => {
                let row = row.map_err(from_spanner)?;
                // Approximate the row's wire size from the values already in hand (see
                // `approx_row_bytes`); base64 BYTES over-estimate the decoded size, which only makes
                // the budget slightly more conservative. This is a rough estimate, not exact.
                bytes = bytes.saturating_add(approx_row_bytes(&row));
                rows.push(row);
                // The row is already buffered, so an oversized single row still yields a one-row
                // chunk rather than looping forever or producing an empty chunk.
                if bytes >= CHUNK_BYTE_BUDGET {
                    break;
                }
            }
            None => break,
        }
    }
    Ok(rows)
}

/// Roughly estimate a row's byte size from its Spanner values, used only to drive the
/// [`CHUNK_BYTE_BUDGET`] early-cut — never for correctness. It sums the string lengths of the
/// values (recursively through lists and structs); scalars count as a few bytes each.
fn approx_row_bytes(row: &Row) -> usize {
    row.raw_values().iter().map(approx_value_bytes).sum()
}

/// Approximate the byte size of a single Spanner [`Value`] (see [`approx_row_bytes`]). Strings —
/// which is how Spanner ships `STRING`, `BYTES` (base64), `INT64`, `NUMERIC`, `DATE`, `TIMESTAMP`,
/// `JSON`, … over the wire — count their UTF-8 length; nested lists/structs recurse; other scalars
/// count as a small fixed size. Deliberately cheap and approximate.
fn approx_value_bytes(value: &Value) -> usize {
    match value.kind() {
        Kind::Null => 0,
        Kind::Bool => 1,
        Kind::Number => 8,
        Kind::String => value.as_string().len(),
        Kind::List => value.as_list().iter().map(approx_value_bytes).sum(),
        Kind::Struct => value
            .as_struct()
            .fields()
            .map(|(_, v)| approx_value_bytes(v))
            .sum(),
    }
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

/// Wrap a sequence of per-bound-row query statements as one streaming Arrow
/// [`RecordBatchReader`], executing every statement inside the same **multi-use read-only
/// transaction** so all bound rows see a single, mutually consistent snapshot.
///
/// The first statement is executed here and its first chunk pulled (settling the schema — every
/// statement is the same SQL, so the schema is shared); the reader then streams the remaining
/// chunks and statements lazily, executing each subsequent statement only once its predecessor's
/// result set drains. Like [`stream_query`], rows are converted to Arrow in bounded chunks of
/// `batch_size` (plus the [`CHUNK_BYTE_BUDGET`]), so the concatenated result is never fully
/// materialised. The reader owns `transaction`, keeping the snapshot alive for as long as it is
/// iterated; Spanner read-only transactions need no commit/rollback, so dropping it is cleanup
/// enough.
pub(crate) async fn stream_bound_query(
    runtime: SharedRuntime,
    cancel: CancelSignal,
    transaction: MultiUseReadOnlyTransaction,
    statements: Vec<SpannerSql>,
    batch_size: usize,
) -> Result<BoundQueryBatchReader> {
    let mut statements = statements.into_iter();
    let mut result_set = match statements.next() {
        Some(statement) => Some(
            transaction
                .execute_query(statement)
                .await
                .map_err(from_spanner)?,
        ),
        // No statements at all: an empty reader with an empty schema.
        None => None,
    };
    let (first, schema) = match result_set.as_mut() {
        Some(rs) => {
            let rows = pull_chunk(rs, batch_size).await?;
            let schema = build_schema(rs.metadata(), rows.first());
            (Some(rows), schema)
        }
        None => (None, Arc::new(Schema::empty())),
    };
    Ok(BoundQueryBatchReader {
        runtime,
        cancel,
        schema,
        transaction,
        statements,
        result_set,
        first,
        batch_size,
    })
}

/// A streaming [`RecordBatchReader`] over the successive result sets of a bound (parameterized)
/// query — one execution per bound row, all inside one shared read-only snapshot. See
/// [`stream_bound_query`].
pub(crate) struct BoundQueryBatchReader {
    runtime: SharedRuntime,
    cancel: CancelSignal,
    schema: SchemaRef,
    /// The shared snapshot every statement executes in; owned so it outlives lazy iteration.
    transaction: MultiUseReadOnlyTransaction,
    /// The not-yet-executed per-bound-row statements.
    statements: std::vec::IntoIter<SpannerSql>,
    /// The live result set of the statement currently being drained, if any.
    result_set: Option<ResultSet>,
    /// The first chunk of rows, fetched up front to settle the schema; emitted on the first `next`.
    first: Option<Vec<Row>>,
    batch_size: usize,
}

/// Pull the next non-empty chunk for [`BoundQueryBatchReader`]: drain the current result set in
/// bounded chunks, and when it ends, execute the next statement in the same `transaction` —
/// looping so a bound row with an empty result never surfaces as a spurious empty batch. `None`
/// means everything is drained.
async fn next_bound_chunk(
    transaction: &MultiUseReadOnlyTransaction,
    statements: &mut std::vec::IntoIter<SpannerSql>,
    result_set: &mut Option<ResultSet>,
    batch_size: usize,
) -> Result<Option<Vec<Row>>> {
    loop {
        if let Some(rs) = result_set.as_mut() {
            let rows = pull_chunk(rs, batch_size).await?;
            if !rows.is_empty() {
                return Ok(Some(rows));
            }
            *result_set = None;
        }
        match statements.next() {
            Some(statement) => {
                *result_set = Some(
                    transaction
                        .execute_query(statement)
                        .await
                        .map_err(from_spanner)?,
                );
            }
            None => return Ok(None),
        }
    }
}

impl Iterator for BoundQueryBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Emit the prefetched first chunk (which settled the schema) before pulling any more; as
        // in `SpannerBatchReader`, this also yields the single (possibly empty) batch of a small
        // or empty result.
        if let Some(rows) = self.first.take() {
            return Some(build_batch(self.schema.clone(), &rows).map_err(to_arrow_error));
        }
        let Self {
            runtime,
            cancel,
            transaction,
            statements,
            result_set,
            batch_size,
            ..
        } = self;
        match block_on_cancellable(
            runtime,
            cancel,
            next_bound_chunk(transaction, statements, result_set, *batch_size),
        ) {
            Ok(None) => None,
            Ok(Some(rows)) => Some(build_batch(self.schema.clone(), &rows).map_err(to_arrow_error)),
            Err(e) => {
                // Stop after surfacing the error: drop the live result set and any statements
                // still pending.
                self.result_set = None;
                self.statements = Vec::new().into_iter();
                Some(Err(to_arrow_error(e)))
            }
        }
    }
}

impl RecordBatchReader for BoundQueryBatchReader {
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
                .map(|(i, name)| match types.get(i) {
                    Some(ty) => arrow_field(name, ty, true),
                    None => Field::new(name, DataType::Utf8, true),
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

/// Build an Arrow [`Field`] for a Spanner column [`Type`], attaching the canonical `arrow.json`
/// extension metadata when the column is `JSON`.
///
/// The storage type stays `Utf8` (the value bytes are the JSON text); only the field metadata marks
/// it as logical JSON, so consumers that understand the extension (pyarrow, DuckDB, polars) can
/// recognize it while others still read plain strings. Only `TypeCode::Json` is tagged — the other
/// Utf8-backed codes (`STRING`, `UUID`, `INTERVAL`, `ENUM`, `PROTO`) stay untagged.
fn arrow_field(name: impl Into<String>, ty: &Type, nullable: bool) -> Field {
    let field = Field::new(name, arrow_type(ty), nullable);
    if ty.code() == TypeCode::Json {
        field.with_metadata(json_extension_metadata())
    } else {
        field
    }
}

/// The two `ARROW:extension:*` field-metadata keys that mark a Utf8 column as canonical `arrow.json`.
/// The metadata value is the empty string, which is valid (and conventional) for `arrow.json`.
fn json_extension_metadata() -> HashMap<String, String> {
    HashMap::from([
        (
            ARROW_EXTENSION_NAME.to_string(),
            ARROW_JSON_EXTENSION.to_string(),
        ),
        (ARROW_EXTENSION_METADATA.to_string(), String::new()),
    ])
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
        TypeCode::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into())),
        TypeCode::Numeric => DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
        TypeCode::Struct => struct_arrow_type(ty),
        TypeCode::Array => match ty.array_element_type() {
            // ARRAY<T> → Arrow List<T> (recursively; T may itself be a STRUCT). The element field is
            // built via `arrow_field`, so an `ARRAY<JSON>` carries the `arrow.json` extension on the
            // list's child (`item`) field, not the top-level List. Spanner does not allow arrays of
            // arrays; fall back to JSON text for anything unexpected.
            Some(element) if !matches!(element.code(), TypeCode::Array | TypeCode::Unspecified) => {
                DataType::List(Arc::new(arrow_field(LIST_ITEM, &element, true)))
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
            arrow_field(&f.name, &field_type, true)
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

/// The error for a present (non-NULL) wire value that cannot be decoded as its column's Spanner
/// type. Decoding must fail loudly: mapping an undecodable value to NULL would silently corrupt
/// data (a value the caller cannot distinguish from a genuine SQL NULL).
fn decode_error(spanner_type: &str, value: &Value) -> adbc_core::error::Error {
    err(
        format!(
            "cannot decode Spanner {spanner_type} wire value {}",
            value_to_json(value)
        ),
        Status::InvalidData,
    )
}

/// Build a primitive Arrow array from one Spanner value per row: SQL NULLs become null slots, and
/// a present value that `parse` cannot decode is an error (see [`decode_error`]), never a null.
fn build_primitive<T: ArrowPrimitiveType>(
    values: &[Option<&Value>],
    spanner_type: &str,
    parse: impl Fn(&Value) -> Option<T::Native>,
) -> Result<PrimitiveArray<T>> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
    for &value in values {
        match present(value) {
            None => builder.append_null(),
            Some(v) => builder.append_value(parse(v).ok_or_else(|| decode_error(spanner_type, v))?),
        }
    }
    Ok(builder.finish())
}

/// Build an Arrow array of the given `data_type` from one Spanner value per row.
///
/// SQL NULLs map to null slots. A present value that cannot be decoded as the column's type is an
/// **error**, not a null — every typed arm goes through [`build_primitive`]/[`decode_error`], so a
/// wire-format surprise cannot silently masquerade as a SQL NULL.
///
/// `pub(crate)` (rather than private) only so `crate::bench_support` can expose it to `benches/`.
pub(crate) fn build_array(data_type: &DataType, values: &[Option<&Value>]) -> Result<ArrayRef> {
    Ok(match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for &value in values {
                match present(value) {
                    None => builder.append_null(),
                    Some(v) => builder
                        .append_value(v.try_as_bool().ok_or_else(|| decode_error("BOOL", v))?),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => Arc::new(build_primitive::<Int64Type>(values, "INT64", parse_int64)?),
        DataType::Float64 => Arc::new(build_primitive::<Float64Type>(
            values, "FLOAT64", parse_f64,
        )?),
        DataType::Float32 => Arc::new(build_primitive::<Float32Type>(values, "FLOAT32", |v| {
            parse_f64(v).map(|f| f as f32)
        })?),
        DataType::Date32 => Arc::new(build_primitive::<Date32Type>(values, "DATE", |v| {
            v.try_as_string().and_then(parse_date_days)
        })?),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            // A genuine SQL NULL (or absent value) becomes a null slot. A present value errors if
            // it is not a timestamp string at all, or — since Arrow stores nanoseconds as an
            // `i64` — if it is a valid instant outside the representable range.
            let mut builder = TimestampNanosecondArray::builder(values.len());
            for &value in values {
                match present(value) {
                    None => builder.append_null(),
                    Some(v) => {
                        let s = v
                            .try_as_string()
                            .ok_or_else(|| decode_error("TIMESTAMP", v))?;
                        builder.append_value(parse_timestamp_nanos(s).ok_or_else(|| {
                            if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                                invalid_argument(format!(
                                    "TIMESTAMP value {s:?} is outside the range representable as \
                                     an Arrow Timestamp(Nanosecond) (~1677-09-21 to 2262-04-11)"
                                ))
                            } else {
                                decode_error("TIMESTAMP", v)
                            }
                        })?)
                    }
                }
            }
            Arc::new(builder.finish().with_timezone_opt(tz.clone()))
        }
        DataType::Decimal128(precision, scale) => {
            let array = build_primitive::<Decimal128Type>(values, "NUMERIC", |v| {
                v.try_as_string().and_then(parse_numeric_i128)
            })?;
            Arc::new(
                array
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(arrow_err)?,
            )
        }
        // Spanner encodes BYTES as base64.
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for &value in values {
                match present(value) {
                    None => builder.append_null(),
                    Some(v) => builder.append_value(
                        v.try_as_string()
                            .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
                            .ok_or_else(|| decode_error("BYTES", v))?,
                    ),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::List(field) => build_list(field, values)?,
        DataType::Struct(fields) => build_struct(fields, values)?,
        // Utf8 and every fallback (JSON, …): keep strings verbatim, render anything else (numbers,
        // bools, nested values) as JSON text.
        _ => {
            let mut builder = StringBuilder::new();
            for &value in values {
                match present(value) {
                    None => builder.append_null(),
                    // Append the string slice directly (no per-value owned String); only the
                    // JSON-render fallback allocates, and only for non-string values.
                    Some(x) => match x.try_as_string() {
                        Some(s) => builder.append_value(s),
                        None => builder.append_value(value_to_json(x).to_string()),
                    },
                }
            }
            Arc::new(builder.finish())
        }
    })
}

/// Build an Arrow `List` array: each Spanner value is a list (or null) of the element type.
///
/// The strict-decode policy of the scalar arms applies here too: a SQL NULL (or absent value)
/// becomes a null slot, but a *present* value that is not a wire list is an error (see
/// [`decode_error`]), never a silent null. Elements recurse through [`build_array`], so an
/// undecodable element — at any nesting depth — errors as well.
fn build_list(field: &FieldRef, values: &[Option<&Value>]) -> Result<ArrayRef> {
    let mut children: Vec<Option<&Value>> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(values.len());
    for &value in values {
        match present(value) {
            None => validity.push(false),
            Some(v) if v.kind() == Kind::List => {
                children.extend(v.as_list().iter().map(Some));
                validity.push(true);
            }
            Some(v) => return Err(decode_error("ARRAY", v)),
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
///
/// The strict-decode policy of the scalar arms applies here too: a SQL NULL (or absent value)
/// becomes a null slot, but a *present* value that is neither a wire list nor a keyed struct is an
/// error (see [`decode_error`]), never a silent null. Field values recurse through
/// [`build_array`], so an undecodable field — at any nesting depth — errors as well.
fn build_struct(fields: &Fields, values: &[Option<&Value>]) -> Result<ArrayRef> {
    let mut children: Vec<Vec<Option<&Value>>> =
        vec![Vec::with_capacity(values.len()); fields.len()];
    let mut validity: Vec<bool> = Vec::with_capacity(values.len());
    for &value in values {
        match present(value) {
            None => {
                children.iter_mut().for_each(|child| child.push(None));
                validity.push(false);
            }
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
            Some(v) => return Err(decode_error("STRUCT", v)),
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

/// Parse a Spanner `INT64` value. Integers always arrive as strings (Spanner encodes `INT64` as a
/// JSON string precisely so magnitudes above 2^53 survive), so we only accept the string form. We
/// deliberately do **not** fall back to a JSON number: an `f64` cannot represent every `i64`, and
/// casting one to `i64` would silently round values above 2^53. A non-string (or non-integer) wire
/// value is therefore a loud decode error rather than a truncated result.
fn parse_int64(value: &Value) -> Option<i64> {
    value.try_as_string()?.parse::<i64>().ok()
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

/// Parse a Spanner `TIMESTAMP` (RFC 3339, e.g. `2024-01-15T12:34:56.789012345Z`) into nanoseconds
/// since the Unix epoch (Arrow `Timestamp(Nanosecond)`), preserving full sub-microsecond precision.
///
/// Returns `None` for a malformed string, and — because Arrow stores nanoseconds as an `i64` — for
/// any otherwise-valid instant outside the representable range (~1677-09-21 to 2262-04-11), via
/// chrono's non-panicking [`DateTime::timestamp_nanos_opt`].
pub(crate) fn parse_timestamp_nanos(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .and_then(|dt| dt.timestamp_nanos_opt())
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
    use arrow_array::{Array, StringArray};
    use google_cloud_spanner::value::ToValue;

    /// The `arrow.json` extension name attached to a Field's metadata, if any.
    fn extension_name(field: &Field) -> Option<&str> {
        field
            .metadata()
            .get(ARROW_EXTENSION_NAME)
            .map(String::as_str)
    }

    #[test]
    fn json_column_is_tagged_arrow_json() {
        let field = arrow_field("payload", &google_cloud_spanner::types::json(), true);
        // Storage type stays Utf8; only the field metadata marks it as logical JSON.
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(extension_name(&field), Some(ARROW_JSON_EXTENSION));
        // Both canonical extension keys are present (metadata value is the empty string).
        assert_eq!(
            field
                .metadata()
                .get(ARROW_EXTENSION_METADATA)
                .map(String::as_str),
            Some("")
        );
    }

    #[test]
    fn string_column_is_not_tagged() {
        // Guards against over-tagging: plain STRING must stay untagged Utf8.
        let field = arrow_field("name", &google_cloud_spanner::types::string(), true);
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(extension_name(&field), None);
        assert!(field.metadata().is_empty());
    }

    #[test]
    fn array_of_json_tags_the_item_field() {
        let element = google_cloud_spanner::types::json();
        let field = arrow_field("tags", &google_cloud_spanner::types::array(element), true);
        // Top-level List field carries no extension; its child (`item`) field does.
        assert_eq!(extension_name(&field), None);
        let DataType::List(item) = field.data_type() else {
            panic!("expected a List data type, got {:?}", field.data_type());
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert_eq!(extension_name(item), Some(ARROW_JSON_EXTENSION));
    }

    #[test]
    fn json_value_round_trips_as_utf8_text() {
        // The value path is unchanged: JSON text is kept verbatim as a Utf8 string.
        let text = r#"{"a":1,"b":[true,null]}"#;
        let value = text.to_value();
        let array = build_array(&DataType::Utf8, &[Some(&value)]).unwrap();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.len(), 1);
        assert_eq!(strings.value(0), text);
    }

    #[test]
    fn string_array_round_trips_values_and_nulls() {
        // Built via StringBuilder (no per-value owned String on the string path). Both a SQL NULL
        // value and a missing slot become null; present strings are kept verbatim, incl. empty.
        let hello = "hello".to_value();
        let empty = "".to_value();
        let unicode = "naïve café — 日本語".to_value();
        let sql_null = None::<&str>.to_value();

        let array = build_array(
            &DataType::Utf8,
            &[
                Some(&hello),
                Some(&empty),
                Some(&unicode),
                Some(&sql_null),
                None,
            ],
        )
        .unwrap();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.len(), 5);
        assert_eq!(strings.value(0), "hello");
        assert_eq!(strings.value(1), "");
        assert!(!strings.is_null(1)); // present empty string, not a null slot
        assert_eq!(strings.value(2), "naïve café — 日本語");
        assert!(strings.is_null(3)); // SQL NULL value
        assert!(strings.is_null(4)); // missing slot
    }

    #[test]
    fn dates_to_epoch_days() {
        assert_eq!(parse_date_days("1970-01-01"), Some(0));
        assert_eq!(parse_date_days("1970-01-02"), Some(1));
        assert_eq!(parse_date_days("1969-12-31"), Some(-1));
        assert_eq!(parse_date_days("2024-01-15"), Some(19737));
        assert_eq!(parse_date_days("not-a-date"), None);
    }

    #[test]
    fn timestamps_to_epoch_nanos() {
        assert_eq!(parse_timestamp_nanos("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_timestamp_nanos("1970-01-01T00:00:00.000001Z"),
            Some(1_000)
        );
        assert_eq!(
            parse_timestamp_nanos("2024-01-15T12:34:56.789012Z"),
            Some(1_705_322_096_789_012_000)
        );
        // Sub-microsecond precision is preserved (not truncated): 999 nanoseconds stays 999.
        assert_eq!(
            parse_timestamp_nanos("1970-01-01T00:00:00.000000999Z"),
            Some(999)
        );
        // Full nine fractional digits round-trip.
        assert_eq!(
            parse_timestamp_nanos("2024-01-15T12:34:56.789012345Z"),
            Some(1_705_322_096_789_012_345)
        );
        assert_eq!(parse_timestamp_nanos("nope"), None);
        // Outside the i64-nanosecond range (~1677-09-21 to 2262-04-11): no representation, so None
        // rather than a wrapped/panicking value.
        assert_eq!(parse_timestamp_nanos("3000-01-01T00:00:00Z"), None);
        assert_eq!(parse_timestamp_nanos("1000-01-01T00:00:00Z"), None);
    }

    #[test]
    fn timestamp_array_preserves_nulls_and_errors_out_of_range() {
        use arrow_array::Array;
        use google_cloud_spanner::value::ToValue;
        let ty = DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into()));
        let null = None::<&str>.to_value();
        let in_range = "1970-01-01T00:00:00.000000999Z".to_value();
        let out_of_range = "3000-01-01T00:00:00Z".to_value();

        // A SQL NULL maps to a null slot; a present in-range value keeps full nanosecond precision.
        let array = build_array(&ty, &[Some(&null), Some(&in_range), None]).unwrap();
        let ts = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert!(ts.is_null(0));
        assert_eq!(ts.value(1), 999);
        assert!(ts.is_null(2));

        // A present but out-of-range value is a real timestamp we cannot encode: it must error,
        // not become a silent null.
        let err = build_array(&ty, &[Some(&out_of_range)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("3000-01-01T00:00:00Z"));
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

    /// The chunk byte-budget estimate: strings count their UTF-8 length, SQL NULLs contribute
    /// nothing, lists recurse, and a single wide value dominates (so it drives the one-row early
    /// cut in `pull_chunk`). The estimate is approximate — only its rough magnitude matters.
    #[test]
    fn approx_value_bytes_sums_string_lengths() {
        assert_eq!(approx_value_bytes(&"hello".to_value()), 5);
        assert_eq!(approx_value_bytes(&"".to_value()), 0);
        // A SQL NULL contributes nothing to the budget.
        assert_eq!(approx_value_bytes(&None::<&str>.to_value()), 0);
        // Lists recurse over their elements.
        assert_eq!(approx_value_bytes(&vec!["a", "bb", "ccc"].to_value()), 6);
        // A single wide value dominates the estimate — this is what makes an oversized row cut the
        // chunk after one row.
        let wide = "x".repeat(2 * CHUNK_BYTE_BUDGET);
        assert!(approx_value_bytes(&wide.as_str().to_value()) >= CHUNK_BYTE_BUDGET);
    }

    /// A present wire value that cannot be decoded as the column's type must error (naming the
    /// type and the offending value), never silently turn into a NULL slot the caller cannot
    /// distinguish from a genuine SQL NULL.
    #[test]
    fn undecodable_values_error_instead_of_becoming_null() {
        use google_cloud_spanner::value::ToValue;
        let garbage = "not-a-value".to_value();
        for (data_type, spanner_type) in [
            (DataType::Boolean, "BOOL"),
            (DataType::Int64, "INT64"),
            (DataType::Float64, "FLOAT64"),
            (DataType::Float32, "FLOAT32"),
            (DataType::Date32, "DATE"),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into())),
                "TIMESTAMP",
            ),
            (
                DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
                "NUMERIC",
            ),
        ] {
            let err = build_array(&data_type, &[Some(&garbage)]).expect_err(spanner_type);
            assert_eq!(err.status, Status::InvalidData, "{spanner_type}");
            assert!(
                err.message.contains(spanner_type) && err.message.contains("not-a-value"),
                "{spanner_type}: {}",
                err.message
            );
        }
        // BYTES: a string that is not valid base64.
        let bad_base64 = "!!!".to_value();
        let err = build_array(&DataType::Binary, &[Some(&bad_base64)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidData);
        assert!(err.message.contains("BYTES"), "{}", err.message);
    }

    /// SQL NULLs (and absent values) still map to null slots — strict decoding only applies to
    /// values that are actually present.
    #[test]
    fn nulls_still_map_to_null_slots_under_strict_decoding() {
        use arrow_array::Array;
        use google_cloud_spanner::value::ToValue;
        let null = None::<i64>.to_value();
        for data_type in [
            DataType::Boolean,
            DataType::Int64,
            DataType::Float64,
            DataType::Float32,
            DataType::Date32,
            DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
            DataType::Binary,
        ] {
            let array = build_array(&data_type, &[Some(&null), None]).unwrap();
            assert_eq!(array.len(), 2, "{data_type}");
            assert!(array.is_null(0) && array.is_null(1), "{data_type}");
        }
    }

    /// A malformed TIMESTAMP string is a decode error; only a well-formed instant outside the
    /// Arrow nanosecond range gets the more specific out-of-range message.
    #[test]
    fn malformed_timestamp_is_a_decode_error_not_out_of_range() {
        use google_cloud_spanner::value::ToValue;
        let ty = DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMESTAMP_TZ.into()));
        let malformed = "2024-13-45T99:99:99Z".to_value();
        let err = build_array(&ty, &[Some(&malformed)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidData);
        assert!(err.message.contains("cannot decode"), "{}", err.message);
        assert!(
            !err.message.contains("outside the range"),
            "{}",
            err.message
        );
    }

    /// The strict-decode policy applies inside `ARRAY` columns too: a present column value that is
    /// not a wire list, and a present list *element* that cannot be decoded as the element type,
    /// are both loud errors — never silent nulls.
    #[test]
    fn list_with_undecodable_element_errors() {
        use google_cloud_spanner::value::ToValue;
        let list_of_int64 = DataType::List(Arc::new(Field::new(LIST_ITEM, DataType::Int64, true)));

        // A present element that is not a valid INT64 errors via the element's scalar arm.
        let bad_element = vec!["42".to_value(), "not-an-int".to_value()].to_value();
        let err = build_array(&list_of_int64, &[Some(&bad_element)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidData);
        assert!(
            err.message.contains("INT64") && err.message.contains("not-an-int"),
            "{}",
            err.message
        );

        // A present column value that is not a wire list at all errors as an ARRAY decode failure.
        let not_a_list = "scalar".to_value();
        let err = build_array(&list_of_int64, &[Some(&not_a_list)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidData);
        assert!(
            err.message.contains("ARRAY") && err.message.contains("scalar"),
            "{}",
            err.message
        );
    }

    /// The strict-decode policy applies inside `STRUCT` columns too: a present column value that is
    /// neither a wire list (positional encoding) nor a keyed struct, and a present *field* value
    /// that cannot be decoded as the field's type, are both loud errors — never silent nulls. This
    /// also covers recursion: an undecodable struct nested inside a list errors as well.
    #[test]
    fn struct_with_undecodable_field_errors() {
        use google_cloud_spanner::value::ToValue;
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        // A present field value that is not a valid INT64 errors via the field's scalar arm.
        // (Spanner encodes struct values positionally, as a wire list in field order.)
        let bad_field = vec!["nope".to_value(), "abc".to_value()].to_value();
        let err = build_array(&struct_type, &[Some(&bad_field)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidData);
        assert!(
            err.message.contains("INT64") && err.message.contains("nope"),
            "{}",
            err.message
        );

        // A present column value that is neither a list nor a keyed struct errors as a STRUCT
        // decode failure.
        let not_a_struct = "scalar".to_value();
        let err = build_array(&struct_type, &[Some(&not_a_struct)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidData);
        assert!(
            err.message.contains("STRUCT") && err.message.contains("scalar"),
            "{}",
            err.message
        );

        // Recursion: ARRAY<STRUCT<..>> whose element is not a struct errors too (the element
        // recurses into the struct arm).
        let list_of_struct =
            DataType::List(Arc::new(Field::new(LIST_ITEM, struct_type.clone(), true)));
        let bad_nested = vec!["not-a-struct".to_value()].to_value();
        let err = build_array(&list_of_struct, &[Some(&bad_nested)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidData);
        assert!(
            err.message.contains("STRUCT") && err.message.contains("not-a-struct"),
            "{}",
            err.message
        );
    }

    /// Genuine wire NULLs still round-trip as nulls under strict list/struct decoding: SQL NULL
    /// (and absent) column values become null list/struct slots, and NULL elements/fields become
    /// null child slots — only *present* undecodable values error.
    #[test]
    fn list_and_struct_nulls_round_trip_as_nulls() {
        use arrow_array::Array;
        use google_cloud_spanner::value::ToValue;
        let sql_null = None::<i64>.to_value();

        // List column: NULL / absent column values and a NULL element inside a present list.
        let list_of_int64 = DataType::List(Arc::new(Field::new(LIST_ITEM, DataType::Int64, true)));
        let with_null_element = vec![1i64.to_value(), sql_null.clone(), 3i64.to_value()].to_value();
        let array = build_array(
            &list_of_int64,
            &[Some(&sql_null), None, Some(&with_null_element)],
        )
        .unwrap();
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 3);
        assert!(list.is_null(0)); // SQL NULL column value
        assert!(list.is_null(1)); // missing slot
        assert!(!list.is_null(2));
        let elements = list.value(2);
        let ints = elements
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(ints.len(), 3);
        assert_eq!(ints.value(0), 1);
        assert!(ints.is_null(1)); // NULL element stays a null slot
        assert_eq!(ints.value(2), 3);

        // Struct column: NULL / absent column values and a NULL field inside a present struct.
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let with_null_field = vec![7i64.to_value(), sql_null.clone()].to_value();
        let array = build_array(
            &struct_type,
            &[Some(&sql_null), None, Some(&with_null_field)],
        )
        .unwrap();
        let structs = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(structs.len(), 3);
        assert!(structs.is_null(0)); // SQL NULL column value
        assert!(structs.is_null(1)); // missing slot
        assert!(!structs.is_null(2));
        let ids = structs
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let names = structs
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(2), 7);
        assert!(names.is_null(2)); // NULL field stays a null slot
    }

    /// `INT64` arrives as a JSON string, so every `i64` — including magnitudes above 2^53 that an
    /// `f64` cannot represent — round-trips exactly. A JSON *number* encoding (which the old f64
    /// fallback would have cast to `i64`, silently rounding) is now a loud decode error rather than
    /// a truncated result.
    #[test]
    fn int64_string_round_trips_exactly_and_number_encoding_is_a_decode_error() {
        use arrow_array::Int64Array;
        use google_cloud_spanner::value::ToValue;

        // Ordinary value and a value above 2^53 both round-trip exactly via the string encoding.
        let above_2_53: i64 = (1i64 << 53) + 1;
        let small = 42i64.to_value();
        let big = above_2_53.to_value();
        let array = build_array(&DataType::Int64, &[Some(&small), Some(&big)]).unwrap();
        let ints = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 42);
        assert_eq!(ints.value(1), above_2_53);

        // A JSON-number encoding of the same magnitude would lose precision if cast through f64,
        // so it must now error rather than decode to a rounded value.
        let as_number = (above_2_53 as f64).to_value();
        let err = build_array(&DataType::Int64, &[Some(&as_number)]).unwrap_err();
        assert_eq!(err.status, Status::InvalidData);
        assert!(err.message.contains("INT64"), "{}", err.message);
    }
}

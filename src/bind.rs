//! Binding Arrow parameter data to Spanner statements — and converting Arrow rows to Spanner
//! **mutations** for bulk ingest.
//!
//! ADBC supplies statement parameters (and bulk-ingest rows) as an Arrow [`RecordBatch`]: each
//! column is one parameter, and each row is one set of bindings. Spanner uses **named** query
//! parameters (`@name`), so a bind column named `id` binds to `@id` in the SQL — how the
//! column→parameter pairing is decided (positionally by default, or by name via the
//! `adbc.statement.bind_by_name` option) is documented on [`resolve_parameter_names`]. Bulk ingest
//! does not go through SQL at all: each bound row becomes one insert [`Mutation`] (see
//! [`insert_mutation`]), whose cells use the exact same Arrow→Spanner value mapping
//! ([`cell_value`]) as parameter binding.
//!
//! Supported Arrow parameter types are `Int8`/`Int16`/`Int32`/`Int64` and the unsigned widths that
//! fit `i64` losslessly, `UInt8`/`UInt16`/`UInt32` (all → Spanner `INT64`; `UInt64` is unsupported
//! — `u64::MAX` exceeds `i64::MAX`),
//! `Float64`, `Float32`, `Float16` (→ Spanner `FLOAT32`; every `f16` is exactly representable in
//! `f32`), `Boolean`, `Utf8`/`LargeUtf8`/`Utf8View`,
//! `Binary`/`LargeBinary`/`BinaryView`/`FixedSizeBinary` (all → Spanner `BYTES`),
//! `Date32`/`Date64` (→ `DATE`), `Timestamp` at any `TimeUnit`
//! (Second/Millisecond/Microsecond/Nanosecond, → `TIMESTAMP`), `Decimal128` (→ `NUMERIC`), and
//! their nulls. `List`/`LargeList` of any of those scalar element types binds to a Spanner
//! `ARRAY<...>` (`ARRAY<INT64|FLOAT64|BOOL|STRING|BYTES|DATE|TIMESTAMP|NUMERIC>`), preserving
//! per-element nulls and typed null arrays; `ARRAY<ARRAY<…>>` and `ARRAY<STRUCT>` element types are
//! rejected. A `Dictionary` column of any key type binds transparently as its **value** type —
//! dictionary encoding is a representation of the same logical values, not a different logical
//! type (it is what pandas categorical columns produce over the C data interface), so
//! `Dictionary(Int32, Utf8)` binds exactly like `Utf8`, each cell's key selecting the dictionary
//! value to bind. Other Arrow types are rejected with an `InvalidArguments` error.
//!
//! Spanner `TIMESTAMP` has **nanosecond** precision (up to nine fractional digits), so a
//! `Timestamp` parameter is bound at its full source precision: a `Nanosecond` input formats up to
//! nine fractional digits, `Microsecond` six, `Millisecond` three, `Second` none — nothing is
//! truncated. The driver's default read path is symmetric: it maps Spanner `TIMESTAMP` to Arrow
//! `Timestamp(Nanosecond, "UTC")` and parses values back at full nanosecond precision (see
//! [`crate::conversion::parse_timestamp_nanos`]), so nanoseconds bound here round-trip
//! full-precision. (Arrow's nanosecond `i64` only spans ~1677-09-21 to 2262-04-11, so a Spanner
//! timestamp outside that range cannot be read back at nanosecond precision and surfaces as an
//! error rather than a silent truncation; set
//! [`spanner.max_timestamp_precision=microseconds`](crate::OPTION_MAX_TIMESTAMP_PRECISION) to read
//! the full 0001–9999 range at microsecond precision instead.)
//!
//! Spanner encodes `DATE` / `TIMESTAMP` / `NUMERIC` values on the wire as strings, and query
//! parameters are sent untyped (Spanner infers the type from the SQL). So these three are formatted
//! straight to their Spanner string forms — `YYYY-MM-DD`, RFC 3339, and a plain decimal — which
//! keeps the full `Decimal128` (`i128`) range rather than routing through a narrower decimal type.
//!
//! **JSON.** A string column tagged with the canonical `arrow.json` extension (the field metadata
//! this driver itself emits when reading a `JSON` column — see [`crate::conversion`]) binds as a
//! Spanner `JSON`-typed parameter instead of `STRING`, and a `List` whose element carries the tag
//! binds as `ARRAY<JSON>`. The distinction matters because Spanner does not coerce `STRING`
//! parameters into `JSON` columns: without the explicit type, `INSERT … VALUES (@doc)` into a
//! `JSON` column fails with a type mismatch (the untagged workaround is `PARSE_JSON(@doc)` in the
//! SQL). Tagged values therefore round-trip: what `execute` reads from a `JSON` column can be
//! bound straight back into one. Unlike the untyped strings above, this uses `add_typed_param`,
//! which sends an explicit `JSON` param type alongside the string-encoded value. The tag is
//! honoured through dictionary encoding too — the Arrow spec allows an extension array to be
//! dictionary-encoded, so a tagged `Dictionary(_, Utf8)` column binds as `JSON` like its plain
//! form (null cells included).

use adbc_core::error::Result;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, Decimal128Type, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch, downcast_dictionary_array};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::StatementBuilder;
use google_cloud_spanner::types::{self, Type};
use google_cloud_spanner::value::{ToValue, Value};

use crate::conversion::is_json_field;
use crate::error::invalid_argument;
use crate::sql::{named_parameters, qualified_table, quote_ident};

/// Bind the columns of `batch` at `row` to the query parameters named by `names`.
///
/// `names[i]` is the parameter that column `i` binds to; it is computed once per (sql, batch) by
/// [`resolve_parameter_names`] and passed in, so binding many rows of the same batch does not re-lex
/// the SQL per row (an O(rows × |sql|) cost that dominated large bound DML). See
/// [`resolve_parameter_names`] for how the column→parameter pairing is decided (positionally by
/// default, or by name).
pub(crate) fn bind_params(
    builder: StatementBuilder,
    names: &[String],
    batch: &RecordBatch,
    row: usize,
) -> Result<StatementBuilder> {
    let mut builder = builder;
    let schema = batch.schema();
    for (i, name) in names.iter().enumerate() {
        builder = bind_one(
            builder,
            name,
            schema.field(i),
            batch.column(i).as_ref(),
            row,
        )?;
    }
    Ok(builder)
}

/// Work out which parameter name each column of `batch` binds to for `sql`.
///
/// ADBC's parameter model is a batch of columns matched to the query's parameters. This driver
/// resolves the pairing two ways, selected by the `adbc.statement.bind_by_name` statement option
/// (`bind_by_name`), following the ADBC SQLite reference driver's convention
/// (apache/arrow-adbc#3362):
///
/// - **Positionally** (`bind_by_name = false`, the default — the ADBC ordinal contract): the
///   *i*-th column binds to the *i*-th distinct `@name` parameter in query order; the counts must
///   line up, and column names are ignored entirely. This is what positional clients expect — most
///   ADBC drivers (PostgreSQL, Snowflake, …) bind by position, and the Python DBAPI / validation
///   suites pass parameters as `$1`/`?` with columns not named after the parameters.
/// - **By name** (`bind_by_name = true`): each column binds to `@<its own name>`,
///   order-independent. A column whose name is not one of the query's parameters is rejected here
///   with `InvalidArguments` naming that column and the parameters the query does declare (a
///   parameter no column names is simply left unbound, which Spanner rejects at execution time).
///   Use this when the bound column names are authoritative and may not match the parameters'
///   textual order.
///
/// Lexing the SQL to find its `@name` parameters is the expensive part, so callers resolve once per
/// (sql, batch) and reuse the result across every row via [`bind_params`].
pub(crate) fn resolve_parameter_names(
    sql: &str,
    batch: &RecordBatch,
    bind_by_name: bool,
) -> Result<Vec<String>> {
    let schema = batch.schema();
    let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let params = named_parameters(sql);

    // Strict by-name: every bound column must correspond to a query parameter.
    if bind_by_name {
        let param_set: std::collections::HashSet<&str> =
            params.iter().map(String::as_str).collect();
        if let Some(missing) = column_names.iter().find(|c| !param_set.contains(*c)) {
            return Err(invalid_argument(format!(
                "cannot bind column {missing:?}: adbc.statement.bind_by_name is true, so every \
                 bound column must be named after one of the query's parameters, which are \
                 {params:?}; rename the column or set adbc.statement.bind_by_name to false to \
                 bind positionally",
            )));
        }
        return Ok(column_names.iter().map(|c| (*c).to_string()).collect());
    }

    // Positional (the default): i-th column -> i-th parameter. Counts must line up.
    if params.len() != column_names.len() {
        return Err(invalid_argument(format!(
            "parameter count mismatch: query references {} parameter(s) {:?} but {} column(s) were bound",
            params.len(),
            params,
            column_names.len(),
        )));
    }
    Ok(params)
}

/// Bind a single `column` value at `row` as parameter `name`. `field` is the column's schema
/// field, consulted for the `arrow.json` extension tag (see the module doc's JSON section).
fn bind_one(
    builder: StatementBuilder,
    name: &str,
    field: &Field,
    column: &dyn Array,
    row: usize,
) -> Result<StatementBuilder> {
    let (value, param_type) = cell_value(name, field, column, row)?;
    Ok(match param_type {
        Some(t) => builder.add_typed_param(name, value, t),
        None => builder.add_param(name, value),
    })
}

/// Build the **insert [`Mutation`]** that ingests `row` of `batch` into `table` (the
/// mutation-form table name from [`mutation_table`]).
///
/// Every cell goes through the exact same Arrow→Spanner mapping as parameter binding
/// ([`cell_value`]), so ingest and parameter binding cannot drift. Mutation values carry no
/// per-value type — Spanner coerces each from the target *column's* declared type — so the
/// explicit JSON parameter type that DML binding needs is unnecessary here: an
/// `arrow.json`-tagged string lands in a `JSON` column as its plain string encoding. Column names
/// travel as raw proto strings (no SQL, no quoting), so any valid Spanner column name works.
pub(crate) fn insert_mutation(table: &str, batch: &RecordBatch, row: usize) -> Result<Mutation> {
    let mut builder = Mutation::new_insert_builder(table);
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let (value, _param_type) = cell_value(field.name(), field, batch.column(i).as_ref(), row)?;
        builder = builder.set(field.name().clone()).to(value);
    }
    Ok(builder.build())
}

/// The table name a [`Mutation`] targets. Mutations name the table directly in the `Commit` RPC —
/// there is no surrounding SQL, so no backtick quoting (unlike [`qualified_table`]) — and a named
/// schema qualifies it with a plain dot. An empty/absent schema targets Spanner's default, unnamed
/// schema.
pub(crate) fn mutation_table(db_schema: Option<&str>, table_name: &str) -> String {
    match db_schema.filter(|s| !s.is_empty()) {
        Some(schema) => format!("{schema}.{table_name}"),
        None => table_name.to_string(),
    }
}

/// Convert one Arrow cell to its Spanner wire [`Value`], plus the explicit parameter [`Type`] that
/// must accompany it when bound as a query parameter (`Some` only for `JSON` / `ARRAY<JSON>` — see
/// the module doc's JSON section; mutation values ignore it).
///
/// This is the single Arrow→Spanner value mapping, shared by parameter binding ([`bind_one`]) and
/// mutation-based bulk ingest ([`insert_mutation`]). `name` labels the column/parameter in error
/// messages only.
fn cell_value(
    name: &str,
    field: &Field,
    column: &dyn Array,
    row: usize,
) -> Result<(Value, Option<Type>)> {
    let data_type = column.data_type();
    match data_type {
        // ARRAY<...>: an Arrow `List`/`LargeList` maps to a Spanner array. See
        // [`list_cell_value`] — nested arrays and STRUCT elements are rejected there.
        DataType::List(item) => {
            let elem = (!column.is_null(row)).then(|| column.as_list::<i32>().value(row));
            list_cell_value(name, item, elem)
        }
        DataType::LargeList(item) => {
            let elem = (!column.is_null(row)).then(|| column.as_list::<i64>().value(row));
            list_cell_value(name, item, elem)
        }
        // A dictionary-encoded column is an index encoding of the same logical values, not a
        // different logical type (see the module doc), so it binds as its *value* type: the key at
        // `row` selects the dictionary value, which re-enters this same mapping — every bindable
        // type, scalar or `ARRAY<...>`, is thereby accepted encoded, and an unsupported value type
        // is rejected with the same error as its plain form. A null cell binds as NULL, its value
        // type still validated so a bad schema fails loudly on every row.
        DataType::Dictionary(_, _) => downcast_dictionary_array!(
            column => match column.key(row) {
                Some(value_row) => cell_value(name, field, column.values().as_ref(), value_row),
                None => null_dictionary_value(name, field, column.values().data_type()),
            },
            _ => unreachable!("downcast_dictionary_array dispatched a non-dictionary {data_type:?}")
        ),
        _ => {
            let bind = scalar_binder(data_type).ok_or_else(|| {
                invalid_argument(format!(
                    "cannot bind parameter {name:?}: unsupported Arrow type {data_type:?}"
                ))
            })?;
            let value = bind(name, data_type, column, row)?;
            // JSON is the only type needing an explicit param type; `is_json_field` is true only
            // for `arrow.json`-tagged string columns, so this is `None` for every other type.
            Ok((value, is_json_field(field).then(types::json)))
        }
    }
}

/// The per-element Arrow→Spanner scalar encoder returned by [`scalar_binder`]: given the parameter
/// `name` (for error messages), the element's Arrow [`DataType`] (equal to the array's own type —
/// consulted for the `Timestamp` unit / `Decimal128` scale), the array, and an element index, it
/// produces that element's Spanner [`Value`]. A plain `fn` (the arms capture nothing), so
/// dispatching once per cell and reusing the result across an array's elements costs no allocation.
type ScalarBinder = fn(&str, &DataType, &dyn Array, usize) -> Result<Value>;

/// The **single** Arrow→Spanner scalar-value mapping — the one place a scalar type's encoding
/// lives.
///
/// Returns the [`ScalarBinder`] for a scalar Arrow `data_type` (reading element `i` of an array of
/// that type into a Spanner scalar [`Value`], nulls preserved), or `None` if the type has no
/// Spanner mapping. Both binding paths funnel through it — [`cell_value`] for a scalar `@param`,
/// and [`list_cell_value`] for every element of an `ARRAY<...>` — so the two can no longer drift,
/// and a scalar type is accepted as parameter *and* array element in one stroke.
///
/// **Adding a new Arrow scalar type touches these sites** (keep them in lockstep):
///   1. **here** (`scalar_binder`) — the Arrow→Spanner *value* mapping, shared by the scalar and
///      array-element binds;
///   2. [`spanner_column_type`] — the Arrow→Spanner *column* type for the ingest `CREATE TABLE`;
///   3. [`spanner_field_type`] — only if the type needs field-aware handling (as `arrow.json` does);
///   4. the read path in [`crate::conversion`] — the reverse Spanner→Arrow mapping, so values
///      round-trip;
///   5. the module-level doc list of supported types (top of this file).
///
/// (A new *container* kind — beyond `List`/`LargeList` — is instead added to [`cell_value`].)
fn scalar_binder(data_type: &DataType) -> Option<ScalarBinder> {
    let bind: ScalarBinder = match data_type {
        // Spanner's only integer type is INT64, so every Arrow int width widens to it; both Arrow
        // floats map to FLOAT64 (f32 widens losslessly).
        DataType::Int64 => primitive_binder::<Int64Type, i64>(),
        DataType::Int32 => primitive_binder::<Int32Type, i64>(),
        DataType::Int16 => primitive_binder::<Int16Type, i64>(),
        DataType::Int8 => primitive_binder::<Int8Type, i64>(),
        // The unsigned integer widths that fit `i64` losslessly (`u8`/`u16`/`u32`, whose max
        // 4_294_967_295 < `i64::MAX`) widen to Spanner INT64 via `i64::from`, exactly like the
        // signed widths. `UInt64` is deliberately absent: `u64::MAX` (1.8e19) exceeds `i64::MAX`
        // (9.2e18), so there is no lossless `From<u64>` for `i64` and no INT64 mapping for it.
        DataType::UInt32 => primitive_binder::<UInt32Type, i64>(),
        DataType::UInt16 => primitive_binder::<UInt16Type, i64>(),
        DataType::UInt8 => primitive_binder::<UInt8Type, i64>(),
        DataType::Float64 => primitive_binder::<Float64Type, f64>(),
        DataType::Float32 => primitive_binder::<Float32Type, f64>(),
        // Spanner has no 16-bit float, but every `f16` is exactly representable in `f32` (and so
        // in `f64`), so a half-float widens losslessly the way the narrow integers widen to INT64.
        DataType::Float16 => primitive_binder::<Float16Type, f64>(),
        DataType::Boolean => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_boolean().value(i)))
        }
        // `Utf8`/`LargeUtf8` differ only in offset width; both map to Spanner STRING (LargeUtf8 is
        // what Arrow-native producers commonly emit, e.g. `pyarrow.Table.from_pandas`). JSON typing
        // is applied by the caller, not here.
        DataType::Utf8 => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_string::<i32>().value(i)))
        }
        DataType::LargeUtf8 => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_string::<i64>().value(i)))
        }
        // `Utf8View`/`BinaryView` are the German-string layouts newer Arrow producers (polars,
        // pyarrow 16+ with view types) emit by default; same Spanner mapping as their offset kin.
        DataType::Utf8View => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_string_view().value(i)))
        }
        DataType::Binary => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_binary::<i32>().value(i)))
        }
        DataType::LargeBinary => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_binary::<i64>().value(i)))
        }
        DataType::BinaryView => {
            |_, _, a, i| Ok(scalar_value(a.is_null(i), || a.as_binary_view().value(i)))
        }
        // `FixedSizeBinary(n)` is a byte string of a fixed width; it maps to Spanner BYTES exactly
        // like the variable-width binary kinds (the width is a layout detail Spanner does not carry
        // — a BYTES column has no fixed length — so the read path returns plain `Binary`).
        DataType::FixedSizeBinary(_) => |_, _, a, i| {
            Ok(scalar_value(a.is_null(i), || {
                a.as_fixed_size_binary().value(i)
            }))
        },
        DataType::Date32 => |name, _, a, i| {
            try_scalar_value(a.is_null(i), || {
                date_string(name, a.as_primitive::<Date32Type>().value(i))
            })
        },
        // `Date64` is milliseconds since the Unix epoch, constrained by Arrow to whole days.
        DataType::Date64 => |name, _, a, i| {
            try_scalar_value(a.is_null(i), || {
                date_string(
                    name,
                    date64_days(name, a.as_primitive::<Date64Type>().value(i))?,
                )
            })
        },
        // Spanner `TIMESTAMP` is UTC with nanosecond precision, so every Arrow timestamp unit is
        // accepted; `timestamp_value` reads the raw i64 from the unit's typed array and
        // `timestamp_string` formats the Spanner value at the unit's full precision.
        DataType::Timestamp(_, _) => |name, dt, a, i| {
            let DataType::Timestamp(unit, _) = dt else {
                unreachable!("scalar_binder dispatched a Timestamp arm on {dt:?}")
            };
            try_scalar_value(a.is_null(i), || {
                timestamp_string(name, unit, timestamp_value(a, unit, i))
            })
        },
        DataType::Decimal128(_, _) => |name, dt, a, i| {
            let DataType::Decimal128(_, scale) = dt else {
                unreachable!("scalar_binder dispatched a Decimal128 arm on {dt:?}")
            };
            // Format the full i128 directly; no narrower decimal type in the way. The scale is
            // validated even for a null so a bad schema fails loudly on every row.
            let scale = numeric_scale(name, *scale)?;
            Ok(scalar_value(a.is_null(i), || {
                numeric_string(a.as_primitive::<Decimal128Type>().value(i), scale)
            }))
        },
        // A `Null`-typed column has no values by definition: every cell binds as NULL. This is
        // the shape ADBC's own contract produces — `get_parameter_schema` types an undetermined
        // parameter `Null` (adbc.h: "the type of the corresponding field will be NA"), so a
        // client that builds its bind batch from the reported schema hands back `Null`-typed
        // columns; pyarrow likewise infers `Null` for an all-None parameter set. The NULL goes
        // on the wire untyped — `add_param` declares no parameter types for *any* bind — and
        // Spanner infers the type from the SQL context, exactly as it does for a NULL cell of a
        // typed column. Deliberately not `a.is_null(i)`: a `NullArray` carries no validity
        // buffer, so Arrow's *physical* `is_null` reports `false` for its all-null cells.
        DataType::Null => |_, _, _, _| Ok(null_value()),
        _ => return None,
    };
    Some(bind)
}

/// The [`ScalarBinder`] for an Arrow primitive type `T` whose values bind as the Spanner-native
/// `V` they convert into — `i64` for every Arrow integer width, `f64` for both float widths. The
/// conversion is [`From`], so it is always lossless (and the identity for `Int64`/`Float64`), and
/// nulls are preserved by [`scalar_value`] as for every other scalar.
///
/// The returned closure captures nothing — `T`/`V` are generic parameters, not captures — so it
/// coerces to the plain `fn` pointer a [`ScalarBinder`] is, keeping [`scalar_binder`]'s dispatch
/// table allocation-free.
fn primitive_binder<T, V>() -> ScalarBinder
where
    T: ArrowPrimitiveType,
    V: From<T::Native> + Into<Value>,
{
    |_, _, a, i| {
        Ok(scalar_value(a.is_null(i), || {
            V::from(a.as_primitive::<T>().value(i))
        }))
    }
}

/// The NULL bind for a null dictionary-encoded cell. The dictionary's *value* type is still
/// validated — an unsupported value type is rejected on every row, null or not, matching the other
/// arms of [`cell_value`] (the `Decimal128` scale precedent) — and a `List`-valued dictionary
/// keeps [`list_cell_value`]'s typed-null-array handling. `field` is the dictionary column's own
/// field: an `arrow.json` tag on it keeps the explicit `JSON` param type on the null, exactly as
/// the plain scalar arm does for a typed null.
fn null_dictionary_value(
    name: &str,
    field: &Field,
    value_type: &DataType,
) -> Result<(Value, Option<Type>)> {
    match value_type {
        DataType::List(item) | DataType::LargeList(item) => list_cell_value(name, item, None),
        _ => {
            scalar_binder(value_type).ok_or_else(|| {
                invalid_argument(format!(
                    "cannot bind parameter {name:?}: unsupported Arrow type {value_type:?}"
                ))
            })?;
            Ok((null_value(), is_json_field(field).then(types::json)))
        }
    }
}

/// The Spanner SQL `NULL` wire value.
fn null_value() -> Value {
    None::<bool>.to_value()
}

/// Convert a scalar (or a null) to its Spanner wire [`Value`].
///
/// The bound is [`Into<Value>`] rather than [`ToValue`] deliberately: `Into` **consumes** the
/// scalar, so an owned `String` (the `DATE`/`TIMESTAMP`/`NUMERIC` encodings) moves into the wire
/// value, where `ToValue` takes `&self` and would copy it. Borrowed scalars (`&str`, `&[u8]`) still
/// convert through the client's blanket `impl<T: ToValue + ?Sized> From<&T> for Value`, so every
/// type a [`ScalarBinder`] produces is accepted either way.
fn scalar_value<T: Into<Value>>(is_null: bool, value: impl FnOnce() -> T) -> Value {
    if is_null {
        null_value()
    } else {
        value().into()
    }
}

/// Like [`scalar_value`] but the conversion is fallible (the string-formatted temporal types).
fn try_scalar_value<T: Into<Value>>(
    is_null: bool,
    value: impl FnOnce() -> Result<T>,
) -> Result<Value> {
    Ok(if is_null {
        null_value()
    } else {
        value()?.into()
    })
}

/// Convert an Arrow `List`/`LargeList` cell to a Spanner `ARRAY<...>` wire [`Value`].
///
/// `item` is the list's element field: its data type selects the [`scalar_binder`] mapping (an
/// `arrow.json` tag on a string element types the whole array as `ARRAY<JSON>`), and `elem` is the
/// child slice for this row, or `None` when the whole cell is null (→ a typed null array). Every
/// element runs through the same `scalar_binder` as a scalar bind, so the element mapping cannot
/// drift from the scalar one (narrower ints widen to `INT64`, floats to `FLOAT64`,
/// `DATE`/`TIMESTAMP`/`NUMERIC` format to their Spanner string forms), and each element keeps its
/// own null. The element type is validated up front, so an unsupported element — including a
/// nested `ARRAY<ARRAY<…>>` or `ARRAY<STRUCT>` (both out of scope for Spanner) — is rejected even
/// for an empty or null array.
fn list_cell_value(
    name: &str,
    item: &Field,
    elem: Option<ArrayRef>,
) -> Result<(Value, Option<Type>)> {
    let item_type = item.data_type();
    let bind = scalar_binder(item_type).ok_or_else(|| {
        invalid_argument(format!(
            "cannot bind ARRAY parameter {name:?}: unsupported element type {item_type:?}"
        ))
    })?;
    let value = match elem {
        // A null cell is a null array; each present element keeps its own null via `scalar_binder`.
        None => null_value(),
        // `Value::from(Vec<Value>)` consumes the elements (`impl<T: Into<Value>> From<Vec<T>>`);
        // `Vec::to_value` would deep-copy every one of them instead.
        Some(a) => Value::from(
            (0..a.len())
                .map(|i| bind(name, item_type, a.as_ref(), i))
                .collect::<Result<Vec<Value>>>()?,
        ),
    };
    Ok((
        value,
        is_json_field(item).then(|| types::array(types::json())),
    ))
}

/// Convert an Arrow `Date64` value (milliseconds since the Unix epoch, at a whole-day boundary
/// per the Arrow spec) to `Date32` days, erroring on values outside the `Date32` range.
fn date64_days(name: &str, millis: i64) -> Result<i32> {
    i32::try_from(millis.div_euclid(86_400_000)).map_err(|_| {
        invalid_argument(format!(
            "cannot bind DATE parameter {name:?}: {millis}ms is out of range"
        ))
    })
}

/// Format an Arrow `Date32` (days since the Unix epoch) as the Spanner `DATE` wire form,
/// `YYYY-MM-DD`. `name` is used only for the out-of-range error message.
fn date_string(name: &str, days: i32) -> Result<String> {
    let date = NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or_else(|| {
            invalid_argument(format!(
                "cannot bind DATE parameter {name:?}: {days} is out of range"
            ))
        })?;
    Ok(date.format("%Y-%m-%d").to_string())
}

/// Read the raw `i64` at `row` from an Arrow timestamp `column` of the given `unit`.
fn timestamp_value(column: &dyn Array, unit: &TimeUnit, row: usize) -> i64 {
    match unit {
        TimeUnit::Second => column.as_primitive::<TimestampSecondType>().value(row),
        TimeUnit::Millisecond => column.as_primitive::<TimestampMillisecondType>().value(row),
        TimeUnit::Microsecond => column.as_primitive::<TimestampMicrosecondType>().value(row),
        TimeUnit::Nanosecond => column.as_primitive::<TimestampNanosecondType>().value(row),
    }
}

/// Validate a `Decimal128` scale for Spanner `NUMERIC` (must be a non-negative `u32` `<= 38`).
fn numeric_scale(name: &str, scale: i8) -> Result<u32> {
    u32::try_from(scale)
        .ok()
        .filter(|s| *s <= 38)
        .ok_or_else(|| {
            invalid_argument(format!(
                "cannot bind NUMERIC parameter {name:?}: unsupported scale {scale}"
            ))
        })
}

/// Convert an Arrow timestamp `value` in `unit` to the Spanner `TIMESTAMP` wire form — an RFC 3339
/// string in UTC, carrying the source unit's **full** precision so nothing is lost.
///
/// Spanner `TIMESTAMP` has nanosecond precision (up to nine fractional digits), so the fractional
/// second is formatted to exactly as many digits as the unit carries: nine for `Nanosecond`, six
/// for `Microsecond`, three for `Millisecond`, none for `Second`. A `Nanosecond` value is decoded
/// via [`DateTime::from_timestamp_nanos`], preserving its sub-microsecond digits (a negative value
/// therefore renders as its exact instant, e.g. `-1 ns` → `…59.999999999Z`, not truncated toward
/// zero). `name` is used only for the out-of-range error message.
fn timestamp_string(name: &str, unit: &TimeUnit, value: i64) -> Result<String> {
    let (ts, format) = match unit {
        TimeUnit::Second => (
            DateTime::<Utc>::from_timestamp(value, 0),
            SecondsFormat::Secs,
        ),
        TimeUnit::Millisecond => (
            DateTime::<Utc>::from_timestamp_millis(value),
            SecondsFormat::Millis,
        ),
        TimeUnit::Microsecond => (
            DateTime::<Utc>::from_timestamp_micros(value),
            SecondsFormat::Micros,
        ),
        // `from_timestamp_nanos` is infallible: every `i64` nanosecond count is in range.
        TimeUnit::Nanosecond => (
            Some(DateTime::<Utc>::from_timestamp_nanos(value)),
            SecondsFormat::Nanos,
        ),
    };
    let ts = ts.ok_or_else(|| {
        invalid_argument(format!(
            "cannot bind TIMESTAMP parameter {name:?}: {value} ({unit:?}) is out of range"
        ))
    })?;
    Ok(ts.to_rfc3339_opts(format, true))
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

/// Map an Arrow parameter/ingest type to the Spanner column type used when creating a table.
///
/// This mirrors the read path's Spanner→Arrow mapping. Narrower integers collapse to `INT64`
/// (Spanner's only integer type); `List` becomes a Spanner `ARRAY<...>`. Types with no Spanner
/// column representation are rejected.
pub(crate) fn spanner_column_type(data_type: &DataType) -> Result<String> {
    Ok(match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "INT64".to_string(),
        // The unsigned widths that fit `i64` losslessly widen to INT64 too (see `scalar_binder`);
        // `UInt64` has no INT64 mapping and is rejected below.
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "INT64".to_string(),
        // `Float16` has no Spanner counterpart, but widens losslessly into FLOAT32 (see
        // `scalar_binder`), so it creates the same column as `Float32`.
        DataType::Float16 | DataType::Float32 => "FLOAT32".to_string(),
        DataType::Float64 => "FLOAT64".to_string(),
        DataType::Boolean => "BOOL".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING(MAX)".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BYTES(MAX)".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Decimal128(_, _) => "NUMERIC".to_string(),
        DataType::List(field) | DataType::LargeList(field) => {
            format!("ARRAY<{}>", spanner_column_type(field.data_type())?)
        }
        // The encoding is transparent (see [`cell_value`]): a dictionary-encoded ingest column
        // creates a column of its value type.
        DataType::Dictionary(_, value) => spanner_column_type(value)?,
        other => {
            return Err(invalid_argument(format!(
                "cannot create a Spanner column for Arrow type {other:?}; cast it to a type with \
                 a Spanner equivalent before ingesting"
            )));
        }
    })
}

/// Map an Arrow ingest field to the Spanner column type used when creating a table — like
/// [`spanner_column_type`], but field-aware: a string field tagged with the `arrow.json` extension
/// becomes a `JSON` column (and a tagged list element `ARRAY<JSON>`), matching how [`bind_one`]
/// binds such values as `JSON`-typed parameters (which Spanner would reject in a `STRING` column).
fn spanner_field_type(field: &Field) -> Result<String> {
    if is_json_field(field) {
        return Ok("JSON".to_string());
    }
    match field.data_type() {
        DataType::List(item) | DataType::LargeList(item) => {
            Ok(format!("ARRAY<{}>", spanner_field_type(item)?))
        }
        other => spanner_column_type(other),
    }
}

/// Build a `CREATE TABLE` statement for bulk ingest from the data's Arrow `schema`.
///
/// Every data column maps to its Spanner type via [`spanner_field_type`], and the statement carries
/// **no `PRIMARY KEY` clause**: Spanner creates such a table with an implicit hidden `rowid` key of
/// its own
/// (<https://cloud.google.com/spanner/docs/primary-key-default-value#tables-without-primary-keys>),
/// which no `SELECT *` returns — so the created table reads back as exactly the Arrow schema that
/// built it. Arrow ingest data carries no key, and inventing one is not the driver's call: a
/// primary key fixes Spanner's physical row layout, so choosing it belongs in the `CREATE TABLE`
/// the user writes, followed by an `append` ingest. (Up to 0.7 this appended a synthetic
/// `adbc_ingest_key` UUID key column, and `spanner.ingest.primary_key` existed to opt out of it;
/// both are gone.)
///
/// Pass `if_not_exists` for `create_append` mode. `db_schema` (the `adbc.ingest.target_db_schema`
/// option) optionally qualifies the created table with a named schema.
pub(crate) fn create_table_sql(
    table: &str,
    db_schema: Option<&str>,
    schema: &arrow_schema::Schema,
    if_not_exists: bool,
) -> Result<String> {
    let mut columns: Vec<String> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        // Name the offending column: the rejection is per-field, and a wide ingest schema gives
        // the caller no other way to find which of its columns has no Spanner type.
        let column_type = spanner_field_type(field).map_err(|e| {
            crate::error::annotate(e, |m| format!("ingest column {:?}: {m}", field.name()))
        })?;
        columns.push(format!("{} {}", quote_ident(field.name()), column_type));
    }
    let guard = if if_not_exists { "IF NOT EXISTS " } else { "" };
    Ok(format!(
        "CREATE TABLE {guard}{} ({})",
        qualified_table(db_schema, table),
        columns.join(", "),
    ))
}

#[cfg(test)]
mod tests;

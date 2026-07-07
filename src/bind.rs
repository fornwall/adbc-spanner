//! Binding Arrow parameter data to Spanner statements.
//!
//! ADBC supplies statement parameters (and bulk-ingest rows) as an Arrow [`RecordBatch`]: each
//! column is one parameter, and each row is one set of bindings. Spanner uses **named** query
//! parameters (`@name`), so a bind column named `id` binds to `@id` in the SQL.
//!
//! Supported Arrow parameter types are `Int16`/`Int32`/`Int64` (all → Spanner `INT64`), `Float64`,
//! `Float32`, `Boolean`, `Utf8`/`LargeUtf8`,
//! `Binary`/`LargeBinary`, `Date32` (→ `DATE`), `Timestamp` at any `TimeUnit`
//! (Second/Millisecond/Microsecond/Nanosecond, → `TIMESTAMP`), `Decimal128` (→ `NUMERIC`), and
//! their nulls. Other Arrow types are rejected with an `InvalidArguments` error.
//!
//! Spanner `TIMESTAMP` has **nanosecond** precision (up to nine fractional digits), so a
//! `Timestamp` parameter is bound at its full source precision: a `Nanosecond` input formats up to
//! nine fractional digits, `Microsecond` six, `Millisecond` three, `Second` none — nothing is
//! truncated. The driver's read path is symmetric: it maps Spanner `TIMESTAMP` to Arrow
//! `Timestamp(Nanosecond, "UTC")` and parses values back at full nanosecond precision (see
//! [`crate::conversion::parse_timestamp_nanos`]), so nanoseconds bound here round-trip
//! full-precision. (Arrow's nanosecond `i64` only spans ~1677-09-21 to 2262-04-11, so a Spanner
//! timestamp outside that range cannot be read back and surfaces as an error rather than a silent
//! truncation.)
//!
//! Spanner encodes `DATE` / `TIMESTAMP` / `NUMERIC` values on the wire as strings, and query
//! parameters are sent untyped (Spanner infers the type from the SQL). So these three are formatted
//! straight to their Spanner string forms — `YYYY-MM-DD`, RFC 3339, and a plain decimal — which
//! keeps the full `Decimal128` (`i128`) range rather than routing through a narrower decimal type.

use adbc_core::error::Result;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use google_cloud_spanner::statement::StatementBuilder;

use crate::error::invalid_argument;

/// Bind every column of `batch` at `row` as a parameter named after that column (`@<column-name>`).
///
/// Used where the parameter names are the column names by construction — bulk ingest builds
/// `INSERT ... (@col, ...)` from the data's own columns. For binding a user query's parameters, use
/// [`bind_params`], which also handles positional binding.
pub(crate) fn bind_row(
    mut builder: StatementBuilder,
    batch: &RecordBatch,
    row: usize,
) -> Result<StatementBuilder> {
    for (i, field) in batch.schema().fields().iter().enumerate() {
        builder = bind_one(builder, field.name(), batch.column(i).as_ref(), row)?;
    }
    Ok(builder)
}

/// Bind the columns of `batch` at `row` to the parameters of `sql`.
///
/// ADBC's parameter model is a batch of columns matched to the query's parameters. This driver
/// resolves the pairing two ways:
///
/// - **By name** (the historical behaviour): when every bound column's name is one of the query's
///   `@name` parameters, each column binds to `@<its own name>`.
/// - **Positionally** (the ADBC ordinal contract): otherwise the *i*-th column binds to the *i*-th
///   distinct `@name` parameter in query order. This is what positional clients expect — most ADBC
///   drivers (PostgreSQL, Snowflake, …) bind by position, and the Python DBAPI / validation suites
///   pass parameters as `$1`/`?` with columns not named after the parameters.
pub(crate) fn bind_params(
    builder: StatementBuilder,
    sql: &str,
    batch: &RecordBatch,
    row: usize,
) -> Result<StatementBuilder> {
    let names = resolve_parameter_names(sql, batch)?;
    let mut builder = builder;
    for (i, name) in names.iter().enumerate() {
        builder = bind_one(builder, name, batch.column(i).as_ref(), row)?;
    }
    Ok(builder)
}

/// Work out which parameter name each column of `batch` binds to for `sql` (see [`bind_params`]).
fn resolve_parameter_names(sql: &str, batch: &RecordBatch) -> Result<Vec<String>> {
    let schema = batch.schema();
    let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let params = named_parameters(sql);
    let param_set: std::collections::HashSet<&str> = params.iter().map(String::as_str).collect();

    // Name mode: every bound column corresponds to a query parameter of the same name.
    if !column_names.is_empty() && column_names.iter().all(|c| param_set.contains(c)) {
        return Ok(column_names.iter().map(|c| (*c).to_string()).collect());
    }

    // Positional mode: i-th column -> i-th parameter. Counts must line up.
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

/// Bind a single `column` value at `row` as parameter `name`.
fn bind_one(
    builder: StatementBuilder,
    name: &str,
    column: &dyn Array,
    row: usize,
) -> Result<StatementBuilder> {
    let is_null = column.is_null(row);
    Ok(match column.data_type() {
        DataType::Int64 => {
            let a = column.as_primitive::<Int64Type>();
            bind_scalar(builder, name, is_null, || a.value(row))
        }
        // Spanner's only integer type is INT64, so narrower Arrow ints widen to it.
        DataType::Int32 => {
            let a = column.as_primitive::<Int32Type>();
            bind_scalar(builder, name, is_null, || i64::from(a.value(row)))
        }
        DataType::Int16 => {
            let a = column.as_primitive::<Int16Type>();
            bind_scalar(builder, name, is_null, || i64::from(a.value(row)))
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
        // `Utf8`/`LargeUtf8` differ only in offset width; both map to Spanner STRING. LargeUtf8
        // is what Arrow-native producers commonly emit (e.g. `pyarrow.Table.from_pandas`).
        DataType::Utf8 => {
            let a = column.as_string::<i32>();
            if is_null {
                builder.add_param(name, &None::<String>)
            } else {
                builder.add_param(name, &a.value(row))
            }
        }
        DataType::LargeUtf8 => {
            let a = column.as_string::<i64>();
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
        DataType::LargeBinary => {
            let a = column.as_binary::<i64>();
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
        // Spanner `TIMESTAMP` is UTC with nanosecond precision, so every Arrow timestamp unit
        // is accepted; the four arms differ only in the typed array they read the raw i64 from,
        // and `timestamp_string` formats the Spanner value at the unit's full precision.
        DataType::Timestamp(unit, _) => {
            if is_null {
                builder.add_param(name, &None::<String>)
            } else {
                let value = match unit {
                    TimeUnit::Second => column.as_primitive::<TimestampSecondType>().value(row),
                    TimeUnit::Millisecond => {
                        column.as_primitive::<TimestampMillisecondType>().value(row)
                    }
                    TimeUnit::Microsecond => {
                        column.as_primitive::<TimestampMicrosecondType>().value(row)
                    }
                    TimeUnit::Nanosecond => {
                        column.as_primitive::<TimestampNanosecondType>().value(row)
                    }
                };
                builder.add_param(name, &timestamp_string(name, unit, value)?)
            }
        }
        DataType::Decimal128(_precision, scale) => {
            let a = column.as_primitive::<Decimal128Type>();
            if is_null {
                builder.add_param(name, &None::<String>)
            } else {
                let scale = u32::try_from(*scale)
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
    })
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

/// Extract the distinct named parameters (`@name`) referenced by `sql`, in order of first
/// appearance.
///
/// Skips `@name` occurrences inside string / identifier literals (`'…'`, `"…"`, `` `…` `` with
/// backslash escapes) and comments (`-- …`, `# …`, `/* … */`), and does not treat statement hints
/// (`@{…}`) or system variables (`@@var`) as parameters — the same lexical rules as
/// [`crate::ddl::split_statements`]. Used by `get_parameter_schema` when no parameter data has been
/// bound yet.
pub(crate) fn named_parameters(sql: &str) -> Vec<String> {
    let mut params: Vec<String> = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                // Skip a quoted literal/identifier, honouring backslash escapes.
                while let Some(ch) = chars.next() {
                    match ch {
                        '\\' => {
                            chars.next();
                        }
                        _ if ch == c => break,
                        _ => {}
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => skip_line_comment(&mut chars),
            '#' => skip_line_comment(&mut chars),
            '/' if chars.peek() == Some(&'*') => {
                chars.next(); // '*'
                let mut prev = '\0';
                for ch in chars.by_ref() {
                    if prev == '*' && ch == '/' {
                        break;
                    }
                    prev = ch;
                }
            }
            '@' => match chars.peek() {
                // `@@var` (system variable) or `@{…}` (statement hint): not a bind parameter.
                Some('@') | Some('{') => {
                    chars.next();
                }
                Some(&ch) if ch == '_' || ch.is_ascii_alphabetic() => {
                    let mut name = String::new();
                    while let Some(&ch) = chars.peek() {
                        if ch == '_' || ch.is_ascii_alphanumeric() {
                            name.push(ch);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if !params.iter().any(|p| p == &name) {
                        params.push(name);
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
    params
}

fn skip_line_comment(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    for ch in chars.by_ref() {
        if ch == '\n' {
            break;
        }
    }
}

/// Backtick-quote a Spanner identifier, escaping embedded backticks. Keeps reserved words
/// (`create`, `index`, …) and otherwise-unsafe names valid, and closes the identifier-injection
/// vector when a caller's table/column names reach the generated SQL.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// Synthetic primary-key column added to tables created by bulk ingest (see [`create_table_sql`]).
/// Spanner requires a primary key, but Arrow ingest data carries none, so we add a hidden
/// UUID-defaulted key. Spanner forbids leading-underscore identifiers, hence the `adbc_`-prefixed
/// (rather than `_adbc_`) name. Ingest `INSERT`s omit it, so the `DEFAULT` fills it per row.
pub(crate) const INGEST_KEY_COLUMN: &str = "adbc_ingest_key";

/// Map an Arrow parameter/ingest type to the Spanner column type used when creating a table.
///
/// This mirrors the read path's Spanner→Arrow mapping. Narrower integers collapse to `INT64`
/// (Spanner's only integer type); `List` becomes a Spanner `ARRAY<...>`. Types with no Spanner
/// column representation are rejected.
pub(crate) fn spanner_column_type(data_type: &DataType) -> Result<String> {
    Ok(match data_type {
        DataType::Int16 | DataType::Int32 | DataType::Int64 => "INT64".to_string(),
        DataType::Float32 => "FLOAT32".to_string(),
        DataType::Float64 => "FLOAT64".to_string(),
        DataType::Boolean => "BOOL".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING(MAX)".to_string(),
        DataType::Binary | DataType::LargeBinary => "BYTES(MAX)".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Decimal128(_, _) => "NUMERIC".to_string(),
        DataType::List(field) | DataType::LargeList(field) => {
            format!("ARRAY<{}>", spanner_column_type(field.data_type())?)
        }
        other => {
            return Err(invalid_argument(format!(
                "cannot create a Spanner column for Arrow type {other:?}"
            )))
        }
    })
}

/// Build a `CREATE TABLE` statement for bulk ingest from the data's Arrow `schema`.
///
/// Every data column maps to its Spanner type via [`spanner_column_type`], and a hidden
/// [`INGEST_KEY_COLUMN`] UUID key is appended as the primary key (Spanner requires one). Pass
/// `if_not_exists` for `create_append` mode.
pub(crate) fn create_table_sql(
    table: &str,
    schema: &arrow_schema::Schema,
    if_not_exists: bool,
) -> Result<String> {
    let mut columns: Vec<String> = Vec::with_capacity(schema.fields().len() + 1);
    for field in schema.fields() {
        columns.push(format!(
            "{} {}",
            quote_ident(field.name()),
            spanner_column_type(field.data_type())?
        ));
    }
    columns.push(format!(
        "{} STRING(36) DEFAULT (GENERATE_UUID())",
        quote_ident(INGEST_KEY_COLUMN)
    ));
    let guard = if if_not_exists { "IF NOT EXISTS " } else { "" };
    Ok(format!(
        "CREATE TABLE {guard}{} ({}) PRIMARY KEY ({})",
        quote_ident(table),
        columns.join(", "),
        quote_ident(INGEST_KEY_COLUMN),
    ))
}

/// Build an `` INSERT INTO `table` (`cols`) VALUES (@cols) `` statement for bulk ingest.
///
/// Identifiers are quoted; the `@name` parameter references reuse the (unquoted) column names,
/// which [`bind_row`] binds by field name.
pub(crate) fn insert_sql(table: &str, columns: &[String]) -> String {
    let names = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let params = columns
        .iter()
        .map(|c| format!("@{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({names}) VALUES ({params})",
        quote_ident(table)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Date32Array, Decimal128Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow_schema::{Field, Schema};
    use google_cloud_spanner::statement::Statement;

    use super::*;

    #[test]
    fn binds_narrow_integers_as_int64() {
        // Int16 / Int32 widen to Spanner INT64 (its only integer type).
        let b = batch(
            vec![
                Field::new("a", DataType::Int16, true),
                Field::new("b", DataType::Int32, true),
            ],
            vec![
                Arc::new(arrow_array::Int16Array::from(vec![Some(7i16), None])),
                Arc::new(Int32Array::from(vec![Some(9i32), None])),
            ],
        );
        assert!(bind_row(Statement::builder("SELECT @a, @b"), &b, 0).is_ok());
        assert!(bind_row(Statement::builder("SELECT @a, @b"), &b, 1).is_ok());
    }

    #[test]
    fn maps_arrow_to_spanner_column_types() {
        assert_eq!(spanner_column_type(&DataType::Int32).unwrap(), "INT64");
        assert_eq!(spanner_column_type(&DataType::Int64).unwrap(), "INT64");
        assert_eq!(spanner_column_type(&DataType::Float32).unwrap(), "FLOAT32");
        assert_eq!(spanner_column_type(&DataType::Utf8).unwrap(), "STRING(MAX)");
        assert_eq!(
            spanner_column_type(&DataType::LargeBinary).unwrap(),
            "BYTES(MAX)"
        );
        assert_eq!(
            spanner_column_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            "TIMESTAMP"
        );
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        assert_eq!(spanner_column_type(&list).unwrap(), "ARRAY<INT64>");
        // No Spanner column type for unsigned integers.
        assert!(spanner_column_type(&DataType::UInt32).is_err());
    }

    #[test]
    fn builds_create_table_sql() {
        let schema = Schema::new(vec![
            Field::new("idx", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert_eq!(
            create_table_sql("my_table", &schema, false).unwrap(),
            "CREATE TABLE `my_table` (`idx` INT64, `name` STRING(MAX), \
             `adbc_ingest_key` STRING(36) DEFAULT (GENERATE_UUID())) \
             PRIMARY KEY (`adbc_ingest_key`)"
        );
        assert!(create_table_sql("t", &schema, true)
            .unwrap()
            .starts_with("CREATE TABLE IF NOT EXISTS `t`"));
    }

    #[test]
    fn builds_insert_sql() {
        assert_eq!(
            insert_sql("Users", &["Id".to_string(), "Name".to_string()]),
            "INSERT INTO `Users` (`Id`, `Name`) VALUES (@Id, @Name)"
        );
        assert_eq!(
            insert_sql("T", &["a".to_string()]),
            "INSERT INTO `T` (`a`) VALUES (@a)"
        );
        // Reserved words and embedded backticks are quoted/escaped so the DDL-shaped names the
        // ADBC ingest-escaping tests use (table `create`, column `index`) are valid identifiers.
        assert_eq!(
            insert_sql("create", &["index".to_string()]),
            "INSERT INTO `create` (`index`) VALUES (@index)"
        );
        assert_eq!(
            insert_sql("a`b", &["c`d".to_string()]),
            "INSERT INTO `a``b` (`c``d`) VALUES (@c`d)"
        );
    }

    fn batch(fields: Vec<Field>, columns: Vec<arrow_array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn int_batch(names: &[&str]) -> RecordBatch {
        batch(
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Int64, false))
                .collect(),
            names
                .iter()
                .map(|_| Arc::new(Int64Array::from(vec![1])) as arrow_array::ArrayRef)
                .collect(),
        )
    }

    #[test]
    fn resolves_parameters_by_name_when_columns_match() {
        // Every bound column names a query parameter -> bind by name, order-independent.
        let b = int_batch(&["b", "a"]);
        assert_eq!(
            resolve_parameter_names("SELECT @a, @b", &b).unwrap(),
            vec!["b", "a"],
        );
    }

    #[test]
    fn resolves_parameters_positionally_when_names_differ() {
        // Columns not named after the parameters (as positional clients / the validation suite
        // produce) -> i-th column binds to the i-th parameter in query order.
        let b = int_batch(&["res", "other"]);
        assert_eq!(
            resolve_parameter_names("INSERT INTO t VALUES (@p1, @p2)", &b).unwrap(),
            vec!["p1", "p2"],
        );
        // A single unmatched column still binds to the single parameter.
        let one = int_batch(&["res"]);
        assert_eq!(
            resolve_parameter_names("INSERT INTO t VALUES (@p1)", &one).unwrap(),
            vec!["p1"],
        );
    }

    #[test]
    fn positional_binding_rejects_count_mismatch() {
        let b = int_batch(&["x", "y"]);
        let err = resolve_parameter_names("SELECT @p1", &b).unwrap_err();
        assert!(
            err.message.contains("parameter count mismatch"),
            "unexpected error: {}",
            err.message
        );
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
    fn timestamp_string_preserves_full_precision() {
        // Microsecond formats six fractional digits: 2024-01-15T12:34:56.789012Z.
        let micros = 1_705_322_096_789_012i64;
        assert_eq!(
            timestamp_string("t", &TimeUnit::Microsecond, micros).unwrap(),
            "2024-01-15T12:34:56.789012Z"
        );
        // Nanosecond keeps all nine fractional digits — the sub-microsecond …012_999 ns is
        // preserved (Spanner TIMESTAMP has nanosecond precision), not truncated to micros.
        assert_eq!(
            timestamp_string("t", &TimeUnit::Nanosecond, micros * 1_000 + 999).unwrap(),
            "2024-01-15T12:34:56.789012999Z"
        );
        // Millisecond formats three fractional digits, Second none — each unit's own precision.
        assert_eq!(
            timestamp_string("t", &TimeUnit::Millisecond, 1_705_322_096_789).unwrap(),
            "2024-01-15T12:34:56.789Z"
        );
        assert_eq!(
            timestamp_string("t", &TimeUnit::Second, 1_705_322_096).unwrap(),
            "2024-01-15T12:34:56Z"
        );
        // A negative nanosecond value renders as its exact instant (one ns before the epoch), with
        // full nanosecond digits — not rounded/truncated toward zero.
        assert_eq!(
            timestamp_string("t", &TimeUnit::Nanosecond, -1).unwrap(),
            "1969-12-31T23:59:59.999999999Z"
        );
    }

    #[test]
    fn binds_timestamp_at_every_unit() {
        // Every Arrow TimestampArray unit binds without error (nanosecond, millisecond, second all
        // previously fell through to the unsupported-type rejection).
        let ns = 1_705_322_096_789_012_999i64;
        let b = batch(
            vec![
                Field::new("s", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new("m", DataType::Timestamp(TimeUnit::Millisecond, None), false),
                Field::new(
                    "n",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    false,
                ),
            ],
            vec![
                Arc::new(TimestampSecondArray::from(vec![1_705_322_096])),
                Arc::new(TimestampMillisecondArray::from(vec![1_705_322_096_789])),
                Arc::new(TimestampNanosecondArray::from(vec![ns]).with_timezone("UTC")),
            ],
        );
        assert!(bind_row(Statement::builder("SELECT @s, @m, @n"), &b, 0).is_ok());
    }

    #[test]
    fn binds_null_nanosecond_timestamp() {
        // A null nanosecond timestamp binds as a typed NULL, like the other temporal types.
        let b = batch(
            vec![Field::new(
                "n",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )],
            vec![Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]))],
        );
        assert!(bind_row(Statement::builder("SELECT @n"), &b, 0).is_ok());
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
    fn binds_large_string_and_binary() {
        // LargeUtf8 / LargeBinary (what pyarrow.Table.from_pandas emits) bind like their 32-bit
        // offset counterparts, including typed nulls.
        let b = batch(
            vec![
                Field::new("s", DataType::LargeUtf8, true),
                Field::new("b", DataType::LargeBinary, true),
            ],
            vec![
                Arc::new(LargeStringArray::from(vec![Some("hello"), None])),
                Arc::new(LargeBinaryArray::from(vec![Some(b"bytes".as_ref()), None])),
            ],
        );
        assert!(bind_row(Statement::builder("SELECT @s, @b"), &b, 0).is_ok());
        assert!(bind_row(Statement::builder("SELECT @s, @b"), &b, 1).is_ok());
    }

    #[test]
    fn extracts_named_parameters() {
        // Basic references, in order, with a later reuse deduped.
        assert_eq!(
            named_parameters("SELECT @a, @b FROM t WHERE @a > 0"),
            vec!["a", "b"]
        );
        // No parameters.
        assert_eq!(named_parameters("SELECT 1"), Vec::<String>::new());
        // `@` inside string literals and comments is not a parameter.
        assert_eq!(named_parameters("SELECT '@x', @y -- @z\n"), vec!["y"]);
        assert_eq!(named_parameters("SELECT @y /* @z */, @w"), vec!["y", "w"]);
        assert_eq!(named_parameters("SELECT `@col`, @p"), vec!["p"]);
        // Statement hints (`@{…}`) and system variables (`@@var`) are not parameters.
        assert_eq!(
            named_parameters("SELECT @{JOIN_METHOD=HASH_JOIN} * FROM t WHERE id = @id"),
            vec!["id"]
        );
        assert_eq!(named_parameters("SELECT @@rows"), Vec::<String>::new());
        // First-seen order is preserved across repeats.
        assert_eq!(named_parameters("@b @a @a @b @c"), vec!["b", "a", "c"]);
    }

    #[test]
    fn rejects_unsupported_arrow_type() {
        // A type with no Spanner mapping (UInt64 — Spanner has no unsigned integer) is rejected.
        let b = batch(
            vec![Field::new("x", DataType::UInt64, false)],
            vec![Arc::new(arrow_array::UInt64Array::from(vec![1u64]))],
        );
        assert!(bind_row(Statement::builder("SELECT @x"), &b, 0).is_err());
    }
}

//! Binding Arrow parameter data to Spanner statements.
//!
//! ADBC supplies statement parameters (and bulk-ingest rows) as an Arrow [`RecordBatch`]: each
//! column is one parameter, and each row is one set of bindings. Spanner uses **named** query
//! parameters (`@name`), so a bind column named `id` binds to `@id` in the SQL.
//!
//! Supported Arrow parameter types are `Int64`, `Float64`, `Float32`, `Boolean`, `Utf8`/`LargeUtf8`,
//! `Binary`/`LargeBinary`, `Date32` (→ `DATE`), `Timestamp` at any `TimeUnit`
//! (Second/Millisecond/Microsecond/Nanosecond, → `TIMESTAMP`), `Decimal128` (→ `NUMERIC`), and
//! their nulls. Other Arrow types are rejected with an `InvalidArguments` error.
//!
//! Spanner `TIMESTAMP` has microsecond precision, so a `Timestamp` parameter of any unit is
//! normalised to microseconds since the epoch before binding; sub-microsecond digits (from a
//! `Nanosecond` input) are truncated toward zero, matching the read path.
//!
//! Spanner encodes `DATE` / `TIMESTAMP` / `NUMERIC` values on the wire as strings, and query
//! parameters are sent untyped (Spanner infers the type from the SQL). So these three are formatted
//! straight to their Spanner string forms — `YYYY-MM-DD`, RFC 3339, and a plain decimal — which
//! keeps the full `Decimal128` (`i128`) range rather than routing through a narrower decimal type.

use adbc_core::error::Result;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int64Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
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
            // Spanner `TIMESTAMP` is UTC with microsecond precision, so every Arrow timestamp unit
            // is accepted; the four arms differ only in the typed array they read the raw i64 from,
            // and `timestamp_string` scales it to microseconds and formats the Spanner value.
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

/// Convert an Arrow timestamp `value` in `unit` to the Spanner `TIMESTAMP` wire form — an RFC 3339
/// string in UTC with microsecond precision, the same shape the read path parses back
/// (`conversion::parse_timestamp_micros`).
///
/// Spanner stores microseconds, so the value is first normalised to microseconds since the Unix
/// epoch: a `Nanosecond` input is divided by 1000, truncating any sub-microsecond digits toward
/// zero (Spanner cannot represent them). `name` is used only for the out-of-range error message.
fn timestamp_string(name: &str, unit: &TimeUnit, value: i64) -> Result<String> {
    let micros = match unit {
        TimeUnit::Second => value * 1_000_000,
        TimeUnit::Millisecond => value * 1_000,
        TimeUnit::Microsecond => value,
        TimeUnit::Nanosecond => value / 1_000,
    };
    let ts = DateTime::<Utc>::from_timestamp_micros(micros).ok_or_else(|| {
        invalid_argument(format!(
            "cannot bind TIMESTAMP parameter {name:?}: {micros} is out of range"
        ))
    })?;
    Ok(ts.to_rfc3339_opts(SecondsFormat::Micros, true))
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
fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
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
        Date32Array, Decimal128Array, Int32Array, LargeBinaryArray, LargeStringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow_schema::{Field, Schema};
    use google_cloud_spanner::statement::Statement;

    use super::*;

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
    fn timestamp_string_truncates_all_units_to_micros() {
        // Microsecond is the baseline: 2024-01-15T12:34:56.789012Z.
        let micros = 1_705_322_096_789_012i64;
        let expected = "2024-01-15T12:34:56.789012Z";
        assert_eq!(
            timestamp_string("t", &TimeUnit::Microsecond, micros).unwrap(),
            expected
        );
        // Nanosecond: the extra sub-microsecond digits (…012_999 ns) are truncated toward zero, so
        // the bound value equals the microsecond-truncated instant, not a rounded one.
        assert_eq!(
            timestamp_string("t", &TimeUnit::Nanosecond, micros * 1_000 + 999).unwrap(),
            expected
        );
        // Millisecond and Second scale up to the same instant (at their coarser precision).
        assert_eq!(
            timestamp_string("t", &TimeUnit::Millisecond, 1_705_322_096_789).unwrap(),
            "2024-01-15T12:34:56.789000Z"
        );
        assert_eq!(
            timestamp_string("t", &TimeUnit::Second, 1_705_322_096).unwrap(),
            "2024-01-15T12:34:56.000000Z"
        );
        // A negative nanosecond value truncates toward zero as well (integer division).
        assert_eq!(
            timestamp_string("t", &TimeUnit::Nanosecond, -1).unwrap(),
            "1970-01-01T00:00:00.000000Z"
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
        // A type with no mapping (Int32 here) is still rejected, unchanged by the new arms.
        let b = batch(
            vec![Field::new("x", DataType::Int32, false)],
            vec![Arc::new(Int32Array::from(vec![1]))],
        );
        assert!(bind_row(Statement::builder("SELECT @x"), &b, 0).is_err());
    }
}

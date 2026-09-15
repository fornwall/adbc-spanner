use std::sync::Arc;

use arrow_array::{
    Date32Array, Decimal128Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    ListArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{Field, Schema};
use google_cloud_spanner::statement::Statement;

use super::*;

/// Bind every column of `batch` at `row` as a synthetic positional parameter (`@p0`, `@p1`, …)
/// — a thin test driver over [`bind_params`] so the shared Arrow→Spanner conversion can be
/// exercised (and its wire encoding asserted via the built statement's `Debug` rendering)
/// without inventing per-test parameter names.
fn bind_row(
    builder: StatementBuilder,
    batch: &RecordBatch,
    row: usize,
) -> Result<StatementBuilder> {
    let names: Vec<String> = (0..batch.num_columns()).map(|i| format!("p{i}")).collect();
    bind_params(builder, &names, batch, row)
}

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
fn binds_unsigned_integers_as_int64() {
    // UInt8 / UInt16 / UInt32 widen losslessly to Spanner INT64; the boundary values (each
    // width's max) must bind as their exact decimal encoding, not overflow or wrap.
    let b = batch(
        vec![
            Field::new("a", DataType::UInt8, true),
            Field::new("b", DataType::UInt16, true),
            Field::new("c", DataType::UInt32, true),
        ],
        vec![
            Arc::new(arrow_array::UInt8Array::from(vec![Some(u8::MAX), None])),
            Arc::new(arrow_array::UInt16Array::from(vec![Some(u16::MAX), None])),
            Arc::new(arrow_array::UInt32Array::from(vec![Some(u32::MAX), None])),
        ],
    );
    let stmt = bind_row(Statement::builder("SELECT @p0, @p1, @p2"), &b, 0)
        .unwrap()
        .build();
    let dbg = format!("{stmt:?}");
    // u32::MAX = 4_294_967_295 round-trips as its exact decimal string (INT64 holds it).
    assert!(
        dbg.contains(r#"StringValue("4294967295")"#),
        "u32::MAX must widen to INT64 4294967295: {dbg}"
    );
    assert!(dbg.contains(r#"StringValue("255")"#), "u8::MAX: {dbg}");
    assert!(dbg.contains(r#"StringValue("65535")"#), "u16::MAX: {dbg}");
    // The null row still binds (typed null).
    assert!(bind_row(Statement::builder("SELECT @p0, @p1, @p2"), &b, 1).is_ok());
}

#[test]
fn binds_half_floats_as_doubles() {
    // Float16 has no Spanner counterpart; it widens into the FLOAT32 column
    // `spanner_column_type` creates. The widening is exact — every `f16` is
    // representable in `f32`/`f64` — so the bound value carries no rounding artefacts.
    let b = batch(
        vec![Field::new("v", DataType::Float16, true)],
        vec![Arc::new(arrow_array::Float16Array::from(vec![
            Some(<Float16Type as ArrowPrimitiveType>::Native::from_f32(-1.5)),
            None,
        ]))],
    );
    let stmt = bind_row(Statement::builder("SELECT @v"), &b, 0)
        .unwrap()
        .build();
    let dbg = format!("{stmt:?}");
    assert!(
        dbg.contains("-1.5"),
        "f16 -1.5 must widen exactly to -1.5: {dbg}"
    );
    // The null row still binds (typed null).
    assert!(bind_row(Statement::builder("SELECT @v"), &b, 1).is_ok());
}

#[test]
fn binds_fixed_size_binary_as_bytes() {
    // FixedSizeBinary(n) binds as Spanner BYTES, exactly like variable-width Binary.
    let values: Vec<Option<&[u8]>> = vec![Some(b"abcd"), None];
    let arr =
        arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 4)
            .unwrap();
    let b = batch(
        vec![Field::new("v", DataType::FixedSizeBinary(4), true)],
        vec![Arc::new(arr)],
    );
    // Ingested as an insert mutation, a 4-byte fixed cell lands as its raw BYTES value.
    let dbg = format!("{:?}", insert_mutation("t", &b, 0).unwrap());
    assert!(
        dbg.contains("StringValue") && dbg.contains("YWJjZA=="),
        "FixedSizeBinary cell must ship as base64 BYTES (\"abcd\" = YWJjZA==): {dbg}"
    );
    // A null fixed cell stays NULL.
    let dbg = format!("{:?}", insert_mutation("t", &b, 1).unwrap());
    assert!(dbg.contains("NullValue"), "null fixed cell: {dbg}");
}

/// A nullable string-family field tagged with the canonical `arrow.json` extension, as the
/// driver's own read path produces for Spanner `JSON` columns.
fn json_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true).with_metadata(std::collections::HashMap::from([(
        "ARROW:extension:name".to_string(),
        "arrow.json".to_string(),
    )]))
}

#[test]
fn binds_json_tagged_strings_as_json_params() {
    let b = batch(
        vec![
            json_field("doc", DataType::Utf8),
            Field::new("plain", DataType::Utf8, true),
        ],
        vec![
            Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#), None])),
            Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#), None])),
        ],
    );
    // Value row: the tagged column binds with an explicit JSON param type, the untagged one
    // stays untyped (Spanner infers STRING). Asserted via the built Statement's Debug
    // rendering, since its params are not otherwise readable from outside the client crate.
    let stmt = bind_row(Statement::builder("SELECT @p0, @p1"), &b, 0)
        .unwrap()
        .build();
    let dbg = format!("{stmt:?}");
    assert!(
        dbg.contains(r#""p0": Type(Type { code: Json"#),
        "no JSON param type for p0 in: {dbg}"
    );
    assert!(
        !dbg.contains(r#""p1": Type"#),
        "untagged p1 must stay untyped: {dbg}"
    );
    // Null row: the JSON type annotation must survive a typed null.
    let stmt = bind_row(Statement::builder("SELECT @p0, @p1"), &b, 1)
        .unwrap()
        .build();
    let dbg = format!("{stmt:?}");
    assert!(
        dbg.contains(r#""p0": Type(Type { code: Json"#),
        "typed null lost JSON type: {dbg}"
    );
}

#[test]
fn binds_json_tagged_list_elements_as_json_arrays() {
    let docs = {
        let mut b =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new())
                .with_field(Arc::new(json_field("item", DataType::Utf8)));
        b.values().append_value(r#"{"a":1}"#);
        b.values().append_null();
        b.append(true);
        b.finish()
    };
    let field = Field::new(
        "docs",
        DataType::List(Arc::new(json_field("item", DataType::Utf8))),
        true,
    );
    let b = batch(vec![field], vec![Arc::new(docs)]);
    let stmt = bind_row(Statement::builder("SELECT @p0"), &b, 0)
        .unwrap()
        .build();
    let dbg = format!("{stmt:?}");
    assert!(
        dbg.contains(r#""p0": Type(Type { code: Array"#)
            && dbg.contains("array_element_type: Some(Type { code: Json"),
        "no ARRAY<JSON> param type in: {dbg}"
    );
}

#[test]
fn json_tag_is_ignored_on_non_string_storage() {
    // `arrow.json` is only defined over string storage; a tag on Int64 must bind as INT64.
    let b = batch(
        vec![json_field("n", DataType::Int64)],
        vec![Arc::new(Int64Array::from(vec![Some(7)]))],
    );
    let stmt = bind_row(Statement::builder("SELECT @p0"), &b, 0)
        .unwrap()
        .build();
    let dbg = format!("{stmt:?}");
    assert!(
        !dbg.contains("Json"),
        "tagged Int64 mis-bound as JSON: {dbg}"
    );
}

#[test]
fn creates_json_columns_for_tagged_ingest_fields() {
    let schema = Schema::new(vec![
        json_field("doc", DataType::Utf8),
        Field::new(
            "docs",
            DataType::List(Arc::new(json_field("item", DataType::Utf8))),
            true,
        ),
        // The tag is honoured through dictionary encoding too (see `cell_value`), so a
        // dictionary-encoded JSON column creates JSON, not the value type's STRING(MAX).
        json_field(
            "cat",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ),
        Field::new("plain", DataType::Utf8, true),
    ]);
    let sql = create_table_sql("t", None, &schema, false).unwrap();
    assert!(
        sql.contains("`doc` JSON, `docs` ARRAY<JSON>, `cat` JSON, `plain` STRING(MAX)"),
        "unexpected DDL: {sql}"
    );
}

#[test]
fn maps_arrow_to_spanner_column_types() {
    assert_eq!(spanner_column_type(&DataType::Int32).unwrap(), "INT64");
    assert_eq!(spanner_column_type(&DataType::Int64).unwrap(), "INT64");
    assert_eq!(spanner_column_type(&DataType::Float32).unwrap(), "FLOAT32");
    // `Float16` has no Spanner type of its own; it widens losslessly into FLOAT32.
    assert_eq!(spanner_column_type(&DataType::Float16).unwrap(), "FLOAT32");
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
    // The unsigned widths that fit i64 losslessly widen to INT64.
    assert_eq!(spanner_column_type(&DataType::UInt8).unwrap(), "INT64");
    assert_eq!(spanner_column_type(&DataType::UInt16).unwrap(), "INT64");
    assert_eq!(spanner_column_type(&DataType::UInt32).unwrap(), "INT64");
    // FixedSizeBinary maps to BYTES like the variable-width binary kinds.
    assert_eq!(
        spanner_column_type(&DataType::FixedSizeBinary(4)).unwrap(),
        "BYTES(MAX)"
    );
    // UInt64 has no lossless INT64 mapping (u64::MAX > i64::MAX), so it is rejected.
    assert!(spanner_column_type(&DataType::UInt64).is_err());
}

#[test]
fn builds_create_table_sql() {
    let schema = Schema::new(vec![
        Field::new("idx", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    // No PRIMARY KEY clause and no extra column — Spanner adds its own hidden `rowid` key,
    // so the created table is exactly the ingest schema.
    assert_eq!(
        create_table_sql("my_table", None, &schema, false).unwrap(),
        "CREATE TABLE `my_table` (`idx` INT64, `name` STRING(MAX))"
    );
    assert!(
        create_table_sql("t", None, &schema, true)
            .unwrap()
            .starts_with("CREATE TABLE IF NOT EXISTS `t`")
    );
    // A named target schema (`adbc.ingest.target_db_schema`) qualifies the created table.
    assert!(
        create_table_sql("t", Some("app"), &schema, false)
            .unwrap()
            .starts_with("CREATE TABLE `app`.`t`")
    );
}

#[test]
fn builds_insert_mutations() {
    // A two-column row becomes one insert mutation carrying the raw column names and the same
    // wire values parameter binding produces (INT64 as a decimal string, STRING as-is, NULL as
    // a null value) — asserted via the mutation's Debug rendering, its only external view.
    let b = batch(
        vec![
            Field::new("Id", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![Some(7), Some(8)])),
            Arc::new(StringArray::from(vec![Some("Alice"), None])),
        ],
    );
    let dbg = format!("{:?}", insert_mutation("Users", &b, 0).unwrap());
    for needle in [
        "Insert",
        "\"Users\"",
        "\"Id\"",
        "\"Name\"",
        r#"StringValue("7")"#,
        r#"StringValue("Alice")"#,
    ] {
        assert!(dbg.contains(needle), "missing {needle} in: {dbg}");
    }
    // A null cell stays a null value in the mutation.
    let dbg = format!("{:?}", insert_mutation("Users", &b, 1).unwrap());
    assert!(dbg.contains("NullValue"), "no NULL for row 1: {dbg}");
    // Reserved words and odd characters need no quoting: mutations carry raw proto names,
    // not SQL.
    let odd = batch(
        vec![Field::new("index", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![42]))],
    );
    let dbg = format!("{:?}", insert_mutation("create", &odd, 0).unwrap());
    assert!(
        dbg.contains("\"create\"") && dbg.contains("\"index\""),
        "reserved-word names must pass through untouched: {dbg}"
    );
    // An unsupported Arrow type is rejected, same as parameter binding.
    let bad = batch(
        vec![Field::new("x", DataType::UInt64, false)],
        vec![Arc::new(arrow_array::UInt64Array::from(vec![1u64]))],
    );
    assert!(insert_mutation("t", &bad, 0).is_err());
}

#[test]
fn json_tagged_strings_ingest_as_plain_strings_in_mutations() {
    // Mutation values carry no per-value type: Spanner types them from the target column, so
    // an `arrow.json`-tagged string must land as its plain string encoding (a JSON column
    // accepts it), not some typed wrapper.
    let b = batch(
        vec![json_field("doc", DataType::Utf8)],
        vec![Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#)]))],
    );
    let dbg = format!("{:?}", insert_mutation("t", &b, 0).unwrap());
    assert!(
        dbg.contains(r#"StringValue("{\"a\":1}")"#),
        "JSON-tagged cell must be its plain string: {dbg}"
    );
}

#[test]
fn mutation_table_joins_schema_with_a_dot() {
    assert_eq!(mutation_table(None, "Users"), "Users");
    assert_eq!(mutation_table(Some(""), "Users"), "Users");
    assert_eq!(mutation_table(Some("app"), "Users"), "app.Users");
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
    // bind_by_name=true: every bound column names a query parameter -> bind by name,
    // order-independent.
    let b = int_batch(&["b", "a"]);
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b", &b, true).unwrap(),
        vec!["b", "a"],
    );
}

#[test]
fn resolves_parameters_positionally_by_default() {
    // The default (bind_by_name=false): i-th column binds to the i-th parameter in query
    // order, column names ignored — what positional clients / the validation suite produce.
    let b = int_batch(&["res", "other"]);
    assert_eq!(
        resolve_parameter_names("INSERT INTO t VALUES (@p1, @p2)", &b, false).unwrap(),
        vec!["p1", "p2"],
    );
    // A single column still binds to the single parameter.
    let one = int_batch(&["res"]);
    assert_eq!(
        resolve_parameter_names("INSERT INTO t VALUES (@p1)", &one, false).unwrap(),
        vec!["p1"],
    );
}

#[test]
fn by_name_mode_binds_by_name_and_leaves_extra_params_unbound() {
    // Strict by-name: order-independent, and a query parameter no column names (@c) is simply
    // left unbound (Spanner rejects that at execution time, not here).
    let b = int_batch(&["b", "a"]);
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b, @c", &b, true).unwrap(),
        vec!["b", "a"],
    );
}

#[test]
fn by_name_mode_rejects_an_unmatched_column_naming_the_parameter() {
    // bind_by_name=true: a bound column with no matching query parameter is a hard
    // InvalidArguments error naming the missing parameter — never a silent positional
    // fallback (which is what the default bind_by_name=false does with this input).
    let b = int_batch(&["a", "x"]);
    let err = resolve_parameter_names("SELECT @a, @b", &b, true).unwrap_err();
    assert_eq!(err.status, adbc_core::error::Status::InvalidArguments);
    assert!(
        err.message.contains("could not find parameter \"x\""),
        "error must name the missing parameter: {}",
        err.message
    );
}

#[test]
fn positional_binding_ignores_coincidental_name_matches() {
    // Every column name matches a query parameter, so by-name binding would reorder the values
    // (column "b" -> @b). The default positional binding must ignore the names entirely:
    // column 0 binds @a, column 1 binds @b.
    let b = int_batch(&["b", "a"]);
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b", &b, false).unwrap(),
        vec!["a", "b"],
    );
    // Contrast: by-name binding name-matches the very same input (see
    // resolves_parameters_by_name_when_columns_match).
    assert_eq!(
        resolve_parameter_names("SELECT @a, @b", &b, true).unwrap(),
        vec!["b", "a"],
    );
}

#[test]
fn positional_mode_still_requires_matching_counts() {
    // Even a column that names a parameter cannot save a count mismatch under the default
    // positional binding.
    let b = int_batch(&["a"]);
    let err = resolve_parameter_names("SELECT @a, @b", &b, false).unwrap_err();
    assert!(
        err.message.contains("parameter count mismatch"),
        "unexpected error: {}",
        err.message
    );
}

#[test]
fn bind_params_reuses_resolved_names_across_rows() {
    // The mapping is resolved once (lexing the SQL), then reused to bind every row — no
    // re-lexing per row. Both rows of a two-row batch bind against the same `names`.
    let b = batch(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![2, 4])),
        ],
    );
    let names = resolve_parameter_names("SELECT @a, @b", &b, false).unwrap();
    assert_eq!(names, vec!["a", "b"]);
    for row in 0..b.num_rows() {
        assert!(bind_params(Statement::builder("SELECT @a, @b"), &names, &b, row).is_ok());
    }
}

#[test]
fn positional_binding_rejects_count_mismatch() {
    let b = int_batch(&["x", "y"]);
    let err = resolve_parameter_names("SELECT @p1", &b, false).unwrap_err();
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
    // Every Arrow TimestampArray unit binds without error.
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
fn binds_view_strings_and_binaries() {
    // Utf8View / BinaryView (the German-string layouts polars and newer pyarrow emit) bind
    // like their offset-based counterparts, including typed nulls.
    let b = batch(
        vec![
            Field::new("s", DataType::Utf8View, true),
            Field::new("b", DataType::BinaryView, true),
        ],
        vec![
            Arc::new(arrow_array::StringViewArray::from(vec![
                Some("hello view"),
                None,
            ])),
            Arc::new(arrow_array::BinaryViewArray::from(vec![
                Some(b"view bytes".as_ref()),
                None,
            ])),
        ],
    );
    assert!(bind_row(Statement::builder("SELECT @s, @b"), &b, 0).is_ok());
    assert!(bind_row(Statement::builder("SELECT @s, @b"), &b, 1).is_ok());
}

#[test]
fn binds_int8_and_date64() {
    // Int8 widens to INT64 like the other narrow ints; Date64 (ms at day boundaries) binds as
    // a DATE string.
    let b = batch(
        vec![
            Field::new("i", DataType::Int8, true),
            Field::new("d", DataType::Date64, true),
        ],
        vec![
            Arc::new(arrow_array::Int8Array::from(vec![Some(7i8), None])),
            // 19737 days = 2024-01-15, at the exact millisecond day boundary.
            Arc::new(arrow_array::Date64Array::from(vec![
                Some(19_737i64 * 86_400_000),
                None,
            ])),
        ],
    );
    assert!(bind_row(Statement::builder("SELECT @i, @d"), &b, 0).is_ok());
    assert!(bind_row(Statement::builder("SELECT @i, @d"), &b, 1).is_ok());
}

#[test]
fn date64_converts_to_days() {
    assert_eq!(date64_days("d", 0).unwrap(), 0);
    assert_eq!(date64_days("d", 19_737i64 * 86_400_000).unwrap(), 19737);
    // Negative dates land on the correct (floored) day.
    assert_eq!(date64_days("d", -86_400_000).unwrap(), -1);
    assert_eq!(date64_days("d", -1).unwrap(), -1);
    // Out of Date32 range errors instead of wrapping.
    assert!(date64_days("d", i64::MAX).is_err());
}

#[test]
fn binds_view_arrays_as_list_elements() {
    use arrow_array::builder::{BinaryViewBuilder, ListBuilder, StringViewBuilder};
    let mut strings = ListBuilder::new(StringViewBuilder::new());
    strings.values().append_value("a");
    strings.values().append_null();
    strings.append(true);
    let mut bytes = ListBuilder::new(BinaryViewBuilder::new());
    bytes.values().append_value(b"b");
    bytes.append(true);
    let b = batch(
        vec![
            Field::new(
                "s",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
                true,
            ),
            Field::new(
                "b",
                DataType::List(Arc::new(Field::new("item", DataType::BinaryView, true))),
                true,
            ),
        ],
        vec![Arc::new(strings.finish()), Arc::new(bytes.finish())],
    );
    assert!(bind_row(Statement::builder("SELECT @s, @b"), &b, 0).is_ok());
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

// ------------------------------------------------------------------------------------------
// Dictionary-encoded columns (pandas categoricals over the C data interface).
// ------------------------------------------------------------------------------------------

#[test]
fn binds_dictionary_encoded_columns_as_their_values() {
    // A dictionary-encoded column binds its decoded values: the key at each row selects the
    // dictionary value, which runs through the same mapping as its plain form — strings bind
    // as STRING, Int32 values still widen to INT64, and a null cell binds NULL.
    let strings: arrow_array::DictionaryArray<Int32Type> =
        vec![Some("a"), None, Some("b"), Some("a")]
            .into_iter()
            .collect();
    let ints = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(1i8), None, Some(0), Some(1)]),
        Arc::new(Int32Array::from(vec![7i32, 9])),
    );
    let b = batch(
        vec![
            Field::new("s", strings.data_type().clone(), true),
            Field::new("i", ints.data_type().clone(), true),
        ],
        vec![Arc::new(strings), Arc::new(ints)],
    );
    let row0 = bound_params_debug(&b, 0);
    assert!(
        row0.contains(r#"StringValue("a")"#) && row0.contains(r#"StringValue("9")"#),
        "row 0 must bind the decoded values: {row0}"
    );
    assert!(
        !row0.contains("NumberValue"),
        "dictionary Int32 values must widen to INT64: {row0}"
    );
    let row1 = bound_params_debug(&b, 1);
    assert!(
        row1.contains("NullValue"),
        "null cells must bind NULL: {row1}"
    );
    let row2 = bound_params_debug(&b, 2);
    assert!(
        row2.contains(r#"StringValue("b")"#) && row2.contains(r#"StringValue("7")"#),
        "row 2 must bind the decoded values: {row2}"
    );
}

#[test]
fn binds_dictionary_encoded_array_cells() {
    // The dictionary value type re-enters the full cell mapping, so an ARRAY<...> value type
    // is accepted encoded too: key 0 selects the [1, NULL] cell.
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1i64), None]),
        Some(vec![Some(2)]),
    ]);
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0i8), None]),
        Arc::new(lists),
    );
    let b = batch(
        vec![Field::new("tags", dict.data_type().clone(), true)],
        vec![Arc::new(dict)],
    );
    let row0 = bound_params_debug(&b, 0);
    assert!(
        row0.contains(r#"StringValue("1")"#) && row0.contains("NullValue"),
        "row 0 must bind the decoded array cell: {row0}"
    );
    // A null cell of a List-valued dictionary is a null array (a bare NullValue).
    let row1 = bound_params_debug(&b, 1);
    assert!(
        row1.contains("NullValue") && !row1.contains("ListValue"),
        "null cell must bind as a null array: {row1}"
    );
}

#[test]
fn binds_dictionary_cells_of_a_sliced_batch_at_their_offset() {
    // A sliced dictionary column reads its keys through the sliced view: slice row 0 is
    // parent row 1 ("beta"), not the buffer-start "alpha".
    let dict: arrow_array::DictionaryArray<Int32Type> = vec![Some("alpha"), Some("beta"), None]
        .into_iter()
        .collect();
    let b = batch(
        vec![Field::new("name", dict.data_type().clone(), true)],
        vec![Arc::new(dict)],
    );
    let sliced = b.slice(1, 2);
    let row0 = bound_params_debug(&sliced, 0);
    assert!(
        row0.contains(r#"StringValue("beta")"#) && !row0.contains("alpha"),
        "row 0 was read from the unsliced buffer: {row0}"
    );
    let row1 = bound_params_debug(&sliced, 1);
    assert!(
        row1.contains("NullValue") && !row1.contains("beta"),
        "row 1 was read from the unsliced buffer: {row1}"
    );
}

#[test]
fn binds_non_null_key_pointing_at_null_dictionary_value_as_null() {
    // The second way a dictionary cell can be null: the key is valid but selects a null entry
    // *inside the values array* (the null-key form is covered above). The delegated scalar
    // binder's own null check must fire on the values array — this exact case stayed latent
    // for years in the ADBC postgres driver's already-supported string dictionaries
    // (apache/arrow-adbc, "insufficient data left in message"), so lock it in.
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0i8), Some(1)]), // both keys valid
        Arc::new(StringArray::from(vec![None, Some("a")])),     // values[0] is null
    );
    let b = batch(
        vec![Field::new("s", dict.data_type().clone(), true)],
        vec![Arc::new(dict)],
    );
    let row0 = bound_params_debug(&b, 0);
    assert!(
        row0.contains("NullValue") && !row0.contains("StringValue"),
        "key 0 selects a null dictionary value and must bind NULL: {row0}"
    );
    let row1 = bound_params_debug(&b, 1);
    assert!(
        row1.contains(r#"StringValue("a")"#),
        "key 1 selects a present value: {row1}"
    );
}

#[test]
fn binds_json_tagged_dictionary_strings_as_json_params() {
    // The `arrow.json` tag is honoured through dictionary encoding (the Arrow spec allows an
    // extension array to be dictionary-encoded, so its storage type is
    // `dictionary<indices, utf8>`): a present cell binds with the explicit JSON param type via
    // the plain-path delegation, and a null cell keeps it — the typed-null rule of the plain
    // path (`binds_json_tagged_strings_as_json_params`).
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0i8), None]),
        Arc::new(StringArray::from(vec![Some(r#"{"a":1}"#)])),
    );
    let b = batch(
        vec![json_field("doc", dict.data_type().clone())],
        vec![Arc::new(dict)],
    );
    for row in 0..2 {
        let dbg = bound_params_debug(&b, row);
        assert!(
            dbg.contains(r#""p0": Type(Type { code: Json"#),
            "row {row} lost the JSON param type: {dbg}"
        );
    }
}

#[test]
fn json_tag_is_ignored_on_non_string_dictionary_values() {
    // `json_tag_is_ignored_on_non_string_storage`, through the encoding: `arrow.json` is only
    // defined over string storage, so a tagged Dictionary(_, Int64) still binds as INT64.
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int8Array::from(vec![Some(0i8)]),
        Arc::new(Int64Array::from(vec![7i64])),
    );
    let b = batch(
        vec![json_field("n", dict.data_type().clone())],
        vec![Arc::new(dict)],
    );
    let dbg = bound_params_debug(&b, 0);
    assert!(
        !dbg.contains("Json"),
        "tagged Int64 dictionary mis-bound as JSON: {dbg}"
    );
}

#[test]
fn rejects_unsupported_dictionary_value_type() {
    // An unsupported *value* type is rejected with the same error as its plain form — on a
    // null cell too (the value type is validated on every row, like the other arms).
    let dict = arrow_array::DictionaryArray::new(
        arrow_array::Int32Array::from(vec![Some(0), None]),
        Arc::new(arrow_array::UInt64Array::from(vec![1u64])),
    );
    let b = batch(
        vec![Field::new("x", dict.data_type().clone(), true)],
        vec![Arc::new(dict)],
    );
    for row in 0..2 {
        let err = bind_row(Statement::builder("SELECT @x"), &b, row).unwrap_err();
        assert!(
            err.message.contains("unsupported Arrow type UInt64"),
            "row {row}: unexpected error: {}",
            err.message
        );
    }
}

#[test]
fn ingests_dictionary_encoded_columns_decoded() {
    // Mutation-based ingest shares `cell_value`, so a dictionary cell lands decoded; the
    // create-mode DDL mapping likewise sees through the encoding to the value type.
    let dict: arrow_array::DictionaryArray<Int32Type> =
        vec![Some("Alice"), None].into_iter().collect();
    let data_type = dict.data_type().clone();
    let b = batch(
        vec![Field::new("Name", data_type.clone(), true)],
        vec![Arc::new(dict)],
    );
    let dbg = format!("{:?}", insert_mutation("Users", &b, 0).unwrap());
    assert!(
        dbg.contains(r#"StringValue("Alice")"#),
        "mutation must carry the decoded value: {dbg}"
    );
    let dbg = format!("{:?}", insert_mutation("Users", &b, 1).unwrap());
    assert!(dbg.contains("NullValue"), "no NULL for row 1: {dbg}");
    assert_eq!(spanner_column_type(&data_type).unwrap(), "STRING(MAX)");
}

// ------------------------------------------------------------------------------------------
// ARRAY (List / LargeList) binding.
// ------------------------------------------------------------------------------------------

#[test]
fn array_elements_widen_and_keep_nulls_through_scalar_binder() {
    // The array path routes every element through the same `scalar_binder` as a scalar bind:
    // Int32 elements widen to Spanner INT64 (`StringValue("<digits>")`), a middle element null
    // stays `NullValue`, and a whole-cell null becomes a `NullValue` (a Spanner null array).
    // Asserted on the built statement's wire rendering, since a bound array's per-element wire
    // encoding is its only external view.
    let arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1i32), None, Some(3)]),
        None,
    ]);
    let b = batch(
        vec![Field::new("tags", arr.data_type().clone(), true)],
        vec![Arc::new(arr)],
    );
    let populated = bound_params_debug(&b, 0);
    for needle in [r#"StringValue("1")"#, "NullValue", r#"StringValue("3")"#] {
        assert!(
            populated.contains(needle),
            "row 0 missing {needle}: {populated}"
        );
    }
    // Widening really happened — no i32-specific `NumberValue` slipped through.
    assert!(
        !populated.contains("NumberValue"),
        "Int32 array element must widen to INT64: {populated}"
    );
    // A null array cell is a bare NullValue (no ListValue wrapper).
    let null_array = bound_params_debug(&b, 1);
    assert!(
        null_array.contains("NullValue") && !null_array.contains("ListValue"),
        "null array cell must be a NullValue: {null_array}"
    );
}

#[test]
fn binds_int64_array_including_null_array_and_null_element() {
    // A row with a populated array (incl. a null element), a fully-null array, and an empty
    // array all bind without error.
    let arr = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1i64), None, Some(3)]),
        None,
        Some(vec![]),
    ]);
    let b = batch(
        vec![Field::new("tags", arr.data_type().clone(), true)],
        vec![Arc::new(arr)],
    );
    for row in 0..3 {
        assert!(
            bind_row(
                Statement::builder("INSERT INTO t (tags) VALUES (@tags)"),
                &b,
                row
            )
            .is_ok()
        );
    }
}

#[test]
fn binds_arrays_of_each_supported_element_type() {
    use arrow_array::builder::{
        BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, ListBuilder,
        StringBuilder, TimestampMicrosecondBuilder,
    };

    let mut floats = ListBuilder::new(Float64Builder::new());
    floats.values().append_value(1.5);
    floats.values().append_null();
    floats.append(true);
    let floats = floats.finish();

    let mut bools = ListBuilder::new(BooleanBuilder::new());
    bools.values().append_value(true);
    bools.append(true);
    let bools = bools.finish();

    let mut strs = ListBuilder::new(StringBuilder::new());
    strs.values().append_value("hi");
    strs.append(true);
    let strs = strs.finish();

    let mut dates = ListBuilder::new(Date32Builder::new());
    dates.values().append_value(19737); // 2024-01-15
    dates.append(true);
    let dates = dates.finish();

    let mut ts = ListBuilder::new(TimestampMicrosecondBuilder::new());
    ts.values().append_value(1_705_322_096_789_012);
    ts.append(true);
    let ts = ts.finish();

    let mut nums = ListBuilder::new(
        Decimal128Builder::new()
            .with_precision_and_scale(38, 9)
            .unwrap(),
    );
    nums.values().append_value(1_500_000_000); // 1.5 at scale 9
    nums.append(true);
    let nums = nums.finish();

    let b = batch(
        vec![
            Field::new("f", floats.data_type().clone(), true),
            Field::new("bo", bools.data_type().clone(), true),
            Field::new("s", strs.data_type().clone(), true),
            Field::new("d", dates.data_type().clone(), true),
            Field::new("t", ts.data_type().clone(), true),
            Field::new("n", nums.data_type().clone(), true),
        ],
        vec![
            Arc::new(floats),
            Arc::new(bools),
            Arc::new(strs),
            Arc::new(dates),
            Arc::new(ts),
            Arc::new(nums),
        ],
    );
    assert!(bind_row(Statement::builder("SELECT @f, @bo, @s, @d, @t, @n"), &b, 0).is_ok());
}

#[test]
fn rejects_nested_array_element_type() {
    use arrow_array::builder::{Int64Builder, ListBuilder};

    // ARRAY<ARRAY<INT64>> has no Spanner representation and must be rejected clearly.
    let mut lb = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    lb.values().values().append_value(1);
    lb.values().append(true);
    lb.append(true);
    let arr = lb.finish();
    let b = batch(
        vec![Field::new("x", arr.data_type().clone(), true)],
        vec![Arc::new(arr)],
    );
    let err = bind_row(Statement::builder("SELECT @x"), &b, 0).unwrap_err();
    assert!(
        err.message.contains("unsupported element type"),
        "unexpected error: {}",
        err.message
    );
}

#[test]
fn spanner_column_type_maps_array_element_types() {
    // The write-path DDL mapping already recurses into the element type.
    let list = |dt: DataType| DataType::List(Arc::new(Field::new("item", dt, true)));
    assert_eq!(
        spanner_column_type(&list(DataType::Utf8)).unwrap(),
        "ARRAY<STRING(MAX)>"
    );
    assert_eq!(
        spanner_column_type(&list(DataType::Date32)).unwrap(),
        "ARRAY<DATE>"
    );
}

// ------------------------------------------------------------------------------------------
// Sliced batches / arrays (non-zero Arrow offsets).
//
// A caller can legitimately bind a sliced `RecordBatch` (e.g. `pyarrow.Table.slice`, or a
// consumer splitting bound data into chunks), whose arrays are views at a non-zero offset
// into shared buffers. Row `i` must then be read from the *sliced view*, not from position
// `i` of the underlying buffers — the upstream PostgreSQL ADBC driver had exactly that bug
// for sliced list arrays (apache/arrow-adbc#4320). These tests pin the behaviour down by
// asserting the actual bound values, via the built statement's `Debug` rendering — the only
// view of its parameters outside the client crate (`Statement.params` is `pub(crate)`
// there). The needles are exact wire encodings: `INT64` binds as `StringValue("<digits>")`,
// strings as `StringValue(..)`, and SQL nulls as `NullValue`.
// ------------------------------------------------------------------------------------------

/// Bind row `row` of `batch` as `@p0…@pN` (via [`bind_row`]) and render the built statement,
/// parameters included, for substring assertions on the bound values.
fn bound_params_debug(batch: &RecordBatch, row: usize) -> String {
    let params = (0..batch.num_columns())
        .map(|i| format!("@p{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let stmt = bind_row(Statement::builder(format!("SELECT {params}")), batch, row)
        .unwrap()
        .build();
    format!("{stmt:?}")
}

#[test]
fn binds_rows_of_a_sliced_batch_at_their_offset() {
    // Three-row parent; the bound batch is `slice(1, 2)`, so slice row 0 is parent row 1 and
    // slice row 1 is parent row 2 (all null). Reading from position 0 of the underlying
    // buffers instead would bind 10 / "alpha".
    let parent = batch(
        vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None])),
            Arc::new(arrow_array::StringArray::from(vec![
                Some("alpha"),
                Some("beta"),
                None,
            ])),
        ],
    );
    let sliced = parent.slice(1, 2);
    assert_eq!(sliced.num_rows(), 2);

    let row0 = bound_params_debug(&sliced, 0);
    assert!(row0.contains(r#"StringValue("20")"#), "row 0: {row0}");
    assert!(row0.contains(r#"StringValue("beta")"#), "row 0: {row0}");
    assert!(
        !row0.contains("alpha") && !row0.contains(r#"StringValue("10")"#),
        "row 0 was read from the unsliced buffer: {row0}"
    );

    // Slice row 1 (parent row 2) is null in both columns; nothing from earlier rows leaks in.
    let row1 = bound_params_debug(&sliced, 1);
    assert!(row1.contains("NullValue"), "row 1: {row1}");
    assert!(
        !row1.contains("beta") && !row1.contains(r#"StringValue("20")"#),
        "row 1 was read from the unsliced buffer: {row1}"
    );
}

#[test]
fn binds_list_cells_of_a_sliced_batch_at_their_offset() {
    // The apache/arrow-adbc#4320 shape: a list column in a sliced batch. Slice row 0 must
    // bind parent row 1's cell [3, NULL, 5] — not the buffer-start cell [1, 2].
    let arr = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1i64), Some(2)]),
        Some(vec![Some(3), None, Some(5)]),
        Some(vec![Some(6)]),
    ]);
    let b = batch(
        vec![Field::new("tags", arr.data_type().clone(), true)],
        vec![Arc::new(arr)],
    );
    let sliced = b.slice(1, 2);

    let row0 = bound_params_debug(&sliced, 0);
    for needle in [r#"StringValue("3")"#, "NullValue", r#"StringValue("5")"#] {
        assert!(row0.contains(needle), "row 0 missing {needle}: {row0}");
    }
    assert!(
        !row0.contains(r#"StringValue("1")"#) && !row0.contains(r#"StringValue("2")"#),
        "row 0 was read from the unsliced buffer: {row0}"
    );

    let row1 = bound_params_debug(&sliced, 1);
    assert!(row1.contains(r#"StringValue("6")"#), "row 1: {row1}");
    assert!(
        !row1.contains(r#"StringValue("3")"#),
        "row 1 was read from the unsliced buffer: {row1}"
    );
}

#[test]
fn binds_string_list_cells_of_a_sliced_batch_at_their_offset() {
    use arrow_array::builder::{ListBuilder, StringBuilder};

    // Same shape with variable-width elements: both the list offsets and the child string
    // offsets are shared with the sliced-away row.
    let mut sb = ListBuilder::new(StringBuilder::new());
    sb.values().append_value("skip");
    sb.append(true);
    sb.values().append_value("keep");
    sb.values().append_null();
    sb.append(true);
    let strings = sb.finish();
    let b = batch(
        vec![Field::new("names", strings.data_type().clone(), true)],
        vec![Arc::new(strings)],
    );
    let sliced = b.slice(1, 1);

    let row0 = bound_params_debug(&sliced, 0);
    assert!(row0.contains(r#"StringValue("keep")"#), "row 0: {row0}");
    assert!(row0.contains("NullValue"), "row 0: {row0}");
    assert!(
        !row0.contains("skip"),
        "row 0 was read from the unsliced buffer: {row0}"
    );
}

//! Criterion benchmarks for the driver's hottest path: decoding Spanner wire values into Arrow
//! arrays (`src/conversion.rs`), reached through the `#[doc(hidden)]` `bench_support` module.
//!
//! Everything runs offline — the synthetic [`Value`]s below mirror exactly what the unit tests in
//! `src/conversion.rs` construct (Spanner ships `INT64`/`DATE`/`TIMESTAMP`/`NUMERIC` as strings,
//! `BOOL`/`FLOAT64` natively, `ARRAY`/`STRUCT` as nested lists) — no network, no emulator.
//!
//! Run with `cargo bench` (or `cargo bench -- --test` for a fast single-pass sanity run). Each
//! benchmark converts one default-size streaming chunk (`spanner.rows_per_batch` = 8192 rows), so
//! results read as "time to convert one chunk"; throughput is reported in rows/second.

use std::hint::black_box;
use std::sync::Arc;

use adbc_spanner::bench_support::build_array;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use google_cloud_spanner::value::{ToValue, Value};

/// Rows per benchmarked chunk — the driver's default `spanner.rows_per_batch`.
const ROWS: usize = 8192;

/// Build one column of `ROWS` wire values.
fn column(f: impl Fn(usize) -> Value) -> Vec<Value> {
    (0..ROWS).map(f).collect()
}

/// Borrow a column the way `build_batch` hands slices to `build_array`.
fn refs(column: &[Value]) -> Vec<Option<&Value>> {
    column.iter().map(Some).collect()
}

/// Convert every column of a prepared batch, as `build_batch` does once per streamed chunk.
fn convert(columns: &[(DataType, Vec<Option<&Value>>)]) {
    for (data_type, values) in columns {
        black_box(build_array(data_type, values).expect("benchmark values must decode"));
    }
}

fn bench_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_to_arrow");
    // Rows per iteration, so criterion reports rows/second per chunk conversion.
    group.throughput(Throughput::Elements(ROWS as u64));

    // Scalar-heavy chunk: INT64 + FLOAT64 + STRING + BOOL, the common flat result shape. INT64
    // arrives as a decimal string (parsed), FLOAT64/BOOL natively, STRING copied verbatim. A
    // sprinkle of SQL NULLs keeps the null path honest.
    {
        let ints = column(|i| {
            if i % 16 == 0 {
                None::<i64>.to_value()
            } else {
                (i as i64).wrapping_mul(2_654_435_761).to_value()
            }
        });
        let floats = column(|i| (i as f64 * 0.25).to_value());
        let strings = column(|i| format!("row-{i}-payload").to_value());
        let bools = column(|i| (i % 3 == 0).to_value());
        let columns = [
            (DataType::Int64, refs(&ints)),
            (DataType::Float64, refs(&floats)),
            (DataType::Utf8, refs(&strings)),
            (DataType::Boolean, refs(&bools)),
        ];
        group.bench_function("scalars_int64_float64_string_bool", |b| {
            b.iter(|| convert(&columns))
        });
    }

    // String-parsed temporal/decimal chunk: DATE ("YYYY-MM-DD" → Date32 days), TIMESTAMP
    // (RFC 3339 → epoch nanoseconds) and NUMERIC (decimal string → unscaled i128 at scale 9) —
    // the per-value parsing arms with real work in them.
    {
        let dates = column(|i| format!("2024-{:02}-{:02}", 1 + i % 12, 1 + i % 28).to_value());
        let timestamps = column(|i| {
            format!(
                "2024-01-15T12:{:02}:{:02}.{:09}Z",
                (i / 60) % 60,
                i % 60,
                i % 1_000_000_000
            )
            .to_value()
        });
        let numerics = column(|i| format!("{}.{:09}", i, i % 1_000_000_000).to_value());
        let columns = [
            (DataType::Date32, refs(&dates)),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                refs(&timestamps),
            ),
            (DataType::Decimal128(38, 9), refs(&numerics)),
        ];
        group.bench_function("temporal_date_timestamp_numeric", |b| {
            b.iter(|| convert(&columns))
        });
    }

    // DATE-only chunk: `YYYY-MM-DD` → Date32 epoch-days, isolated from the combined temporal bench
    // so the per-cell DATE conversion cost (PERF-3) reads directly. A sprinkle of SQL NULLs keeps
    // the null path honest.
    {
        let dates = column(|i| {
            if i % 16 == 0 {
                None::<String>.to_value()
            } else {
                format!("2024-{:02}-{:02}", 1 + i % 12, 1 + i % 28).to_value()
            }
        });
        let columns = [(DataType::Date32, refs(&dates))];
        group.bench_function("dates_date32", |b| b.iter(|| convert(&columns)));
    }

    // BYTES chunk: base64-encoded payloads (48 decoded bytes per cell) — the per-cell decode arm
    // (which reuses one scratch buffer across cells, PERF-2). A sprinkle of SQL NULLs keeps the
    // null path honest.
    {
        use base64::Engine as _;
        let bytes = column(|i| {
            if i % 16 == 0 {
                None::<String>.to_value()
            } else {
                let payload: Vec<u8> = (0..48).map(|j| ((i + j) % 256) as u8).collect();
                base64::engine::general_purpose::STANDARD
                    .encode(&payload)
                    .to_value()
            }
        });
        let columns = [(DataType::Binary, refs(&bytes))];
        group.bench_function("bytes_binary", |b| b.iter(|| convert(&columns)));
    }

    // Nested chunk: ARRAY<INT64> (8 elements per row) and STRUCT<id INT64, name STRING> (encoded
    // positionally, as on the wire) — the recursive list/struct assembly paths.
    {
        let lists = column(|i| {
            (0..8)
                .map(|j| ((i * 8 + j) as i64).to_value())
                .collect::<Vec<_>>()
                .to_value()
        });
        let structs =
            column(|i| vec![(i as i64).to_value(), format!("name-{i}").to_value()].to_value());
        let columns = [
            (
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                refs(&lists),
            ),
            (
                DataType::Struct(Fields::from(vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("name", DataType::Utf8, true),
                ])),
                refs(&structs),
            ),
        ];
        group.bench_function("nested_array_int64_struct", |b| {
            b.iter(|| convert(&columns))
        });
    }

    group.finish();
}

criterion_group!(benches, bench_conversion);
criterion_main!(benches);

# Benchmarks

Compare Arrow-to-Polars reads with the official `google-cloud-spanner` Python client on the
Spanner emulator. Run from the repository root with Docker available. All scripts require
`SPANNER_EMULATOR_HOST`; `scripts/with-emulator.sh` starts and removes a temporary emulator.

## Setup (Linux)

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install 'adbc-driver-manager[dbapi]' polars google-cloud-spanner
cargo build --release
cp target/release/libspanner_adbc.so python/adbc_driver_spanner/
```

Re-copy the library after rebuilding so benchmarks use the new binary.

## End-to-end comparison

`benchmark_polars.py` downloads a mixed-type table into Polars and computes a column mean.
It checks that both clients return the same row count and mean, then reports minimum and median
elapsed times after warm-up. Table setup is excluded from those timings.

```sh
PYTHONPATH=python scripts/with-emulator.sh \
  .venv/bin/python python/benchmarks/benchmark_polars.py --rows 1000000 --repeat 3
```

## Stage timings

`profile_stages.py` separates query execution, fetching, DataFrame construction, and aggregation,
and varies `spanner.rows_per_batch`. Set `PROFILE_ROWS` to change the default 200,000 rows.

```sh
PYTHONPATH=python scripts/with-emulator.sh \
  .venv/bin/python python/benchmarks/profile_stages.py
```

## CPU profiling

`perf_read.py` sets up a table, then repeatedly reads Arrow results without Polars or the official
client. `PERF_ROWS` and `PERF_LOOPS` default to 500,000 and 8. For frame-pointer profiling, rebuild
and stage the library before recording:

```sh
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile profiling
cp target/profiling/libspanner_adbc.so python/adbc_driver_spanner/
PYTHONPATH=python scripts/with-emulator.sh \
  perf record -F 199 --call-graph fp -o perf.data -- \
  .venv/bin/python python/benchmarks/perf_read.py
perf report -i perf.data
```

The recording includes setup; focus on the repeated read loop when inspecting samples.
Record the driver revision, dependency versions, row count, and machine with any results.
Emulator timings do not predict production Spanner throughput.

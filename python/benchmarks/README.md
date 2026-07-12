# Benchmarks

Micro-benchmarks comparing this ADBC driver against the official
`google-cloud-spanner` Python client, run against the Spanner **emulator**. The
question they answer: is it worth handing Arrow straight to Polars through this
driver, or is there no real difference versus the generic client?

All three scripts share setup helpers and self-skip without `SPANNER_EMULATOR_HOST`.
Run them under `scripts/with-emulator.sh`, which starts a throwaway emulator.

## Setup

```sh
python -m venv .venv && . .venv/bin/activate
pip install adbc_driver_manager pyarrow polars google-cloud-spanner
# Build + stage the driver library next to the package (the wheel would bundle it):
cargo build --release
cp target/release/libadbc_spanner.so python/adbc_driver_spanner/
```

## `benchmark_polars.py` — the headline comparison

Downloads a whole table, hands it to Polars, computes a column mean, and times
both engines end to end. ADBC ingests the result as Arrow and Polars takes it
zero-copy (`pl.from_arrow`); the official client yields per-row Python lists that
are rebuilt into a `DataFrame`.

```sh
PYTHONPATH=python scripts/with-emulator.sh \
  .venv/bin/python python/benchmarks/benchmark_polars.py --rows 1000000 --repeat 3
```

## `profile_stages.py` — where the time goes

Splits each engine into stages (SQL execute / download / Polars ingest / mean) and
sweeps `spanner.rows_per_batch`, to separate download cost from conversion cost.

## `perf_read.py` — a clean `perf` target

A minimal read-only loop (no Polars, no official client) so a CPU profile is
dominated by the driver + client read path:

```sh
PYTHONPATH=python scripts/with-emulator.sh bash -c \
  'perf record -F 199 --call-graph fp -o perf.data -- \
   .venv/bin/python python/benchmarks/perf_read.py'
perf report -i perf.data
```

(Use `--call-graph fp` with a frame-pointer build —
`RUSTFLAGS="-C force-frame-pointers=yes" CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --release` —
since DWARF can't unwind through libc's `memmove`.)

## Note on results

Read throughput is currently gated by an O(n²) row-assembly loop in the pinned
`google-cloud-spanner` client (`process_partial_result_set`), which caps it at a
flat ~40k rows/s. The fix (fork PR `fornwall/google-cloud-rust#2`) lifts it to
~290k rows/s (~7×) and makes the ADBC driver ~2× faster than the official client
on this workload. Until that fix lands in the pinned rev, these benchmarks will
show the driver at parity-or-slower — the Arrow/Polars advantage is real but
masked by the client read cost.

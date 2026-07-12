#!/usr/bin/env python3
"""Benchmark: official google-cloud-spanner client vs. this ADBC driver.

Both engines do the *same* job against the *same* emulator table: download the
whole table, hand it to Polars, and compute a column mean. The only thing that
differs is how the rows get from Spanner into a ``polars.DataFrame``:

* **ADBC** streams the result as Apache Arrow record batches and Polars ingests
  the ``pyarrow.Table`` zero-copy (``pl.from_arrow``). No per-row Python objects
  are ever created.
* **Official client** yields each row as a Python ``list`` from a
  ``StreamedResultSet``; we collect them and build the ``DataFrame`` row-wise
  (``pl.DataFrame(rows, orient="row")``) — the idiomatic way to get columnar
  data out of the client. Every cell becomes a Python object on the way.

That difference is the whole point of an Arrow-native driver, and this
benchmark measures it end to end (download + convert + one Polars aggregation).

Run it against a throwaway emulator:

    scripts/with-emulator.sh <venv>/bin/python python/benchmarks/benchmark_polars.py

Options: ``--rows N`` (default 100000), ``--repeat K`` (default 5),
``--batch B`` (ingest batch size for setup, default 20000).

Requires ``SPANNER_EMULATOR_HOST`` (set by ``with-emulator.sh``) and the
bundled driver library next to ``adbc_driver_spanner`` (see the benchmark
README). The instance/database are created here over the emulator's REST admin
API, exactly like the Python test suite's conftest.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request

# Fixed emulator ids, matching the Rust integration test and the Python conftest
# so the benchmark reuses whatever those already created.
PROJECT = "test-project"
INSTANCE = "test-instance"
DATABASE = "adbc-test"
TABLE = "BenchPolars"

# Columns of the benchmark table. A deliberately mixed set of types so the
# row->columnar conversion the official client forces is representative, not a
# single-column best case. `value` is the FLOAT64 we take the mean of.
COLUMNS = "id, value, quantity, category, active"
DDL = (
    f"CREATE TABLE {TABLE} ("
    "  id INT64 NOT NULL,"
    "  value FLOAT64,"
    "  quantity INT64,"
    "  category STRING(64),"
    "  active BOOL,"
    ") PRIMARY KEY (id)"
)


# --------------------------------------------------------------------------- #
# Emulator admin bootstrap (REST, stdlib only) — same approach as conftest.py.
# --------------------------------------------------------------------------- #
def _rest_base() -> str:
    grpc = os.environ["SPANNER_EMULATOR_HOST"]
    host = grpc.rsplit(":", 1)[0] or "localhost"
    port = os.environ.get("SPANNER_EMULATOR_REST_PORT", "9020")
    return f"http://{host}:{port}"


def _post(url: str, body: dict):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        if exc.code == 409:  # already exists -> idempotent
            return None
        raise


def _await_operation(op) -> None:
    if not op or op.get("done"):
        return
    name = op.get("name")
    if not name:
        return
    for _ in range(120):
        with urllib.request.urlopen(f"{_rest_base()}/v1/{name}", timeout=30) as resp:
            got = json.loads(resp.read() or b"{}")
        if got.get("done"):
            if "error" in got:
                raise RuntimeError(f"operation {name} failed: {got['error']}")
            return
        time.sleep(0.5)
    raise RuntimeError(f"operation {name} did not complete in time")


def ensure_database() -> str:
    """Create the instance + database if absent; return the database path."""
    base = _rest_base()
    _await_operation(
        _post(
            f"{base}/v1/projects/{PROJECT}/instances",
            {
                "instanceId": INSTANCE,
                "instance": {
                    "config": f"projects/{PROJECT}/instanceConfigs/emulator-config",
                    "displayName": "ADBC benchmark instance",
                    "nodeCount": 1,
                },
            },
        )
    )
    _await_operation(
        _post(
            f"{base}/v1/projects/{PROJECT}/instances/{INSTANCE}/databases",
            {"createStatement": f"CREATE DATABASE `{DATABASE}`"},
        )
    )
    return f"projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}"


# --------------------------------------------------------------------------- #
# Setup: (re)create and populate the benchmark table via the ADBC driver's
# bulk-ingest path. Population is NOT part of the measured workload — it just
# needs to be fast, and native insert mutations are the quickest way in.
# --------------------------------------------------------------------------- #
def build_data(n: int):
    import pyarrow as pa

    ids = list(range(n))
    # Deterministic, mildly varied values so the mean is a real (non-trivial)
    # number and both engines must agree on it exactly.
    value = [((i * 7) % 10_000) / 100.0 for i in ids]
    quantity = [i % 500 for i in ids]
    category = [f"cat-{i % 20}" for i in ids]
    active = [(i % 2) == 0 for i in ids]
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array(value, type=pa.float64()),
            "quantity": pa.array(quantity, type=pa.int64()),
            "category": pa.array(category, type=pa.string()),
            "active": pa.array(active, type=pa.bool_()),
        }
    )


def setup_table(database: str, n: int, batch: int) -> None:
    import adbc_driver_spanner.dbapi as spanner_adbc

    data = build_data(n)
    conn = spanner_adbc.connect(
        db_kwargs={"uri": f"spanner:///{database}", "spanner.emulator": "true"},
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
            cur.execute(DDL)
        # Ingest in slices purely for progress reporting; the driver chunks and
        # commits internally under the emulator's per-commit mutation cap.
        loaded = 0
        for start in range(0, n, batch):
            slice_ = data.slice(start, min(batch, n - start))
            with conn.cursor() as cur:
                cur.adbc_ingest(TABLE, slice_, mode="append")
            loaded += slice_.num_rows
            print(f"    populated {loaded}/{n} rows", end="\r", flush=True)
        print(f"    populated {loaded}/{n} rows")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# The two engines under test. Each returns (mean, n_rows) and is timed as a
# whole: SQL execute + full download + Polars ingest + one aggregation.
# --------------------------------------------------------------------------- #
def run_adbc(database: str):
    """ADBC: Arrow record batches -> zero-copy Polars -> mean."""
    import polars as pl

    import adbc_driver_spanner.dbapi as spanner_adbc

    conn = spanner_adbc.connect(
        db_kwargs={"uri": f"spanner:///{database}", "spanner.emulator": "true"},
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {COLUMNS} FROM {TABLE}")
            table = cur.fetch_arrow_table()  # pyarrow.Table, columnar
        df = pl.from_arrow(table)  # zero-copy: shares Arrow buffers
        return df["value"].mean(), df.height
    finally:
        conn.close()


def run_official(database: str):
    """Official client: per-row Python lists -> Polars (row-wise) -> mean."""
    import polars as pl
    from google.cloud import spanner

    schema = {
        "id": pl.Int64,
        "value": pl.Float64,
        "quantity": pl.Int64,
        "category": pl.Utf8,
        "active": pl.Boolean,
    }
    # With SPANNER_EMULATOR_HOST set the client auto-uses anonymous credentials
    # and plaintext gRPC against the emulator.
    _, project, _, instance_id, _, database_id = database.split("/")
    client = spanner.Client(project=project)
    instance = client.instance(instance_id)
    db = instance.database(database_id)
    with db.snapshot() as snapshot:
        result = snapshot.execute_sql(f"SELECT {COLUMNS} FROM {TABLE}")
        rows = list(result)  # each row materialized as a Python list
    df = pl.DataFrame(rows, schema=schema, orient="row")
    return df["value"].mean(), df.height


# --------------------------------------------------------------------------- #
# Timing harness + report.
# --------------------------------------------------------------------------- #
def timeit(fn, repeat: int):
    # One warm-up run (not counted) to fault in gRPC channels, sessions, imports.
    result = fn()
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return result, times


def summarize(name: str, times, rows: int) -> dict:
    best = min(times)
    med = statistics.median(times)
    return {
        "name": name,
        "min": best,
        "median": med,
        "rows_per_s": rows / best,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--batch", type=int, default=20_000)
    args = parser.parse_args()

    if not os.environ.get("SPANNER_EMULATOR_HOST"):
        print(
            "SPANNER_EMULATOR_HOST is not set. Run under scripts/with-emulator.sh, e.g.\n"
            "  scripts/with-emulator.sh <venv>/bin/python "
            "python/benchmarks/benchmark_polars.py",
            file=sys.stderr,
        )
        return 2

    print(f">> emulator: {os.environ['SPANNER_EMULATOR_HOST']}")
    database = ensure_database()
    print(f">> database: {database}")

    print(f">> setup: creating {TABLE} and loading {args.rows} rows")
    setup_table(database, args.rows, args.batch)

    print(f">> benchmarking (repeat={args.repeat}) — download whole table, Polars mean(value)")
    adbc_mean, adbc_rows = None, None
    results = []
    for name, fn in (("adbc", run_adbc), ("official", run_official)):
        print(f"    running: {name} ...", flush=True)
        (mean, rows), times = timeit(lambda fn=fn: fn(database), args.repeat)
        results.append((summarize(name, times, rows), mean, rows))

    # Correctness: both engines must agree on the mean and the row count.
    (_, mean_a, rows_a), (_, mean_b, rows_b) = results
    assert rows_a == rows_b == args.rows, (rows_a, rows_b, args.rows)
    assert abs(mean_a - mean_b) < 1e-9, (mean_a, mean_b)

    print()
    print(f"  rows            : {rows_a}")
    print(f"  mean(value)     : {mean_a:.6f}  (both engines agree)")
    print()
    header = f"  {'engine':<10}{'min (s)':>12}{'median (s)':>14}{'rows/s':>14}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    summaries = [s for s, _, _ in results]
    for s in summaries:
        print(
            f"  {s['name']:<10}{s['min']:>12.4f}{s['median']:>14.4f}{s['rows_per_s']:>14,.0f}"
        )
    by_name = {s["name"]: s for s in summaries}
    speedup = by_name["official"]["min"] / by_name["adbc"]["min"]
    print()
    print(f"  => ADBC is {speedup:.1f}x faster than the official client on this workload")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

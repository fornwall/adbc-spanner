#!/usr/bin/env python3
"""Minimal ADBC read loop for `perf record` — isolates the driver+client read path.

Populates a table once, then reads the whole thing back as Arrow N times so the
profile is dominated by the streaming read (client `rs.next()` + our Arrow
build), with no Polars / official-client / timing code to muddy the samples.

    scripts/with-emulator.sh bash -c \
      'perf record -g --call-graph dwarf -o perf.data -- \
       <venv>/bin/python python/benchmarks/perf_read.py'
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import benchmark_polars as bp  # noqa: E402

N = int(os.environ.get("PERF_ROWS", "500000"))
LOOPS = int(os.environ.get("PERF_LOOPS", "8"))


def main() -> int:
    if not os.environ.get("SPANNER_EMULATOR_HOST"):
        print("SPANNER_EMULATOR_HOST not set", file=sys.stderr)
        return 2
    database = bp.ensure_database()
    print(f">> setup {N} rows", flush=True)
    bp.setup_table(database, N, 20000)

    import adbc_driver_spanner.dbapi as spanner_adbc

    conn = spanner_adbc.connect(
        db_kwargs={"uri": f"spanner:///{database}", "spanner.emulator": "true"},
        autocommit=True,
    )
    try:
        print(f">> {LOOPS} read loops of {N} rows (profile window)", flush=True)
        t0 = time.perf_counter()
        total = 0
        for i in range(LOOPS):
            with conn.cursor() as cur:
                cur.execute(f"SELECT {bp.COLUMNS} FROM {bp.TABLE}")
                table = cur.fetch_arrow_table()
            total += table.num_rows
        dt = time.perf_counter() - t0
        print(f">> read {total} rows in {dt:.2f}s ({total / dt:,.0f} rows/s)", flush=True)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

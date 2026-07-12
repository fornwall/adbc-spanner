#!/usr/bin/env python3
"""Localize where the ADBC read path spends its time vs. the official client.

Breaks each engine into stages so we can see whether the cost is the network
download, the Spanner->Arrow conversion, or the Polars ingest — and sweeps
`spanner.rows_per_batch` to separate per-chunk round-trip overhead from
per-row conversion cost.
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import benchmark_polars as bp  # noqa: E402

N = int(os.environ.get("PROFILE_ROWS", "200000"))


def t(fn):
    t0 = time.perf_counter()
    r = fn()
    return r, time.perf_counter() - t0


def profile_adbc(database, rows_per_batch=None):
    import polars as pl

    import adbc_driver_spanner.dbapi as spanner_adbc
    from adbc_driver_spanner import DatabaseOptions, StatementOptions

    conn = spanner_adbc.connect(
        db_kwargs={
            DatabaseOptions.URI.value: f"spanner:///{database}",
            DatabaseOptions.EMULATOR.value: "true",
        },
        autocommit=True,
    )
    try:
        cur = conn.cursor()
        if rows_per_batch is not None:
            cur.adbc_statement.set_options(
                **{StatementOptions.ROWS_PER_BATCH.value: str(rows_per_batch)}
            )
        _, t_exec = t(lambda: cur.execute(f"SELECT {bp.COLUMNS} FROM {bp.TABLE}"))
        table, t_fetch = t(lambda: cur.fetch_arrow_table())
        df, t_poll = t(lambda: pl.from_arrow(table))
        _, t_mean = t(lambda: df["value"].mean())
        cur.close()
        tag = f"adbc rpb={rows_per_batch or 'default'}"
        print(
            f"  {tag:<22} exec={t_exec:7.4f} fetch_arrow={t_fetch:7.4f} "
            f"from_arrow={t_poll:7.4f} mean={t_mean:7.4f} "
            f"TOTAL={t_exec + t_fetch + t_poll + t_mean:7.4f}"
        )
    finally:
        conn.close()


def profile_official(database):
    import polars as pl
    from google.cloud import spanner

    schema = {
        "id": pl.Int64,
        "value": pl.Float64,
        "quantity": pl.Int64,
        "category": pl.Utf8,
        "active": pl.Boolean,
    }
    _, project, _, instance_id, _, database_id = database.split("/")
    client = spanner.Client(project=project)
    db = client.instance(instance_id).database(database_id)
    with db.snapshot() as snapshot:
        result, t_exec = t(lambda: snapshot.execute_sql(f"SELECT {bp.COLUMNS} FROM {bp.TABLE}"))
        rows, t_list = t(lambda: list(result))
        df, t_build = t(lambda: pl.DataFrame(rows, schema=schema, orient="row"))
        _, t_mean = t(lambda: df["value"].mean())
    print(
        f"  {'official':<22} exec={t_exec:7.4f} list_rows={t_list:7.4f} "
        f"build_df={t_build:7.4f} mean={t_mean:7.4f} "
        f"TOTAL={t_exec + t_list + t_build + t_mean:7.4f}"
    )


def main():
    database = bp.ensure_database()
    print(f">> setup {N} rows")
    bp.setup_table(database, N, 20000)
    print(f">> profiling {N} rows (one warm run each first)\n")

    # Warm up.
    profile_adbc(database)
    profile_official(database)
    print("  --- measured ---")
    for rpb in (None, 65536, 262144, 1_000_000):
        profile_adbc(database, rpb)
    profile_official(database)


if __name__ == "__main__":
    raise SystemExit(main())

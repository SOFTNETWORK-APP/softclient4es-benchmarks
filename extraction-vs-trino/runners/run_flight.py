#!/usr/bin/env python3.12
"""One measured run against the SoftClient4ES Arrow Flight SQL sidecar.

Run in a FRESH subprocess per measurement (orchestrate.py does this): peak RSS
and allocator state must not leak between runs.

Scenarios:
  S1   full extraction into an Arrow table   (the headline)
  S1r  time-to-DataFrame: the same SQL landed as a pandas.DataFrame
  S2   S1 landed in DuckDB, zero-copy
  S3   GROUP BY control
  S4   LIMIT 100 control

S1r REDEFINED 2026-08-11 (see scenarios.py). It used to be "the same wire fetched
row-wise via cursor.fetchall()" -- an artifact no user builds, measured on one
stack only. It is now the question a data engineer actually has: how long until I
have a pandas.DataFrame? Each engine takes its most idiomatic route (here:
fetch_arrow_table().to_pandas(); Trino: pandas.read_sql over trino.sqlalchemy),
and the --dtype-backend flag measures both the default numpy-backed frame and the
Arrow-backed one (types_mapper=pd.ArrowDtype) -- publish BOTH, never silently the
flattering one.

Timers: `wall_s` is the headline end-to-end number (connect included -- that is
what a user waits for). `connect_s` and `query_wall_s` are recorded separately so
the S4 control can say whether near-parity is the query or the handshake.
"""
import argparse
import time

import adbc_driver_flightsql.dbapi as dbapi

from scenarios import (DEFAULT_INDEX, EXPECTED_MIN_COLS_FLIGHT_S1, EXPECTED_ROWS,
                       SQL_AGG_DUCK, check, emit, guard_environment,
                       memory_pressure, net_bytes, net_delta, peak_footprint_mb,
                       peak_rss_mb, sql_for)

# IP literal, NOT "localhost" (fix for arrow#151). The ADBC Flight SQL driver is
# Go; for a HOSTNAME its resolver issues real DNS queries (grpc-go's resolver,
# not getaddrinfo, so /etc/hosts does not short-circuit it), and Go's per-query
# DNS timeout is 5 s -- one dropped UDP response = +5.05 s connect, two = +10.1 s,
# which is exactly the 5.2/10.1 s outlier signature in every recorded session.
# Measured here: 60 connects to "localhost" -> median 92 ms, outliers 5.2/10.1 s;
# 60 connects to "127.0.0.1" -> median 3 ms, no outliers. Raw TCP (0.04 ms) and
# the C++ pyarrow client (1.6 ms) show the server was never the cost.
FLIGHT_URL = "grpc://127.0.0.1:32010"
SCENARIOS = ["S1", "S1r", "S2", "S3", "S4"]


def run(scenario, index, dtype_backend="default", frame="pandas"):
    sql = sql_for(scenario, index)
    t0, c0 = time.perf_counter(), time.process_time()
    conn = dbapi.connect(FLIGHT_URL)
    cur = conn.cursor()
    connect_s = time.perf_counter() - t0
    q0 = time.perf_counter()
    out = {}
    if scenario == "S1":
        cur.execute(sql)
        tbl = cur.fetch_arrow_table()          # materialized client-side table
        out["rows"], out["cols"] = tbl.num_rows, tbl.num_columns
        out["col_names"] = list(tbl.schema.names)
    elif scenario == "S1r":
        cur.execute(sql)
        tbl = cur.fetch_arrow_table()
        if frame == "polars":
            # polars is Arrow-native: from_arrow is (near-)zero-copy, so this
            # route skips the numpy materialization to_pandas() pays. That is
            # not deck-stacking -- it is the finding: an Arrow wire feeds an
            # Arrow-native frame for free. (pl.read_database(sql, conn) over
            # the ADBC connection is the same path with the same internals.)
            import polars as pl                # imported here: never weighs on S1/S3/S4 RSS
            df = pl.from_arrow(tbl)
            out["rows"], out["cols"] = df.height, df.width
        else:
            import pandas as pd                # imported here so it never weighs on S1/S3/S4 RSS
            # Plain to_pandas() on purpose: it is what a user writes. The transient
            # double-hold (Arrow table + DataFrame) is the true cost of this route --
            # do not "optimize" it away with self_destruct and then publish the number
            # as if it were the idiomatic path.
            if dtype_backend == "pyarrow":
                df = tbl.to_pandas(types_mapper=pd.ArrowDtype)
            else:
                df = tbl.to_pandas()
            out["rows"], out["cols"] = len(df), df.shape[1]
            out["dtype_backend"] = dtype_backend
        out["frame"] = frame
    elif scenario == "S2":
        import duckdb                          # imported here so it never weighs on S1/S3/S4 RSS
        cur.execute(sql)
        tbl = cur.fetch_arrow_table()
        con = duckdb.connect()
        con.register("events", tbl)            # zero-copy Arrow scan
        agg = con.execute(SQL_AGG_DUCK).fetchall()
        out["rows"], out["groups"] = tbl.num_rows, len(agg)
    elif scenario in ("S3", "S4"):
        # Same fetch API for both controls, so they stay comparable to each other.
        cur.execute(sql)
        tbl = cur.fetch_arrow_table()
        out["rows"], out["cols"] = tbl.num_rows, tbl.num_columns
    else:
        raise SystemExit(f"unknown scenario {scenario}")
    out["query_wall_s"] = time.perf_counter() - q0
    out["wall_s"] = time.perf_counter() - t0
    out["cpu_s"] = time.process_time() - c0
    out["connect_s"] = connect_s
    out["peak_rss_mb"] = peak_rss_mb()             # legacy, compression-eroded
    out["peak_footprint_mb"] = peak_footprint_mb()  # headline client memory
    out["mem_pressure"] = memory_pressure()
    cur.close()
    conn.close()

    check(scenario, out)
    if scenario == "S1":
        assert out["cols"] >= EXPECTED_MIN_COLS_FLIGHT_S1, (
            f"S1: expected >= {EXPECTED_MIN_COLS_FLIGHT_S1} columns, got {out['cols']}")
        # Hit-metadata columns beyond the 8 selected -- recorded, never hidden.
        # METHODOLOGY section 6 publishes the observed list.
        out["extra_cols"] = out["cols"] - EXPECTED_MIN_COLS_FLIGHT_S1
    return out


if __name__ == "__main__":
    guard_environment()
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", required=True, choices=SCENARIOS)
    p.add_argument("--index", default=DEFAULT_INDEX)
    p.add_argument("--variant", default="", help="e.g. 4shard -- keeps S1b out of S1's medians")
    p.add_argument("--dtype-backend", default="default", choices=["default", "pyarrow"],
                   help="S1r only: numpy-backed (default) or Arrow-backed DataFrame")
    p.add_argument("--frame", default="pandas", choices=["pandas", "polars"],
                   help="S1r only: which frame library the result lands in")
    p.add_argument("--out")
    a = p.parse_args()
    assert a.scenario in EXPECTED_ROWS, a.scenario
    if a.frame == "polars" and a.dtype_backend != "default":
        raise SystemExit("--dtype-backend is a pandas concept; do not combine with --frame polars")

    before = net_bytes("flight-sql")
    result = run(a.scenario, a.index, a.dtype_backend, a.frame)
    result["net"] = net_delta(before, net_bytes("flight-sql"))
    emit({"stack": "flight", "scenario": a.scenario, "index": a.index,
          "variant": a.variant,
          "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          **result}, a.out)

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

from scenarios import (DEFAULT_INDEX, ENGINE_SERVICES, EXPECTED_MIN_COLS_FLIGHT_S1,
                       EXPECTED_ROWS, SQL_AGG_DUCK, check, emit, guard_environment,
                       compose_variant, host_load, memory_pressure, net_bytes, net_bytes_all, net_delta,
                       peak_footprint_mb, peak_rss_mb, sql_for)

# IP literal, NOT "localhost" (fix for arrow#151). The ADBC Flight SQL driver is
# Go; for a HOSTNAME its resolver issues real DNS queries (grpc-go's resolver,
# not getaddrinfo, so /etc/hosts does not short-circuit it), and Go's per-query
# DNS timeout is 5 s -- one dropped UDP response = +5.05 s connect, two = +10.1 s,
# which is exactly the 5.2/10.1 s outlier signature in every recorded session.
# Measured here: 60 connects to "localhost" -> median 92 ms, outliers 5.2/10.1 s;
# 60 connects to "127.0.0.1" -> median 3 ms, no outliers. Raw TCP (0.04 ms) and
# the C++ pyarrow client (1.6 ms) show the server was never the cost.
#
# ⚠️ THE DIAL IS A PUBLISHED FIGURE, NOT A HIDDEN OPTIMISATION. It is worth ~90 ms
# of connect time, which is larger than the whole margin in the S4 control
# (40 ms vs 57 ms), so S4 measured only by IP would rest a parity claim on an
# undisclosed choice. `--dial hostname` measures the other side of it so both are
# published, and the deployment guidance (IP literals or connection reuse with
# the Go driver) is what the difference is FOR.
FLIGHT_URLS = {"ip": "grpc://127.0.0.1:32010", "hostname": "grpc://localhost:32010"}
FLIGHT_URL = FLIGHT_URLS["ip"]
SCENARIOS = ["S1", "S1m", "S1r", "S2", "S3", "S4"]


def run(scenario, index, dtype_backend="default", frame="pandas", dial="ip"):
    sql = sql_for(scenario, index)
    t0, c0 = time.perf_counter(), time.process_time()
    conn = dbapi.connect(FLIGHT_URLS[dial])
    cur = conn.cursor()
    connect_s = time.perf_counter() - t0
    q0 = time.perf_counter()
    out = {"dial": dial}
    if scenario in ("S1", "S1m"):
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
    if scenario in ("S1", "S1m"):
        assert out["cols"] >= EXPECTED_MIN_COLS_FLIGHT_S1, (
            f"{scenario}: expected >= {EXPECTED_MIN_COLS_FLIGHT_S1} columns, "
            f"got {out['cols']}")
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
    p.add_argument("--dial", default="ip", choices=list(FLIGHT_URLS),
                   help="how the sidecar is addressed: IP literal (default) or "
                        "hostname, whose Go-driver DNS lookup is published as a "
                        "cost rather than avoided silently")
    p.add_argument("--out")
    a = p.parse_args()
    assert a.scenario in EXPECTED_ROWS, a.scenario
    if a.frame == "polars" and a.dtype_backend != "default":
        raise SystemExit("--dtype-backend is a pandas concept; do not combine with --frame polars")

    before = net_bytes_all(ENGINE_SERVICES["flight"])
    load_before = host_load()
    # Bytes that actually LEFT Elasticsearch. Summing the engine stack instead
    # double-counts internal traffic once the engine is a cluster: measured
    # 2026-08-16, engine-stack rx for S1 was 3,671 MB against ~2,950 MB truly
    # read from ES, the difference being the worker->coordinator exchange.
    # Sampled at the source, the number is independent of engine topology.
    before_es = net_bytes("elasticsearch")
    result = run(a.scenario, a.index, a.dtype_backend, a.frame, a.dial)
    result["net"] = net_delta(before, net_bytes_all(ENGINE_SERVICES["flight"]))
    result["net_es"] = net_delta(before_es, net_bytes("elasticsearch"))
    result["host_load_before"], result["host_load_after"] = load_before, host_load()
    emit({"stack": "flight", "scenario": a.scenario, "index": a.index,
          # A non-default dial must never blend into the headline medians; the tag
          # composes with an explicit --variant and is deduped (orchestrate.py has
          # already put the same tag in the filename).
          "variant": compose_variant(a.variant,
                                     "" if a.dial == "ip" else f"dial{a.dial}"),
          "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          **result}, a.out)

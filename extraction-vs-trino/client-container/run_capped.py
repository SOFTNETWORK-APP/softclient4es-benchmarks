#!/usr/bin/env python3
"""S5 — does the extraction FIT? One engine, one memory cap, one verdict.

Every other scenario in this benchmark asks "how fast" and answers with a ratio.
This one asks "does it complete at all" and answers yes or no. That is a
different kind of claim and a stronger one: a threshold cannot be argued down to
a percentage, and it maps directly onto the question an operator actually has --
"can I run this in a 4 GB container?"

Run INSIDE a container with `docker run --memory=<cap> --memory-swap=<cap>`.
The cap must be a real cgroup bound: RLIMIT_AS is unusable on macOS (see the
Dockerfile), and swap must be pinned equal to memory or the kernel will page
instead of killing, turning a clean "does not fit" into a slow "sort of fits".

TWO MODES, because the honest comparison needs both:

  full     both engines materialise the WHOLE result as one pandas.DataFrame.
           This is the common analytical workflow and the claim we make.

  chunked  both engines STREAM and never hold the whole result. Trino via
           `pandas.read_sql(..., chunksize=)`, ours via `fetch_record_batch()`.
           This exists because "just use chunksize" is the first thing a
           competent Trino user will say, and a claim that dodges its strongest
           objection is worth nothing. If chunking rescues Trino, we publish
           that -- scoped, not hidden.

Emits one JSON line. A run killed by the OOM killer emits nothing at all (the
process dies), so ABSENCE of output is itself the result -- the caller records
the exit code (137 = SIGKILL = OOM).
"""
import argparse
import json
import os
import resource
import sys
import time

COLUMNS = ["id", "event_ts", "amount", "qty", "status", "country", "category", "name"]
SQL = f"SELECT {', '.join(COLUMNS)} FROM {os.environ.get('BENCH_INDEX', 'bench_events_10m')}"
EXPECTED_ROWS = int(os.environ.get("BENCH_EXPECTED_ROWS", "10000000"))

FLIGHT_HOST = os.environ.get("FLIGHT_HOST", "flight-sql")
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")


def peak_rss_mb():
    """ru_maxrss is KiB on Linux. Inside a container with a hard cgroup cap and
    no swap there is no compressor to erode it, so it is honest here -- unlike on
    the macOS host, where arrow#150 showed it under-reports badly."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def flight_full():
    import adbc_driver_flightsql.dbapi as dbapi
    conn = dbapi.connect(f"grpc://{FLIGHT_HOST}:32010")
    cur = conn.cursor()
    cur.execute(SQL)
    df = cur.fetch_arrow_table().to_pandas()
    n = len(df)
    cur.close()
    conn.close()
    return n


def flight_chunked():
    """Stream record batches; never hold the whole result."""
    import adbc_driver_flightsql.dbapi as dbapi
    conn = dbapi.connect(f"grpc://{FLIGHT_HOST}:32010")
    cur = conn.cursor()
    cur.execute(SQL)
    reader = cur.fetch_record_batch()
    n = 0
    for batch in reader:
        # to_pandas per batch: the same per-chunk DataFrame work Trino's
        # chunksize path does, so the two streaming arms stay comparable.
        n += len(batch.to_pandas())
    cur.close()
    conn.close()
    return n


def trino_full():
    import pandas as pd
    from sqlalchemy import create_engine
    engine = create_engine(f"trino://bench@{TRINO_HOST}:8080/elasticsearch/default")
    conn = engine.connect()
    df = pd.read_sql(SQL, conn)
    n = len(df)
    conn.close()
    engine.dispose()
    return n


def trino_full_cx():
    """Trino's BEST route to a whole DataFrame: connectorx (Rust JSON->columnar,
    no Python row objects). Added 2026-08-14 to turn C-FIT's middle tier --
    'their best route needs ~2x our memory' -- from a host-side inference into a
    measured container cell. Schema qualified in the SQL because connectorx's
    Trino source has no session-schema setting (same namespace, not a
    different query)."""
    import connectorx as cx
    cx_sql = SQL.replace(" FROM ", " FROM default.", 1)
    df = cx.read_sql(f"trino://bench@{TRINO_HOST}:8080/elasticsearch",
                     cx_sql, return_type="pandas")
    return len(df)


def trino_chunked(chunksize):
    import pandas as pd
    from sqlalchemy import create_engine
    engine = create_engine(f"trino://bench@{TRINO_HOST}:8080/elasticsearch/default")
    conn = engine.connect()
    n = 0
    for chunk in pd.read_sql(SQL, conn, chunksize=chunksize):
        n += len(chunk)
    conn.close()
    engine.dispose()
    return n


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--engine", required=True, choices=["flight", "trino"])
    p.add_argument("--mode", default="full", choices=["full", "chunked", "full-cx"])
    p.add_argument("--chunksize", type=int, default=100_000)
    p.add_argument("--cap-label", default="", help="recorded verbatim, e.g. 4g")
    a = p.parse_args()

    t0, c0 = time.perf_counter(), time.process_time()
    try:
        if a.mode == "full-cx":
            if a.engine != "trino":
                raise SystemExit("full-cx is a Trino-only mode (connectorx)")
            rows = trino_full_cx()
        elif a.engine == "flight":
            rows = flight_full() if a.mode == "full" else flight_chunked()
        else:
            rows = trino_full() if a.mode == "full" else trino_chunked(a.chunksize)
        out = {"engine": a.engine, "mode": a.mode, "cap": a.cap_label,
               "outcome": "completed", "rows": rows,
               "wall_s": time.perf_counter() - t0,
               "cpu_s": time.process_time() - c0,
               "peak_rss_mb": peak_rss_mb(),
               "rows_ok": rows == EXPECTED_ROWS}
        if a.mode == "chunked":
            out["chunksize"] = a.chunksize
        print(json.dumps(out), flush=True)
        # A wrong row count is a failed run even though the process survived.
        sys.exit(0 if rows == EXPECTED_ROWS else 3)
    except MemoryError as e:
        print(json.dumps({"engine": a.engine, "mode": a.mode, "cap": a.cap_label,
                          "outcome": "MemoryError", "error": str(e)[:200],
                          "wall_s": time.perf_counter() - t0}), flush=True)
        sys.exit(4)
    except Exception as e:
        print(json.dumps({"engine": a.engine, "mode": a.mode, "cap": a.cap_label,
                          "outcome": "error", "error_type": type(e).__name__,
                          "error": str(e).replace("\n", " ")[:300],
                          "wall_s": time.perf_counter() - t0}), flush=True)
        sys.exit(5)

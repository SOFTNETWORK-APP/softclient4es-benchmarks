#!/usr/bin/env python3.12
"""One measured run against Trino's Elasticsearch connector (stock client).

Same measurement contract as run_flight.py: fresh subprocess per run, same SQL
(imported from scenarios.py so the two stacks cannot drift apart).

Trino's client is used the way its own documentation uses it -- cursor.execute
then fetchall. There is no Arrow output in the trino package; that asymmetry in
what the two client protocols can offer is the finding, not a handicap imposed
here. run_flight.py's S1r arm pays the same row-object cost over the Arrow wire,
so the comparison can be decomposed rather than asserted.
"""
import argparse
import time

from trino.dbapi import connect

from scenarios import (DEFAULT_INDEX, ENGINE_SERVICES, EXPECTED_COLS_TRINO_S1,
                       SQL_AGG_DUCK, check, emit, guard_environment, memory_pressure,
                       net_bytes, net_bytes_all, net_delta, peak_footprint_mb, peak_rss_mb,
                       sql_for)

SCENARIOS = ["S1", "S1r", "S2", "S3", "S4"]


def run_s1r_dataframe(sql, dtype_backend, frame="pandas"):
    """S1r (time-to-DataFrame): the route a data engineer actually writes.

    pandas       pandas.read_sql over trino.sqlalchemy -- NOT trino.dbapi +
                 fetchall. pandas.read_sql officially supports SQLAlchemy
                 connectables (raw DBAPI connections are sqlite-only and warn),
                 and the trino package ships the dialect (trino.sqlalchemy).
                 dtype_backend="pyarrow" is pandas' own switch for an
                 Arrow-backed frame, mirroring types_mapper=pd.ArrowDtype on
                 the Flight side so the two variants stay comparable.
    polars       pl.read_database over the same trino.sqlalchemy connection --
                 polars' documented route for engines it has no native driver
                 for. Rows are still Python objects underneath; the frame
                 build is the cost of landing a row protocol, same as pandas.
    polars-cx    connectorx (cx.read_sql(..., return_type='polars')) -- the
                 route the polars docs recommend for speed where connectorx
                 supports the source, and it supports Trino. Its Rust core
                 parses Trino's JSON pages straight into columnar buffers,
                 skipping Python row objects entirely: this is Trino's MOST
                 flattering client path and is published for exactly that
                 reason. connect+query are one call, so connect_s is not
                 separable and is recorded as 0.
    """
    t0, c0 = time.perf_counter(), time.process_time()
    if frame == "polars-cx":
        import connectorx as cx                # imported here: never weighs on S1/S3/S4
        connect_s = 0.0                        # one-shot API: connect not separable
        # connectorx's Trino source has no session-schema setting, so the schema
        # every OTHER route gets from the connection URL path must be qualified
        # in the SQL here. Same namespace, not a different query.
        cx_sql = sql.replace(" FROM ", " FROM default.", 1)
        q0 = time.perf_counter()
        df = cx.read_sql("trino://bench@localhost:8080/elasticsearch",
                         cx_sql, return_type="polars")
        rows, cols = df.height, df.width
        dispose = None
    elif frame == "pandas-cx":
        # connectorx's pandas destination -- closes the same best-route hole for
        # C-S1-DF that polars-cx closed for the polars destination.
        import connectorx as cx                # imported here: never weighs on S1/S3/S4
        connect_s = 0.0                        # one-shot API: connect not separable
        cx_sql = sql.replace(" FROM ", " FROM default.", 1)
        q0 = time.perf_counter()
        df = cx.read_sql("trino://bench@localhost:8080/elasticsearch",
                         cx_sql, return_type="pandas")
        rows, cols = len(df), df.shape[1]
        dispose = None
    elif frame == "pandas-adbc":
        # The pandas twin of polars-adbc: ADBC Foundry driver + fetch_arrow_table
        # + to_pandas -- character-for-character the Flight side's pandas route,
        # differing only in the driver underneath. Completes the symmetry the
        # polars destination already has.
        import pandas as pd                    # noqa: F401  (to_pandas needs it importable)
        from adbc_driver_manager import dbapi as adbc_dbapi
        conn = adbc_dbapi.connect(
            driver="trino",
            db_kwargs={"uri": "http://bench@localhost:8080"
                              "?catalog=elasticsearch&schema=default"})
        cur = conn.cursor()
        connect_s = time.perf_counter() - t0
        q0 = time.perf_counter()
        cur.execute(sql)
        df = cur.fetch_arrow_table().to_pandas()
        rows, cols = len(df), df.shape[1]
        dispose = (cur.close, conn.close)
    elif frame == "polars-adbc":
        # The ADBC Driver Foundry ships a real Trino ADBC driver (v0.5.1, tested
        # with Trino 483 -- the exact engine version here; installed via
        # `dbc install --level user trino`). This route is CHARACTER-FOR-CHARACTER
        # the Flight side's polars route -- adbc dbapi connect, fetch_arrow_table,
        # pl.from_arrow -- differing only in the driver underneath. The most
        # symmetric client comparison this benchmark has.
        import polars as pl                    # imported here: never weighs on S1/S3/S4
        from adbc_driver_manager import dbapi as adbc_dbapi
        conn = adbc_dbapi.connect(
            driver="trino",
            db_kwargs={"uri": "http://bench@localhost:8080"
                              "?catalog=elasticsearch&schema=default"})
        cur = conn.cursor()
        connect_s = time.perf_counter() - t0
        q0 = time.perf_counter()
        cur.execute(sql)
        df = pl.from_arrow(cur.fetch_arrow_table())
        rows, cols = df.height, df.width
        dispose = (cur.close, conn.close)
    else:
        from sqlalchemy import create_engine
        engine = create_engine("trino://bench@localhost:8080/elasticsearch/default")
        conn = engine.connect()
        connect_s = time.perf_counter() - t0
        q0 = time.perf_counter()
        if frame == "polars":
            import polars as pl                # imported here: never weighs on S1/S3/S4
            df = pl.read_database(sql, conn)
            rows, cols = df.height, df.width
        else:
            import pandas as pd                # imported here: never weighs on S1/S3/S4
            kwargs = {} if dtype_backend == "default" else {"dtype_backend": "pyarrow"}
            df = pd.read_sql(sql, conn, **kwargs)
            rows, cols = len(df), df.shape[1]
        dispose = (conn.close, engine.dispose)
    out = {"rows": rows, "cols": cols,
           "frame": frame,
           "query_wall_s": time.perf_counter() - q0,
           "wall_s": time.perf_counter() - t0,
           "cpu_s": time.process_time() - c0,
           "connect_s": connect_s,
           "peak_rss_mb": peak_rss_mb(),
           "peak_footprint_mb": peak_footprint_mb(),
           "mem_pressure": memory_pressure()}
    if frame == "pandas":
        out["dtype_backend"] = dtype_backend
    if dispose:
        for closer in dispose:
            closer()
    check("S1r", out)
    assert out["cols"] == EXPECTED_COLS_TRINO_S1, (
        f"S1r: expected {EXPECTED_COLS_TRINO_S1} columns from Trino, got {out['cols']}")
    return out


def run(scenario, index, encoding=None, request_timeout=None, dtype_backend="default",
        frame="pandas", route="stock"):
    sql = sql_for(scenario, index)
    if scenario == "S1r":
        return run_s1r_dataframe(sql, dtype_backend, frame)
    if scenario == "S1" and route == "adbc":
        # A REAL ADBC driver for Trino exists (ADBC Driver Foundry v0.5.1,
        # tested with Trino 483 -- the exact engine version here). This makes S1
        # the identical client API on both stacks: adbc dbapi + fetch_arrow_table,
        # differing only in the driver underneath. The stock trino.dbapi route
        # remains the headline (it is what the trino package ships); this is the
        # best Arrow-landing route Trino has.
        from adbc_driver_manager import dbapi as adbc_dbapi
        t0, c0 = time.perf_counter(), time.process_time()
        conn = adbc_dbapi.connect(
            driver="trino",
            db_kwargs={"uri": "http://bench@localhost:8080"
                              "?catalog=elasticsearch&schema=default"})
        cur = conn.cursor()
        connect_s = time.perf_counter() - t0
        q0 = time.perf_counter()
        cur.execute(sql)
        tbl = cur.fetch_arrow_table()
        out = {"rows": tbl.num_rows, "cols": tbl.num_columns,
               "col_names": list(tbl.schema.names),
               "route": route,
               "query_wall_s": time.perf_counter() - q0,
               "wall_s": time.perf_counter() - t0,
               "cpu_s": time.process_time() - c0,
               "connect_s": connect_s,
               "peak_rss_mb": peak_rss_mb(),
               "peak_footprint_mb": peak_footprint_mb(),
               "mem_pressure": memory_pressure()}
        cur.close()
        conn.close()
        check("S1", out)
        assert out["cols"] == EXPECTED_COLS_TRINO_S1, (
            f"S1: expected {EXPECTED_COLS_TRINO_S1} columns from Trino, got {out['cols']}")
        return out
    if scenario == "S1" and route == "connectorx":
        # The de-facto ADBC-equivalent for Trino: no ADBC driver exists (Trino's
        # wire is JSON-paged REST, not Arrow, and it serves no Flight SQL
        # endpoint), but connectorx's Rust core parses those JSON pages straight
        # into Arrow buffers -- landing the SAME artifact S1 measures (a client-
        # side pyarrow Table) without Python row objects. This is Trino's most
        # flattering route to an Arrow table and is published for exactly that
        # reason. Same schema-qualification note as the polars-cx route.
        import connectorx as cx                # imported here: never weighs on stock runs
        t0, c0 = time.perf_counter(), time.process_time()
        cx_sql = sql.replace(" FROM ", " FROM default.", 1)
        tbl = cx.read_sql("trino://bench@localhost:8080/elasticsearch",
                          cx_sql, return_type="arrow")
        out = {"rows": tbl.num_rows, "cols": tbl.num_columns,
               "col_names": list(tbl.schema.names),
               "route": route,
               "query_wall_s": time.perf_counter() - t0,
               "wall_s": time.perf_counter() - t0,
               "cpu_s": time.process_time() - c0,
               "connect_s": 0.0,               # one-shot API: connect not separable
               "peak_rss_mb": peak_rss_mb(),
               "peak_footprint_mb": peak_footprint_mb(),
               "mem_pressure": memory_pressure()}
        check("S1", out)
        assert out["cols"] == EXPECTED_COLS_TRINO_S1, (
            f"S1: expected {EXPECTED_COLS_TRINO_S1} columns from Trino, got {out['cols']}")
        return out
    kwargs = dict(host="localhost", port=8080, user="bench",
                  catalog="elasticsearch", schema="default")
    if encoding:                       # optional S1-spooled variant
        kwargs["encoding"] = encoding
    if request_timeout:                # per-HTTP-poll timeout; recorded when used
        kwargs["request_timeout"] = request_timeout
    t0, c0 = time.perf_counter(), time.process_time()
    conn = connect(**kwargs)
    cur = conn.cursor()
    connect_s = time.perf_counter() - t0
    q0 = time.perf_counter()
    out = {}
    if scenario == "S1":
        cur.execute(sql)
        rows = cur.fetchall()                  # materialized client-side table
        out["rows"], out["cols"] = len(rows), len(cur.description)
        out["col_names"] = [d[0] for d in cur.description]
    elif scenario == "S2":
        import duckdb                          # imported here so they never weigh
        import pandas as pd                    # on S1/S3/S4's peak RSS
        cur.execute(sql)
        rows = cur.fetchall()
        # Charitable stock path: a DataFrame + register beats a row-by-row INSERT
        # loop. Building the DataFrame IS the cost of landing a row protocol.
        df = pd.DataFrame(rows, columns=[d[0] for d in cur.description])
        con = duckdb.connect()
        con.register("events", df)
        agg = con.execute(SQL_AGG_DUCK).fetchall()
        out["rows"], out["groups"] = len(rows), len(agg)
    elif scenario in ("S3", "S4"):
        cur.execute(sql)
        out["rows"] = len(cur.fetchall())
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
        assert out["cols"] == EXPECTED_COLS_TRINO_S1, (
            f"S1: expected {EXPECTED_COLS_TRINO_S1} columns from Trino, got {out['cols']}")
    return out


if __name__ == "__main__":
    guard_environment()
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", required=True, choices=SCENARIOS)
    p.add_argument("--index", default=DEFAULT_INDEX)
    p.add_argument("--variant", default="", help="e.g. 4shard -- keeps S1b out of S1's medians")
    p.add_argument("--encoding")         # e.g. json+zstd for the spooled variant
    p.add_argument("--request-timeout", type=float)
    p.add_argument("--dtype-backend", default="default", choices=["default", "pyarrow"],
                   help="S1r only: numpy-backed (default) or Arrow-backed DataFrame")
    p.add_argument("--frame", default="pandas",
                   choices=["pandas", "polars", "polars-cx", "polars-adbc", "pandas-cx", "pandas-adbc"],
                   help="S1r only: which frame library (and route) the result lands in")
    p.add_argument("--route", default="stock", choices=["stock", "connectorx", "adbc"],
                   help="S1 only: stock trino.dbapi fetchall, connectorx->Arrow, or "
                        "the ADBC Driver Foundry trino driver->Arrow")
    p.add_argument("--out")
    a = p.parse_args()
    if a.frame != "pandas" and a.dtype_backend != "default":
        raise SystemExit("--dtype-backend is a pandas concept; do not combine with --frame")
    if a.route != "stock" and a.scenario != "S1":
        raise SystemExit("--route only applies to S1")

    before = net_bytes_all(ENGINE_SERVICES["trino"])
    # Bytes that actually LEFT Elasticsearch. Summing the engine stack instead
    # double-counts internal traffic once the engine is a cluster: measured
    # 2026-08-16, engine-stack rx for S1 was 3,671 MB against ~2,950 MB truly
    # read from ES, the difference being the worker->coordinator exchange.
    # Sampled at the source, the number is independent of engine topology.
    before_es = net_bytes("elasticsearch")
    result = run(a.scenario, a.index, a.encoding, a.request_timeout, a.dtype_backend,
                 a.frame, a.route)
    result["net"] = net_delta(before, net_bytes_all(ENGINE_SERVICES["trino"]))
    result["net_es"] = net_delta(before_es, net_bytes("elasticsearch"))
    if a.encoding:
        result["encoding"] = a.encoding
    if a.request_timeout:
        result["request_timeout"] = a.request_timeout
    emit({"stack": "trino-spooled" if a.encoding else "trino",
          "scenario": a.scenario, "index": a.index, "variant": a.variant,
          "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          **result}, a.out)

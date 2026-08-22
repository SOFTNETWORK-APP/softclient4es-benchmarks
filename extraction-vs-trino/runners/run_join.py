#!/usr/bin/env python3.12
"""One measured cross-index JOIN run, against either engine.

Same measurement contract as run_flight.py / run_trino.py: a FRESH subprocess per
run, the SQL defined once here so the two engines cannot drift apart, and the row
count asserted BEFORE any timing is reported -- a wrong-but-fast join must never
be reportable as a win.

Why this exists as a runner instead of the scratch script that produced RESULTS
Appendix A: that script wrote no provenance, and the 0.2.5-SNAPSHOT tag was
republished mid-session, so its leg-size sweep could only be attributed to a build
by timestamp inference. A benchmark that cannot name the build it measured cannot
be cited. orchestrate_join.py records the digest AND the per-jar checksums.

The JOIN is the one scenario where the sidecar can die rather than merely lose
(arrow#147: the 10M-row leg exhausted the JVM heap and TERMINATED the process,
taking every other client's connection with it), so each run also samples the
sidecar's /health -- allocator peak, the fatal-error latch, per-check status.
A run that "passed" while the allocator latched a fatal error is not a pass.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request

from scenarios import (HOST, emit, guard_environment, memory_pressure, net_bytes,
                       net_delta, peak_footprint_mb, peak_rss_mb)

FLIGHT_URL = "grpc://127.0.0.1:32010"          # IP literal, never a hostname (arrow#151)
HEALTH_URL = "http://127.0.0.1:32011/health"
ES_URL = f"http://{HOST}:9200"

SMALL_LEG = "bench_1m"
LARGE_LEG = "bench_events_10m"

# Three JOIN shapes. J0 alone is a worst case nobody runs: real queries carry a
# predicate or an aggregate, and BOTH engines document predicate pushdown to
# Elasticsearch, so J1/J2 are where a JOIN benchmark stops being a strawman.
#
#   J0  no predicate      -- leg b is the full 10M index; 11M rows extracted to emit 1M.
#                            Kept because it is the shape that used to kill the sidecar
#                            (arrow#147) and it bounds the worst case.
#   J1  + WHERE           -- a predicate ON THE LARGE LEG, which is what lets an engine
#                            avoid extracting rows it will discard.
#   J2  + GROUP BY        -- the join result aggregated. Failed on Flight SQL until
#                            arrow#158 (numeric aggregates advertised utf8 in the probe
#                            schema); FIX VERIFIED 2026-08-11 across all 18 aggregate
#                            shapes on the corpus, so it runs by default again.
#
# Expected row counts are ORACLES read from Elasticsearch directly, not from either
# engine -- an engine must never certify its own correctness. Derivation, against this
# corpus (bench_1m = ids 8_810_000..9_809_999, an exact value-for-value slice of
# bench_events_10m, so the join is 1:1):
#   J0  _count over that id range                                  -> 1,000,000
#   J1  _count over that id range AND status=paid                  ->   125,361
#   J2  cardinality(category) over that id range                   ->       100
# Re-derive these if the corpus is ever regenerated; they are data-specific, and a
# stale oracle turns the correctness gate into a rubber stamp.
JOIN_SCENARIOS = {
    "J0": {"where": None,                  "group_by": None},
    "J1": {"where": "b.status = 'paid'",   "group_by": None},
    "J2": {"where": None,                  "group_by": "b.category"},
}


def derive_oracles(small=None, large=None):
    """Read the three expected row counts from Elasticsearch, for THIS corpus.

    They used to be literals (1_000_000 / 125_361 / 100) derived by hand against a
    corpus whose small leg spanned ids 8,810,000..9,809,999. On 2026-08-20 bench_1m
    was regenerated at offset 0 instead, and J1 failed on BOTH engines with the same
    count -- 125,044 against the literal's 125,361. Two engines agreeing exactly is
    the signature of a wrong oracle, not a wrong engine: the join was still a valid
    1:1 value-identical slice, only its selectivity had moved.

    The file already said "re-derive these if the corpus is ever regenerated; a stale
    oracle turns the correctness gate into a rubber stamp". Deriving them here is that
    instruction made automatic, and it keeps the property that matters -- the oracle
    comes from ELASTICSEARCH, never from either engine, so neither can certify itself.

    The id window is read from the small leg rather than assumed, because that is
    precisely the assumption that broke.
    """
    small, large = small or SMALL_LEG, large or LARGE_LEG

    def es(path, body):
        req = urllib.request.Request(
            f"{ES_URL}/{path}", data=json.dumps(body).encode(), method="POST",
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())

    try:
        agg = es(f"{small}/_search",
                 {"size": 0, "aggs": {"lo": {"min": {"field": "id"}},
                                      "hi": {"max": {"field": "id"}}}})["aggregations"]
        lo, hi = int(agg["lo"]["value"]), int(agg["hi"]["value"])
        rng = {"range": {"id": {"gte": lo, "lte": hi}}}

        j0 = es(f"{large}/_count", {"query": rng})["count"]
        j1 = es(f"{large}/_count",
                {"query": {"bool": {"filter": [rng, {"term": {"status": "paid"}}]}}})["count"]
        j2 = es(f"{large}/_search",
                {"size": 0, "query": rng,
                 "aggs": {"c": {"cardinality": {"field": "category",
                                                "precision_threshold": 40000}}}}
                )["aggregations"]["c"]["value"]
    except Exception as e:
        sys.exit(f"could not derive the JOIN oracles from Elasticsearch "
                 f"({type(e).__name__}: {e}). Refusing to measure: without an oracle "
                 f"the correctness gate is a rubber stamp.")

    small_n = es(f"{small}/_count", {"query": {"match_all": {}}})["count"]
    if j0 != small_n:
        sys.exit(f"the join is not 1:1: {small} holds {small_n:,} docs but {large} has "
                 f"{j0:,} in its id window {lo:,}..{hi:,}. Every J scenario assumes a "
                 f"value-for-value slice; fix the corpus before measuring.")

    print(f"[oracle] {small} ids {lo:,}..{hi:,}  ->  J0={j0:,}  J1={j1:,}  J2={j2:,}",
          flush=True)
    return {"J0": j0, "J1": j1, "J2": j2}


ORACLES = None          # filled by derive_oracles() before any run


def expected_rows(scenario, small=None, large=None):
    global ORACLES
    if ORACLES is None:
        ORACLES = derive_oracles(small, large)
    return ORACLES[scenario]


def check_rows(got, expected, label):
    """Correctness gate. Raises AssertionError before any timing is reported.

    Deliberately local rather than added to scenarios.py: that module is imported
    by every subprocess of a running matrix, and a JOIN runner is not a reason to
    edit a file a multi-hour measurement depends on.
    """
    assert got == expected, (
        f"{label}: expected {expected:,} rows, got {got:,}. "
        "If this is 10,000 the SoftClient4ES licence did not lift the Community "
        "maxQueryResults cap; if it is 0 or short, the join dropped rows and no "
        "timing from this run means anything.")


def join_sql(scenario="J0", small=SMALL_LEG, large=LARGE_LEG):
    """One SQL string per scenario, shared by BOTH engines.

    Defined once here for the same reason scenarios.py owns the extraction SQL: if
    the two runners could each build their own, the benchmark would silently stop
    comparing like with like.
    """
    spec = JOIN_SCENARIOS[scenario]
    if spec["group_by"]:
        projection = (f"{spec['group_by']}, COUNT(*) AS cnt, "
                      f"AVG(b.amount) AS avg_amount")
    else:
        projection = "a.id, b.amount"
    sql = f"SELECT {projection} FROM {small} a JOIN {large} b ON a.id = b.id"
    if spec["where"]:
        sql += f" WHERE {spec['where']}"
    if spec["group_by"]:
        sql += f" GROUP BY {spec['group_by']}"
    return sql


def health():
    """Sidecar health at this instant, or None. Never fatal to a run."""
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=10) as r:
            return json.loads(r.read().decode())
    except (urllib.error.URLError, OSError, ValueError, TimeoutError):
        return None


# Both engines land the SAME client artifact: a pandas.DataFrame, each by its most
# idiomatic route (redefined 2026-08-11, same decision as S1r in scenarios.py).
# Before this, Flight returned an Arrow table and Trino a list of Python tuples --
# different artifacts, so the wall/memory columns compared different work. The
# join itself was always comparable (both engines execute it SERVER-side: ours in
# the sidecar's DuckDB, Trino's in its own engine); only the client landing needed
# equalising. ⚠️ JOIN numbers from before this change measure the old artifacts.

def run_flight(sql):
    import adbc_driver_flightsql.dbapi as dbapi
    t0, c0 = time.perf_counter(), time.process_time()
    conn = dbapi.connect(FLIGHT_URL)
    cur = conn.cursor()
    connect_s = time.perf_counter() - t0
    q0 = time.perf_counter()
    cur.execute(sql)
    df = cur.fetch_arrow_table().to_pandas()
    out = {"rows": len(df), "cols": df.shape[1],
           "col_names": list(df.columns),
           "query_wall_s": time.perf_counter() - q0,
           "wall_s": time.perf_counter() - t0,
           "cpu_s": time.process_time() - c0,
           "connect_s": connect_s}
    cur.close()
    conn.close()
    return out


def run_trino(sql):
    import pandas as pd
    from sqlalchemy import create_engine
    t0, c0 = time.perf_counter(), time.process_time()
    engine = create_engine(f"trino://bench@{HOST}:8080/elasticsearch/default")
    conn = engine.connect()
    connect_s = time.perf_counter() - t0
    q0 = time.perf_counter()
    df = pd.read_sql(sql, conn)
    out = {"rows": len(df), "cols": df.shape[1],
           "col_names": list(df.columns),
           "query_wall_s": time.perf_counter() - q0,
           "wall_s": time.perf_counter() - t0,
           "cpu_s": time.process_time() - c0,
           "connect_s": connect_s}
    conn.close()
    engine.dispose()
    return out


if __name__ == "__main__":
    guard_environment()
    p = argparse.ArgumentParser()
    p.add_argument("--engine", required=True, choices=["flight", "trino"])
    p.add_argument("--scenario", default="J0", choices=sorted(JOIN_SCENARIOS))
    p.add_argument("--small", default=SMALL_LEG)
    p.add_argument("--large", default=LARGE_LEG)
    p.add_argument("--expect-rows", type=int, default=None,
                   help="override the scenario's oracle (rarely correct to do)")
    p.add_argument("--variant", default="")
    p.add_argument("--out")
    a = p.parse_args()

    expect = (a.expect_rows if a.expect_rows is not None
              else expected_rows(a.scenario, a.small, a.large))
    sql = join_sql(a.scenario, a.small, a.large)
    service = "flight-sql" if a.engine == "flight" else "trino"
    before = net_bytes(service)
    health_before = health() if a.engine == "flight" else None

    result = (run_flight if a.engine == "flight" else run_trino)(sql)

    result["peak_rss_mb"] = peak_rss_mb()              # legacy, compression-eroded
    result["peak_footprint_mb"] = peak_footprint_mb()  # headline client memory
    result["mem_pressure"] = memory_pressure()
    result["net"] = net_delta(before, net_bytes(service))
    if a.engine == "flight":
        after = health()
        result["health_before"] = health_before
        result["health_after"] = after
        # A latched fatal error means the process is degraded even though this
        # run returned rows -- the exact blindness arrow#146 was filed about.
        # Fail the run rather than let a poisoned sidecar report a clean number.
        assert not (after or {}).get("fatal"), (
            f"sidecar latched a fatal error during the run: {(after or {}).get('fatal')}")

    # Correctness gate LAST-but-before-emit, same as the other runners.
    check_rows(result["rows"], expect, f"{a.scenario} {a.small} x {a.large}")

    emit({"stack": a.engine, "scenario": a.scenario, "small": a.small,
          "large": a.large, "variant": a.variant, "sql": sql,
          "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          **result}, a.out)

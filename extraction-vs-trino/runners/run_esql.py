#!/usr/bin/env python3.12
"""One measured run against Elasticsearch's own query language, ES|QL.

The third stack, added 2026-08-16 after an external reviewer pointed out that a
benchmark comparing two SQL engines over Elasticsearch never measured what
Elasticsearch itself can do. The objection is correct and the answer is not
flattering everywhere: below its ceiling ES|QL is very fast, because it reads
`doc_values` -- already columnar on disk -- instead of `_source`, and it can hand
the client Apache Arrow directly. Where it beats us, we publish that.

WHAT IT CAN AND CANNOT ENTER, and why that is the finding rather than a gap:

    S3, S4   aggregation and small-result controls -- ES|QL competes fully
    S1m      1,000,000-row extraction -- the largest cell it can enter AT ALL
    S1/S2    10,000,000 rows: IMPOSSIBLE. `esql.query.result_truncation_max_size`
             is declared with a hard maximum of 1,000,000 (EsqlPlugin.java,
             v8.18.3) and a request above it comes back TRUNCATED, HTTP 200, with
             no Warning header. Not a licence gate: measured on a `basic` cluster
             whose `_xpack/usage` reports esql.available=true.
    S5/S6    constrained-memory and concurrency: same ceiling, same reason.

TWO ROUTES, because the wire format is the subject of this benchmark:

    json   `format=json` -- the row-shaped default every ES|QL client speaks
    arrow  `format=arrow` -- Arrow IPC stream, `application/vnd.apache.arrow.stream`

Publishing both is the point: it is the same engine, the same query and the same
result, differing only in what crosses the wire.

⚠️ THE CLUSTER SETTING IS DISCLOSED, NEVER SET FROM HERE. S1m needs
`esql.query.result_truncation_max_size` raised from its 10,000 default to
1,000,000. A harness that silently reconfigures the cluster mid-session is how
you get a plausible number nobody can attribute, so this runner READS the
effective value, records it in every run, and refuses with the exact command to
run rather than issuing it itself.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request

from scenarios import (es_wire_bytes, es_wire_delta,
                       COLUMNS, DEFAULT_INDEX, ESQL_MAX_RESULT_ROWS, ESQL_URL, HOST, S1M_ROWS,
                       check, compose_variant, emit, esql_for, guard_environment,
                       host_load, memory_pressure, net_bytes, net_delta,
                       peak_footprint_mb, peak_rss_mb)

ES = f"http://{HOST}:9200"
SCENARIOS = ["S1m", "S3", "S4"]
ROUTES = ["json", "arrow"]
SETTING = "esql.query.result_truncation_max_size"


def effective_max_rows():
    """The cluster's effective ES|QL row ceiling, or None if it cannot be read.

    Recorded in every run: an ES|QL figure measured with the setting raised is a
    different configuration from the shipped default, and a reader is entitled to
    know which one produced the number.
    """
    try:
        with urllib.request.urlopen(
                f"{ES}/_cluster/settings?include_defaults=true&flat_settings=true",
                timeout=30) as r:
            doc = json.loads(r.read().decode())
    except (urllib.error.URLError, OSError, ValueError):
        return None
    for scope in ("transient", "persistent", "defaults"):
        v = doc.get(scope, {}).get(SETTING)
        if v is not None:
            return int(v)
    return None


def esql(query, route):
    """Issue one ES|QL query; return (rows, columns, warning_header, bytes)."""
    req = urllib.request.Request(
        f"{ESQL_URL}?format={route}", method="POST",
        data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=1800) as resp:
        warning = resp.headers.get("Warning")
        body = resp.read()
    if route == "arrow":
        import pyarrow as pa                  # pre-imported by run(), outside the timer
        tbl = pa.ipc.open_stream(pa.BufferReader(pa.py_buffer(body))).read_all()
        return tbl.num_rows, tbl.num_columns, warning, len(body)
    doc = json.loads(body)
    return len(doc["values"]), len(doc["columns"]), warning, len(body)


def run(scenario, index, route):
    query = esql_for(scenario, index)
    max_rows = effective_max_rows()
    if scenario == "S1m" and max_rows is not None and max_rows < S1M_ROWS:
        raise SystemExit(
            f"refusing to run S1m: the cluster's {SETTING} is {max_rows:,}, below the "
            f"{S1M_ROWS:,} rows this cell measures. ES|QL would return {max_rows:,} rows "
            "with HTTP 200 and no Warning header, i.e. a silently wrong answer.\n"
            "Raise it deliberately, and remember it is part of what the run reports:\n"
            f"  curl -XPUT localhost:9200/_cluster/settings -H 'Content-Type: application/json' \\\n"
            f"    -d '{{\"persistent\":{{\"{SETTING}\":{S1M_ROWS}}}}}'\n"
            f"(the product maximum is {ESQL_MAX_RESULT_ROWS:,}; higher values are refused "
            "by Elasticsearch itself)")

    # Import BEFORE the timers. Every measured run is a fresh process, so importing
    # pyarrow inside the timed region would charge the arrow route a ~0.3-1 s
    # one-off module load the json route never pays -- decisive on S4, whose whole
    # point is comparing the two wire formats. A real client imports its libraries
    # once at startup, not per query.
    if route == "arrow":
        import pyarrow                        # noqa: F401
    t0, c0 = time.perf_counter(), time.process_time()
    rows, cols, warning, nbytes = esql(query, route)
    out = {"rows": rows, "cols": cols, "route": route, "query": query,
           "response_bytes": nbytes,
           "warning_header": warning,
           "esql_result_truncation_max_size": max_rows,
           "query_wall_s": time.perf_counter() - t0,
           "wall_s": time.perf_counter() - t0,
           "cpu_s": time.process_time() - c0,
           "connect_s": 0.0,               # stateless HTTP: no session to establish
           "peak_rss_mb": peak_rss_mb(),              # legacy, compression-eroded
           "peak_footprint_mb": peak_footprint_mb(),  # headline client memory
           "mem_pressure": memory_pressure()}
    # The gate that matters most on this stack: ES|QL truncates ABOVE its ceiling
    # with a 200 and (measured) no Warning header, so a run that "succeeded" can
    # still have answered a smaller question than it was asked.
    check(scenario, out)
    return out


def probe_join(left, right):
    """Record what ES|QL does when asked for the benchmark's cross-index JOIN.

    Not a timed scenario: a refusal has no wall clock. It exists so the join
    table can carry Elasticsearch's own answer verbatim instead of an empty cell,
    which reads as "not tested" when it is in fact a product boundary --
    `LOOKUP JOIN` resolves only against an index in `lookup` mode, and such an
    index is always single-sharded, i.e. it is a dimension lookup rather than a
    join between two fact indices.
    """
    query = f"FROM {left} | LOOKUP JOIN {right} ON id | LIMIT 5"
    try:
        rows, cols, warning, nbytes = esql(query, "json")
        return {"query": query, "outcome": "completed", "rows": rows, "cols": cols}
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            reason = json.loads(body)["error"]["root_cause"][0]["reason"]
        except (ValueError, KeyError, IndexError):
            reason = body[:400]
        return {"query": query, "outcome": "rejected", "status": e.code,
                "reason": reason}


def probe_truncation(index):
    """Record what ES|QL does when asked for more rows than it can return.

    The benchmark's most load-bearing ES|QL statement -- that a request above the
    ceiling comes back truncated, HTTP 200, with no Warning header -- was for a
    while the one claim in RESULTS with no artifact behind it. It is a probe, not
    a timed scenario: what it records is a row count and a header, not a duration.
    """
    ceiling = effective_max_rows()
    query = f"FROM {index} | KEEP {', '.join(COLUMNS)} | LIMIT 10000000"
    rows, cols, warning, nbytes = esql(query, "json")
    return {"query": query, "requested_rows": 10_000_000, "rows_returned": rows,
            "http_status": 200,          # urlopen raises on anything else
            "warning_header": warning,
            "esql_result_truncation_max_size": ceiling,
            "truncated_silently": rows < 10_000_000 and warning is None}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", default="S3", choices=SCENARIOS)
    p.add_argument("--index", default=DEFAULT_INDEX)
    p.add_argument("--route", default="json", choices=ROUTES,
                   help="wire format: row-shaped json (default) or Arrow IPC")
    p.add_argument("--variant", default="")
    p.add_argument("--probe-join", nargs=2, metavar=("LEFT", "RIGHT"),
                   help="record ES|QL's answer to a cross-index JOIN and exit")
    p.add_argument("--probe-truncation", action="store_true",
                   help="record what a request above the row ceiling returns, and exit")
    p.add_argument("--out")
    a = p.parse_args()

    # The environment guard protects TIMINGS -- it refuses a host whose memory or
    # CPU state would make a wall clock fiction. A probe records a row count and a
    # response header and publishes no duration, so gating it on host fitness only
    # means the evidence cannot be captured on a busy machine. Measured runs below
    # are still guarded.
    if not (a.probe_truncation or a.probe_join):
        guard_environment(a.scenario)

    # `--probe-join` takes TWO index names, so `--probe-join --out f.json` silently
    # binds LEFT="--out". Measured 2026-08-18: that produced a probe recording
    # `outcome: rejected, status 400` -- which reads exactly like the finding this
    # probe exists to capture (ES|QL refusing a cross-index join) while actually
    # saying "invalid index name [-out]", and consumed --out so nothing was written.
    # An artifact that mimics the expected result is worse than a missing one.
    if a.probe_join and any(x.startswith("-") for x in a.probe_join):
        sys.exit(f"--probe-join takes two INDEX NAMES, got {a.probe_join!r}. "
                 "A leading '-' means a flag was swallowed as an index; put "
                 "--probe-join LEFT RIGHT before any other option.")

    if a.probe_truncation:
        emit({"stack": "esql", "scenario": "truncation-probe", "index": a.index,
              "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              **probe_truncation(a.index)}, a.out)
        raise SystemExit(0)

    if a.probe_join:
        emit({"stack": "esql", "scenario": "J-probe",
              "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              **probe_join(*a.probe_join)}, a.out)
        raise SystemExit(0)

    before_es = es_wire_bytes()
    load_before = host_load()
    result = run(a.scenario, a.index, a.route)
    result["net_es"] = es_wire_delta(before_es, es_wire_bytes())
    result["host_load_before"], result["host_load_after"] = load_before, host_load()
    # The json route is the headline (it is what every ES|QL client speaks), so it
    # carries the base variant and sits in the cross-stack comparison; the arrow
    # route is tagged, which keeps it out of those medians while still publishing
    # it. The tag composes with an explicit --variant rather than replacing it.
    emit({"stack": "esql", "scenario": a.scenario, "index": a.index,
          "variant": compose_variant(a.variant,
                                     "" if a.route == "json" else a.route),
          "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          **result}, a.out)

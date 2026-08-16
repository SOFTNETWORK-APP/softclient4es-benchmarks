#!/usr/bin/env python3.12
"""Cross-stack data-equivalence gate: do the stacks return the SAME data?

Row counts alone do not close the most damaging rebuttal a benchmark can face --
"it was faster because it returned nulls, or truncated values, or the wrong
types". This runs the same aggregate checks through every engine and compares.

⚠️ S3 IS THE REASON THIS FILE MATTERS MOST. The push-down result -- 100 groups
computed in the cluster against 10M rows scanned into Trino -- is the strongest
claim in the benchmark, and until 2026-08-16 the harness asserted only the NUMBER
of groups. An Elasticsearch `terms` aggregation is approximate on a multi-shard
index when `shard_size` is too small: the 100 groups returned can be the wrong
100, and their averages can be computed over subsets. On the 1-shard index that
cannot happen; on the 5-shard topology of RESULTS section 6 it is not automatic.
A push-down is only a win if it gives the SAME ANSWER, so this gate now compares
every group's count and average across the stacks AND records Elasticsearch's own
error bounds (`doc_count_error_upper_bound`, `sum_other_doc_count`), which are
what turn "we believe it is exact" into "Elasticsearch says it is exact".

Run once at bring-up and once per measured session. It is a correctness gate, not
a timed scenario, so nothing here is published as a performance number.

    python runners/verify_equivalence.py
    python runners/verify_equivalence.py --index bench_events_10m_s5 --out gate.json
"""
import argparse
import json
import pathlib
import sys
import urllib.request

import adbc_driver_flightsql.dbapi as dbapi
from trino.dbapi import connect

from scenarios import (DEFAULT_INDEX, EXPECTED_GROUPS, esql_for,
                       guard_environment, sql_for)

ES = "http://localhost:9200"
# The bucket size SoftClient4ES itself emits for an un-LIMITed GROUP BY
# (Bucket.DefaultSize since core#206 -- Elasticsearch's own search.max_buckets
# ceiling, chosen so the cluster fails loudly instead of truncating silently).
# The gate asks Elasticsearch with the same size the measured path uses, or it
# would be certifying a different query from the one under test.
TERMS_SIZE = 65536
# Averages: Elasticsearch combines per-shard compensated sums, Trino sums doubles
# in scan order, ES|QL does its own. Equality here is equality of VALUE, not of
# floating-point history.
AVG_TOLERANCE = 1e-9

# Each check is issued separately so that one unsupported function degrades to a
# single reported SKIP instead of voiding the whole gate.
CHECKS = [
    ("count", "SELECT COUNT(*) FROM {idx}", 0),
    ("sum_id", "SELECT SUM(id) FROM {idx}", 0),
    ("sum_qty", "SELECT SUM(qty) FROM {idx}", 0),
    # Floating-point summation order differs between an ES aggregation and a Trino
    # scan, so this one is compared with a relative tolerance rather than exactly.
    ("sum_amount", "SELECT SUM(amount) FROM {idx}", 1e-9),
]


def flight_scalar(sql):
    # IP literal, not "localhost": the Go driver resolves hostnames over real
    # DNS with a 5 s per-query timeout (arrow#151) -- see run_flight.py.
    conn = dbapi.connect("grpc://127.0.0.1:32010")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows[0][0]
    finally:
        conn.close()


def trino_scalar(sql):
    conn = connect(host="localhost", port=8080, user="bench",
                   catalog="elasticsearch", schema="default")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows[0][0]
    finally:
        conn.close()


def flight_rows(sql):
    conn = dbapi.connect("grpc://127.0.0.1:32010")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetch_arrow_table().to_pylist()
        cur.close()
        return rows
    finally:
        conn.close()


def trino_rows(sql):
    conn = connect(host="localhost", port=8080, user="bench",
                   catalog="elasticsearch", schema="default")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def esql_rows(query):
    req = urllib.request.Request(f"{ES}/_query?format=json", method="POST",
                                 data=json.dumps({"query": query}).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=600) as r:
        doc = json.loads(r.read().decode())
    names = [c["name"] for c in doc["columns"]]
    return [dict(zip(names, v)) for v in doc["values"]]


def terms_error(index):
    """Elasticsearch's own verdict on whether the `terms` aggregation was exact.

    `doc_count_error_upper_bound` is the maximum count a returned bucket could be
    missing; `sum_other_doc_count` is how many documents fell outside the returned
    buckets. Both zero means the aggregation is exact for this field and size --
    the fact that makes the push-down comparison a comparison of equal answers.
    Published beside the S3 figures rather than asserted in prose.
    """
    body = {"size": 0, "aggs": {"cats": {"terms": {"field": "category",
                                                   "size": TERMS_SIZE}}}}
    req = urllib.request.Request(f"{ES}/{index}/_search", method="POST",
                                 data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=600) as r:
        doc = json.loads(r.read().decode())
    agg = doc["aggregations"]["cats"]
    return {"buckets": len(agg["buckets"]),
            "doc_count_error_upper_bound": agg.get("doc_count_error_upper_bound"),
            "sum_other_doc_count": agg.get("sum_other_doc_count"),
            "terms_size": TERMS_SIZE}


def as_groups(rows):
    """[{category, cnt, avg_amount}] -> {category: (cnt, avg)}; None if unusable."""
    out = {}
    for r in rows:
        key = r.get("category")
        if key is None:
            return None
        out[key] = (r.get("cnt"), r.get("avg_amount"))
    return out


def compare_groups(name, left, right, failures):
    """Every group, both fields, both stacks. Reports the first differences found."""
    if left is None or right is None:
        # A stack that returned a NULL group key produced rows this gate cannot
        # verify. Silence here would publish an UNVERIFIED push-down result under a
        # PASS verdict, which is the one outcome this file exists to prevent.
        failures.append(f"{name}: one side returned no usable group rows "
                        "(a NULL category?) -- the group values were never compared")
        return f"{name}: FAILED -- one side returned no usable group rows"
    if set(left) != set(right):
        only_l = sorted(set(left) - set(right))[:5]
        only_r = sorted(set(right) - set(left))[:5]
        failures.append(f"{name}: group KEYS differ (only left: {only_l}, "
                        f"only right: {only_r})")
        return f"{name}: {len(left)} vs {len(right)} groups, keys differ"
    bad = []
    for k in sorted(left):
        (lc, la), (rc, ra) = left[k], right[k]
        if lc != rc:
            bad.append(f"{k}: count {lc} vs {rc}")
        elif not close_enough(la, ra, AVG_TOLERANCE):
            bad.append(f"{k}: avg {la!r} vs {ra!r}")
    if bad:
        failures.append(f"{name}: {len(bad)} group(s) differ -- " + "; ".join(bad[:5]))
        return f"{name}: {len(bad)}/{len(left)} groups differ"
    return f"{name}: {len(left)} groups identical (counts exact, "\
           f"averages within {AVG_TOLERANCE:g})"


def close_enough(a, b, tolerance):
    if a is None or b is None:
        return False
    if tolerance == 0:
        return a == b
    scale = max(abs(float(a)), abs(float(b)), 1.0)
    return abs(float(a) - float(b)) / scale <= tolerance


def main():
    guard_environment()
    p = argparse.ArgumentParser()
    p.add_argument("--index", default=DEFAULT_INDEX)
    p.add_argument("--out", help="write the gate's verdict as JSON (session record)")
    a = p.parse_args()

    failures, skipped, record = [], [], {"index": a.index}
    for name, template, tolerance in CHECKS:
        sql = template.format(idx=a.index)
        try:
            f = flight_scalar(sql)
        except Exception as e:
            skipped.append(f"{name}: flight could not run it ({type(e).__name__}: {e})")
            continue
        try:
            t = trino_scalar(sql)
        except Exception as e:
            skipped.append(f"{name}: trino could not run it ({type(e).__name__}: {e})")
            continue
        ok = close_enough(f, t, tolerance)
        print(f"{'ok  ' if ok else 'FAIL'} {name}: flight={f!r} trino={t!r}")
        if not ok:
            failures.append(f"{name}: flight={f!r} trino={t!r}")

    # ── S3: the push-down result, compared VALUE BY VALUE ───────────────────
    groups = {}
    for stack, fn in (("flight", lambda: flight_rows(sql_for("S3", a.index))),
                      ("trino", lambda: trino_rows(sql_for("S3", a.index))),
                      ("esql", lambda: esql_rows(esql_for("S3", a.index)))):
        try:
            groups[stack] = as_groups(fn())
        except Exception as e:
            skipped.append(f"S3 groups/{stack}: {type(e).__name__}: {e}")
    for stack, g in groups.items():
        if g is None:
            failures.append(f"S3 groups/{stack}: returned rows with a NULL category "
                            "-- not comparable, and not publishable")
        elif len(g) != EXPECTED_GROUPS:
            failures.append(f"S3 groups/{stack}: {len(g)} groups, expected "
                            f"{EXPECTED_GROUPS}")
    record["s3_groups"] = {k: (len(v) if v else None) for k, v in groups.items()}
    for left, right in (("flight", "trino"), ("flight", "esql")):
        if left in groups and right in groups:
            print(compare_groups(f"S3 groups {left} vs {right}",
                                 groups[left], groups[right], failures))

    try:
        record["terms_error"] = terms_error(a.index)
        te = record["terms_error"]
        exact = (te["doc_count_error_upper_bound"] == 0
                 and te["sum_other_doc_count"] == 0)
        print(f"{'ok  ' if exact else 'FAIL'} terms exactness: "
              f"doc_count_error_upper_bound={te['doc_count_error_upper_bound']}, "
              f"sum_other_doc_count={te['sum_other_doc_count']} "
              f"(size={te['terms_size']}, {te['buckets']} buckets)")
        if not exact:
            failures.append(
                "the terms aggregation is APPROXIMATE at this shard count and size "
                f"({te}) -- the push-down result is not the same answer as a full "
                "scan, and S3 cannot be published as if it were")
    except Exception as e:
        skipped.append(f"terms exactness: {type(e).__name__}: {e}")

    for s in skipped:
        print(f"SKIP {s}")
    record["failures"], record["skipped"] = failures, skipped
    record["verdict"] = "FAIL" if failures else ("PASS_WITH_SKIPS" if skipped else "PASS")
    if a.out:
        pathlib.Path(a.out).write_text(json.dumps(record, indent=2))
    if failures:
        sys.exit("\nDATA EQUIVALENCE FAILED -- no timing from this stack pair is "
                 "publishable until this is explained:\n  " + "\n  ".join(failures))
    if skipped:
        print("\nEquivalence gate passed on the checks that ran, but some were "
              "skipped (above). Record which, or the gate overstates its coverage.")
    else:
        print("\nAll equivalence checks passed: every stack returns the same data.")


if __name__ == "__main__":
    main()

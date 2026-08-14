#!/usr/bin/env python3.12
"""Cross-stack data-equivalence gate: do both stacks return the SAME data?

Row counts alone do not close the most damaging rebuttal a benchmark can face --
"it was faster because it returned nulls, or truncated values, or the wrong
types". This runs the same aggregate checks through both engines and compares.

Run once at bring-up and once per measured session. It is a correctness gate, not
a timed scenario, so nothing here is published as a performance number.

    python runners/verify_equivalence.py
"""
import argparse
import sys

import adbc_driver_flightsql.dbapi as dbapi
from trino.dbapi import connect

from scenarios import DEFAULT_INDEX, guard_environment

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
    a = p.parse_args()

    failures, skipped = [], []
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

    for s in skipped:
        print(f"SKIP {s}")
    if failures:
        sys.exit("\nDATA EQUIVALENCE FAILED -- no timing from this stack pair is "
                 "publishable until this is explained:\n  " + "\n  ".join(failures))
    if skipped:
        print("\nEquivalence gate passed on the checks that ran, but some were "
              "skipped (above). Record which, or the gate overstates its coverage.")
    else:
        print("\nAll equivalence checks passed: both stacks return the same data.")


if __name__ == "__main__":
    main()

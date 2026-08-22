#!/usr/bin/env python3.12
"""Decide whether an equivalence-gate record may authorise timing. Exit 0 = yes.

WHY THIS IS NOT A grep. The session scripts used to guard with
    grep -q '"verdict": "PASS' equivalence-gate.json
which has no closing quote, so it also matches "PASS_WITH_SKIPS". On 2026-08-20 that
let a session proceed in which Trino was not running and EVERY Trino leg had been
skipped: the gate reported a pass, and the harness went on to time an engine whose
agreement with ours had never been checked.

A skipped leg is the ABSENCE of evidence. For the engines a session actually TIMES,
absence is disqualifying -- a wrong answer and no answer are equally unusable as a
basis for publishing a speed comparison. ES|QL is best-effort by design (it cannot
express several of the scenarios), so its skips are recorded and tolerated.

    check_gate.py <gate.json> [--timed flight trino]
"""
import argparse
import json
import sys

def main():
    p = argparse.ArgumentParser()
    p.add_argument("record")
    p.add_argument("--timed", nargs="+", default=["flight", "trino"],
                   help="stacks this session will time; a skip naming one is fatal")
    a = p.parse_args()

    try:
        rec = json.load(open(a.record))
    except Exception as e:
        print(f"gate record unreadable ({type(e).__name__}: {e}) -- refusing")
        return 1

    if rec.get("verdict") == "FAIL":
        print(f"gate FAILED: {rec.get('failures')}")
        return 1

    skipped = rec.get("skipped", [])
    blocking = [s for s in skipped
                if any(f"{t} could not" in s or s.startswith(f"{t}:")
                       or f"/{t}:" in s or f"/{t} " in s for t in a.timed)]
    if blocking:
        print("gate did not cover an engine this session measures:")
        for s in blocking:
            print(f"  SKIPPED  {s}")
        return 1

    print(f"gate verdict {rec.get('verdict')}; {len(skipped)} non-blocking skip(s)")
    return 0

if __name__ == "__main__":
    sys.exit(main())

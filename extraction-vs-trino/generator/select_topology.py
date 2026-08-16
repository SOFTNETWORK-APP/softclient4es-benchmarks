#!/usr/bin/env python3.12
"""Make exactly ONE benchmark index resident, so page-cache conditions are comparable.

    python generator/select_topology.py --index bench_events_10m_s5
    python generator/select_topology.py --index bench_events_10m      # back to 1 shard
    python generator/select_topology.py --status                      # show, change nothing

The single-shard figures were measured with one 10M index in a 4 GB Elasticsearch
container. Keeping a second topology resident silently changes that: both indices
compete for the same page cache, so a "warm cache" is no longer the same warm cache
and the two topologies are no longer being compared like for like.

Rebuilding the other index each time would cost ~20 minutes per switch. Closing it
costs seconds and is exactly equivalent for our purposes: a closed index releases its
shards -- no heap, no page cache, no shard count reported to Trino -- while its data
stays on disk, so reopening restores it byte-for-byte.

This is deliberately a SEPARATE, EXPLICIT step rather than something a runner does
implicitly. A benchmark that silently reconfigures the cluster under the measurement
is how you get a plausible number nobody can attribute.
"""
import argparse
import json
import sys
import urllib.error
import urllib.request

ES = "http://localhost:9200"
PATTERN = "bench_events_10m*"


def req(method, path, timeout=300):
    r = urllib.request.Request(f"{ES}{path}", method=method)
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.loads(resp.read().decode() or "{}")


def indices():
    try:
        return req("GET", f"/_cat/indices/{PATTERN}?format=json&h=index,status,docs.count")
    except urllib.error.URLError as e:
        sys.exit(f"cannot reach Elasticsearch at {ES}: {e}")


def shards_of(name):
    try:
        s = req("GET", f"/{name}/_settings")
        return s[name]["settings"]["index"]["number_of_shards"]
    except Exception:
        return "?"          # a closed index does not answer; not worth failing over


def show(rows):
    if not rows:
        print(f"no index matches {PATTERN}")
        return
    for r in sorted(rows, key=lambda r: r["index"]):
        mark = "OPEN  " if r["status"] == "open" else "closed"
        shards = f"{shards_of(r['index'])} shard(s)" if r["status"] == "open" else "-"
        # A CLOSED index reports docs.count as null, not 0 -- it has no open shards
        # to count. Rendering that as 0 would read as "the data is gone".
        docs = r.get("docs.count") or "-"
        print(f"  {mark}  {r['index']:<28} {shards:>12}  {docs:>12} docs")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--index", help="the index to leave open; every other match is closed")
    p.add_argument("--status", action="store_true", help="report and exit")
    a = p.parse_args()

    rows = indices()
    if a.status or not a.index:
        show(rows)
        if not a.index:
            sys.exit(0 if a.status else "nothing to do: pass --index or --status")
        return

    names = {r["index"] for r in rows}
    if a.index not in names:
        sys.exit(f"{a.index} does not exist. Known: {', '.join(sorted(names)) or '(none)'}")

    for r in rows:
        if r["index"] == a.index and r["status"] != "open":
            req("POST", f"/{r['index']}/_open")
            print(f"opened  {r['index']}")
        elif r["index"] != a.index and r["status"] == "open":
            req("POST", f"/{r['index']}/_close")
            print(f"closed  {r['index']}")

    # Block until the survivor is actually serving: _open returns before the shards
    # are allocated, and a run started in that window measures a recovering index.
    req("GET", f"/_cluster/health/{a.index}?wait_for_status=green&timeout=120s")
    print(f"\nresident topology -> {a.index} ({shards_of(a.index)} shard(s))\n")
    show(indices())


if __name__ == "__main__":
    main()

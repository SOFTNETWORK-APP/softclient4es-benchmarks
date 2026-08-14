#!/usr/bin/env python3.12
"""S0 -- the floor both stacks pay: a raw Elasticsearch scroll, stdlib only.

Neither engine is involved. This pulls the same 8 fields, in the same 1000-doc
pages, straight from Elasticsearch over the REST API, parses each page of JSON,
counts the hits and throws them away.

Why it exists: the LOCKED framing says "everyone pays the Elasticsearch
serialization leg, the moat is what happens after". Without S0 that is an
assertion. With it, it is a number -- and every S1 comparison can be stated as
"the ES leg costs X; SoftClient4ES adds Y on top of it, Trino adds Z", which is
both more honest and a stronger claim than the bare ratio.

Deliberately does NOT build client-side objects beyond parsing: this is the floor,
not a third contender.
"""
import argparse
import json
import time
import urllib.request

from scenarios import (COLUMNS, DEFAULT_INDEX, check, emit, guard_environment,
                       memory_pressure, net_bytes, net_delta, peak_footprint_mb,
                       peak_rss_mb)

ES = "http://localhost:9200"
PAGE = 1000            # == ARROW_BATCH_SIZE == elasticsearch.scroll-size
SCROLL_TTL = "5m"


def _post(path, body):
    req = urllib.request.Request(f"{ES}{path}", data=json.dumps(body).encode(),
                                 method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=1800) as resp:
        return json.loads(resp.read().decode())


def _delete_scroll(scroll_id):
    try:
        req = urllib.request.Request(f"{ES}/_search/scroll",
                                     data=json.dumps({"scroll_id": scroll_id}).encode(),
                                     method="DELETE",
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=60).close()
    except Exception:
        pass           # a leaked scroll context expires on its own; never fail a run for it


def run(index):
    t0, c0 = time.perf_counter(), time.process_time()
    page = _post(f"/{index}/_search?scroll={SCROLL_TTL}",
                 {"size": PAGE, "_source": COLUMNS,
                  "query": {"match_all": {}},
                  "sort": ["_doc"]})
    scroll_id = page.get("_scroll_id")
    rows = len(page["hits"]["hits"])
    try:
        while True:
            page = _post("/_search/scroll",
                         {"scroll": SCROLL_TTL, "scroll_id": scroll_id})
            scroll_id = page.get("_scroll_id", scroll_id)
            hits = page["hits"]["hits"]
            if not hits:
                break
            rows += len(hits)
    finally:
        if scroll_id:
            _delete_scroll(scroll_id)

    return {"rows": rows,
            "query_wall_s": time.perf_counter() - t0,
            "wall_s": time.perf_counter() - t0,
            "cpu_s": time.process_time() - c0,
            "connect_s": 0.0,          # stateless HTTP: no session to establish
            "peak_rss_mb": peak_rss_mb(),              # legacy, compression-eroded
            "peak_footprint_mb": peak_footprint_mb(),  # headline client memory
            "mem_pressure": memory_pressure()}


if __name__ == "__main__":
    guard_environment()
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", default="S0", choices=["S0"])
    p.add_argument("--index", default=DEFAULT_INDEX)
    p.add_argument("--variant", default="")
    p.add_argument("--out")
    a = p.parse_args()

    before = net_bytes("elasticsearch")
    out = run(a.index)
    out["net"] = net_delta(before, net_bytes("elasticsearch"))
    check("S0", out)
    emit({"stack": "es-raw", "scenario": "S0", "index": a.index,
          "variant": a.variant, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          **out}, a.out)

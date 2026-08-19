#!/usr/bin/env python3.12
"""S0/S0p -- the floor both stacks pay: a raw Elasticsearch scroll, stdlib only.

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

TWO FLOORS, since 2026-08-16:

  S0   one process, one scroll. The obvious approach, and the one a first script
       looks like.
  S0p  SLICED scroll: N processes, one Elasticsearch slice each, run at the same
       time. Added after an external reviewer observed -- correctly -- that a
       single-threaded scroll is the LAZIEST approach rather than the most
       obvious one, and that calling it "the floor" without measuring the
       parallel version is a claim a reader who knows Elasticsearch will not
       grant. It may well beat both engines on wall clock. It is published
       whatever it says, and its cost is published beside it: N processes and the
       sum of their CPU, which is the metric the resource claims are made of.

  ⚠️ Slices are only as parallel as the index allows. On a single-shard index
  Elasticsearch still has one shard to read, so S0p buys little there and more on
  the 5-shard topology -- which is exactly why the floor is measured on the same
  index as the figures it is a floor for, and never quoted across topologies.

CPU accounting for S0p is deliberately the SUM over the client processes
(`RUSAGE_CHILDREN` plus the parent), not the wall-clock winner's share: five
processes that finish in a fifth of the time have not made the work cheaper, and
a floor that hid that would flatter the floor.
"""
import argparse
import json
import multiprocessing
import resource
import time
import urllib.request

from scenarios import (COLUMNS, DEFAULT_INDEX, HOST, check, emit, guard_environment,
                       host_load, memory_pressure, net_bytes, net_delta,
                       peak_footprint_mb, peak_rss_mb)

ES = f"http://{HOST}:9200"
PAGE = 1000            # == ARROW_BATCH_SIZE == elasticsearch.scroll-size
SCROLL_TTL = "5m"
# One slice per shard of the 5-shard topology. Elasticsearch's own guidance is to
# keep max <= the shard count; above it, slices share shards and contend.
SLICES = 5


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


def _scroll(index, slice_spec=None):
    """Scroll one (optionally sliced) view of the index; return the row count."""
    body = {"size": PAGE, "_source": COLUMNS,
            "query": {"match_all": {}},
            "sort": ["_doc"]}
    if slice_spec is not None:
        body["slice"] = slice_spec
    page = _post(f"/{index}/_search?scroll={SCROLL_TTL}", body)
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
    return rows


def _slice_worker(args):
    """One slice, in its own PROCESS.

    Processes, not threads: the work is JSON parsing, which holds the GIL, so a
    threaded version would measure Python's interpreter lock rather than
    Elasticsearch's parallelism -- and would understate the CPU cost of the
    approach, which is half of what this floor is for.

    Returns its own CPU and footprint so the parent can report the client's TOTAL
    cost rather than one process's share.
    """
    index, slice_id, slice_max = args
    c0 = time.process_time()
    rows = _scroll(index, {"id": slice_id, "max": slice_max})
    return {"slice": slice_id, "rows": rows,
            "cpu_s": time.process_time() - c0,
            "peak_footprint_mb": peak_footprint_mb()}


def run(index):
    t0, c0 = time.perf_counter(), time.process_time()
    rows = _scroll(index)
    return {"rows": rows,
            "query_wall_s": time.perf_counter() - t0,
            "wall_s": time.perf_counter() - t0,
            "cpu_s": time.process_time() - c0,
            "connect_s": 0.0,          # stateless HTTP: no session to establish
            "slices": 1,
            "peak_rss_mb": peak_rss_mb(),              # legacy, compression-eroded
            "peak_footprint_mb": peak_footprint_mb(),  # headline client memory
            "mem_pressure": memory_pressure()}


def run_sliced(index, slices=SLICES):
    t0, c0 = time.perf_counter(), time.process_time()
    with multiprocessing.Pool(slices) as pool:
        parts = pool.map(_slice_worker, [(index, i, slices) for i in range(slices)])
    wall = time.perf_counter() - t0
    kids = resource.getrusage(resource.RUSAGE_CHILDREN)
    # Client CPU is the SUM across the client's processes: the parent, plus the
    # children measured by the kernel. The per-slice cpu_s the workers report is
    # kept alongside it so the split is visible, but the headline is the total --
    # parallelism buys wall clock by spending more CPU, and both belong in the row.
    return {"rows": sum(p["rows"] for p in parts),
            "query_wall_s": wall,
            "wall_s": wall,
            "cpu_s": (time.process_time() - c0) + kids.ru_utime + kids.ru_stime,
            "cpu_s_parent_only": time.process_time() - c0,
            "connect_s": 0.0,          # stateless HTTP: no session to establish
            "slices": slices,
            "per_slice": parts,
            "peak_rss_mb": peak_rss_mb(),              # legacy, compression-eroded
            # Concurrent processes: the client's peak is what they hold TOGETHER,
            # so the parts are summed rather than maximised.
            "peak_footprint_mb": sum(
                p["peak_footprint_mb"] for p in parts
                if p.get("peak_footprint_mb") is not None) or peak_footprint_mb(),
            "mem_pressure": memory_pressure()}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", default="S0", choices=["S0", "S0p"])
    p.add_argument("--index", default=DEFAULT_INDEX)
    p.add_argument("--slices", type=int, default=SLICES,
                   help="S0p only: number of Elasticsearch slices, one process each")
    p.add_argument("--variant", default="")
    p.add_argument("--out")
    a = p.parse_args()
    # Parse BEFORE guarding: the memory floor is sized to the scenario about to
    # run (scenarios.min_available_gb), and a flat floor sized for the heaviest
    # arm in the matrix refuses light ones for no reason -- it blocked an S4
    # re-run on 2026-08-17 over an arm whose client holds 83 MB.
    guard_environment(a.scenario)

    before = net_bytes("elasticsearch")
    load_before = host_load()
    out = run(a.index) if a.scenario == "S0" else run_sliced(a.index, a.slices)
    out["net"] = net_delta(before, net_bytes("elasticsearch"))
    out["host_load_before"], out["host_load_after"] = load_before, host_load()
    check(a.scenario, out)
    emit({"stack": "es-raw", "scenario": a.scenario, "index": a.index,
          "variant": a.variant, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          **out}, a.out)

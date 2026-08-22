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

By default it does NOT build client-side objects beyond parsing: that is the floor,
not a third contender.

  --build arrow   builds the real artifact instead -- one Arrow table, 10M x 8,
                  assembled per page and concatenated once, the same deliverable
                  S1 produces. Added 2026-08-19 after a reviewer pointed out that
                  the sliced floor beats us on S1's own metric (22.5 s against
                  34.0 s) while being credited with a cost it does not pay: a
                  count-and-discard scroll produces nothing you can compute on, so
                  comparing its CPU against ours compares a cheaper task, not a
                  cheaper implementation. This variant removes that asymmetry and
                  is published whatever it says.

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
import sys
import time
import urllib.request

from scenarios import (COLUMNS, es_wire_bytes, es_wire_delta, DEFAULT_INDEX, HOST, check, emit, guard_environment,
                       host_load, memory_pressure, net_bytes, net_delta,
                       peak_footprint_mb, peak_rss_mb, server_cpu_delta,
                       server_cpu_sample)

ES = f"http://{HOST}:9200"
PAGE = 1000            # == ARROW_BATCH_SIZE == elasticsearch.scroll-size
SCROLL_TTL = "5m"
# One slice per PRIMARY SHARD -- resolved from the index at run time, never hardcoded.
#
# It was `SLICES = 5`, a literal left over from the era when the main index had 5
# shards. The topology was later inverted (main is 6 shards, the 1-shard index is the
# sensitivity arm) and this constant did not follow, so the floor ran 5 slices over 6
# shards: Elasticsearch hands one slice TWO shards, that slice does double the work,
# and since the wall clock is the slowest slice the floor came in ~2/6 of the corpus
# behind instead of ~1/6.
#
# That understates the competitor, and it understates it in OUR favour -- the floor is
# the number our own extraction is compared against. Deriving it removes the whole
# class of error: a floor measured at the wrong parallelism is not a floor.
SLICES = None          # None => one per primary shard, see resolve_slices()


def resolve_slices(index, requested=None):
    """Slice count for the floor: what a competent engineer would actually use."""
    if requested:
        return requested
    try:
        with urllib.request.urlopen(f"{ES}/{index}/_settings", timeout=10) as r:
            s = json.load(r)[index]["settings"]["index"]
        return int(s["number_of_shards"])
    except Exception as e:
        sys.exit(f"could not resolve the primary shard count for {index} "
                 f"({type(e).__name__}: {e}). Refusing to guess: a floor measured at "
                 f"the wrong slice count silently flatters whatever it is compared to.")


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


def _batches(hits, pa):
    """One Arrow RecordBatch per ES page, built column-wise from the parsed JSON.

    Per PAGE, not per run: holding 10M rows as Python objects before converting
    would measure Python's object overhead rather than the approach's real cost,
    and no competent implementation would do it. This is the charitable version.
    """
    cols = {c: [] for c in COLUMNS}
    for h in hits:
        src = h["_source"]
        for c in COLUMNS:
            cols[c].append(src.get(c))
    return pa.RecordBatch.from_pydict(cols)


def _scroll(index, slice_spec=None, build=None):
    """Scroll one (optionally sliced) view of the index.

    Returns the row count, or (row count, Arrow IPC bytes) when `build == "arrow"`.
    """
    body = {"size": PAGE, "_source": COLUMNS,
            "query": {"match_all": {}},
            "sort": ["_doc"]}
    if slice_spec is not None:
        body["slice"] = slice_spec
    pa = batches = None
    if build == "arrow":
        import pyarrow as pa                            # noqa: F811  (local by design)
        batches = []
    page = _post(f"/{index}/_search?scroll={SCROLL_TTL}", body)
    scroll_id = page.get("_scroll_id")
    hits = page["hits"]["hits"]
    rows = len(hits)
    if batches is not None and hits:
        batches.append(_batches(hits, pa))
    try:
        while True:
            page = _post("/_search/scroll",
                         {"scroll": SCROLL_TTL, "scroll_id": scroll_id})
            scroll_id = page.get("_scroll_id", scroll_id)
            hits = page["hits"]["hits"]
            if not hits:
                break
            rows += len(hits)
            if batches is not None:
                batches.append(_batches(hits, pa))
    finally:
        if scroll_id:
            _delete_scroll(scroll_id)
    if batches is None:
        return rows
    # Hand the slice back as Arrow IPC bytes. A multi-PROCESS pipeline has to
    # serialize somewhere, and IPC is the cheapest honest way; the cost is inside
    # the measured CPU, because it is a real cost of the approach.
    tbl = pa.Table.from_batches(batches)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, tbl.schema) as w:
        w.write_table(tbl)
    return rows, sink.getvalue().to_pybytes()


def _slice_worker(args):
    """One slice, in its own PROCESS.

    Processes, not threads: the work is JSON parsing, which holds the GIL, so a
    threaded version would measure Python's interpreter lock rather than
    Elasticsearch's parallelism -- and would understate the CPU cost of the
    approach, which is half of what this floor is for.

    Returns its own CPU and footprint so the parent can report the client's TOTAL
    cost rather than one process's share.
    """
    index, slice_id, slice_max, build = args
    if build == "arrow":
        import pyarrow                                   # noqa: F401  before the clock
    c0 = time.process_time()
    try:
        got = _scroll(index, {"id": slice_id, "max": slice_max}, build)
    except Exception as exc:
        # ⚠️ RE-RAISE AS A PLAIN STRING. multiprocessing pickles a worker's
        # exception back to the parent, and urllib's HTTPError holds an open
        # BufferedReader -- which is NOT picklable, so the real failure was
        # replaced by "TypeError: cannot pickle 'BufferedReader' instances" and
        # the cause vanished. It cost a 5-shard block on 2026-08-19: the index
        # was CLOSED (phase 3 closes it to spare the 2 GB heap) and the only
        # visible error named a pickling problem. Carry the body too -- an
        # Elasticsearch refusal explains itself there and nowhere else.
        body = ""
        try:
            body = exc.read().decode()[:400]             # HTTPError is also a file
        except Exception:
            pass
        raise RuntimeError(
            f"slice {slice_id}/{slice_max} on {index} failed: "
            f"{type(exc).__name__}: {exc}{(' -- ' + body) if body else ''}") from None
    rows, ipc = got if isinstance(got, tuple) else (got, None)
    return {"slice": slice_id, "rows": rows, "ipc": ipc,
            "cpu_s": time.process_time() - c0,
            "peak_footprint_mb": peak_footprint_mb()}


def run(index, build=None):
    if build == "arrow":
        import pyarrow                                   # noqa: F401  before the clock
    t0, c0 = time.perf_counter(), time.process_time()
    got = _scroll(index, None, build)
    rows = got[0] if isinstance(got, tuple) else got
    return {"rows": rows, "build": build or "count",
            "query_wall_s": time.perf_counter() - t0,
            "wall_s": time.perf_counter() - t0,
            "cpu_s": time.process_time() - c0,
            "connect_s": 0.0,          # stateless HTTP: no session to establish
            "slices": 1,
            "peak_rss_mb": peak_rss_mb(),              # legacy, compression-eroded
            "peak_footprint_mb": peak_footprint_mb(),  # headline client memory
            "mem_pressure": memory_pressure()}


def run_sliced(index, slices=None, build=None):
    slices = resolve_slices(index, slices)
    if build == "arrow":
        import pyarrow as pa
    t0, c0 = time.perf_counter(), time.process_time()
    with multiprocessing.Pool(slices) as pool:
        parts = pool.map(_slice_worker,
                         [(index, i, slices, build) for i in range(slices)])
    if build == "arrow":
        # ONE table, like every stack this floor is a floor for. Leaving five
        # per-slice tables would be a cheaper artifact than S1's and not the same
        # deliverable, so the concatenation is measured, not skipped.
        tbl = pa.concat_tables([pa.ipc.open_stream(p["ipc"]).read_all()
                                for p in parts])
        assert tbl.num_rows == sum(p["rows"] for p in parts), tbl.num_rows
        assert tbl.column_names == list(COLUMNS), tbl.column_names
        for p in parts:
            p.pop("ipc", None)                            # never serialize into the JSON
    else:
        for p in parts:
            p.pop("ipc", None)
    wall = time.perf_counter() - t0
    kids = resource.getrusage(resource.RUSAGE_CHILDREN)
    # Client CPU is the SUM across the client's processes: the parent, plus the
    # children measured by the kernel. The per-slice cpu_s the workers report is
    # kept alongside it so the split is visible, but the headline is the total --
    # parallelism buys wall clock by spending more CPU, and both belong in the row.
    return {"rows": sum(p["rows"] for p in parts),
            "build": build or "count",
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
    p.add_argument("--slices", type=int, default=None,
                   help="S0p only: Elasticsearch slices, one process each "
                        "(default: one per primary shard of the index under test)")
    p.add_argument("--build", default=None, choices=["arrow"],
                   help="build a real client artifact (one Arrow table) instead of "
                        "counting rows and discarding them")
    p.add_argument("--variant", default="")
    p.add_argument("--out")
    a = p.parse_args()
    # Parse BEFORE guarding: the memory floor is sized to the scenario about to
    # run (scenarios.min_available_gb), and a flat floor sized for the heaviest
    # arm in the matrix refuses light ones for no reason -- it blocked an S4
    # re-run on 2026-08-17 over an arm whose client holds 83 MB.
    guard_environment(a.scenario)

    before = es_wire_bytes()
    # The floors run no engine, so "server" is Elasticsearch alone -- which is the
    # point: it is what makes S0p's total cost comparable to a stack's.
    cpu_before = server_cpu_sample([])
    load_before = host_load()
    out = (run(a.index, a.build) if a.scenario == "S0"
           else run_sliced(a.index, a.slices, a.build))
    out["net"] = es_wire_delta(before, es_wire_bytes())
    out["server_cpu"] = server_cpu_delta(cpu_before, server_cpu_sample([]))
    out["host_load_before"], out["host_load_after"] = load_before, host_load()
    check(a.scenario, out)
    emit({"stack": "es-raw", "scenario": a.scenario, "index": a.index,
          "variant": a.variant, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          **out}, a.out)

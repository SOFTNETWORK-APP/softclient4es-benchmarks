#!/usr/bin/env python3.12
"""Record how Trino actually spread the scan across its cluster.

Section 6 of RESULTS claims the multi-shard sensitivity run genuinely exercised
parallelism -- one split per shard, spread over the workers -- rather than merely
running against a cluster that happened to be up. That claim needs an artifact.

⚠️ IT MUST BE CAPTURED WHILE THE QUERY RUNS. `system.runtime.tasks` holds only
LIVE tasks: the rows vanish the moment the query finishes, so reading them from
`system.runtime.queries` history afterwards returns nothing at all (measured
2026-08-18, five hours after the run that motivated this probe). The query is
therefore driven on a background thread and the table is polled underneath it.

This is a probe, not a timed scenario: it records a split distribution, not a
duration, so it publishes no wall clock and is not guarded on host fitness.
"""
import argparse
import json
import threading
import time

import trino

from scenarios import COLUMNS, HOST, emit


def scan_sql(index):
    return f"SELECT {', '.join(COLUMNS)} FROM {index}"


def connect():
    return trino.dbapi.connect(host=HOST, port=8080, user="bench",
                               catalog="elasticsearch", schema="default")


def nodes():
    cur = connect().cursor()
    cur.execute("SELECT node_id, coordinator, state FROM system.runtime.nodes")
    return [{"node_id": n, "coordinator": bool(c), "state": s}
            for n, c, s in cur.fetchall()]


def poll_splits(qid_box, stop, seen):
    cur = connect().cursor()
    while not stop.is_set():
        qid = qid_box.get("id")
        if qid:
            cur.execute(
                "SELECT node_id, stage_id, sum(splits) FROM system.runtime.tasks "
                f"WHERE query_id = '{qid}' GROUP BY node_id, stage_id")
            for node, stage, splits in cur.fetchall():
                # stage_id is "<query_id>.<n>", not a number -- int() on it raises,
                # and a crashed poller writes an artifact with an EMPTY split list
                # that reads exactly like "Trino used no parallelism".
                n = int(str(stage).rsplit(".", 1)[-1])
                # Keep the MAXIMUM ever observed per (node, stage): splits are
                # assigned as the scan proceeds, so an early poll under-counts.
                key = (node, n)
                seen[key] = max(seen.get(key, 0), int(splits or 0))
        time.sleep(0.25)


def probe(index):
    conn = connect()
    cur = conn.cursor()
    qid_box, stop, seen = {}, threading.Event(), {}
    poller = threading.Thread(target=poll_splits, args=(qid_box, stop, seen),
                              daemon=True)
    poller.start()

    def drive():
        cur.execute(scan_sql(index))
        qid_box["id"] = cur.stats.get("queryId")
        while cur.fetchmany(10000):
            pass

    runner = threading.Thread(target=drive)
    runner.start()
    # The query id only exists once execute() has returned, so give the poller a
    # moment to see it before the scan is over on a small index.
    runner.join()
    time.sleep(0.5)
    stop.set()
    poller.join(timeout=2)

    # The LEAF stage is the table scan: Trino numbers stages from 0 at the output,
    # so the highest stage number is the one holding the connector's splits.
    scan_stage = max((s for _, s in seen), default=0)
    return {"index": index, "query": scan_sql(index),
            "query_id": qid_box.get("id"),
            "nodes": nodes(),
            "splits_by_node_stage": [
                {"node_id": n, "stage_id": s, "splits": v}
                for (n, s), v in sorted(seen.items())],
            "scan_stage_id": scan_stage,
            "scan_splits_total": sum(v for (n, s), v in seen.items()
                                     if s == scan_stage),
            "scan_splits_by_node": {n: v for (n, s), v in seen.items()
                                    if s == scan_stage},
            "nodes_with_scan_splits": len({n for (n, s) in seen if s == scan_stage})}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--index", required=True)
    p.add_argument("--out")
    a = p.parse_args()
    emit({"stack": "trino", "scenario": "splits-probe",
          "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          **probe(a.index)}, a.out)

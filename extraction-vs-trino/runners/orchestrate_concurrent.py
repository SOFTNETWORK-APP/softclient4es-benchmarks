#!/usr/bin/env python3.12
"""S6 — how many concurrent extractions fit in a fixed client memory budget?

    python runners/orchestrate_concurrent.py --budget 8 --engines flight trino

ADDITIVE, like S5: new file, new `results/concurrent-*` namespace, nothing
existing touched.

WHY. The claim "one machine serves N times more analysts" was originally reached
by DIVIDING a memory budget by a measured footprint (8 GB / 1.5 GB -> 5). The PM
rejected that, correctly: publishing arithmetic inside a document whose entire
credibility rests on measurement is the error this benchmark exists to prevent.
Concurrency is not division -- clients contend for the engine, for the network,
and for the VM's page cache, and the honest answer can only come from running
them at the same time.

METHOD. Fix a total client memory budget. Give each concurrent client
`budget / N` and launch N of them simultaneously against the same engine. Report
how many COMPLETED with the exact expected row count, and what it cost in wall
time. A client that is OOM-killed (137) or returns short is a failure, and N is
then the answer to "how many actually fit", not "how many we launched".

The engine under test is the only one running (the idle one is stopped), because
inside a 15.6 GiB Docker VM every GiB the idle engine holds is a GiB the clients
cannot have -- the same contention trap that invalidated a first attempt at S5.

⚠️ This measures CLIENT-side capacity, which is the claim. Server-side capacity
is a different question this scenario does not answer: the sidecar and Trino are
both capped at 4 GB / 4 CPU here, and at high N the engine, not the client, may
be the limit. Where that happens the result is reported as-is and labelled.

⚠️ FAIRNESS: RUN TRINO'S BEST CLIENT TOO (--route connectorx). S1 grants Trino
its fastest clients and publishes where they beat us; measuring S6 with only the
stock client and reporting "5 versus 0" would abandon that rule three scenarios
later, which is the internal inconsistency an external reviewer flagged on
2026-08-16. connectorx peaks around 2.9 GB on this workload, so it is expected to
complete about two concurrent clients in an 8 GB budget rather than none -- and
"5 versus 2" is the result to publish, because it is the one that is true.
"""
import argparse
import concurrent.futures
import datetime
import json
import pathlib
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
RESULTS = ROOT / "results"
from scenarios import ENGINE_SERVICES, wait_trino_cluster

IMAGE = "sc4es-bench-client:latest"
NETWORK = "extraction-vs-trino_default"
# Trino is a 3-node cluster, so both sides of this are LISTS: stopping only the
# coordinator would leave two idle JVMs holding CPU and RAM inside the shared VM
# while the other engine is measured. Single source of truth in scenarios.py.
IDLE_ENGINE = {"flight": ENGINE_SERVICES["trino"], "trino": ENGINE_SERVICES["flight"]}
ENGINE_SERVICE = ENGINE_SERVICES


def set_engines(active):
    subprocess.run(["docker", "compose", "stop", *IDLE_ENGINE[active]],
                   cwd=str(ROOT), capture_output=True, timeout=180)
    subprocess.run(["docker", "compose", "start", *ENGINE_SERVICE[active]],
                   cwd=str(ROOT), capture_output=True, timeout=180)
    deadline = time.time() + 300
    while time.time() < deadline:
        r = subprocess.run(["docker", "compose", "ps", "--format", "json",
                            *ENGINE_SERVICE[active]],
                           cwd=str(ROOT), capture_output=True, text=True, timeout=60)
        ready = set()
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            # A service with no healthcheck reports "" -- running is the best signal.
            if row.get("Health") in ("healthy", "") and row.get("State") == "running":
                ready.add(row.get("Service") or row.get("Name"))
        # EVERY service of the active stack must be up, not merely the first one to
        # answer: a healthy Trino coordinator whose workers have not started would
        # otherwise read as ready, and the run would measure a 1-node cluster.
        if len(ready) >= len(ENGINE_SERVICE[active]):
            if active == "trino":
                wait_trino_cluster()
            return
        time.sleep(5)
    sys.exit(f"{ENGINE_SERVICE[active]} did not all become healthy -- aborting")


def one(engine, cap_mb, idx, route="stock", timeout=3600):
    # The client-container already implements Trino's connectorx route as
    # `--mode full-cx` (added for S5); S6 only ever lacked the plumbing to ask
    # for it.
    mode = "full-cx" if route == "connectorx" else "full"
    cmd = ["docker", "run", "--rm", "--network", NETWORK,
           "--memory", f"{cap_mb}m", "--memory-swap", f"{cap_mb}m",
           "--name", f"s6-{engine}-{route}-{idx}",
           IMAGE, "--engine", engine, "--mode", mode,
           "--cap-label", f"{cap_mb}m"]
    t0 = time.perf_counter()
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"idx": idx, "outcome": "timeout", "wall_s": time.perf_counter() - t0}
    payload = None
    for line in r.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                pass
    ok = r.returncode == 0 and payload and payload.get("rows_ok")
    return {"idx": idx, "route": route,
            "outcome": "completed" if ok else ("OOM-KILLED" if r.returncode == 137
                                               else f"failed(exit {r.returncode})"),
            "exit_code": r.returncode,
            "wall_s": (payload or {}).get("wall_s", time.perf_counter() - t0),
            "peak_rss_mb": (payload or {}).get("peak_rss_mb"),
            "rows": (payload or {}).get("rows")}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--budget", type=float, default=8.0,
                   help="TOTAL client memory budget in GiB, split evenly across N")
    p.add_argument("--levels", nargs="+", type=int, default=[1, 2, 3, 4, 5],
                   help="concurrency levels to try")
    p.add_argument("--engines", nargs="+", default=["flight", "trino"],
                   choices=["flight", "trino"])
    p.add_argument("--route", default="stock", choices=["stock", "connectorx"],
                   help="Trino client route. 'connectorx' is Trino's fastest "
                        "client and the fair comparison S1 already publishes; "
                        "ignored for the flight engine, which has one client.")
    p.add_argument("--session")
    a = p.parse_args()

    session = (pathlib.Path(a.session) if a.session else RESULTS /
               ("concurrent-" + datetime.datetime.now().strftime("%Y%m%dT%H%M%S")))
    session.mkdir(parents=True, exist_ok=True)
    print(f"session: {session}", flush=True)
    print(f"budget: {a.budget} GiB total client memory", flush=True)

    all_results = []
    for engine in a.engines:
        # The route names the client, and the client is the subject of S6 -- so it
        # is in the record and in the filename, never inferable only from a shell
        # history.
        route = a.route if engine == "trino" else "stock"
        set_engines(engine)
        for n in a.levels:
            cap_mb = int(a.budget * 1024 / n)
            label = f"[{engine}/{route} N={n} cap={cap_mb}m each]"
            print(f"{label} launching", flush=True)
            t0 = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
                runs = list(ex.map(lambda i: one(engine, cap_mb, i, route), range(n)))
            wall = time.perf_counter() - t0
            ok = sum(1 for r in runs if r["outcome"] == "completed")
            rec = {"engine": engine, "route": route, "n": n, "cap_mb": cap_mb,
                   "budget_gib": a.budget, "completed": ok,
                   "total_wall_s": wall, "runs": runs}
            all_results.append(rec)
            suffix = "" if route == "stock" else f"-{route}"
            (session / f"{engine}{suffix}-n{n}.json").write_text(json.dumps(rec, indent=2))
            print(f"{label} -> {ok}/{n} completed in {wall:.1f}s total", flush=True)
            if ok == 0:
                print(f"{label} none fit at this split; stopping this engine", flush=True)
                break

    (session / "summary.json").write_text(json.dumps(all_results, indent=2))
    print("\n=== S6: concurrent extractions within a fixed client budget ===", flush=True)
    print(f"{'engine':<9}{'route':<11}{'N':>3}{'cap each':>11}{'completed':>11}"
          f"{'total wall':>12}", flush=True)
    for r in all_results:
        print(f"{r['engine']:<9}{r.get('route', 'stock'):<11}{r['n']:>3}"
              f"{r['cap_mb']:>10}m{r['completed']:>8}/{r['n']:<2}"
              f"{r['total_wall_s']:>11.1f}s", flush=True)
    print(f"\nwrote {session}/summary.json", flush=True)


if __name__ == "__main__":
    main()

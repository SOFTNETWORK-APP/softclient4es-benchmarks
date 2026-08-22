#!/usr/bin/env python3.12
"""Bring every engine the session will measure UP, and prove it, before anything times.

WHY THIS EXISTS. verify_equivalence.py runs FIRST in a session, before any
orchestrator has touched a container -- and an orchestrator is the only thing that
ever started Trino. On 2026-08-20 that ordering silently halved the correctness
gate: Trino was not running, every Trino leg recorded a SKIP, the gate reported
PASS_WITH_SKIPS, and the session proceeded to time an engine whose agreement with
ours had never been checked. The gate is the one step whose whole purpose is to run
before the timings; it must not be the step that inherits an unprepared world.

A leg that CANNOT ANSWER and a leg that AGREES are not the same evidence, and the
difference is invisible in a summary that only prints a verdict.
"""
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from scenarios import ENGINE_SERVICES, ES_SERVICES, TRINO_WORKERS, wait_trino_cluster

SERVICES = ES_SERVICES + ENGINE_SERVICES["flight"] + ENGINE_SERVICES["trino"]


def compose(*args, timeout=180):
    return subprocess.run(["docker", "compose", *args], cwd=str(HERE.parent),
                          capture_output=True, text=True, timeout=timeout)


def healthy(service):
    """True once the service is running and either healthy or has no healthcheck."""
    r = compose("ps", "--format", "json", service, timeout=60)
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows = json.loads(line)
        except json.JSONDecodeError:
            continue
        for row in (rows if isinstance(rows, list) else [rows]):
            if row.get("State") == "running" and row.get("Health") in ("healthy", ""):
                return True
    return False


def main(timeout=420):
    # `up -d`, not `start`: a service that was never created has nothing to start.
    r = compose("up", "-d", *SERVICES, timeout=600)
    if r.returncode != 0:
        sys.exit(f"compose up failed:\n{r.stderr.strip()}")

    for service in SERVICES:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if healthy(service):
                break
            time.sleep(5)
        else:
            sys.exit(f"{service} did not become healthy within {timeout}s -- aborting. "
                     "Measuring against an engine that never came up is worse than stopping.")
        print(f"[engines] {service} healthy", flush=True)

    # Container health is NOT cluster readiness: a registered-but-not-yet-joined
    # worker reports healthy. Ask Trino itself, which also proves it can plan SQL.
    wait_trino_cluster(expected_workers=TRINO_WORKERS)
    ensure_esql_ceiling()
    print("[engines] all engines up and the Trino cluster has assembled", flush=True)


ESQL_SETTING = "esql.query.result_truncation_max_size"
ESQL_CEILING = 1_000_000          # the product maximum; ES refuses anything higher


def ensure_esql_ceiling():
    """Raise ES|QL's truncation ceiling to the product maximum, as METHODOLOGY documents.

    This is a CLUSTER setting, so it does not live in the compose file and it does not
    survive a wiped data volume -- which is exactly how it was lost. It used to be a
    manual setup step, and on 2026-08-20 a session ran for 71 minutes and then aborted
    in block 5 because the ceiling was back at its 10,000 default. The guard that caught
    it was right to abort (ES|QL returns 10,000 rows with HTTP 200 and NO warning header,
    a silently wrong answer), but a precondition that can only be discovered an hour in
    is a precondition in the wrong place.

    Setting it here is not a hidden thaw of the competitor's configuration: it RAISES a
    limit in ES|QL's favour, it is disclosed in RESULTS section 1, and every ES|QL run
    records the effective value it actually ran under.
    """
    url = "http://127.0.0.1:9200/_cluster/settings"
    try:
        with urllib.request.urlopen(f"{url}?include_defaults=true&flat_settings=true",
                                    timeout=15) as r:
            s = json.load(r)
        cur = next((s[k].get(ESQL_SETTING) for k in ("transient", "persistent", "defaults")
                    if s.get(k, {}).get(ESQL_SETTING) is not None), None)
        if cur is not None and int(cur) >= ESQL_CEILING:
            print(f"[engines] {ESQL_SETTING}={cur} (already at the ceiling)", flush=True)
            return
        req = urllib.request.Request(
            url, method="PUT", headers={"Content-Type": "application/json"},
            data=json.dumps({"persistent": {ESQL_SETTING: ESQL_CEILING}}).encode())
        with urllib.request.urlopen(req, timeout=30) as r:
            json.load(r)
        print(f"[engines] {ESQL_SETTING} raised {cur} -> {ESQL_CEILING}", flush=True)
    except Exception as e:
        sys.exit(f"could not set {ESQL_SETTING} ({type(e).__name__}: {e}). ES|QL cells "
                 f"would abort mid-session; fix the cluster before measuring.")


if __name__ == "__main__":
    main()

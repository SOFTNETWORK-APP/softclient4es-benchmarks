#!/usr/bin/env python3.12
"""Block until both engines are actually idle, then let the next block be timed.

WHY THIS EXISTS -- measured 2026-08-19, and it invalidated a whole join block.

S6 kills its client on purpose: that IS the scenario (an extraction that does not
fit the memory budget is OOM-killed, and Trino's stock client is the one that dies).
What the harness did not account for is what the SERVER does afterwards. Trino's own
event log for that session:

    FAILED (ABANDONED_QUERY) :: elapsed 223,935 ms :: finishing 174,524 ms
    FAILED (ABANDONED_TASK)  :: running 1,000,564 ms

i.e. after the client vanished, Trino went on executing and then "finishing" those
queries for minutes. The join block started 2 minutes after S6 ended and was timed
on top of that ghost work. The damage was not subtle and it was not one-sided:

    J2 wall, ours   last night 28.8 28.8 28.8 28.7 28.8
                    that night 29.2 27.7 29.0 108.0 127.4
    J2 wall, Trino  last night 27.9 27.4 27.9 27.4 27.9
                    that night 123.5 100.9 118.6 108.2 114.4
    J0 Trino        5/5 runs FAILED

BOTH engines degraded ~4x, which is the signature of a shared resource rather than a
regression in either one -- and the host was provably clean throughout (pressure
level 1, ~17.2 GB reclaimable on every run). A benchmark that cannot tell "the
competitor is slow" from "the competitor is still finishing the previous block's
work" is not measuring what it claims to measure, so this gate is a correctness
control, not a convenience.

It is deliberately NOT a fixed sleep. A sleep long enough for the worst case wastes
minutes per block, and one shorter than the worst case fails silently -- which is
precisely the failure being fixed.
"""
import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

# Trino states that mean work is still on the cluster. FINISHING matters as much as
# RUNNING: the abandoned queries above spent 174 s of their 224 s there.
BUSY_STATES = {"QUEUED", "WAITING_FOR_RESOURCES", "DISPATCHING", "PLANNING",
               "STARTING", "RUNNING", "FINISHING"}
# A container under 15% of one core is bookkeeping, not query execution. Elasticsearch
# is excluded from the CPU test on purpose: it runs merges and refreshes on its own
# schedule, and waiting for those to stop would mean waiting forever.
IDLE_CPU_PERCENT = 15.0


def trino_busy(coordinator):
    """Queries still on the Trino cluster, or a str explaining why that is unknown.

    "Unknown" is NEVER treated as idle: this gate exists to stop a block being timed
    against an engine state nobody looked at.

    ⚠️ `/v1/query` REQUIRES `X-Trino-User`. Without it Trino answers 401, not a
    connection error -- and the first version of this gate mapped that to "coordinator
    unreachable" and sat waiting for its full timeout against a perfectly healthy,
    perfectly idle cluster. The distinction is kept in the message because the two
    have opposite remedies: 401 means fix the request, refused/timeout means fix the
    cluster.
    """
    req = urllib.request.Request(f"{coordinator}/v1/query",
                                 headers={"X-Trino-User": "benchmark-idle-gate"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            queries = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return f"trino coordinator returned HTTP {e.code} for /v1/query"
    except (urllib.error.URLError, OSError) as e:
        return f"trino coordinator unreachable ({e})"
    except ValueError:
        return "trino coordinator returned unparseable JSON"
    return [q for q in queries if q.get("state") in BUSY_STATES]


def container_cpu(name):
    """Instantaneous CPU% for one container, or None if it is not running."""
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", name],
            capture_output=True, text=True, timeout=60)
    except (subprocess.SubprocessError, OSError):
        return None
    text = out.stdout.strip().rstrip("%")
    try:
        return float(text)
    except ValueError:
        return None          # container stopped: nothing to wait for


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--coordinator", default="http://localhost:8080")
    p.add_argument("--containers", nargs="*",
                   default=["extraction-vs-trino-flight-sql-1"],
                   help="containers whose CPU must fall to idle (NOT elasticsearch: "
                        "its merges and refreshes never fully stop)")
    p.add_argument("--timeout", type=float, default=600.0)
    p.add_argument("--settle", type=int, default=3,
                   help="consecutive idle samples required; one quiet sample can land "
                        "between two stages of the same query")
    p.add_argument("--interval", type=float, default=5.0)
    p.add_argument("--out")
    a = p.parse_args()

    deadline = time.monotonic() + a.timeout
    consecutive = 0
    waited_for = []
    while time.monotonic() < deadline:
        busy = trino_busy(a.coordinator)
        cpus = {c: container_cpu(c) for c in a.containers}
        hot = [f"{c}={v:.0f}%" for c, v in cpus.items()
               if v is not None and v > IDLE_CPU_PERCENT]

        if isinstance(busy, str):
            reason = busy
        elif busy:
            reason = (f"{len(busy)} trino queries busy: "
                      + ", ".join(f"{q['queryId']}({q['state']})" for q in busy[:3]))
        elif hot:
            reason = "container CPU above idle: " + ", ".join(hot)
        else:
            reason = None

        if reason is None:
            consecutive += 1
            if consecutive >= a.settle:
                record = {"verdict": "idle", "waited_s": round(
                    a.timeout - (deadline - time.monotonic()), 1),
                    "observations": waited_for[-10:]}
                print(f"engines idle after {record['waited_s']:.0f}s")
                if a.out:
                    with open(a.out, "w") as f:
                        json.dump(record, f, indent=2)
                return 0
        else:
            consecutive = 0
            waited_for.append(reason)
            print(f"waiting: {reason}", flush=True)
        time.sleep(a.interval)

    # Refuse rather than measure. The caller treats a non-zero exit as a guard
    # refusal and stops the session, which is the right outcome: the alternative is
    # a table nobody can attribute.
    print(f"refusing to continue: engines still busy after {a.timeout:.0f}s "
          f"({waited_for[-1] if waited_for else 'unknown'}). The next block would be "
          "timed against the previous one's unfinished work.", file=sys.stderr)
    if a.out:
        with open(a.out, "w") as f:
            json.dump({"verdict": "timeout", "observations": waited_for[-10:]}, f,
                      indent=2)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

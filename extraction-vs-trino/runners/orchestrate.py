#!/usr/bin/env python3.12
"""Warmups + measured runs for the full scenario matrix.

Every MEASURED run is a fresh subprocess: clean peak RSS, clean allocator.
Warmups also warm the Elasticsearch page cache for the stack about to be measured.

Ordering is SCENARIO-major, stack-minor: the two numbers that get compared to each
other are produced adjacent in time, which is what keeps thermal drift and page-cache
state from turning into an apparent wire-format effect over a multi-hour session.

    python runners/orchestrate.py                          # full matrix, new session
    python runners/orchestrate.py --scenarios S4           # one scenario (bring-up)
    python runners/orchestrate.py --limit 10000            # Community-scale rehearsal
    python runners/orchestrate.py --session results/2026...  # resume / top up

Resume is by output file: an existing <stack>-<scenario>-run<N>.json is left alone,
so an interrupted session continues where it stopped instead of restarting.
"""
import argparse
import atexit
import datetime
import json
import os
import pathlib
import subprocess
import sys
import time

from scenarios import REQUIRED_STACKS, SCENARIOS

HERE = pathlib.Path(__file__).resolve().parent
RESULTS = HERE.parent / "results"
RUNNERS = {"es-raw": "run_es.py", "flight": "run_flight.py", "trino": "run_trino.py"}
# Which compose service each stack needs up; used by --stop-idle-engine.
ENGINE_OF = {"flight": "flight-sql", "trino": "trino"}


# Set once in main() from --dtype-backend / --frame. Only the flight/trino
# runners accept the flags (run_es.py does not), and they only change S1r.
DTYPE_BACKEND = "default"
FRAME = "default"
ROUTE = "default"


def one(stack, scenario, index, variant, out=None, timeout=7200):
    cmd = [sys.executable, str(HERE / RUNNERS[stack]), "--scenario", scenario,
           "--index", index]
    if variant:
        cmd += ["--variant", variant]
    if DTYPE_BACKEND != "default" and stack != "es-raw":
        cmd += ["--dtype-backend", DTYPE_BACKEND]
    if FRAME != "default" and stack != "es-raw":
        cmd += ["--frame", FRAME]
    if ROUTE != "default" and stack == "trino":
        cmd += ["--route", ROUTE]
    if out:
        cmd += ["--out", str(out)]
    return subprocess.run(cmd, timeout=timeout, capture_output=True, text=True)


def run_with_retry(stack, scenario, index, variant, out=None):
    """Retry once on a transient failure; never retry a correctness failure.

    A failed assert means the run returned the wrong number of rows -- a licence
    cap, a short index, a broken scenario. Retrying that just wastes an hour and
    risks burying it, so it aborts the session immediately.
    """
    for attempt in (1, 2):
        r = one(stack, scenario, index, variant, out)
        if r.returncode == 0:
            return r
        sys.stderr.write(r.stderr)
        if "AssertionError" in r.stderr:
            sys.exit(f"\n{stack}/{scenario}: correctness gate failed -- aborting the "
                     "session rather than reporting a run that did not return the "
                     "expected data.")
        if attempt == 1:
            print(f"[{stack} {scenario}] failed (exit {r.returncode}), retrying once",
                  flush=True)
    sys.exit(f"{stack}/{scenario}: failed twice -- aborting.")


def environment(session):
    """Freeze the version/environment facts RESULTS section 1 has to report.

    Captured mechanically: hand-transcribed version tables rot.
    """
    def capture(name, cmd, cwd=None):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                               cwd=cwd)
            (session / name).write_text(r.stdout or r.stderr)
        except Exception as e:                  # never let bookkeeping kill a session
            (session / name).write_text(f"capture failed: {e}\n")

    capture("pip-freeze.txt", [sys.executable, "-m", "pip", "freeze"])
    capture("docker-images.txt", ["docker", "compose", "images"])
    # Pin the sidecar's IDENTITY, not just its tag. SIDECAR_TAG may point at a -SNAPSHOT,
    # which is republished in place: two sessions can name the same tag and measure
    # different builds. The digest is what makes a number attributable to a build.
    #
    # Interrogate the RUNNING CONTAINER, never a tag string rebuilt from os.environ.
    # SIDECAR_TAG lives in .env, which docker compose reads and Python does NOT: the
    # first version of this reconstructed the ref, silently fell back to the default
    # tag, and recorded a stale local image -- labelling a fixed-build session as the
    # pre-fix build. The container knows what it is actually running; ask it.
    capture("sidecar-image.txt", ["sh", "-c",
            # -a: --stop-idle-engine may have this service stopped when the capture
            # runs; a stopped container still knows which image it was created from.
            'cid=$(docker compose ps -aq flight-sql | head -1) || exit 1; '
            '[ -n "$cid" ] || { echo "flight-sql container not found"; exit 1; }; '
            'docker inspect "$cid" --format '
            '"config_image={{.Config.Image}}{{println}}image_id={{.Image}}"; '
            'docker image inspect "$(docker inspect "$cid" --format "{{.Image}}")" '
            '--format "repo_tags={{.RepoTags}}{{println}}repo_digests={{.RepoDigests}}'
            '{{println}}created={{.Created}}"'],
            cwd=str(HERE.parent))
    # The digest identifies the build; this says what is IN it. Both matter, because
    # every artefact in the sidecar is itself a moving -SNAPSHOT: on 2026-08-11 the tag
    # 0.2.5-SNAPSHOT was republished with new arrow-core, arrow-join AND softclient4es-core
    # jars while the core's own version string (0.20.4-SNAPSHOT) did not change. A reader
    # comparing two sessions can diff these checksums and see exactly which components
    # moved -- something neither the tag nor the version strings can tell them.
    capture("sidecar-jars.txt", ["sh", "-c",
            'cid=$(docker compose ps -aq flight-sql | head -1) || exit 1; '
            '[ -n "$cid" ] || { echo "flight-sql container not found"; exit 1; }; '
            'img=$(docker inspect "$cid" --format "{{.Image}}"); '
            'docker run --rm --entrypoint sh "$img" -c '
            '"sha256sum /opt/docker/lib/app.softnetwork.*.jar" 2>&1'],
            cwd=str(HERE.parent))
    capture("docker-info.txt", ["docker", "info"])
    capture("uname.txt", ["uname", "-a"])
    capture("cpu.txt", ["sysctl", "-n", "machdep.cpu.brand_string"])


def wait_healthy(service, timeout=300):
    """Block until `service` reports healthy.

    `docker compose start` returns as soon as the container is RUNNING, which is not
    the same as the engine inside it being able to serve: Trino needs 1-2 minutes to
    reach ACTIVE. Without this wait the first warmup hits a closed port, the single
    retry hits it again moments later, and the whole block aborts with
    "failed twice" -- which is exactly what killed the trino/S1 block on 2026-08-07,
    seconds before the container went healthy.

    A timeout here is a hard failure: silently measuring an engine that never became
    healthy would be worse than stopping.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(["docker", "compose", "ps", "--format", "json", service],
                           cwd=str(HERE.parent), capture_output=True, text=True,
                           timeout=60)
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows = json.loads(line)
            except json.JSONDecodeError:
                continue
            for row in (rows if isinstance(rows, list) else [rows]):
                # A service with no healthcheck reports "" -- treat running as ready,
                # since there is nothing better to wait on.
                if row.get("Health") in ("healthy", "") and row.get("State") == "running":
                    return
        time.sleep(3)
    sys.exit(f"{service} did not become healthy within {timeout}s -- aborting rather "
             f"than measuring an engine that may not be serving")


def set_engines(active_stack, enabled):
    """Stop the engine that is not under test, so an idle JVM cannot hold pages
    or CPU while the other stack is being measured."""
    if not enabled:
        return
    for stack, service in ENGINE_OF.items():
        action = "start" if stack == active_stack else "stop"
        subprocess.run(["docker", "compose", action, service],
                       cwd=str(HERE.parent), capture_output=True, timeout=300)
    # Only AFTER the stops, so the engine under test warms up without the other
    # competing for CPU, and so we never start measuring before it can serve.
    service = ENGINE_OF.get(active_stack)
    if service:
        wait_healthy(service)


def acquire_single_run_lock():
    """Refuse to start while another orchestrator is measuring. Never expires.

    Two concurrent orchestrators do not merely share a host -- they actively
    sabotage each other. `set_engines()` runs `docker compose stop` on whichever
    engine IT considers idle, so orchestrator A stops the very container
    orchestrator B is mid-extraction against. Observed 2026-08-11: the sidecar
    took SIGTERM 16 s into a measured S1 run (graceful goaway, exit 137), B's run
    failed twice and aborted, and BOTH sessions' earlier blocks were silently
    invalid anyway -- they had been competing for the same CPUs and page cache
    for 11 minutes. Nothing in the output said so; the numbers looked ordinary.

    That is the failure mode this benchmark cannot tolerate: not a crash, but a
    plausible number produced under conditions nobody recorded. Hence a hard
    refusal rather than a warning.

    The lock is held by PID and checked for liveness, so a killed orchestrator
    does not strand it -- but a STALE lock is never assumed: if the recorded
    process is alive, this one exits.
    """
    lock = RESULTS / ".orchestrator.lock"
    RESULTS.mkdir(parents=True, exist_ok=True)
    if lock.exists():
        try:
            other = int(lock.read_text().split()[0])
        except (ValueError, IndexError, OSError):
            other = None
        if other and other != os.getpid():
            try:
                os.kill(other, 0)          # signal 0 = liveness probe only
                sys.exit(
                    f"refusing to start: orchestrator pid {other} is already "
                    f"running (lock: {lock}).\nTwo orchestrators stop each "
                    "other's containers mid-run and silently invalidate BOTH "
                    "sessions. Wait for it, or kill it and delete the lock.")
            except ProcessLookupError:
                print(f"stale lock from dead pid {other} -- taking it over",
                      file=sys.stderr)
    lock.write_text(f"{os.getpid()} {datetime.datetime.now().isoformat()}\n")
    atexit.register(lambda: lock.unlink(missing_ok=True))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--warmups", type=int, default=2)
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--scenarios", nargs="+", default=SCENARIOS, choices=SCENARIOS)
    p.add_argument("--stacks", nargs="+", default=list(RUNNERS), choices=list(RUNNERS))
    p.add_argument("--index", default="bench_events_10m")
    p.add_argument("--variant", default="",
                   help="tag for a sensitivity variant, e.g. 4shard; keeps it out of "
                        "the headline medians")
    p.add_argument("--session", help="existing session dir to resume or top up")
    p.add_argument("--stop-idle-engine", action="store_true",
                   help="stop the engine not under test during each block")
    p.add_argument("--dtype-backend", default="default", choices=["default", "pyarrow"],
                   help="S1r sensitivity variant: Arrow-backed DataFrames on both "
                        "engines. Auto-tags the runs (variant 'arrowdtype') so they "
                        "can never blend into the headline medians.")
    p.add_argument("--frame", default="default",
                   choices=["default", "polars", "polars-cx", "polars-adbc", "pandas-cx", "pandas-adbc"],
                   help="S1r sensitivity variant: land a polars frame instead of "
                        "pandas. 'polars' = each engine's documented route "
                        "(Flight: from_arrow; Trino: pl.read_database over "
                        "trino.sqlalchemy). 'polars-cx' = connectorx and "
                        "'polars-adbc' = the ADBC Foundry trino driver, both "
                        "Trino only -- use with --stacks trino. Auto-tags the "
                        "variant so these runs never blend into the headline "
                        "medians.")
    p.add_argument("--route", default="default", choices=["default", "connectorx", "adbc"],
                   help="S1 sensitivity variant, Trino only: land the Arrow table "
                        "via connectorx or the ADBC Foundry trino driver instead "
                        "of stock trino.dbapi fetchall. Use with --stacks trino.")
    a = p.parse_args()

    global DTYPE_BACKEND, FRAME, ROUTE
    DTYPE_BACKEND = a.dtype_backend
    FRAME = a.frame
    ROUTE = a.route
    if a.frame != "default" and a.dtype_backend != "default":
        sys.exit("--dtype-backend is a pandas concept; do not combine with --frame")
    if a.frame in ("polars-cx", "polars-adbc", "pandas-cx", "pandas-adbc") and a.stacks != ["trino"]:
        sys.exit(f"--frame {a.frame} is a Trino-only route; pass --stacks trino")
    if a.route != "default" and a.stacks != ["trino"]:
        sys.exit("--route is a Trino-only S1 variant; pass --stacks trino")
    if a.dtype_backend != "default" and not a.variant:
        a.variant = "arrowdtype"
    if a.frame != "default" and not a.variant:
        a.variant = a.frame.replace("-", "")   # polars / polarscx / polarsadbc
    if a.route != "default" and not a.variant:
        a.variant = "arrow" + ("cx" if a.route == "connectorx" else a.route)

    if a.runs < 5 or a.warmups < 2:
        print(f"WARNING: {a.warmups} warmups / {a.runs} runs is below the "
              "benchmark's >=2 / >=5 -- bring-up only, not publishable.",
              file=sys.stderr)

    acquire_single_run_lock()

    if a.session:
        session = pathlib.Path(a.session)
        session.mkdir(parents=True, exist_ok=True)
    else:
        stamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
        session = RESULTS / stamp
        session.mkdir(parents=True, exist_ok=True)
    # Printed FIRST: after a crash the session dir is what you need to resume.
    print(f"session: {session}", flush=True)
    environment(session)

    plan = [(sc, st) for sc in a.scenarios
            for st in sorted(REQUIRED_STACKS[sc] & set(a.stacks))]
    (session / "plan.json").write_text(json.dumps(
        {"warmups": a.warmups, "runs": a.runs, "index": a.index,
         "variant": a.variant, "plan": plan}, indent=2))
    if not plan:
        sys.exit("empty plan: the requested scenarios and stacks do not intersect")

    done = 0
    prefix = f"{a.variant}-" if a.variant else ""
    for scenario, stack in plan:
        set_engines(stack, a.stop_idle_engine)
        pending = [i for i in range(1, a.runs + 1)
                   if not (session / f"{prefix}{stack}-{scenario}-run{i}.json").exists()]
        if not pending:
            print(f"[{stack} {scenario}] already complete, skipping", flush=True)
            continue
        for i in range(a.warmups):
            print(f"[{stack} {scenario}] warmup {i + 1}/{a.warmups}", flush=True)
            run_with_retry(stack, scenario, a.index, a.variant)
        for i in pending:
            out = session / f"{prefix}{stack}-{scenario}-run{i}.json"
            print(f"[{stack} {scenario}] run {i}/{a.runs}", flush=True)
            run_with_retry(stack, scenario, a.index, a.variant, out)
            done += 1
    print(f"\ncompleted {done} measured run(s)")
    print(f"session: {session}")


if __name__ == "__main__":
    main()

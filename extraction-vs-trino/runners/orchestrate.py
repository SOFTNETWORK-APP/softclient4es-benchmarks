#!/usr/bin/env python3.12
"""Warmups + measured runs for the full scenario matrix.

Every MEASURED run is a fresh subprocess: clean peak RSS, clean allocator.
Warmups also warm the Elasticsearch page cache for the stack about to be measured.

Ordering is SCENARIO-major, stack-minor: the two numbers that get compared to each
other are produced adjacent in time, which is what keeps thermal drift and page-cache
state from turning into an apparent wire-format effect over a multi-hour session.

    python runners/orchestrate.py                          # full matrix, new session
    python runners/orchestrate.py --scenarios S4           # one scenario (bring-up)
    python runners/orchestrate.py --session results/2026...  # resume / top up

    # the blocks added 2026-08-16 for the external-review campaign
    python runners/orchestrate.py --stacks esql --scenarios S1m S3 S4
    python runners/orchestrate.py --stacks es-raw --scenarios S0 S0p
    python runners/orchestrate.py --stacks flight --scenarios S1 --drift-scenarios S1
    python runners/orchestrate.py --stacks trino --scenarios S1 --trino-catalog elasticsearch_tuned
    python runners/orchestrate.py --stacks flight --scenarios S4 --dial hostname

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

from scenarios import (ENGINE_SERVICES, REQUIRED_STACKS, SCENARIOS, stacks_for,
                       wait_trino_cluster)

HERE = pathlib.Path(__file__).resolve().parent
RESULTS = HERE.parent / "results"
RUNNERS = {"es-raw": "run_es.py", "flight": "run_flight.py", "trino": "run_trino.py",
           "esql": "run_esql.py"}
# Which compose services each stack needs up; used by --stop-idle-engine.
# Trino is a 3-node cluster, so this is a LIST per stack -- see scenarios.py.
ENGINE_OF = ENGINE_SERVICES


# Set once in main() from --dtype-backend / --frame. Only the flight/trino
# runners accept the flags (run_es.py does not), and they only change S1r.
DTYPE_BACKEND = "default"
FRAME = "default"
ROUTE = "default"
ESQL_ROUTE = "json"
DIAL = "ip"
TRINO_CATALOG = "elasticsearch"


def one(stack, scenario, index, variant, out=None, timeout=7200):
    cmd = [sys.executable, str(HERE / RUNNERS[stack]), "--scenario", scenario,
           "--index", index]
    if variant:
        cmd += ["--variant", variant]
    # Flags are stack-scoped on purpose: passing a Trino client-route flag to the
    # ES|QL runner (or a pandas dtype flag to the raw scroll) would fail the run
    # rather than be ignored, and a failed run inside a block reads as a defect.
    if DTYPE_BACKEND != "default" and stack in ("flight", "trino"):
        cmd += ["--dtype-backend", DTYPE_BACKEND]
    if FRAME != "default" and stack in ("flight", "trino"):
        cmd += ["--frame", FRAME]
    if ROUTE != "default" and stack == "trino":
        cmd += ["--route", ROUTE]
    if stack == "esql":
        cmd += ["--route", ESQL_ROUTE]
    if DIAL != "ip" and stack == "flight":
        cmd += ["--dial", DIAL]
    if TRINO_CATALOG != "elasticsearch" and stack == "trino":
        cmd += ["--catalog", TRINO_CATALOG]
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
        if "refusing to" in r.stderr:
            # A guard refusal (host unfit, assertions disabled, ES|QL ceiling below
            # the cell) is a decision, not a transient. Retrying it burns a run and
            # arrives at the same place, one screen further from the reason.
            sys.exit(f"\n{stack}/{scenario}: a guard refused this run -- aborting. "
                     "The refusal above says what to change.")
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
    # BOTH page sizes, read from what is actually running. METHODOLOGY's fairness
    # rule is that the two sides page identically; a rule stated in prose and never
    # captured is a rule nobody can check afterwards -- and since the tuned arm
    # exists, "which page size produced this session" stopped being a constant.
    capture("page-sizes.txt", ["sh", "-c",
            'cid=$(docker compose ps -aq flight-sql | head -1); '
            '[ -n "$cid" ] && docker inspect "$cid" --format '
            '"sidecar_env={{range .Config.Env}}{{println .}}{{end}}" '
            '| grep -i ARROW_BATCH_SIZE; '
            'echo "--- trino catalogs ---"; '
            'grep -H "scroll-size" trino/catalog/*.properties'],
            cwd=str(HERE.parent))
    # ES|QL is a third stack whose behaviour depends on a CLUSTER setting, not on a
    # container: record the effective ceiling, or an ES|QL figure cannot be
    # attributed to the configuration that produced it.
    capture("esql-settings.txt", ["sh", "-c",
            "curl -s 'localhost:9200/_cluster/settings"
            "?include_defaults=true&flat_settings=true' "
            "| tr ',' '\\n' | grep -i esql.query.result_truncation; "
            "curl -s localhost:9200/_license | tr ',' '\\n' | grep -i '\"type\"'"])


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
    for stack, services in ENGINE_OF.items():
        action = "start" if stack == active_stack else "stop"
        subprocess.run(["docker", "compose", action, *services],
                       cwd=str(HERE.parent), capture_output=True, timeout=300)
    # Only AFTER the stops, so the engine under test warms up without the other
    # competing for CPU, and so we never start measuring before it can serve.
    for service in ENGINE_OF.get(active_stack, []):
        wait_healthy(service)
    # Every container healthy still is not a formed cluster: the workers register
    # with the coordinator afterwards, and a query issued in that window runs on
    # fewer nodes than the run claims to have measured.
    if active_stack == "trino":
        wait_trino_cluster()


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
    # ES|QL is opt-in, NOT part of the default matrix: its cells need a cluster
    # setting raised deliberately, so a default run on a stock cluster would abort
    # hours in, after S0/S0p/S1 had already completed.
    p.add_argument("--stacks", nargs="+", default=["es-raw", "flight", "trino"],
                   choices=list(RUNNERS))
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
                   help="S1/S1m sensitivity variant, Trino only: land the Arrow table "
                        "via connectorx or the ADBC Foundry trino driver instead "
                        "of stock trino.dbapi fetchall. Use with --stacks trino.")
    p.add_argument("--esql-route", default="json", choices=["json", "arrow"],
                   help="ES|QL wire format: the row-shaped json every client speaks "
                        "(default, and the one that sits in the cross-stack "
                        "comparison), or Elasticsearch's native Arrow IPC stream.")
    p.add_argument("--dial", default="ip", choices=["ip", "hostname"],
                   help="Flight only: how the sidecar is addressed. 'hostname' "
                        "measures the Go-driver DNS cost the IP literal avoids, so "
                        "the S4 control can publish both.")
    p.add_argument("--trino-catalog", default="elasticsearch",
                   choices=["elasticsearch", "elasticsearch_tuned"],
                   help="Trino page-size arm: the default catalog (scroll-size 1000, "
                        "symmetric with ARROW_BATCH_SIZE) or the tuned one (5000). "
                        "The tuned arm auto-tags its runs so they cannot blend into "
                        "the headline medians.")
    p.add_argument("--drift-scenarios", nargs="+", default=[], choices=SCENARIOS,
                   help="after the plan, re-run the FIRST stack of each named "
                        "scenario and write it as drift-*. An A-B-A block: the two "
                        "A blocks bound the session drift that would otherwise be "
                        "confounded with engine identity.")
    a = p.parse_args()

    global DTYPE_BACKEND, FRAME, ROUTE, ESQL_ROUTE, DIAL, TRINO_CATALOG
    DTYPE_BACKEND = a.dtype_backend
    FRAME = a.frame
    ROUTE = a.route
    ESQL_ROUTE = a.esql_route
    DIAL = a.dial
    TRINO_CATALOG = a.trino_catalog
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
    # Every sensitivity flag must reach the FILENAME, not only the run JSON: the
    # resume logic keys on `<variant>-<stack>-<scenario>-runN.json`, so an untagged
    # tuned/hostname/arrow block would find the headline block's files and report
    # "already complete, skipping" -- producing zero runs and looking like success.
    for flag, tag in ((a.esql_route != "json", a.esql_route),
                      (a.dial != "ip", f"dial{a.dial}"),
                      (a.trino_catalog != "elasticsearch", "tuned")):
        if flag and tag not in a.variant.split("-"):
            a.variant = f"{a.variant}-{tag}" if a.variant else tag

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
            for st in sorted(stacks_for(sc) & set(a.stacks))]
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
    # ── drift control ────────────────────────────────────────────────────────
    # Runs are blocked by stack (one engine up at a time is a hard constraint of a
    # 10-vCPU VM), so within a session the A block and the B block are separated in
    # time and any monotonic drift -- thermal, page cache, host state -- is
    # confounded with engine identity. Interleaving run-by-run would trade that
    # confound for a worse one: three JVM restarts per run, i.e. a JIT warm-up
    # confound. Re-running the FIRST stack at the END bounds the drift instead of
    # assuming it away: if drift-A matches A inside the published spread, the
    # objection is closed with data.
    for scenario in a.drift_scenarios:
        stacks = sorted(stacks_for(scenario) & set(a.stacks))
        if not stacks:
            continue
        stack = stacks[0]
        # A DISTINCT variant, not just a distinct filename: summarize.py groups by
        # what the run FILE says, so a drift run tagged like its own A block would
        # silently double the sample and hide the very drift it was run to expose.
        drift_variant = f"{a.variant}-drift" if a.variant else "drift"
        pending = [i for i in range(1, a.runs + 1)
                   if not (session / f"{drift_variant}-{stack}-{scenario}-run{i}.json"
                           ).exists()]
        if not pending:
            print(f"[drift {stack} {scenario}] already complete, skipping", flush=True)
            continue
        set_engines(stack, a.stop_idle_engine)
        for i in range(a.warmups):
            print(f"[drift {stack} {scenario}] warmup {i + 1}/{a.warmups}", flush=True)
            run_with_retry(stack, scenario, a.index, drift_variant)
        for i in pending:
            out = session / f"{drift_variant}-{stack}-{scenario}-run{i}.json"
            print(f"[drift {stack} {scenario}] run {i}/{a.runs}", flush=True)
            run_with_retry(stack, scenario, a.index, drift_variant, out)
            done += 1

    print(f"\ncompleted {done} measured run(s)")
    print(f"session: {session}")


if __name__ == "__main__":
    main()

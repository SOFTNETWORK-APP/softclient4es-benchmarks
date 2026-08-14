#!/usr/bin/env python3.12
"""Warmups + measured runs for the cross-index JOIN, with recorded provenance.

    python runners/orchestrate_join.py                    # 1 warmup + 5 runs, both engines
    python runners/orchestrate_join.py --runs 3
    python runners/orchestrate_join.py --engines flight   # one side only

Ordering is ENGINE-minor for the same reason the extraction matrix is
scenario-major: the two numbers that get compared are produced adjacent in time,
so thermal drift and page-cache state cannot masquerade as an engine difference.

Why a separate orchestrator rather than a scenario in orchestrate.py: JOIN is the
one query that can take the sidecar down instead of merely losing (arrow#147), so
it gets a health assertion per run and a heavier failure policy -- and keeping it
out of the extraction matrix means a JOIN regression can never abort the session
that produces the headline extraction ledger.

RESULTS Appendix A's numbers came from a scratch script that wrote no provenance,
during a session in which the 0.2.5-SNAPSHOT tag was republished underneath the
measurement. Recording the digest AND the per-jar checksums is the entire reason
this file exists.
"""
import argparse
import datetime
import json
import pathlib
import statistics
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
RESULTS = HERE.parent / "results"

# The JOIN suite needs BOTH engines, unlike orchestrate.py whose --stop-idle-engine
# deliberately leaves only the engine under test running. Chaining the two therefore
# starts this suite with one engine stopped, and on 2026-08-12 that recorded
# "flight/J0 FAILS on this engine" for a sidecar that was simply not running --
# a harness artifact indistinguishable in the output from a product failure.
# So this orchestrator ensures its own preconditions instead of inheriting state.
ENGINES = ("flight-sql", "trino")


def ensure_engines_up(timeout=300):
    for service in ENGINES:
        subprocess.run(["docker", "compose", "start", service],
                       cwd=str(HERE.parent), capture_output=True, timeout=120)
    for service in ENGINES:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = subprocess.run(["docker", "compose", "ps", "--format", "json", service],
                               cwd=str(HERE.parent), capture_output=True, text=True,
                               timeout=60)
            ok = False
            for line in r.stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rows = json.loads(line)
                except json.JSONDecodeError:
                    continue
                for row in (rows if isinstance(rows, list) else [rows]):
                    if row.get("Health") in ("healthy", "") and row.get("State") == "running":
                        ok = True
            if ok:
                break
            time.sleep(5)
        else:
            # Hard failure on purpose: measuring against an engine that never became
            # healthy is worse than stopping.
            sys.exit(f"{service} did not become healthy within {timeout}s -- aborting")
        print(f"[engines] {service} healthy", flush=True)


def capture(session, name, cmd, cwd=None):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=180, cwd=cwd)
        (session / name).write_text(r.stdout or r.stderr)
    except Exception as e:                     # never let bookkeeping kill a session
        (session / name).write_text(f"capture failed: {e}\n")


def environment(session):
    """Freeze what was measured. The tag is NOT provenance -- the digest is, and
    even the digest does not say which SNAPSHOT jars are inside it, so record both."""
    capture(session, "pip-freeze.txt", [sys.executable, "-m", "pip", "freeze"])
    capture(session, "sidecar-image.txt", ["sh", "-c",
            'cid=$(docker compose ps -aq flight-sql | head -1) || exit 1; '
            '[ -n "$cid" ] || { echo "flight-sql container not found"; exit 1; }; '
            'docker inspect "$cid" --format '
            '"config_image={{.Config.Image}}{{println}}image_id={{.Image}}"; '
            'docker image inspect "$(docker inspect "$cid" --format "{{.Image}}")" '
            '--format "repo_tags={{.RepoTags}}{{println}}repo_digests={{.RepoDigests}}'
            '{{println}}created={{.Created}}"'], cwd=str(HERE.parent))
    capture(session, "sidecar-jars.txt", ["sh", "-c",
            'cid=$(docker compose ps -aq flight-sql | head -1) || exit 1; '
            '[ -n "$cid" ] || { echo "flight-sql container not found"; exit 1; }; '
            'img=$(docker inspect "$cid" --format "{{.Image}}"); '
            'docker run --rm --entrypoint sh "$img" -c '
            '"sha256sum /opt/docker/lib/app.softnetwork.*.jar" 2>&1'],
            cwd=str(HERE.parent))
    capture(session, "cpu.txt", ["sysctl", "-n", "machdep.cpu.brand_string"])


def one(engine, scenario, small, large, out):
    cmd = [sys.executable, str(HERE / "run_join.py"), "--engine", engine,
           "--scenario", scenario, "--small", small, "--large", large,
           "--out", str(out)]
    return subprocess.run(cmd, timeout=3600, capture_output=True, text=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--warmups", type=int, default=1)
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--engines", nargs="+", default=["flight", "trino"],
                   choices=["flight", "trino"])
    p.add_argument("--scenarios", nargs="+", default=["J0", "J1", "J2"],
                   choices=["J0", "J1", "J2"],
                   help="J2 (join + GROUP BY) runs by default again since the "
                        "arrow#158 fix (verified 2026-08-11); a failing scenario is "
                        "recorded as a result either way, never silently omitted.")
    p.add_argument("--small", default="bench_1m")
    p.add_argument("--large", default="bench_events_10m")
    p.add_argument("--session")
    a = p.parse_args()

    session = (pathlib.Path(a.session) if a.session else RESULTS /
               ("join-" + datetime.datetime.now().strftime("%Y%m%dT%H%M%S")))
    session.mkdir(parents=True, exist_ok=True)
    print(f"session: {session}", flush=True)
    ensure_engines_up()
    environment(session)

    failures = {}
    for scenario in a.scenarios:
        for engine in a.engines:
            label = f"[{engine} {scenario}]"
            broken = False
            for w in range(1, a.warmups + 1):
                print(f"{label} warmup {w}/{a.warmups}", flush=True)
                r = one(engine, scenario, a.small, a.large,
                        session / f"warmup-{engine}-{scenario}-{w}.json")
                if r.returncode != 0:
                    # A scenario one engine cannot run is a RESULT, not a crash:
                    # record it and keep measuring the others. Aborting here would
                    # have thrown away J0/J1 because J2 is broken.
                    tail = (r.stderr or "").strip().splitlines()
                    failures[f"{engine}/{scenario}"] = tail[-1][:300] if tail else "unknown"
                    print(f"{label} ⚠️  FAILS on this engine — recorded, "
                          f"skipping its measured runs", flush=True)
                    broken = True
                    break
            if broken:
                continue
            for i in range(1, a.runs + 1):
                out = session / f"{engine}-{scenario}-run{i}.json"
                if out.exists():
                    print(f"{label} run {i}/{a.runs} (already present, skipped)", flush=True)
                    continue
                print(f"{label} run {i}/{a.runs}", flush=True)
                r = one(engine, scenario, a.small, a.large, out)
                if r.returncode != 0:
                    sys.stderr.write(r.stderr)
                    # Past the warmup the engine HAS run this shape, so a failure
                    # now is a correctness or health fault: never averaged away.
                    sys.exit(f"\n{engine}/{scenario} run {i} FAILED after a clean "
                             "warmup -- aborting the session.")

    summarize(session, a.engines, a.scenarios, failures)


def summarize(session, engines, scenarios, failures=None):
    summary = {"failures": failures or {}}
    for scenario in scenarios:
        print(f"\n{scenario}  {'engine':<12}{'n':>3}{'median wall':>14}{'spread':>9}"
              f"{'cpu':>9}{'footprint':>12}{'rows':>12}", flush=True)
        per = {}
        for engine in engines:
            runs = sorted(session.glob(f"{engine}-{scenario}-run*.json"))
            if not runs:
                # Absence must never read as completeness (the summarize.py defect
                # noted in RESULTS section 6).
                why = (failures or {}).get(f"{engine}/{scenario}")
                print(f"    {engine:<12}  0   {'FAILED — ' + why[:60] if why else 'NO RUNS RECORDED'}",
                      flush=True)
                continue
            data = [json.loads(f.read_text()) for f in runs]
            walls = sorted(d["wall_s"] for d in data)
            med = statistics.median(walls)
            spread = (walls[-1] - walls[0]) / med * 100 if med else 0
            cpu = statistics.median(d["cpu_s"] for d in data)
            fp = [d.get("peak_footprint_mb") for d in data if d.get("peak_footprint_mb")]
            fpm = statistics.median(fp) if fp else float("nan")
            rows = sorted({d["rows"] for d in data})
            per[engine] = {"n": len(data), "median_wall_s": med,
                           "spread_pct": spread, "median_cpu_s": cpu,
                           "median_footprint_mb": fpm, "walls_s": walls,
                           "rows": rows}
            print(f"    {engine:<12}{len(data):>3}{med:>13.2f}s{spread:>8.1f}%"
                  f"{cpu:>8.2f}s{fpm:>11.0f}M{rows[0]:>12,}", flush=True)
        if len(per) == 2:
            f, t = per["flight"]["median_wall_s"], per["trino"]["median_wall_s"]
            faster, ratio = ("Trino", f / t) if t < f else ("SoftClient4ES", t / f)
            print(f"    -> {faster} faster on wall clock by {ratio:.2f}x", flush=True)
        summary[scenario] = per
    if failures:
        print("\n⚠️  scenarios that FAILED (recorded, not measured):", flush=True)
        for k, v in failures.items():
            print(f"    {k}: {v[:160]}", flush=True)
    (session / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"\nwrote {session}/summary.json", flush=True)


if __name__ == "__main__":
    main()

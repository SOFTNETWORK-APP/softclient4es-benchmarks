#!/usr/bin/env python3.12
"""S5 — the constrained-memory sweep: at what client memory does each engine stop fitting?

    python runners/orchestrate_capped.py                      # full sweep, both modes
    python runners/orchestrate_capped.py --caps 8g 4g 2g
    python runners/orchestrate_capped.py --modes full         # skip the chunked arm

ADDITIVE. This scenario adds files and a `results/capped-*` session namespace; it
does not touch S0-S4, S1r, J0-J2, their runners, or any recorded session. Nothing
already measured changes because of it.

WHY IT EXISTS. Every other scenario answers "how fast" with a ratio, and a ratio
can always be argued down. This one answers "does it complete" with yes or no.
The claim it supports -- *Trino's client needs ~8 GB to hand you 10M rows as a
DataFrame; ours needs ~1.5 GB* -- was until now an INFERENCE from footprint
measurements. Publishing an inference dressed as a demonstration is precisely the
error this benchmark keeps correcting, so: measure it.

THE CHUNKED ARM IS NOT OPTIONAL. "Just use chunksize" is the first thing a
competent Trino user will say, and a threshold claim that dodges its strongest
objection is worthless. Both engines get a streaming arm (Trino:
`read_sql(chunksize=)`; ours: `fetch_record_batch()`), and if chunking rescues
Trino at a cap where the full arm dies, that result gets published too -- it
scopes the claim to "the whole result set resident as one DataFrame", which is
the common analytical workflow and an honest boundary.

Each run is a fresh container with a real cgroup cap. `--memory-swap` is pinned
equal to `--memory` on purpose: without it the kernel pages instead of killing,
turning a clean "does not fit" into a slow "sort of fits" that measures nothing.
Exit 137 (SIGKILL) is the OOM killer, and it is a RESULT, not an error.

⚠️ THE CLIENT SHARES THE DOCKER VM WITH THE ENGINES. This host's VM is 15.6 GiB
and the three engine containers hold ~5.9 GiB of it, so a client cap much above
8g oversubscribes the VM and measures CONTENTION, not the client's requirement.
Learned the hard way: a first attempt at cap=12g left our Flight client thrashing
for a full hour before the harness timed it out -- a "result" that says nothing
about the product. Two guards now:

  * the idle engine is stopped for each run (frees 1.5-1.9 GiB, same policy as
    orchestrate.py's --stop-idle-engine);
  * caps above MAX_SAFE_CAP_GB are refused outright rather than run and
    misinterpreted.

This costs nothing analytically: Trino's requirement is ~8.5 GB and ours ~1.5 GB,
so the entire interesting range is 1-8g and it fits comfortably.
"""
import argparse
import datetime
import json
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
RESULTS = ROOT / "results"
from scenarios import DEFAULT_INDEX, ENGINE_SERVICES, wait_trino_cluster

IMAGE = "sc4es-bench-client:latest"
# Compose derives the network name from the project directory.
NETWORK = "extraction-vs-trino_default"
# Above this the client contends with the engines inside the 15.6 GiB Docker VM
# and the run measures contention rather than the client's requirement.
MAX_SAFE_CAP_GB = 8
# Which engine to stop while the other is under test. Elasticsearch stays up for both.
# Trino is a 3-node cluster, so both sides of this are LISTS: stopping only the
# coordinator would leave two idle JVMs holding CPU and RAM inside the shared VM
# while the other engine is measured. Single source of truth in scenarios.py.
IDLE_ENGINE = {"flight": ENGINE_SERVICES["trino"], "trino": ENGINE_SERVICES["flight"]}
ENGINE_SERVICE = ENGINE_SERVICES


def parse_cap_gb(cap):
    c = cap.strip().lower()
    if c.endswith("g"):
        return float(c[:-1])
    if c.endswith("m"):
        return float(c[:-1]) / 1024
    raise ValueError(f"unrecognised cap {cap!r}: use e.g. 4g or 512m")


def set_engines(active):
    """Stop the idle engine, ensure the active one is up and healthy.

    Same policy as orchestrate.py --stop-idle-engine, and here it is not just
    about CPU: every GiB the idle engine holds is a GiB the capped client cannot
    have inside the shared VM."""
    idle = IDLE_ENGINE[active]
    subprocess.run(["docker", "compose", "stop", *idle],
                   cwd=str(ROOT), capture_output=True, timeout=180)
    subprocess.run(["docker", "compose", "start", *ENGINE_SERVICE[active]],
                   cwd=str(ROOT), capture_output=True, timeout=180)
    import time as _t
    deadline = _t.time() + 300
    while _t.time() < deadline:
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
        _t.sleep(5)
    sys.exit(f"{ENGINE_SERVICE[active]} did not all become healthy -- aborting")


def build_image():
    print(f"[build] {IMAGE}", flush=True)
    r = subprocess.run(["docker", "build", "-q", "-t", IMAGE, str(ROOT / "client-container")],
                       capture_output=True, text=True, timeout=900)
    if r.returncode != 0:
        sys.exit(f"image build failed:\n{r.stderr[-2000:]}")
    print(f"[build] ok {r.stdout.strip()[:20]}", flush=True)


def one(engine, mode, cap, chunksize, index, timeout=3600):
    """One capped run. Returns (parsed_json_or_None, exit_code)."""
    cmd = ["docker", "run", "--rm", "--network", NETWORK,
           "--memory", cap, "--memory-swap", cap,
           "-e", f"BENCH_INDEX={index}",
           IMAGE, "--engine", engine, "--mode", mode, "--cap-label", cap]
    if mode == "chunked":
        cmd += ["--chunksize", str(chunksize)]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"outcome": "timeout"}, -1
    payload = None
    for line in r.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                pass
    return payload, r.returncode


def verdict(payload, code):
    """Translate an exit code into the yes/no this scenario exists to answer.

    137 = SIGKILL from the cgroup OOM killer: the process was killed mid-flight
    and printed nothing, so absence of output IS the finding."""
    if code == 137:
        return "OOM-KILLED"
    if code == 0 and payload and payload.get("rows_ok"):
        return "completed"
    if payload and payload.get("outcome") == "MemoryError":
        return "MemoryError"
    if payload and payload.get("outcome") == "timeout":
        return "timeout"
    if code == 3:
        return "WRONG ROW COUNT"
    return f"failed(exit {code})"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--caps", nargs="+", default=["8g", "6g", "4g", "3g", "2g"],
                   help="descending client memory caps to sweep")
    p.add_argument("--engines", nargs="+", default=["flight", "trino"],
                   choices=["flight", "trino"])
    p.add_argument("--modes", nargs="+", default=["full", "chunked"],
                   choices=["full", "chunked", "full-cx"])
    p.add_argument("--chunksize", type=int, default=100_000)
    p.add_argument("--index", default=DEFAULT_INDEX,
                   help=f"index under test (default {DEFAULT_INDEX}). The multi-shard "
                        "sensitivity topology lives in its own index, so this MUST be "
                        "passed for that campaign -- otherwise the capped run silently "
                        "measures the 1-shard index while the label says 5 shards.")
    p.add_argument("--session")
    p.add_argument("--skip-build", action="store_true")
    a = p.parse_args()

    over = [c for c in a.caps if parse_cap_gb(c) > MAX_SAFE_CAP_GB]
    if over:
        sys.exit(
            f"refusing caps {over}: above {MAX_SAFE_CAP_GB}g the client contends with the\n"
            f"engines inside the shared Docker VM (15.6 GiB, ~5.9 GiB held by engines) and the\n"
            "run measures contention, not the client's requirement. A cap=12g attempt on\n"
            "2026-08-12 thrashed for an hour and produced a meaningless 'timeout'.\n"
            "Trino needs ~8.5 GB and we need ~1.5 GB, so 1-8g covers the whole question.")

    session = (pathlib.Path(a.session) if a.session else RESULTS /
               ("capped-" + datetime.datetime.now().strftime("%Y%m%dT%H%M%S")))
    session.mkdir(parents=True, exist_ok=True)
    print(f"session: {session}", flush=True)

    if not a.skip_build:
        build_image()

    # Provenance: the sidecar digest and its jars, same contract as the other
    # orchestrators -- a threshold result is worthless without knowing the build.
    for name, cmd in [
        ("sidecar-image.txt",
         'cid=$(docker compose ps -aq flight-sql | head -1); '
         'docker image inspect "$(docker inspect "$cid" --format "{{.Image}}")" '
         '--format "repo_digests={{.RepoDigests}}{{println}}created={{.Created}}"'),
        ("sidecar-jars.txt",
         'cid=$(docker compose ps -aq flight-sql | head -1); '
         'img=$(docker inspect "$cid" --format "{{.Image}}"); '
         'docker run --rm --entrypoint sh "$img" -c "sha256sum /opt/docker/lib/app.softnetwork.*.jar"'),
    ]:
        try:
            r = subprocess.run(["sh", "-c", cmd], cwd=str(ROOT),
                               capture_output=True, text=True, timeout=180)
            (session / name).write_text(r.stdout or r.stderr)
        except Exception as e:
            (session / name).write_text(f"capture failed: {e}\n")

    # Provenance: which index -- and therefore which shard topology -- this session
    # measured. A capped result labelled "5 shards" that silently ran against the
    # 1-shard index would be indistinguishable from a real one.
    (session / "topology.txt").write_text(
        "index={}\ncaps={}\nengines={}\n".format(
            a.index, " ".join(a.caps), " ".join(a.engines)))

    results = []
    for mode in a.modes:
        for cap in a.caps:
            for engine in a.engines:
                label = f"[{engine} {mode} cap={cap}]"
                set_engines(engine)
                print(f"{label} running", flush=True)
                payload, code = one(engine, mode, cap, a.chunksize, a.index)
                v = verdict(payload, code)
                rec = {"engine": engine, "mode": mode, "cap": cap,
                       "verdict": v, "exit_code": code, "payload": payload}
                results.append(rec)
                (session / f"{engine}-{mode}-{cap}.json").write_text(json.dumps(rec, indent=2))
                extra = ""
                if payload and payload.get("wall_s"):
                    extra = f"  wall={payload['wall_s']:.1f}s"
                    if payload.get("peak_rss_mb"):
                        extra += f" peak={payload['peak_rss_mb']:.0f}MB"
                print(f"{label} -> {v}{extra}", flush=True)

    (session / "summary.json").write_text(json.dumps(results, indent=2))

    print("\n=== S5: does it fit? ===", flush=True)
    for mode in a.modes:
        print(f"\nmode={mode}", flush=True)
        print(f"{'cap':>6}  {'SoftClient4ES':<28} {'Trino':<28}", flush=True)
        for cap in a.caps:
            cells = []
            for engine in a.engines:
                r = next((x for x in results if x["engine"] == engine
                          and x["mode"] == mode and x["cap"] == cap), None)
                if not r:
                    cells.append("-")
                    continue
                pl = r.get("payload") or {}
                s = r["verdict"]
                if s == "completed" and pl.get("wall_s"):
                    s = f"completed  {pl['wall_s']:.1f}s  peak {pl.get('peak_rss_mb', 0):.0f}MB"
                cells.append(s)
            print(f"{cap:>6}  {cells[0]:<28} {cells[1] if len(cells) > 1 else '-':<28}", flush=True)
    print(f"\nwrote {session}/summary.json", flush=True)


if __name__ == "__main__":
    main()

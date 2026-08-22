#!/bin/bash
# One session, every published cell of the single-shard matrix.
#
# ⚠️ /bin/bash ABSOLUTE, NOT `env bash`, AND INVOKE IT AS `/bin/bash …`.
# Measured 2026-08-17: `bash` on this PATH is /usr/local/bin/bash, an Intel-only
# Homebrew build. It runs under Rosetta and TRANSLATES EVERY CHILD, so the Python
# that measures client CPU -- the headline metric -- would have been translated for
# the whole session. `uname -m` reports x86_64 and sysctl.proc_translated reports 1
# under it; /bin/bash is a universal binary and reports arm64 and 0.
# guard_environment() caught it in under a second, which is what it is for.
#
# WHY THIS EXISTS. RESULTS quoted four sessions because the matrix was run in
# pieces over four days, and five of the fifteen corrections an architect review
# raised in August 2026 existed only to label that mixture carefully. "Carefully
# labelled" is a much weaker answer to a reviewer than "one session, one table",
# so every invocation below shares one --session directory and the document can
# then be written against it.
#
# ORDER MATTERS. The equivalence gate runs FIRST and a failure aborts everything:
# there is no point timing two engines that disagree about the data. The drift
# control runs LAST, because its whole job is to measure how far the machine moved
# between the first block and the end of the session.
#
# Not included, deliberately: the 5-shard topology sensitivity. It regenerates a
# second ten-million-row index and reconfigures the cluster, so it is its own
# session and its own decision.
#
#   bash runners/run_full_session.sh [session-name]
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PY=.venv/bin/python
SESSION="results/${1:-$(date +%Y%m%dT%H%M%S)}"
LOG="$SESSION/session.log"
mkdir -p "$SESSION"

say() { printf '\n=== %s === %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG"; }
run() {
  say "$*"
  if ! "$@" >>"$LOG" 2>&1; then
    say "FAILED: $*"
    # A guard refusal is a decision, not a transient: stop rather than press on and
    # leave a session that is part fit and part not, with nothing saying which.
    if grep -q "refusing to" "$LOG"; then
      say "a guard refused -- aborting the session"; exit 1
    fi
    say "continuing; the summary will show the gap"
  fi
}

say "session $SESSION"

# ── 0. correctness before timing ─────────────────────────────────────────────
# Engines FIRST. The gate is the only step that runs before any orchestrator, and an
# orchestrator is the only thing that ever started Trino -- so on 2026-08-20 the gate
# ran against a stopped Trino, skipped every Trino leg, and still reported PASS.
run $PY runners/ensure_engines.py
if ! grep -q "the Trino cluster has assembled" "$LOG"; then
  say "engines did not come up -- refusing to measure"; exit 1
fi

run $PY runners/verify_equivalence.py --out "$SESSION/equivalence-gate.json"
# Parse the verdict; do NOT grep it. `grep '"verdict": "PASS'` has no closing quote,
# so it also matches "PASS_WITH_SKIPS" -- a gate in which a whole engine was skipped
# read as a pass. A skipped leg is the ABSENCE of evidence, and for the two engines
# this session actually times, absence is disqualifying. ES|QL is best-effort by
# design (it cannot express several scenarios), so its skips are recorded, not fatal.
if ! $PY - "$SESSION/equivalence-gate.json" <<'EOF'
import json, sys
rec = json.load(open(sys.argv[1]))
timed = ("flight", "trino")
blocking = [s for s in rec.get("skipped", [])
            if any(f"{t} could not" in s or s.startswith(f"{t}:")
                   or f"/{t}:" in s or f"/{t} " in s for t in timed)]
if rec.get("verdict") == "FAIL":
    print(f"gate FAILED: {rec.get('failures')}"); sys.exit(1)
if blocking:
    print("gate did not cover an engine this session measures:")
    for s in blocking:
        print(f"  SKIPPED  {s}")
    sys.exit(1)
print(f"gate verdict {rec.get('verdict')}; "
      f"{len(rec.get('skipped', []))} non-blocking skip(s)")
EOF
then
  say "equivalence gate did not cover both timed engines -- refusing to measure"; exit 1
fi

# ── 0b. warm the corpus to STEADY STATE before the first timed block ─────────
# Review 2026-08-21, point 1.1: the v030 session's first engine block measured
# 13.07 s where the same cell measured 9.78 s at session end -- a gradual
# cluster-side warm-in whose mechanism is not established (NOT a cold page cache:
# two warm-ups already preceded the first timed run, and Elasticsearch CPU was
# still declining after SEVEN full reads, 50.2 -> 45.4 s). So a fixed pass count
# is exactly the mistake already made once; instead, warm until the cluster says
# it is warm: full untimed extractions until the per-pass Elasticsearch CPU
# changes by <5% twice in a row (max 12 passes -- ~2 min). The evidence lands in
# warm-in.json, and the drift control at the END of the session is the check
# that this worked: if S1 drift still exceeds the block's own spread, the
# session's early blocks may not be published.
say "warming the corpus to steady state before the first timed block"
if ! $PY - "$SESSION" >>"$LOG" 2>&1 <<'EOF'
import json, sys, time
sys.path.insert(0, "runners")
from scenarios import server_cpu_sample, server_cpu_delta
import adbc_driver_flightsql.dbapi as dbapi

passes, stable = [], 0
for i in range(12):
    before = server_cpu_sample([])
    t0 = time.perf_counter()
    with dbapi.connect("grpc://127.0.0.1:32010") as c, c.cursor() as cur:
        cur.execute("SELECT id, event_ts, amount, qty, status, country, category, name "
                    "FROM bench_events_10m")
        rows = cur.fetch_arrow_table().num_rows
    wall = time.perf_counter() - t0
    es = (server_cpu_delta(before, server_cpu_sample([])) or {}).get("elasticsearch_cpu_s")
    passes.append({"pass": i + 1, "rows": rows, "wall_s": round(wall, 2),
                   "elasticsearch_cpu_s": es})
    print(f"warm pass {i+1}: {rows} rows, wall {wall:.2f} s, ES CPU {es} s", flush=True)
    assert rows == 10_000_000, rows
    if len(passes) >= 2 and es and passes[-2]["elasticsearch_cpu_s"]:
        prev = passes[-2]["elasticsearch_cpu_s"]
        stable = stable + 1 if abs(es - prev) / prev < 0.05 else 0
        if stable >= 2:
            print(f"steady state after {i+1} passes", flush=True)
            break
json.dump({"passes": passes, "steady": stable >= 2},
          open(sys.argv[1] + "/warm-in.json", "w"), indent=1)
sys.exit(0 if stable >= 2 else 1)
EOF
then
  say "corpus did NOT reach steady state in 12 passes -- refusing to measure"; exit 1
fi
say "corpus warmed to steady state (see warm-in.json)"

O="$PY runners/orchestrate.py --session $SESSION --stop-idle-engine"

# ── 1. floors ────────────────────────────────────────────────────────────────
run $O --stacks es-raw --scenarios S0 S0p

# ── 2. the headline matrix, both engines' documented clients ─────────────────
run $O --stacks flight trino --scenarios S1 S1m S1r S2 S3 S4

# ── 3. Trino's FASTEST clients, which S1 grants it and RESULTS publishes ──────
run $O --stacks trino --scenarios S1 --route connectorx
run $O --stacks trino --scenarios S1 --route adbc

# ── 4. every S1r destination, both sides, no flattering subset ───────────────
run $O --stacks flight --scenarios S1r --dtype-backend pyarrow
run $O --stacks trino  --scenarios S1r --dtype-backend pyarrow
for f in polars polars-cx polars-adbc pandas-cx pandas-adbc; do
  case "$f" in polars) run $O --stacks flight trino --scenarios S1r --frame "$f" ;;
               *)      run $O --stacks trino        --scenarios S1r --frame "$f" ;;
  esac
done

# ── 5. ES|QL, both wire formats, wherever it can run at all ──────────────────
run $O --stacks esql --scenarios S1m S3 S4
run $O --stacks esql --scenarios S1m S3 S4 --esql-route arrow
run $PY runners/run_esql.py --probe-truncation --out "$SESSION/esql-truncation-probe.json"
# ⚠️ TWO INDEX NAMES, BEFORE any other option. `--probe-join` is nargs=2, so
# `--probe-join --out f.json` binds LEFT="--out" -- which once produced a probe
# recording "rejected, status 400", i.e. an artifact that reads exactly like the
# ES|QL refusal this probe exists to capture. run_esql.py now REFUSES that shape,
# and on 2026-08-19 the refusal cost a full session its join evidence because the
# guard was added and this call site was not. Pass the pair the J scenarios use
# (orchestrate_join.py's --large / --small defaults).
run $PY runners/run_esql.py --probe-join bench_events_10m bench_1m \
        --out "$SESSION/esql-join-probe.json"

# ── 6. sensitivity arms, each tagged so it cannot blend into a median ────────
run $O --stacks trino  --scenarios S1 --trino-catalog elasticsearch_tuned
run $O --stacks flight --scenarios S4 --dial hostname
run $PY runners/probe_connect.py --repeat 30 --out "$SESSION/connect-probe.json"

# ── 7. drift LAST: how far did the machine move under the session? ───────────
# BOTH engines. v030's lesson, written in RESULTS and then not codified here: a
# drift arm for one engine proves nothing about the other -- the asymmetry (we
# moved 25%, Trino 0.3%) WAS the finding. The 2026-08-21 session ran flight-only
# from this script and Trino's arm had to be appended by hand.
run $O --scenarios S1 --drift-scenarios S1 --stacks flight trino

say "matrix complete -- summarizing"
# summarize.py PRINTS the report; it does not write it. run() sends stdout to the
# log, so redirect explicitly or the session ends with no summary.md.
if $PY runners/summarize.py "$SESSION" > "$SESSION/summary.md" 2>>"$LOG"; then
  say "wrote $SESSION/summary.md"
else
  say "FAILED: summarize"; rm -f "$SESSION/summary.md"
fi
say "DONE  $SESSION"

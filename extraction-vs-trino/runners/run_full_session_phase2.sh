#!/bin/bash
# Phase 2: the scenarios that need their own container topology.
#
# ⚠️ INVOKE AS `/bin/bash runners/run_full_session_phase2.sh` -- see the header of
# run_full_session.sh. `bash` on this PATH is an Intel-only Homebrew build that
# translates every child under Rosetta, which silently corrupts client CPU.
#
# WHY SEPARATE FROM PHASE 1. These three do not measure a client against a warm
# engine in the host's Python; each rebuilds its own world:
#   S5  runs the client INSIDE a memory-capped Linux container, once per cap
#   S6  launches N capped containers simultaneously against one engine
#   J   uses a second index (bench_1m) and a different runner entirely
# They therefore write to their own results/ namespaces (capped-*, concurrent-*,
# join-*) and cannot share phase 1's --session directory. That is a real limit on
# "one session, one table": the extraction matrix is now one session, but S5/S6/J
# remain separately attributable and RESULTS must keep saying so.
#
# S5 sweeps the caps RESULTS publishes (8/6/4/3/2 GB) for both modes and all three
# client routes, so the whole table comes from one run rather than three sessions
# as the published one did.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PY=.venv/bin/python
STAMP="${1:-$(date +%Y%m%dT%H%M%S)}"
LOG="results/phase2-$STAMP.log"

say() { printf '\n=== %s === %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG"; }
run() {
  say "$*"
  if ! "$@" >>"$LOG" 2>&1; then
    say "FAILED: $*"
    if grep -q "refusing to" "$LOG"; then say "a guard refused -- aborting"; exit 1; fi
    say "continuing; the gap will be visible in the results"
  fi
}

say "phase 2 -- S5, S6, joins"

# ── S5: does the extraction fit in a small container? ────────────────────────
# full = whole result as one DataFrame; chunked = streaming; full-cx = connectorx,
# Trino's fastest client, which S1 already grants it.
run $PY runners/orchestrate_capped.py --session "results/capped-$STAMP" \
        --caps 8g 6g 4g 3g 2g --engines flight trino --modes full chunked --skip-build
run $PY runners/orchestrate_capped.py --session "results/capped-cx-$STAMP" \
        --caps 8g 6g 4g 3g 2g --engines trino --modes full-cx --skip-build

# ── S6: how many concurrent extractions fit in 8 GB? ─────────────────────────
run $PY runners/orchestrate_concurrent.py --budget 8 --session "results/concurrent-$STAMP"
run $PY runners/orchestrate_concurrent.py --budget 8 --route connectorx \
        --engines trino --session "results/concurrent-cx-$STAMP"

# Both blocks above KILL their clients on purpose (that is the scenario). Trino
# keeps executing and "finishing" an abandoned query for minutes afterwards --
# measured 2026-08-19: ABANDONED_QUERY with 174s of "finishing", and an
# ABANDONED_TASK still running after 1,000s. The join block below started two
# minutes after S6 and was timed on top of it: BOTH engines' J2 degraded ~4x and
# Trino's J0 failed 5/5, on a host that was provably clean. Wait for the engines
# to actually drain instead of assuming a block ends when its client exits.
run $PY runners/wait_engines_idle.py --out "results/idle-gate-$STAMP.json"

# ── J0-J2: cross-index JOIN, the scenarios Trino wins ────────────────────────
run $PY runners/orchestrate_join.py --session "results/join-$STAMP"

say "DONE  phase 2 ($STAMP)"

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
run $PY runners/verify_equivalence.py --out "$SESSION/equivalence-gate.json"
if ! grep -q '"verdict": "PASS' "$SESSION/equivalence-gate.json" 2>/dev/null; then
  say "equivalence gate did not PASS -- refusing to measure"; exit 1
fi

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
run $O --scenarios S1 --drift-scenarios S1 --stacks flight

say "matrix complete -- summarizing"
# summarize.py PRINTS the report; it does not write it. run() sends stdout to the
# log, so redirect explicitly or the session ends with no summary.md.
if $PY runners/summarize.py "$SESSION" > "$SESSION/summary.md" 2>>"$LOG"; then
  say "wrote $SESSION/summary.md"
else
  say "FAILED: summarize"; rm -f "$SESSION/summary.md"
fi
say "DONE  $SESSION"

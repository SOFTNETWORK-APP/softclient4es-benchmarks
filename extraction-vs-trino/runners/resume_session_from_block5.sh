#!/bin/bash
# Resume run_full_session.sh from block 5 into an EXISTING session directory.
#
# ⚠️ INVOKE AS `/bin/bash runners/resume_session_from_block5.sh results/<session>`.
#
# WHY. On 2026-08-20 a session completed blocks 0-4 and then aborted in block 5: ES|QL's
# `esql.query.result_truncation_max_size` had returned to its 10,000 default (it is a
# CLUSTER setting, so wiping the data volumes to fix a split brain took it with them).
# The guard was right to abort -- ES|QL answers HTTP 200 with 10,000 rows and no warning
# header, so measuring it would have published a silently truncated cell. Blocks 0-4 are
# unaffected: they were measured before the failure and never touch ES|QL.
#
# The commands below are copied VERBATIM from run_full_session.sh blocks 5-7 so the
# resumed session is the same session, not a similar one.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PY=.venv/bin/python
SESSION="${1:?usage: resume_session_from_block5.sh results/<session>}"
[ -d "$SESSION" ] || { echo "no such session: $SESSION"; exit 1; }
LOG="$SESSION/session.log"

say() { printf '\n=== %s === %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG"; }
run() {
  say "$*"
  if ! "$@" >>"$LOG" 2>&1; then
    say "FAILED: $*"
    if grep -q "refusing to" "$LOG"; then
      say "a guard refused -- aborting the session"; exit 1
    fi
    say "continuing; the summary will show the gap"
  fi
}

say "RESUMING $SESSION at block 5 (blocks 0-4 already measured)"
run $PY runners/ensure_engines.py

O="$PY runners/orchestrate.py --session $SESSION --stop-idle-engine"

# ── 5. ES|QL, both wire formats, wherever it can run at all ──────────────────
run $O --stacks esql --scenarios S1m S3 S4
run $O --stacks esql --scenarios S1m S3 S4 --esql-route arrow
run $PY runners/run_esql.py --probe-truncation --out "$SESSION/esql-truncation-probe.json"
# ⚠️ TWO INDEX NAMES, BEFORE any other option -- `--probe-join` is nargs=2.
run $PY runners/run_esql.py --probe-join bench_events_10m bench_1m \
        --out "$SESSION/esql-join-probe.json"

# ── 6. sensitivity arms, each tagged so it cannot blend into a median ────────
run $O --stacks trino  --scenarios S1 --trino-catalog elasticsearch_tuned
run $O --stacks flight --scenarios S4 --dial hostname
run $PY runners/probe_connect.py --repeat 30 --out "$SESSION/connect-probe.json"

# ── 7. drift LAST: how far did the machine move under the session? ───────────
run $O --scenarios S1 --drift-scenarios S1 --stacks flight

say "matrix complete -- summarizing"
if $PY runners/summarize.py "$SESSION" > "$SESSION/summary.md" 2>>"$LOG"; then
  say "wrote $SESSION/summary.md"
else
  say "FAILED: summarize"; rm -f "$SESSION/summary.md"
fi
say "DONE  $SESSION"

#!/bin/bash
# Phase 3: index-topology sensitivity -- the same corpus at 5 shards.
#
# ⚠️ INVOKE AS `/bin/bash runners/run_full_session_phase3.sh` (see run_full_session.sh).
#
# WHY IT IS ITS OWN SESSION. Only ONE benchmark index may be resident: two 10M
# indices share a single 4 GB Elasticsearch container's page cache, which is not
# the condition the single-shard figures were measured under. select_topology.py
# opens the one under test and CLOSES the other -- a closed index releases its
# shards but keeps its data on disk, so this costs seconds rather than a reload.
#
# The corpus is byte-identical: same generator, same seed, 5 primary shards instead
# of 1. Trino is the 3-node cluster the compose file already describes (coordinator
# + 2 workers, 6 CPU / 8 GB against the sidecar's 4 CPU / 4 GB) -- deliberately NOT
# at parity, and RESULTS says so wherever these figures appear.
#
# Every run is tagged `--variant 5shard` so a topology figure can never blend into
# a single-shard median.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PY=.venv/bin/python
STAMP="${1:-$(date +%Y%m%dT%H%M%S)}"
SESSION="results/$STAMP-5shard"
LOG="$SESSION/session.log"
mkdir -p "$SESSION"

say() { printf '\n=== %s === %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG"; }
run() {
  say "$*"
  if ! "$@" >>"$LOG" 2>&1; then
    say "FAILED: $*"
    if grep -q "refusing to" "$LOG"; then say "a guard refused -- aborting"; exit 1; fi
    say "continuing"
  fi
}

say "phase 3 -- 5-shard sensitivity, session $SESSION"

# Make the 5-shard index the only resident one.
run $PY generator/select_topology.py --index bench_events_10m_s5

# Correctness first, on THIS index: a topology that returned different data would
# make every timing below meaningless. The published sensitivity run never did this.
run $PY runners/verify_equivalence.py --index bench_events_10m_s5 \
        --out "$SESSION/equivalence-gate.json"
if ! grep -q '"verdict": "PASS' "$SESSION/equivalence-gate.json" 2>/dev/null; then
  say "gate did not PASS on the 5-shard index -- refusing to measure"; exit 1
fi

# S1 (extraction) and S3 (push-down) are the two cells RESULTS publishes here: one
# says whether wall-clock is topology-sensitive, the other whether push-down is.
run $PY runners/orchestrate.py --session "$SESSION" --stop-idle-engine \
        --index bench_events_10m_s5 --variant 5shard \
        --stacks flight trino --scenarios S1 S3

say "restoring the single-shard topology"
run $PY generator/select_topology.py --index bench_events_10m

if $PY runners/summarize.py "$SESSION" > "$SESSION/summary.md" 2>>"$LOG"; then
  say "wrote $SESSION/summary.md"
else
  say "FAILED: summarize"; rm -f "$SESSION/summary.md"
fi
say "DONE  phase 3 ($SESSION)"

#!/bin/bash
# Phase 3: index-topology sensitivity -- the same corpus on ONE primary shard.
#
# ⚠️ The MAIN topology is 6 shards (bench_events_10m) and this is the 1-shard arm.
# It used to be the other way round, and every label in this file said so; the
# session directory was still called '-5shard' while measuring a 1-shard index,
# which would have filed the figures under a topology that did not produce them.
#
# ⚠️ INVOKE AS `/bin/bash runners/run_full_session_phase3.sh` (see run_full_session.sh).
#
# WHY IT IS ITS OWN SESSION. Only ONE benchmark index may be resident: two 10M
# indices share the 3-node cluster's page cache, which is not the condition the
# 6-shard figures were measured under. select_topology.py
# opens the one under test and CLOSES the other -- a closed index releases its
# shards but keeps its data on disk, so this costs seconds rather than a reload.
#
# The corpus is byte-identical: same generator, same seed, 1 primary shard instead
# of 6 -- so this arm is also the control for core #238's sliced PIT paging, which
# resolves to min(primary shards, max-slices) and therefore CANNOT slice here.
# A 1-shard index pages sequentially no matter what max-slices says. Trino is the 3-node cluster the compose file already describes (coordinator
# + 2 workers, 6 CPU / 8 GB against the sidecar's 4 CPU / 4 GB) -- deliberately NOT
# at parity, and RESULTS says so wherever these figures appear.
#
# Every run is tagged `--variant 1shard` so a topology figure can never blend into
# the 6-shard median.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PY=.venv/bin/python
STAMP="${1:-$(date +%Y%m%dT%H%M%S)}"
SESSION="results/$STAMP-1shard"
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

say "phase 3 -- 1-shard sensitivity, session $SESSION"

# Make the 1-shard index the only resident one.
run $PY generator/select_topology.py --index bench_events_10m_s1

# Correctness first, on THIS index: a topology that returned different data would
# make every timing below meaningless. The published sensitivity run never did this.
# Engines first: the gate cannot check an engine that is not running, and a gate
# that skipped one still used to report a pass. See check_gate.py.
run $PY runners/ensure_engines.py
run $PY runners/verify_equivalence.py --index bench_events_10m_s1 \
        --out "$SESSION/equivalence-gate.json"
if ! $PY runners/check_gate.py "$SESSION/equivalence-gate.json" >>"$LOG" 2>&1; then
  say "gate did not cover both timed engines on the 1-shard index -- refusing to measure"
  tail -5 "$LOG" | tee -a "$LOG" >/dev/null
  exit 1
fi

# WARM THE INDEX BEFORE MEASURING ANYTHING. select_topology.py has just OPENED this
# index, so its pages are cold, and whichever engine is measured FIRST would pay a
# cold-page-cache tax that no later block pays. Measured on 2026-08-20 against the
# 6-shard index: the first engine block ran 13.07 s and the same cell 70 minutes later
# ran 9.78 s -- 25 % -- while Trino, measured after us, gained 0.3 % because our block
# had already warmed the cache for it. The tell is server-side: our Elasticsearch CPU
# fell 47.5 -> 34.9 s and converged on Trino's stable 30-31 s.
#
# Two full extractions are enough to make the corpus resident; they are timed by
# nobody and recorded nowhere. Cheaper than the alternative, which is a drift arm for
# every stack. See results/<session>/COLD-CACHE-NOTE.md.
say "warming the page cache before the first timed block"
for i in 1 2; do
  $PY - <<'EOF' >>"$LOG" 2>&1
import adbc_driver_flightsql.dbapi as dbapi
with dbapi.connect("grpc://127.0.0.1:32010") as c, c.cursor() as cur:
    cur.execute("SELECT id, event_ts, amount, qty, status, country, category, name "
                "FROM bench_events_10m_s1")
    print("warm pass rows:", cur.fetch_arrow_table().num_rows)
EOF
done
say "page cache warmed"

# S1 (extraction) and S3 (push-down) are the two cells RESULTS publishes here: one
# says whether wall-clock is topology-sensitive, the other whether push-down is.
run $PY runners/orchestrate.py --session "$SESSION" --stop-idle-engine \
        --index bench_events_10m_s1 --variant 1shard \
        --stacks flight trino --scenarios S1 S3

# Evidence that the parallelism under test was actually exercised -- one split per
# shard, spread over the workers. It has to be captured LIVE: system.runtime.tasks
# drops a query's rows the moment it finishes, so this cannot be recovered from the
# session afterwards, and RESULTS would be left asserting it from an older run.
run $PY runners/probe_trino_splits.py --index bench_events_10m_s1 \
        --out "$SESSION/trino-splits-probe.json"

say "restoring the 6-shard topology"
run $PY generator/select_topology.py --index bench_events_10m

if $PY runners/summarize.py "$SESSION" > "$SESSION/summary.md" 2>>"$LOG"; then
  say "wrote $SESSION/summary.md"
else
  say "FAILED: summarize"; rm -f "$SESSION/summary.md"
fi
say "DONE  phase 3 ($SESSION)"

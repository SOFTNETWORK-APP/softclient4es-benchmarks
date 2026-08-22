#!/bin/bash
# Re-measure the S0/S0p floors after the SLICES fix, into an existing session.
#
# ⚠️ INVOKE AS `/bin/bash runners/rerun_floors.sh <session-dir>` and ONLY when nothing
# else is running -- a floor measured while another block competes for CPU is no floor.
#
# WHY. run_es.py carried `SLICES = 5` ("one slice per shard of the 5-shard topology"),
# a literal left over from before the topology inverted. Against the 6-shard index that
# gives one slice TWO shards; the wall clock is the slowest slice, so the floor came in
# roughly 2/6 of the corpus behind instead of 1/6. The floor is the number OUR OWN
# extraction is compared against, so the error flattered us. It is now derived from the
# index's actual primary shard count.
#
# The old records are QUARANTINED rather than deleted. They are a real measurement of a
# real configuration -- just not of a floor -- and a session that silently loses records
# is worse evidence than one that explains them.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
PY=.venv/bin/python
SESSION="${1:?usage: rerun_floors.sh results/<session>}"
[ -d "$SESSION" ] || { echo "no such session: $SESSION"; exit 1; }
LOG="$SESSION/session.log"

say() { printf '\n=== %s === %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG"; }

VOID="$SESSION/void-5slice-floors"
if compgen -G "$SESSION/es-raw-S0*-run*.json" > /dev/null; then
  mkdir -p "$VOID"
  mv "$SESSION"/es-raw-S0*-run*.json "$VOID"/
  cat > "$VOID/README.md" <<'MD'
# Void: floors measured at 5 slices over a 6-shard index

These records were produced with `run_es.py`'s old hardcoded `SLICES = 5`, a leftover
from the era when the main index had 5 shards. Against `bench_events_10m` (6 primary
shards) Elasticsearch assigns one slice two shards; that slice does double the work and
the wall clock is the slowest slice, so the sliced floor is roughly 2/6 of the corpus
behind rather than 1/6.

They are kept because they are an honest measurement of a real (mis)configuration, and
because deleting them would leave the session's history unexplained. They are NOT a
floor and must never be quoted as one, or compared with any engine figure.

The replacement floors in the parent directory derive the slice count from the index's
actual `number_of_shards`.
MD
  say "quarantined the 5-slice floors -> $VOID"
else
  say "no existing floor records to quarantine"
fi

say "re-measuring floors with one slice per primary shard"
$PY runners/ensure_engines.py >>"$LOG" 2>&1 || { say "engines unhealthy"; exit 1; }
$PY runners/orchestrate.py --session "$SESSION" --stop-idle-engine \
    --stacks es-raw --scenarios S0 S0p >>"$LOG" 2>&1 || say "FAILED (see $LOG)"

# Prove the parallelism actually changed, rather than trusting the constant.
$PY - "$SESSION" <<'EOF' | tee -a "$LOG"
import glob, json, os, sys
n = set()
for f in glob.glob(os.path.join(sys.argv[1], "es-raw-S0p-run*.json")):
    n.add(json.load(open(f)).get("slices"))
print(f"[floors] S0p slice counts recorded: {sorted(n) or 'none'}")
if n and n != {6}:
    print("[floors] WARNING: expected 6 slices (one per primary shard)")
EOF
say "DONE floors ($SESSION)"

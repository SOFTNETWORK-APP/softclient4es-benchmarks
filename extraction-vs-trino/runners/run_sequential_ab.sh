#!/bin/bash
# Phase 4: the #238 A/B -- sliced vs sequential PIT paging, identical hardware.
#
# ⚠️ INVOKE AS `/bin/bash runners/run_sequential_ab.sh` (see run_full_session.sh).
#
# WHY THIS EXISTS. Every session up to and including sidecar 0.2.5.1 measured a
# SEQUENTIAL reader: one PIT, one page at a time, so a 10M-row extraction was ~10,000
# round trips no matter how many shards the index had. That is why our wall did not
# improve at all going from 1 shard to 6 while the competing engine gained, and it is
# the objection an external reviewer raised in August 2026.
#
# core 0.21.0 (#238) pages with min(primary shards, max-slices) concurrent slices.
# Comparing the new headline against the OLD session's 34.6 s would confound the fix
# with everything else that changed between two images and two months of host state.
# So both arms run here, back to back, on one machine, in one session.
#
# The knob is the only difference:
#   ELASTIC_SCROLL_MAX_SLICES=1  -> the pre-0.21.0 behaviour, exactly
#   (default 8)                  -> min(6, 8) = 6 slices on bench_events_10m
#
# S1 and S2 are the cells that can move: both stream every row. S3 (push-down) and S4
# (LIMIT 100) are included as CONTROLS -- an aggregation never takes the sliced path
# and a 100-row LIMIT never pages, so if either arm moves them, the arms differ by
# something other than the knob and the comparison is void.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

PY=.venv/bin/python
STAMP="${1:-$(date +%Y%m%dT%H%M%S)}"
SESSION="results/$STAMP-sliced-ab"
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

# Recreate the sidecar with a given slice ceiling and PROVE the value took effect.
# The lesson is SOFTCLIENT4ES_LOG_LEVEL's: it was inert for a whole session because
# nothing ever checked, and the resulting figures were quietly mislabelled. An arm
# that silently ran the other configuration is worse than no arm.
# TWO recreates per arm, on purpose.
#
# The paging decision ("Sliced PIT paging: N slices ..." / "PIT paging stays sequential")
# is logged at INFO, but every measured cell in this campaign was taken at WARN -- and
# the sidecar's logging is O(rows / batch), roughly 731 lines per 10M-row extraction at
# INFO, so measuring one arm at INFO would inflate it and make it incomparable with the
# rest of the session. Observing and measuring therefore cannot be the same container.
#
# Pass 1 (INFO) OBSERVES that this slice setting produces the intended paging.
# Pass 2 (WARN) MEASURES, and asserts the container carries the same env var, so the
# observation transfers. Verifying the variable is set is not enough on its own -- that
# is what SOFTCLIENT4ES_LOG_LEVEL taught, being set and inert for a whole session -- so
# the behavioural check in pass 1 is what makes pass 2's env assertion meaningful.
set_slices() {
  local n="$1" want="$2"

  # EXPORT, do not prefix. A `VAR=x docker compose ...` prefix configures only that one
  # command, and the very next step (ensure_engines.py) runs `docker compose up -d`,
  # which RE-RESOLVES the compose file from ITS OWN environment and silently recreates
  # the sidecar back to the defaults. That is not hypothetical: it is what happened on
  # the first run of this script, and the pass-1 guard caught it -- "observed: <nothing
  # logged>", because by probe time the container no longer had the setting.
  #
  # orchestrate.py is safe either way (it uses `compose start/stop`, which preserves a
  # container's config), but exporting removes the whole class of problem.
  say "pass 1/2 (INFO): observing the paging decision at ELASTIC_SCROLL_MAX_SLICES=$n"
  export ELASTIC_SCROLL_MAX_SLICES="$n"
  export SOFTCLIENT4ES_LOG_LEVEL=INFO
  docker compose up -d --force-recreate flight-sql >>"$LOG" 2>&1 || {
      say "compose failed"; exit 1; }
  $PY runners/ensure_engines.py >>"$LOG" 2>&1 || { say "engines unhealthy"; exit 1; }

  $PY - >>"$LOG" 2>&1 <<'EOF'
import adbc_driver_flightsql.dbapi as dbapi
with dbapi.connect("grpc://127.0.0.1:32010") as c, c.cursor() as cur:
    cur.execute("SELECT id FROM bench_events_10m")
    print("probe rows:", cur.fetch_arrow_table().num_rows)
EOF

  local line
  # Match on the SLICE COUNT the run actually used, not on the decision message.
  # With max-slices = 1 the sidecar skips the shard-count lookup altogether and logs
  # NO decision line -- so a guard keyed on "Sliced PIT paging|stays sequential" finds
  # nothing in the sequential arm and refuses a correctly configured run. (Verified by
  # hand: env applied, 10,000 batches, 32.4 s, and not one decision line in the log.)
  # "PIT search_after completed (N slice(s))" is emitted by BOTH branches, so it is the
  # observable that can actually discriminate them.
  line=$(docker compose logs flight-sql 2>/dev/null \
         | grep -E "slice\(s\)|Sliced PIT paging" | tail -1)
  say "observed: ${line:-<nothing logged>}"
  if ! grep -qE "$want" <<<"$line"; then
    say "REFUSING: sidecar did not adopt ELASTIC_SCROLL_MAX_SLICES=$n"
    say "  wanted /$want/ in the paging decision, got: ${line:-<nothing>}"
    exit 1
  fi

  say "pass 2/2 (WARN): recreating for measurement"
  export SOFTCLIENT4ES_LOG_LEVEL=WARN
  docker compose up -d --force-recreate flight-sql >>"$LOG" 2>&1 || {
      say "compose failed"; exit 1; }
  $PY runners/ensure_engines.py >>"$LOG" 2>&1 || { say "engines unhealthy"; exit 1; }

  local got
  got=$(docker inspect extraction-vs-trino-flight-sql-1 \
        --format '{{range .Config.Env}}{{println .}}{{end}}' \
        | grep '^ELASTIC_SCROLL_MAX_SLICES=' | cut -d= -f2)
  if [ "$got" != "$n" ]; then
    say "REFUSING: measured container has ELASTIC_SCROLL_MAX_SLICES=${got:-<unset>}, wanted $n"
    exit 1
  fi
  local lvl
  lvl=$(docker inspect extraction-vs-trino-flight-sql-1 \
        --format '{{range .Config.Env}}{{println .}}{{end}}' \
        | grep '^SOFTCLIENT4ES_LOG_LEVEL=' | cut -d= -f2)
  if [ "$lvl" != "WARN" ]; then
    say "REFUSING: measuring at log level ${lvl:-<unset>}, but the campaign is WARN"
    exit 1
  fi
  say "measuring with ELASTIC_SCROLL_MAX_SLICES=$n at WARN"

  # Warm the corpus for THIS container: a freshly recreated sidecar plus a cold-ish
  # cache would hand the first arm measured a penalty the second never pays -- the
  # exact asymmetry documented in COLD-CACHE-NOTE.md.
  $PY - >>"$LOG" 2>&1 <<'EOF'
import adbc_driver_flightsql.dbapi as dbapi
for _ in range(2):
    with dbapi.connect("grpc://127.0.0.1:32010") as c, c.cursor() as cur:
        cur.execute("SELECT id, event_ts, amount, qty, status, country, category, name "
                    "FROM bench_events_10m")
        print("warm rows:", cur.fetch_arrow_table().num_rows)
EOF
}

say "phase 4 -- sliced vs sequential, session $SESSION"

# ── A. sequential: exactly what every pre-0.21.0 session measured ─────────────
# The arm asserts on the slice count the extraction REPORTS, which is the only
# signal present in both branches -- see the note in set_slices.
set_slices 1 "\(1 slice\(s\)\)"
run $PY runners/orchestrate.py --session "$SESSION" --stop-idle-engine \
        --variant sequential --stacks flight --scenarios S1 S2 S3 S4

# ── B. sliced: the shipped default ───────────────────────────────────────────
set_slices 8 "\(6 slice\(s\)\)"
run $PY runners/orchestrate.py --session "$SESSION" --stop-idle-engine \
        --variant sliced --stacks flight --scenarios S1 S2 S3 S4

# Leave the sidecar as a normal session expects to find it: shipped default, WARN.
say "restoring the shipped sidecar configuration"
unset ELASTIC_SCROLL_MAX_SLICES SOFTCLIENT4ES_LOG_LEVEL
docker compose up -d --force-recreate flight-sql >>"$LOG" 2>&1

if $PY runners/summarize.py "$SESSION" > "$SESSION/summary.md" 2>>"$LOG"; then
  say "wrote $SESSION/summary.md"
else
  say "FAILED: summarize"; rm -f "$SESSION/summary.md"
fi
say "DONE  phase 4 ($SESSION)"

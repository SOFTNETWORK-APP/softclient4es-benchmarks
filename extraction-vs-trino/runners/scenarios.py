"""Scenario definitions and shared measurement plumbing, used by ALL runners.

Single source of truth on purpose: if the S1 SQL could drift between run_flight.py
and run_trino.py, the benchmark would silently stop comparing like with like.
No runner defines its own SQL.
"""
import ctypes
import ctypes.util
import json
import os
import pathlib
import platform
import re
import resource
import subprocess
import sys
import urllib.error
import urllib.request

DEFAULT_INDEX = "bench_events_10m"

# ── The host every timed client dials ────────────────────────────────────────
# An IP LITERAL, never a name, for EVERY stack. This is a fairness rule, not a
# micro-optimisation, and it was got wrong until 2026-08-17: the Flight runner
# dialled 127.0.0.1 while every Trino route dialled "localhost", so one arm was
# configured to resolve a name and the other was not.
#
# The reason it mattered is that the two clients resolve names by different
# machinery. Trino's Python clients go through the OS resolver, which answers
# "localhost" from /etc/hosts in microseconds -- its whole connect is 1.8 ms. The
# stock ADBC Flight SQL driver is Go and uses grpc-go's own resolver, which does
# NOT consult /etc/hosts: a name costs it 33.4 ms of connect against 12.7 ms for
# a literal, with a 5 s per-query DNS timeout behind it (softclient4es-arrow#151).
#
# So "both dial a name" and "both dial a literal" are not two settings of one
# experiment. The first measures a third-party driver's resolver -- on S4, a
# 100-row control, it is worth more than the entire margin between the engines.
# Dialling a literal everywhere removes name resolution from every arm, which is
# the only configuration in which S4 compares the two engines rather than their
# clients' DNS paths. The driver defect is a PRODUCT finding, published as a
# deployment note; `run_flight.py --dial hostname` still measures it on demand.
HOST = "127.0.0.1"

# ── Engine topology ──────────────────────────────────────────────────────────
# Every compose service a stack needs. Trino is a real cluster (1 coordinator +
# 2 workers), so "stop the idle engine" has to stop THREE containers: stopping
# only the coordinator would leave two idle JVMs holding CPU and page cache while
# the other stack is measured -- the exact contamination --stop-idle-engine exists
# to prevent. All orchestrators read this table; do not re-declare it locally.
ENGINE_SERVICES = {
    "flight": ["flight-sql"],
    "trino": ["trino", "trino-worker-1", "trino-worker-2"],
}
# Elasticsearch is a 3-node cluster since 2026-08-19, so every ES-side metric has to
# be read over three containers rather than one. Wire bytes need MORE than summing --
# see es_wire_bytes().
ES_SERVICES = ["elasticsearch", "elasticsearch-2", "elasticsearch-3"]
# Workers the Trino cluster must have registered before a run may start. A healthy
# coordinator is NOT a ready cluster: it answers /v1/info while workers are still
# registering, and a query issued in that window silently runs on fewer nodes than
# the published topology claims.
TRINO_WORKERS = 2
TRINO_URL = f"http://{HOST}:8080"

# Host memory fitness, checked by guard_environment().
#
# NOT swap utilisation. That was the first attempt and it is measurably wrong:
# macOS RESIZES the swap file as pressure changes and never proactively drains it,
# so after a host recovers, stale pages sit in a now-smaller file and utilisation
# goes UP while the machine gets better. Observed 2026-08-16 within one hour:
#   sick    27.6 GB swap, 96% used, free 0.03 GB, compressor 5.55 GB
#   healthy 16.4 GB swap, 91% used, free 2.45 GB, compressor 1.69 GB, pressure NORMAL
# Same utilisation, opposite condition. Gate on what macOS itself reports instead.
MIN_AVAILABLE_GB = 4.0          # free + inactive; inactive is reclaimable without I/O
VM_PRESSURE_NORMAL = 1          # kern.memorystatus_vm_pressure_level: 1 normal, 2 warn, 4 critical

# ── The floor is sized to the ARM, not to the matrix ─────────────────────────
# MIN_AVAILABLE_GB is one constant covering every scenario, which means it is
# simultaneously wrong in both directions: it demanded 4 GB to fetch 100 rows
# (S4's heaviest client holds 83 MB -- 49x oversized, and on 2026-08-17 it
# refused a legitimate S4 re-run on a host with 3.3 GB free), while the heaviest
# arm in the matrix, S1r to a polars DataFrame over SQLAlchemy, peaks at 11 GB
# and the same 4 GB was cheerfully declared sufficient for it.
#
# Heaviest client peak actually recorded per scenario, across BOTH stacks and
# every route (footprint on macOS, ru_maxrss in the capped container). These are
# measurements from results/, not estimates -- update them when a route moves.
SCENARIO_PEAK_MB = {
    "S0": 27,        # es-raw scroll, counts and discards
    "S0p": 135,      # 5 sliced scroll processes, summed
    "S1": 4472,      # trino stock client -> list of tuples
    "S1m": 474,      # trino stock client, 1M rows
    "S1r": 11052,    # trino -> polars via SQLAlchemy, the heaviest arm measured
    "S2": 7779,      # trino -> pandas -> DuckDB
    "S3": 82,        # flight (aggregation returns 100 rows)
    "S4": 83,        # flight (LIMIT 100)
}
# Slack over the arm's own peak: enough for the interpreter, the driver and the
# OS to breathe without the process reaching for swap.
GUARD_HEADROOM_MB = 512


def min_available_gb(scenario=None):
    """Memory floor for this scenario. NEVER above MIN_AVAILABLE_GB.

    The `min()` is the whole safety argument, so do not "simplify" it away: this
    function can only ever LOWER the requirement for an arm that provably does
    not need it. It cannot admit a heavy run that the flat floor would have
    refused, and it therefore cannot retroactively weaken any published figure --
    every run in results/ was gated at 4.0 GB or, for the light scenarios, at a
    threshold their own peak clears many times over.

    An unknown scenario falls back to the flat floor, so a new arm is protected
    by the conservative rule until someone measures it and adds it to the table.
    """
    peak = SCENARIO_PEAK_MB.get(scenario)
    if peak is None:
        return MIN_AVAILABLE_GB
    return min(MIN_AVAILABLE_GB, (peak + GUARD_HEADROOM_MB) / 1024.0)


# Set by guard_environment() and folded into every run's mem_pressure block, so a
# reader can see WHICH floor protected a given figure rather than having to infer
# it from the scenario name. Module state because the guard runs long before the
# record is built, and threading it through six runners' signatures would buy
# nothing a comment cannot.
GUARD_FLOOR_GB = MIN_AVAILABLE_GB

# Host CPU fitness, checked by guard_environment() and RECORDED per run.
#
# The confound this exists to bound: the measured client runs on the HOST while the
# engines run in the Docker VM, on one machine. Trino's stock client burns ~25 s of
# CPU per S1 against our ~4.6 s, so if the host were saturated, host contention
# could CONTRIBUTE to the wall-clock gap instead of merely reflecting it -- and the
# wall-clock gap is the headline. Client CPU seconds are immune (process_time
# counts CPU, not waiting); wall clock is not.
#
# Gate on the 1-minute load average against the LOGICAL core count, not something
# tighter: the Docker VM's own 10 busy vCPUs legitimately show up in the host's
# load while a run is in flight, and our own client is part of the load too. A
# threshold at 1.0x logical cores does not trip on the benchmark itself; it trips
# on the thing that actually ruins a session -- a compile, an indexer or a second
# VM running alongside. Recorded before AND after every run so a reader can judge
# each figure rather than trust that the machine was quiet.
MAX_HOST_LOAD_FACTOR = 1.0
# Only a host this far past its core count is refused outright; at the ordinary
# ceiling the run is warned about and recorded. See guard_environment().
PATHOLOGICAL_HOST_LOAD_FACTOR = 2.0
# Compose derives the project name from the directory holding docker-compose.yml unless
# COMPOSE_PROJECT_NAME overrides it. Deriving it the same way matters: a wrong value makes
# net_bytes() return None SILENTLY and the wire-byte column just disappears from RESULTS.
COMPOSE_PROJECT = os.environ.get(
    "COMPOSE_PROJECT_NAME",
    pathlib.Path(__file__).resolve().parent.parent.name)

# The 8 selected columns. Explicit columns are LOAD-BEARING on the SoftClient4ES
# side: `SELECT *` has no field list, which routes the query to the bounded
# searchAsync path (<= 10k rows) regardless of licence tier. Explicit columns with
# no LIMIT is what routes it to the streaming scroll path being measured here.
COLUMNS = ["id", "event_ts", "amount", "qty", "status", "country", "category", "name"]


def trino_query(sql, timeout=30):
    """Run one SQL statement over Trino's HTTP protocol; return all data rows.

    Stdlib only, and drained to completion (`nextUri` followed until it is gone) so
    no query is left running behind us. Used for cluster introspection, never for
    measurement -- the measured runs go through the trino Python client like a user's.
    """
    import urllib.request

    req = urllib.request.Request(f"{TRINO_URL}/v1/statement", data=sql.encode(),
                                 headers={"X-Trino-User": "bench-harness"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        doc = json.loads(r.read().decode())
    rows = []
    for _ in range(200):
        if doc.get("error"):
            raise RuntimeError(doc["error"].get("message", "trino error"))
        rows.extend(doc.get("data") or [])
        nxt = doc.get("nextUri")
        if not nxt:
            return rows
        with urllib.request.urlopen(nxt, timeout=timeout) as r:
            doc = json.loads(r.read().decode())
    raise RuntimeError("trino statement did not terminate")


def wait_trino_cluster(expected_workers=TRINO_WORKERS, timeout=300, log=print):
    """Block until `expected_workers` workers are active, or die.

    Asks `system.runtime.nodes` rather than probing an HTTP endpoint, for three
    reasons: it is the documented interface (Trino 483 has no /v1/node -- verified,
    it 404s); it distinguishes workers from the coordinator, so "coordinator plus
    one worker" cannot pass a count check meant to require two; and a successful
    answer proves the cluster can actually plan and run SQL, which no readiness
    probe does.

    Returns the node rows so the caller can capture them: a published figure must be
    attributable to the topology it was measured on, and "3 nodes" in the write-up
    has to be something observed rather than something configured.

    A timeout is a hard failure. Measuring a cluster that never assembled would
    produce an ordinary-looking number for a topology that never existed.

    ⚠️ WHAT THIS DOES AND DOES NOT CATCH. It catches the case that actually happens:
    a worker that has not registered yet, because an unregistered node is simply
    absent from system.runtime.nodes. It does NOT instantly catch a worker that dies
    moments before the check -- Trino's failure detector needs tens of seconds to
    drop it, and until then it is still reported active (measured: still listed 20 s
    after `docker compose stop`, gone by ~60 s). That gap is covered by the caller,
    which waits for every compose service to report healthy BEFORE calling this; a
    stopped container never reports running. Keep the two checks in that order, and
    do not call this one on its own and conclude the cluster is sound.
    """
    import time as _t

    deadline, last = _t.time() + timeout, None
    while _t.time() < deadline:
        try:
            rows = trino_query(
                "SELECT node_id, coordinator FROM system.runtime.nodes "
                "WHERE state = 'active'")
            workers = [r[0] for r in rows if not r[1]]
            if len(workers) == expected_workers:
                log(f"Trino cluster ready: {len(rows)} active nodes "
                    f"({len(workers)} workers: {', '.join(sorted(workers))})")
                return rows
            last = f"{len(workers)} active worker(s) of {expected_workers}"
        except Exception as e:                      # not up yet, or still planning
            last = f"not answering ({type(e).__name__}: {e})"
        _t.sleep(3)
    sys.exit(
        f"Trino cluster did not reach {expected_workers} active workers within "
        f"{timeout}s (last: {last}).\nCheck `docker compose ps` for trino-worker-1 / "
        f"trino-worker-2 and their logs.\nDo not lower TRINO_WORKERS to make this "
        f"pass: the worker count is part of what the results claim.")

# The aggregate both stacks run against their own landed result in S2.
SQL_AGG_DUCK = ("SELECT category, AVG(amount) AS avg_amount "
                "FROM events GROUP BY category")

# Correctness gates. Timing is only recorded for a run that returned exactly this.
#
# S1r REDEFINED 2026-08-11: it is now time-to-DataFrame -- the same SQL landed as
# a pandas.DataFrame on BOTH stacks, each by its most idiomatic route (Flight:
# fetch_arrow_table().to_pandas(); Trino: pandas.read_sql over trino.sqlalchemy).
# The old S1r (Arrow wire fetched row-wise via cursor.fetchall(), Flight only)
# compared an artifact no user builds against a stack that wasn't measured at all:
# there is no common client speaking both protocols (no trino.adbc, no Arrow
# output in the trino package), so equalising the ARTIFACT is the only honest
# like-for-like. ⚠️ S1r numbers from sessions before 2026-08-11T15:00 measure the
# OLD definition and are not comparable.
EXPECTED_ROWS = {"S0": 10_000_000, "S0p": 10_000_000, "S1": 10_000_000,
                 "S1m": 1_000_000, "S1r": 10_000_000,
                 "S2": 10_000_000, "S3": 100, "S4": 100}
EXPECTED_GROUPS = 100
SCENARIOS = list(EXPECTED_ROWS)

# Which stacks each scenario requires before its numbers mean anything. S0/S0p are
# the shared floors (raw Elasticsearch, no engine). S1r runs on BOTH stacks since
# the time-to-DataFrame redefinition. S1m is the only cell where all THREE stacks
# can compete -- see ESQL_MAX_RESULT_ROWS for why ES|QL is absent everywhere else.
REQUIRED_STACKS = {"S0": {"es-raw"}, "S0p": {"es-raw"}, "S1": {"flight", "trino"},
                   "S1m": {"flight", "trino"},
                   "S1r": {"flight", "trino"}, "S2": {"flight", "trino"},
                   "S3": {"flight", "trino"}, "S4": {"flight", "trino"}}

# Stacks a scenario ACCEPTS but does not require. ES|QL is optional rather than
# required for two reasons, both of which cost a session if it is required:
# every session recorded before 2026-08-16 has no ES|QL runs and must stay
# summarizable, and the ES|QL cells need a deliberately raised cluster setting,
# so a stock cluster would abort a full matrix hours in. Whether ES|QL is
# actually RUN is a campaign decision (it is: see the review-triage brief), not
# something a correctness gate can enforce.
OPTIONAL_STACKS = {"S1m": {"esql"}, "S3": {"esql"}, "S4": {"esql"}}


def compose_variant(explicit, *tags):
    """Join an explicit --variant with the runner's own auto-tags, DEDUPED.

    Both layers tag: orchestrate.py must put the tag in the run FILENAME (its
    resume logic keys on it), and each runner tags what it was actually asked to
    do, because a run file has to be self-describing when it is read alone. Left
    to themselves the two produce `arrow-arrow` / `tuned-tuned`, which is not
    merely ugly: summarize.py looks the variant up by name, so a doubled tag
    SILENTLY DROPS the arm from the published table (measured 2026-08-16 -- the
    ES|QL Arrow runs were all recorded and none of them appeared).
    """
    parts = []
    for t in (explicit, *tags):
        for piece in (t or "").split("-"):
            if piece and piece not in parts:
                parts.append(piece)
    return "-".join(parts)


def stacks_for(scenario):
    """Every stack that may legitimately produce runs for this scenario."""
    return REQUIRED_STACKS.get(scenario, set()) | OPTIONAL_STACKS.get(scenario, set())

# Both engines return exactly the 8 selected columns.
#
# HISTORY, because this used to say something else and the harness must not
# outlive its own facts: until SoftClient4ES core#226 the Flight schema also
# carried _id/_index/_score/_sort hit-metadata columns (the sidecar infers the
# statement schema from row keys, not from the SQL projection), so SoftClient4ES
# moved MORE bytes per row than the SQL asked for and this gate was a >=. That
# metadata was removed in core#226 (_index/_score/_sort deleted, _id opt-in and
# off by default); the released 0.3.0 image measured here returns 8 of 8, which
# is what RESULTS S1 publishes. `extra_cols` is still recorded per run so a
# regression shows up in the data rather than in nobody's memory.
EXPECTED_COLS_TRINO_S1 = len(COLUMNS)
EXPECTED_MIN_COLS_FLIGHT_S1 = len(COLUMNS)

# ── ES|QL, the third stack ───────────────────────────────────────────────────
ESQL_URL = f"http://{HOST}:9200/_query"
# ES|QL cannot return more than this, and the ceiling is NOT a licence gate:
# `esql.query.result_truncation_max_size` is declared
#   Setting.intSetting("esql.query.result_truncation_max_size", 10000, 1, 1000000,
#                      NodeScope, Dynamic)          -- EsqlPlugin.java, v8.18.3
# so 10,000 is the default, 1,000,000 the hard maximum, and a cluster set to the
# maximum answers `LIMIT 10000000` with 1,000,000 rows, HTTP 200 and NO Warning
# header (measured 2026-08-16 on this cluster, which runs a `basic` licence with
# `_xpack/usage` reporting esql.available=true -- a trial changes nothing).
#
# Consequences the runners encode: ES|QL competes on S1m/S3/S4 and CANNOT enter
# S1/S2/S5/S6, and the silent truncation is why run_esql.py asserts the row count
# rather than trusting a 200.
ESQL_MAX_RESULT_ROWS = 1_000_000
# The S1m size. Deliberately equal to ES|QL's hard ceiling: the one scale at which
# all three stacks can be compared like for like, and the boundary a reader wants
# to see. Verified against the released 0.3.0 sidecar in every session (LIMIT 1000000
# returns 1,000,000 rows over Flight SQL, so the explicit-LIMIT path is not the
# bounded <=10k one). ⚠️ A predicate was considered and rejected: the generator's
# ids are random draws, not a permutation (10,000,000 docs, 9,931,188 distinct),
# so `WHERE id <= 1000000` yields 1,000,001 rows -- one row over ES|QL's ceiling,
# which it would have TRUNCATED SILENTLY. The bug that fix avoids is the exact
# class this benchmark exists to catch.
S1M_ROWS = 1_000_000


def sql_for(scenario, index=DEFAULT_INDEX):
    projection = ", ".join(COLUMNS)
    if scenario in ("S1", "S1r", "S2", "S0", "S0p"):
        return f"SELECT {projection} FROM {index}"
    if scenario == "S1m":
        return f"SELECT {projection} FROM {index} LIMIT {S1M_ROWS}"
    if scenario == "S3":
        return ("SELECT category, COUNT(*) AS cnt, AVG(amount) AS avg_amount "
                f"FROM {index} GROUP BY category")
    if scenario == "S4":
        return f"SELECT {projection} FROM {index} LIMIT 100"
    raise SystemExit(f"no SQL for scenario {scenario}")


def esql_for(scenario, index=DEFAULT_INDEX):
    """The ES|QL translation of the same scenario.

    ES|QL is a different LANGUAGE, not a different driver, so this is the one
    place where a runner's statement is not `sql_for()` verbatim. It lives here
    for the same reason every other statement does: two stacks that could drift
    apart would stop comparing like with like. Keep the projection, the
    aggregation and the row bound identical to sql_for() -- only the syntax moves.

    Every query carries an explicit LIMIT because ES|QL's DEFAULT is 1,000 rows
    (`esql.query.result_truncation_default_size`), i.e. a query with no LIMIT
    silently answers a different question than the SQL it mirrors.
    """
    projection = ", ".join(COLUMNS)
    if scenario == "S1m":
        return f"FROM {index} | KEEP {projection} | LIMIT {S1M_ROWS}"
    if scenario == "S3":
        return (f"FROM {index} | STATS cnt = COUNT(*), avg_amount = AVG(amount) "
                "BY category | LIMIT 1000")
    if scenario == "S4":
        return f"FROM {index} | KEEP {projection} | LIMIT 100"
    raise SystemExit(
        f"no ES|QL for scenario {scenario}. S1/S1r/S2 extract 10,000,000 rows, "
        f"which is above ES|QL's hard ceiling of {ESQL_MAX_RESULT_ROWS:,} "
        "(see ESQL_MAX_RESULT_ROWS) -- that absence is a published finding, "
        "not a gap to fill by lowering the row count.")


def guard_environment(scenario=None):
    """Refuse to produce numbers from an environment that would misreport them.

    `scenario` sizes the memory floor to the arm about to run (see
    min_available_gb). Omitting it keeps the flat, conservative 4 GB floor, so a
    caller that does not know what it is about to measure is never under-protected.

    Two ways a run can look fine and be worthless:

    1. Assertions disabled (-O / PYTHONOPTIMIZE). Every correctness gate in this
       harness is an assert, including the one that catches a licence-truncated
       S1, so a run without them can report 10,000 rows as if it were 10,000,000.
    2. A translated interpreter. An x86_64 CPython under Rosetta on Apple Silicon
       inflates client CPU and does so unevenly across libraries, which is exactly
       the metric the moat claim rests on.
    3. A host already deep in swap. `memory_pressure()` has recorded this per run
       since arrow#150, and its own docstring notes that such a host runs the same
       S1 in 70 s or 87 s -- but recording it only helps a reader who thinks to
       look. Measured 2026-08-16: a pilot on a host at 94% swap utilisation
       returned client CPU 11% above the published figure while peak memory
       matched it to 0.1%, i.e. exactly the signature of paging inflating time
       while leaving footprint intact. The heaviest measured arm (Trino's stock
       client to a pandas DataFrame) needs ~8.3 GB, so there has to be somewhere
       for it to go.
    4. A host whose CPUs are already busy. The client is measured ON THE HOST
       while the engines run in the Docker VM, so competing host work inflates
       the client's wall clock -- and wall clock is the headline ratio. Client
       CPU seconds are immune, which is precisely why the two must be recorded
       together (see MAX_HOST_LOAD_FACTOR and host_load()).
    """
    global GUARD_FLOOR_GB
    floor = GUARD_FLOOR_GB = min_available_gb(scenario)

    if not __debug__:
        sys.exit("refusing to run with assertions disabled: the asserts ARE the "
                 "correctness gates (never use -O / PYTHONOPTIMIZE)")
    if sys.platform == "darwin":
        try:
            translated = subprocess.run(["sysctl", "-n", "sysctl.proc_translated"],
                                        capture_output=True, text=True, timeout=10)
            if translated.stdout.strip() == "1":
                sys.exit("refusing to run under Rosetta translation -- client CPU "
                         "would be misreported. Use a native arm64 CPython 3.12.")
            brand = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                   capture_output=True, text=True, timeout=10)
            if "Apple" in brand.stdout and platform.machine() != "arm64":
                sys.exit(f"refusing to run a {platform.machine()} interpreter on "
                         "Apple Silicon -- client CPU would be misreported. Use a "
                         "native arm64 CPython 3.12.")
        except (subprocess.SubprocessError, OSError):
            pass          # cannot determine: do not block, the check is defensive

    # Host memory fitness. Gate on macOS's own pressure verdict plus reclaimable
    # memory -- NOT on swap utilisation, which rises as a recovering host shrinks
    # its swap file (see the MIN_AVAILABLE_GB comment for the measured pair).
    mem = memory_pressure()
    if mem:
        level = mem.get("pressure_level")
        if level is not None and level != VM_PRESSURE_NORMAL:
            sys.exit(
                f"refusing to measure: macOS reports memory pressure level {level} "
                f"({'warning' if level == 2 else 'critical'}).\n"
                "Wall clock and client CPU inflate under paging while peak footprint "
                "does not, so the run would look ordinary and be wrong.\n"
                "Free memory (JVMs, browsers, editors) and re-run.")
        avail = mem.get("available_mb")
        if avail is not None and avail / 1024 < floor:
            need = SCENARIO_PEAK_MB.get(scenario)
            sized = (f"{scenario}'s heaviest measured client holds {need} MB"
                     if need is not None else
                     "the heaviest arm in the matrix holds ~11 GB")
            sys.exit(
                f"refusing to measure: only {avail / 1024:.1f} GB of reclaimable memory "
                f"(free + inactive), below the {floor:.2f} GB floor for "
                f"{scenario or 'this run'}.\n"
                f"The client is measured ON THE HOST and {sized}; below this floor the "
                "run pages and its timings are fiction.\n"
                "Free memory and re-run.")

    # Host CPU fitness. The client is measured on the host while the engines run in
    # the VM: a busy host inflates the client's WALL clock (its CPU seconds are
    # immune) and the wall clock is the headline. See MAX_HOST_LOAD_FACTOR.
    load = host_load()
    if load and load.get("logical_cpus"):
        ceiling = load["logical_cpus"] * MAX_HOST_LOAD_FACTOR
        if load["loadavg_1m"] > ceiling:
            # WARN, do not refuse, at the ordinary ceiling. The 1-minute average is
            # DECAYED, so a run started right after a heavy block inherits that
            # block's load -- including the benchmark's own Docker VM at ~10 busy
            # vCPUs. A refusal there would let the benchmark abort itself hours into
            # a session, which is a worse failure than a noisy figure that is
            # recorded as noisy: every run carries host_load_before/after, and
            # summarize.py publishes the maximum.
            print(f"WARNING: host 1-minute load average is {load['loadavg_1m']:.1f} "
                  f"against {load['logical_cpus']} logical cores -- wall clock may be "
                  "inflated by host contention (recorded per run).", file=sys.stderr)
        if load["loadavg_1m"] > ceiling * PATHOLOGICAL_HOST_LOAD_FACTOR:
            sys.exit(
                f"refusing to measure: host 1-minute load average is "
                f"{load['loadavg_1m']:.1f} against {load['logical_cpus']} logical "
                f"cores, i.e. more than {PATHOLOGICAL_HOST_LOAD_FACTOR:g}x "
                "oversubscribed.\n"
                "The measured client runs on the host, so a saturated host inflates "
                "its wall clock -- the metric the headline ratio is made of.\n"
                "Stop the other work (compiles, indexers, a second VM) and re-run.")


def peak_rss_mb():
    """Peak resident set size of THIS process.

    ru_maxrss is bytes on macOS and KiB on Linux -- normalize to MB.

    KEPT FOR CONTINUITY ONLY -- do not quote it as client memory (arrow#150).
    On macOS the memory compressor steals idle pages from the resident set, so
    under host memory pressure ru_maxrss under-reports by whatever happened to
    be compressed at the time: within single sessions, identical back-to-back
    S1 runs measured spreads like 430-794 MB and 978-1333 MB -- including
    readings BELOW the 1,138 MB Arrow table the process was provably holding.
    peak_footprint_mb() is the honest metric.
    """
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return ru / (1024 * 1024) if sys.platform == "darwin" else ru / 1024


def peak_footprint_mb():
    """Lifetime peak physical footprint of THIS process, in MB.

    The headline client-memory metric (fix for arrow#150). On macOS this is
    ri_lifetime_max_phys_footprint from proc_pid_rusage(RUSAGE_INFO_V4): the
    kernel's own ledger of the process's memory high-water mark INCLUDING
    compressed pages, so the reading does not depend on how starved the host
    happened to be during the run. (Verified on this harness: three identical
    S1 runs read 1407/1415/1414 MB footprint while ru_maxrss read 932/950/940.)

    Elsewhere (Linux CI) return None: peak_rss_mb is emitted separately and
    is honest there (no default-config compressor erodes it), and labelling a
    ru_maxrss value "footprint" would misidentify the metric on the one
    platform where the two differ -- summarize.py falls back with an explicit
    legacy label instead.
    """
    if sys.platform != "darwin":
        return None
    try:
        libproc = ctypes.CDLL(ctypes.util.find_library("proc"), use_errno=True)
        buf = (ctypes.c_uint64 * 64)()
        if libproc.proc_pid_rusage(os.getpid(), 4, buf) != 0:  # RUSAGE_INFO_V4
            return None
        # rusage_info_v4 layout: 16-byte uuid, then uint64 fields. Index 28
        # after the uuid is ri_lifetime_max_phys_footprint (index 7 is the
        # instantaneous ri_phys_footprint). Verified empirically: allocate and
        # free 800 MB -> field 7 falls back to 8 MB, field 28 stays at 808 MB.
        words = ctypes.cast(ctypes.addressof(buf) + 16,
                            ctypes.POINTER(ctypes.c_uint64))
        return words[28] / (1024 * 1024)
    except (OSError, AttributeError):
        return None


def memory_pressure():
    """Host memory-pressure provenance, recorded per run (arrow#150).

    Footprint is immune to compression, but WALL CLOCK is not: a host deep in
    swap runs the same S1 in 70 s or 87 s. Recording swap and free-memory state
    alongside each run lets a reader judge which sessions' wall numbers were
    taken under duress instead of trusting run-to-run spread to reveal it.
    Also carries `guard_floor_gb`, the floor guard_environment() actually applied
    to this run. It lives here rather than at the top level because it is part of
    the same question -- what the memory situation was, and what was demanded of
    it -- and because a floor that varies by scenario is unreadable unless each
    figure says which one protected it.

    Best-effort; None where unavailable.
    """
    if sys.platform != "darwin":
        return None
    out = {"guard_floor_gb": GUARD_FLOOR_GB}
    try:
        r = subprocess.run(["sysctl", "-n", "vm.swapusage"],
                           capture_output=True, text=True, timeout=10)
        # "total = 28672.00M  used = 27837.62M  free = 834.38M  (encrypted)"
        parts = r.stdout.replace("M", "").split()
        out["swap_total_mb"] = float(parts[2])
        out["swap_used_mb"] = float(parts[5])
    except (subprocess.SubprocessError, OSError, IndexError, ValueError):
        return None
    try:
        r = subprocess.run(["sysctl", "-n", "vm.page_free_count", "hw.pagesize"],
                           capture_output=True, text=True, timeout=10)
        free_pages, page_size = (int(x) for x in r.stdout.split())
        out["free_mb"] = free_pages * page_size / (1024 * 1024)
    except (subprocess.SubprocessError, OSError, ValueError):
        pass
    # macOS's own verdict: 1 normal, 2 warning, 4 critical. This is the signal that
    # tracks the condition; swap figures alone do not (a recovering host shrinks its
    # swap file, so utilisation RISES as pressure falls).
    try:
        r = subprocess.run(["sysctl", "-n", "kern.memorystatus_vm_pressure_level"],
                           capture_output=True, text=True, timeout=10)
        out["pressure_level"] = int(r.stdout.strip())
    except (subprocess.SubprocessError, OSError, ValueError):
        pass
    # Reclaimable = free + inactive. Inactive pages are backed and can be handed to a
    # new allocation without touching disk, so counting only "free" understates what
    # is actually available by several GB on a warm machine.
    #
    # ⚠️ THE PAGE SIZE COMES FROM vm_stat ITSELF, never a constant. This block used to
    # multiply by a hard-coded 4096, which is the x86 page and NOT the page on the
    # Apple Silicon host every published figure was measured on: `vm_stat` there counts
    # 16,384-byte pages, so `available_mb` was reported at exactly a QUARTER of the
    # host's real reclaimable memory (measured 2026-08-18: 3.0 GB reported against
    # 12.1 GB actual). The error is fail-SAFE -- an over-strict floor can only refuse
    # runs that would have been fine, never admit one that should have been refused --
    # so no published figure is weakened by the correction; but it did abort legitimate
    # sessions, and every `mem_pressure.available_mb` recorded before this date is 4x
    # low on that host and must not be read as an absolute quantity.
    try:
        r = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=10)
        # Header: "Mach Virtual Memory Statistics: (page size of 16384 bytes)"
        m = re.search(r"page size of (\d+) bytes", r.stdout)
        page_size = int(m.group(1)) if m else os.sysconf("SC_PAGE_SIZE")
        pages = {}
        for line in r.stdout.splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                v = v.strip().rstrip(".")
                if v.isdigit():
                    pages[k.strip()] = int(v)
        free = pages.get("Pages free", 0) + pages.get("Pages inactive", 0)
        out["available_mb"] = free * page_size / (1024 * 1024)
        out["page_size_bytes"] = page_size
    except (subprocess.SubprocessError, OSError, ValueError, AttributeError):
        pass
    return out


def host_load():
    """Host CPU load provenance, recorded before and after every run.

    Answers the "the client and the engines share one machine" objection with
    data instead of prose: if the host kept idle capacity throughout, host
    contention cannot be what produced the wall-clock gap. Cheap by design --
    `os.getloadavg()` is a syscall, not a sampling window, so it neither costs
    measurement time nor perturbs what it measures. Coarse by the same token: a
    1-minute decayed average is a bound, not an instantaneous reading, which is
    why it is published as "N cores stayed idle", never as a CPU percentage.

    Best-effort; None where unavailable.
    """
    out = {}
    try:
        l1, l5, l15 = os.getloadavg()
        out["loadavg_1m"], out["loadavg_5m"], out["loadavg_15m"] = l1, l5, l15
    except (OSError, AttributeError):
        return None
    out["logical_cpus"] = os.cpu_count()
    if sys.platform == "darwin":
        try:
            r = subprocess.run(["sysctl", "-n", "hw.physicalcpu"],
                               capture_output=True, text=True, timeout=10)
            out["physical_cpus"] = int(r.stdout.strip())
        except (subprocess.SubprocessError, OSError, ValueError):
            pass
    return out


def _container_id(service):
    try:
        r = subprocess.run(
            ["docker", "ps", "-q",
             "-f", f"label=com.docker.compose.project={COMPOSE_PROJECT}",
             "-f", f"label=com.docker.compose.service={service}"],
            capture_output=True, text=True, timeout=30)
        return r.stdout.strip().splitlines()[0] if r.stdout.strip() else None
    except (subprocess.SubprocessError, OSError):
        return None


def net_bytes(service):
    """(rx, tx) byte counters of a container's eth0, or None if unavailable.

    For a benchmark whose whole thesis is about what goes over the wire, bytes are
    the one metric no one can argue with: immune to "your Python client is slow",
    and it quantifies the disclosed hit-metadata column leak. Read from
    /proc/net/dev inside the container -- exact counters, not docker stats' rounded
    human strings. Best-effort: a missing docker CLI or shell just yields None.
    """
    cid = _container_id(service)
    if not cid:
        return None
    try:
        r = subprocess.run(["docker", "exec", cid, "cat", "/proc/net/dev"],
                           capture_output=True, text=True, timeout=30)
    except (subprocess.SubprocessError, OSError):
        return None
    for line in r.stdout.splitlines():
        if ":" not in line:
            continue
        name, _, rest = line.partition(":")
        if name.strip() != "eth0":
            continue
        f = rest.split()
        if len(f) >= 9:
            return int(f[0]), int(f[8])
    return None


def net_bytes_each(services):
    """Per-service (rx, tx), keyed by service name, or None if none readable.

    net_bytes_all() sums a stack, which is right for the ES-wire axis but wrong
    for the CLIENT leg on a multi-container engine: Trino's summed tx blends the
    coordinator->client responses with worker->coordinator exchange, and the
    published 0.26 GB client-leg figure for the v030 session had to be DERIVED
    (tx_sum minus the internal share inferred from rx_sum - ES wire). Recorded
    per container, the coordinator's own counters give it directly
    (review 2026-08-21, point 1.3)."""
    out = {}
    for svc in services:
        v = net_bytes(svc)
        if v is not None:
            out[svc] = v
    return out or None


def net_delta_each(before, after):
    """Per-service {rx_bytes, tx_bytes} deltas for net_bytes_each() samples."""
    if not before or not after:
        return None
    out = {}
    for svc, b in before.items():
        a_ = after.get(svc)
        if a_ is not None:
            out[svc] = {"rx_bytes": a_[0] - b[0], "tx_bytes": a_[1] - b[1]}
    return out or None


def net_bytes_all(services):
    """Summed (rx, tx) across every container of a stack, or None if unavailable.

    ⚠️ SAMPLE THE WHOLE STACK, NOT ONE CONTAINER. Trino runs as a 3-node cluster and
    `node-scheduler.include-coordinator=false` means the COORDINATOR SCANS NOTHING --
    the workers read Elasticsearch. Sampling only the `trino` container therefore
    misses the entire ES->Trino wire and reports the exchange instead. Measured
    2026-08-16 on the 5-shard index: coordinator-only gave 721 MB for an S1 whose
    real volume is ~2.9 GB, and 0.2 MB for an S3 that scans 10M rows. Wire volume is
    the metric the aggregation-pushdown claim rests on, and it is the one number a
    reader cannot argue with, so it must be summed over the stack.
    """
    total, seen = [0, 0], False
    for s in services:
        v = net_bytes(s)
        if v:
            seen = True
            total[0] += v[0]
            total[1] += v[1]
    return (total[0], total[1]) if seen else None


def container_cpu_stat(service):
    """The whole cgroup v2 cpu.stat of a container as a dict, or None.

    `usage_usec` is the exact CPU cost; `nr_throttled`/`throttled_usec` say
    whether the container HIT ITS QUOTA during the window -- the counter an
    external reviewer pointed out was in the same file the harness already
    reads and never published (review of 2026-08-21, point 1.2). Without them,
    a low `usage_usec` is ambiguous between "did not need more CPU" and "was
    not allowed more CPU", and the handicap-never-consumed argument rests on
    the difference.
    """
    cid = _container_id(service)
    if not cid:
        return None
    try:
        r = subprocess.run(["docker", "exec", cid, "cat", "/sys/fs/cgroup/cpu.stat"],
                           capture_output=True, text=True, timeout=30)
    except (subprocess.SubprocessError, OSError):
        return None
    out = {}
    for line in r.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].lstrip("-").isdigit():
            out[parts[0]] = int(parts[1])
    return out or None


def container_cpu_usec(service):
    """Cumulative CPU microseconds a container has burned, or None."""
    st = container_cpu_stat(service)
    return st.get("usage_usec") if st else None


def engine_cpu_usec(services):
    """Summed cumulative CPU across every container of a stack, or None.

    Summed for the same reason net_bytes_all() is: Trino is three containers and
    `node-scheduler.include-coordinator=false` means the coordinator does not
    scan, so a coordinator-only reading would miss the work.
    """
    total, seen = 0, False
    for svc in services:
        v = container_cpu_usec(svc)
        if v is not None:
            seen = True
            total += v
    return total if seen else None


def _stack_throttle(services):
    """Summed (nr_throttled, throttled_usec) across a stack, or None."""
    nr, tu, seen = 0, 0, False
    for svc in services:
        st = container_cpu_stat(svc)
        if st is not None:
            seen = True
            nr += st.get("nr_throttled", 0)
            tu += st.get("throttled_usec", 0)
    return (nr, tu) if seen else None


def server_cpu_sample(engine_services):
    """One reading of the SERVER side: the engine's containers and Elasticsearch.

    Includes the cgroup throttling counters so every run can publish whether any
    container hit its CPU limit while it ran (review 2026-08-21, point 1.2)."""
    return {"engine_usec": engine_cpu_usec(engine_services),
            "es_usec": engine_cpu_usec(ES_SERVICES),
            "engine_throttle": _stack_throttle(engine_services),
            "es_throttle": _stack_throttle(ES_SERVICES)}


def server_cpu_delta(before, after):
    """Server CPU seconds burned between two samples.

    ⚠️ WHY THIS EXISTS. Until 2026-08-19 the benchmark measured only the CLIENT's
    CPU, and said so in Appendix A -- while four passages described Trino as
    "holding half again our CPU". That describes an ALLOCATION (6 vCPU against our
    4) and was being read as a CONSUMPTION, which nothing in the harness measured.
    An external reviewer called it the one strong rhetorical claim with no number
    behind it, and was right. It also left the S0p floor comparison lopsided: a
    sliced scroll pays everything in its client processes, where SoftClient4ES
    pays a smaller client bill PLUS a sidecar JVM that went uncounted -- so "2.9x
    less client CPU" compared a total against a part. Both are now measurable.
    """
    if not before or not after:
        return None
    out = {}
    for key, label in (("engine_usec", "engine_cpu_s"), ("es_usec", "elasticsearch_cpu_s")):
        b, a_ = before.get(key), after.get(key)
        out[label] = round((a_ - b) / 1e6, 3) if b is not None and a_ is not None else None
    if out["engine_cpu_s"] is not None and out["elasticsearch_cpu_s"] is not None:
        out["total_cpu_s"] = round(out["engine_cpu_s"] + out["elasticsearch_cpu_s"], 3)
    for key, label in (("engine_throttle", "engine"), ("es_throttle", "elasticsearch")):
        b, a_ = before.get(key), after.get(key)
        if b is not None and a_ is not None:
            out[label + "_nr_throttled"] = a_[0] - b[0]
            out[label + "_throttled_s"] = round((a_[1] - b[1]) / 1e6, 3)
    return out


def _transport_tx_bytes():
    """Bytes the cluster has sent on its INTER-NODE transport layer (port 9300).

    Read from `_nodes/stats/transport` (`tx_size_in_bytes`, cumulative per node).
    """
    # ⚠️ ASKED FROM INSIDE A CONTAINER, OVER LOOPBACK -- NEVER FROM THE HOST.
    # This probe reads a counter that its own request would otherwise increment: the
    # stats response is ~13 KB, and over the host's HTTP port it crosses eth0 and lands
    # in the very tx counter being sampled. Measured before the fix: an IDLE cluster
    # appeared to move ~24 KB per sample, identical at a 1.2 s and a 3.2 s window --
    # a fixed cost, not a rate, which is the signature of self-pollution. It would have
    # inflated the S3 push-down figure (whose true value is ~24 KB) by ~180%, i.e. the
    # single number this benchmark's central claim rests on. Over loopback inside the
    # container the response never touches eth0.
    cid = _container_id(ES_SERVICES[0])
    if not cid:
        return None
    try:
        r = subprocess.run(
            ["docker", "exec", cid, "curl", "-s",
             "http://localhost:9200/_nodes/stats/transport"],
            capture_output=True, text=True, timeout=30)
        nodes = json.loads(r.stdout)["nodes"]
    except (subprocess.SubprocessError, OSError, ValueError, KeyError):
        return None            # cluster unreachable or answering something unexpected
    return sum(n["transport"]["tx_size_in_bytes"] for n in nodes.values())


def es_wire_bytes():
    """What left the Elasticsearch CLUSTER, decomposed so it stays comparable.

    ⚠️ SUMMING eth0 OVER THREE NODES IS NOT THE OLD METRIC. The headline claim of this
    benchmark -- an aggregation that moves 24 KB where a scan moves 1.4 GB -- is bytes
    that leave the cluster toward the client. On a single node, eth0 tx WAS that. On a
    3-node cluster, a search is gathered by a coordinating node from the data nodes, so
    the same payload crosses the internal transport layer before it leaves over HTTP:
    summing eth0 across the three would count much of it twice and silently inflate
    every wire figure, including the one the pushdown result rests on.

    So both halves are recorded and the published figure is the difference:

        http_tx = sum(eth0 tx over the 3 nodes) - sum(transport tx over the 3 nodes)

    Both components are kept in the artifact so the correction is auditable rather than
    asserted -- and it has an independent oracle: S3 returns 100 rows and must land at
    ~24 KB whatever the topology, so a correction that is wrong shows up immediately in
    a cell whose right answer is already known.
    """
    raw = net_bytes_all(ES_SERVICES)
    return {"eth0": raw, "transport_tx": _transport_tx_bytes()}


def es_wire_delta(before, after):
    """Bytes off the cluster between two es_wire_bytes() samples, JSON-friendly."""
    if not before or not after or not before.get("eth0") or not after.get("eth0"):
        return None
    rx = after["eth0"][0] - before["eth0"][0]
    tx = after["eth0"][1] - before["eth0"][1]
    out = {"rx_bytes": rx, "tx_bytes_eth0_sum": tx}
    b, a_ = before.get("transport_tx"), after.get("transport_tx")
    if b is not None and a_ is not None:
        out["transport_tx_bytes"] = a_ - b
        # Never negative: a clamp here would hide a broken decomposition, so it is
        # recorded as measured and the S3 oracle is what says whether it is right.
        out["tx_bytes"] = tx - (a_ - b)
    else:
        out["tx_bytes"] = tx            # single-node fallback: transport is ~0 anyway
        out["transport_tx_bytes"] = None
    return out


def net_delta(before, after):
    """Bytes moved between two net_bytes()/net_bytes_all() samples, JSON-friendly."""
    if not before or not after:
        return None
    return {"rx_bytes": after[0] - before[0], "tx_bytes": after[1] - before[1]}


def check(scenario, out):
    """Correctness gate. Raises AssertionError before any timing is reported."""
    expected = EXPECTED_ROWS[scenario]
    assert out["rows"] == expected, (
        f"{scenario}: expected {expected:,} rows, got {out['rows']:,}. "
        "If this is 10,000 on the flight stack the SoftClient4ES licence did not "
        "lift the Community maxQueryResults cap (METHODOLOGY section 4); if it is "
        "10,000 on the esql stack the cluster still has the default "
        "esql.query.result_truncation_max_size (ES|QL truncates ABOVE its ceiling "
        "with HTTP 200 and no Warning header); if it is smaller than the index, the "
        "generator did not finish a full load.")
    if scenario == "S2":
        assert out["groups"] == EXPECTED_GROUPS, (
            f"S2: expected {EXPECTED_GROUPS} groups, got {out['groups']}")


def emit(result, out_path=None):
    payload = json.dumps(result)
    if out_path:
        with open(out_path, "w") as f:
            f.write(payload)
    print(payload)

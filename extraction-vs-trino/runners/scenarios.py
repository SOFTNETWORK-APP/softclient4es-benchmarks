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
import resource
import subprocess
import sys

DEFAULT_INDEX = "bench_events_10m"
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
EXPECTED_ROWS = {"S0": 10_000_000, "S1": 10_000_000, "S1r": 10_000_000,
                 "S2": 10_000_000, "S3": 100, "S4": 100}
EXPECTED_GROUPS = 100
SCENARIOS = list(EXPECTED_ROWS)

# Which stacks each scenario requires before its numbers mean anything. S0 is the
# shared floor (raw Elasticsearch, no engine). S1r runs on BOTH stacks since the
# time-to-DataFrame redefinition.
REQUIRED_STACKS = {"S0": {"es-raw"}, "S1": {"flight", "trino"},
                   "S1r": {"flight", "trino"}, "S2": {"flight", "trino"},
                   "S3": {"flight", "trino"}, "S4": {"flight", "trino"}}

# Trino returns exactly the 8 selected columns. The Flight schema additionally
# carries _id/_index/_score/_sort hit-metadata columns, because the sidecar infers
# the statement schema from the row keys rather than from the SQL projection
# (VERIFIED: ElasticConversion appends metadata to every hit row;
# ElasticFlightProducer.probeSchema -> ArrowTypeMapping.inferSchema(rows)).
# So SoftClient4ES moves MORE bytes per row than the SQL asked for -- the bias
# runs against it. Gate on >= and RECORD the real count.
EXPECTED_COLS_TRINO_S1 = len(COLUMNS)
EXPECTED_MIN_COLS_FLIGHT_S1 = len(COLUMNS)


def sql_for(scenario, index=DEFAULT_INDEX):
    projection = ", ".join(COLUMNS)
    if scenario in ("S1", "S1r", "S2"):
        return f"SELECT {projection} FROM {index}"
    if scenario == "S3":
        return ("SELECT category, COUNT(*) AS cnt, AVG(amount) AS avg_amount "
                f"FROM {index} GROUP BY category")
    if scenario == "S4":
        return f"SELECT {projection} FROM {index} LIMIT 100"
    raise SystemExit(f"no SQL for scenario {scenario}")


def guard_environment():
    """Refuse to produce numbers from an environment that would misreport them.

    Two ways a run can look fine and be worthless:

    1. Assertions disabled (-O / PYTHONOPTIMIZE). Every correctness gate in this
       harness is an assert, including the one that catches a licence-truncated
       S1, so a run without them can report 10,000 rows as if it were 10,000,000.
    2. A translated interpreter. An x86_64 CPython under Rosetta on Apple Silicon
       inflates client CPU and does so unevenly across libraries, which is exactly
       the metric the moat claim rests on.
    """
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
    Best-effort; None where unavailable.
    """
    if sys.platform != "darwin":
        return None
    out = {}
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


def net_delta(before, after):
    """Bytes moved between two net_bytes() samples, as a JSON-friendly dict."""
    if not before or not after:
        return None
    return {"rx_bytes": after[0] - before[0], "tx_bytes": after[1] - before[1]}


def check(scenario, out):
    """Correctness gate. Raises AssertionError before any timing is reported."""
    expected = EXPECTED_ROWS[scenario]
    assert out["rows"] == expected, (
        f"{scenario}: expected {expected:,} rows, got {out['rows']:,}. "
        "If this is 10,000 the SoftClient4ES licence did not lift the Community "
        "maxQueryResults cap (METHODOLOGY section 4); if it is smaller than the "
        "index, the generator did not finish a full load.")
    if scenario == "S2":
        assert out["groups"] == EXPECTED_GROUPS, (
            f"S2: expected {EXPECTED_GROUPS} groups, got {out['groups']}")


def emit(result, out_path=None):
    payload = json.dumps(result)
    if out_path:
        with open(out_path, "w") as f:
            f.write(payload)
    print(payload)

#!/usr/bin/env python3.12
"""Re-derive every published figure from the session artifacts, and diff the report
against RESULTS cell by cell.

    python3.12 runners/verify_claims.py                    # verify the sessions RESULTS names
    python3.12 runners/verify_claims.py --session 2026…    # override the extraction session

WHY THIS EXISTS, in two failures it would have caught.

1. A DOCUMENT FIGURE THAT NO ARTIFACT SUPPORTS. Numbers reach RESULTS by hand, so a
   cell can keep a value from a retired session indefinitely: the reader cannot tell,
   because a stale figure looks exactly like a fresh one. Here every claim is recomputed
   from `results/<session>/*-run*.json` and compared against what the document prints.

2. A PRESENCE CHECK MASQUERADING AS A PARITY CHECK. An earlier ad-hoc check asked "does
   this token from the report appear anywhere in RESULTS?" and reported 0 divergences
   while three cells were wrong -- because `45.7 s` DID appear in RESULTS (as Trino's
   5-shard figure, not S0's floor), and `5.2 s` / `24.2 s` appeared as S1r's and the
   5-shard CPU. A token that exists somewhere proves nothing about the cell it sits in.
   `compare_report_cells()` therefore matches a LABEL to its value, never a bare number.

3. A DOCUMENT THAT AGREES WITH ITSELF ABOUT THE WRONG BUILD. Cells are matched by
   label, so the version string is invisible to them: the PDF footer credited all 21
   pages to `0.2.5` long after every figure had been re-measured on `0.2.5.1` -- the
   exact build whose defect forced the re-measurement. `verify_image_provenance()`
   reads the tag and digest from the session's own `sidecar-image.txt` and requires
   every site that names a build to name that one, plus flags any version string not
   on an explicit historical allowlist.

Exit code is 1 if any claim fails, so a session can gate a publish.
"""
import argparse
import json
import pathlib
import re
import statistics
import sys
from collections import defaultdict

HERE = pathlib.Path(__file__).resolve().parent.parent
RESULTS = HERE / "RESULTS.md"
REPORT = HERE / "report" / "report.html"

# How close a document figure must be to the re-derived one. Documents round (34.044 ->
# "34.0 s"), so the tolerance is half a printed unit plus a hair for float noise.
TOL = 0.06


def load_runs(session):
    """Every measured run in a session, grouped by (scenario, stack, variant)."""
    out = defaultdict(list)
    for f in sorted((HERE / "results" / session).glob("*-run*.json")):
        r = json.loads(f.read_text())
        out[(r.get("scenario"), r.get("stack"), r.get("variant") or "")].append(r)
    return out


def med(runs, key):
    vals = [r.get(key) for r in runs if isinstance(r.get(key), (int, float))]
    return statistics.median(vals) if vals else None


def es_bytes(runs):
    vals = [r["net_es"]["tx_bytes"] for r in runs
            if isinstance(r.get("net_es"), dict) and "tx_bytes" in r["net_es"]]
    return statistics.median(vals) if vals else None


class Check:
    """One published claim, its re-derived value, and the verdict."""

    def __init__(self):
        self.rows = []

    def add(self, name, derived, published, tol=TOL):
        ok = derived is not None and published is not None and abs(derived - published) <= tol
        self.rows.append((name, derived, published, ok))
        return ok

    def report(self, title):
        bad = [r for r in self.rows if not r[3]]
        print(f"\n=== {title}: {len(self.rows) - len(bad)}/{len(self.rows)} claims verified ===")
        for name, derived, published, ok in self.rows:
            if not ok:
                d = "—" if derived is None else f"{derived:,.3f}"
                p = "—" if published is None else f"{published:,.3f}"
                print(f"  FAIL {name}: artifacts say {d}, document says {p}")
        return not bad


def doc_number(text, pattern, group=1):
    """Pull one number out of the document, or None if the anchor moved."""
    m = re.search(pattern, text)
    return float(m.group(group).replace(",", "")) if m else None


def verify_extraction(session, results_md, chk):
    """The 6-shard matrix: wall, client CPU, peak memory, server CPU and ES egress.

    Anchors are matched against the CURRENT prose. When RESULTS is rewritten these must
    be rewritten with it -- a lost anchor reports as a failure rather than passing, which
    is the intended behaviour: an unverified claim and a false claim are both unpublishable.
    """
    runs = load_runs(session)

    def cell(sc, stack, variant=""):
        return runs.get((sc, stack, variant), [])

    # S1 -- the headline. Published from the FIRST timed block, measured warm behind
    # the steady-state gate; the end-of-session drift block is the favourable bound
    # the document quotes but does NOT use (RESULTS section 1, "the warm-in").
    f, t = cell("S1", "flight"), cell("S1", "trino")
    chk.add("S1 flight wall (first block)", med(f, "wall_s"),
            doc_number(results_md, r"\| Wall median \| \*\*([\d.]+) s\*\* \[[\d.–]+\] \| [\d.]+ s"))
    chk.add("S1 trino wall (first block)", med(t, "wall_s"),
            doc_number(results_md, r"\| Wall median \| \*\*[\d.]+ s\*\* \[[\d.–]+\] \| ([\d.]+) s"))
    chk.add("S1 flight cpu", med(f, "cpu_s"),
            doc_number(results_md, r"\| Client CPU \| \*\*([\d.]+) s\*\* \| [\d.]+ s \| 8\.5× less"))
    chk.add("S1 trino cpu", med(t, "cpu_s"),
            doc_number(results_md, r"\| Client CPU \| \*\*[\d.]+ s\*\* \| ([\d.]+) s \| 8\.5× less"))
    chk.add("S1 flight mem", med(f, "peak_footprint_mb"),
            doc_number(results_md, r"\| Peak client memory \| \*\*([\d,]+) MB\*\* \| [\d,]+ MB \| 4\.8× less"), tol=1.0)
    chk.add("S1 trino mem", med(t, "peak_footprint_mb"),
            doc_number(results_md, r"\| Peak client memory \| \*\*[\d,]+ MB\*\* \| ([\d,]+) MB \| 4\.8× less"), tol=1.0)
    chk.add("S1 ratio", med(t, "wall_s") / med(f, "wall_s"),
            doc_number(results_md, r"\| \*\*([\d.]+)× faster\*\* \|"), tol=0.005)

    # The drift block is quoted as the bound NOT used. If it ever matches the first
    # block, the warm-in has stopped happening and the note must be rewritten.
    drift_f = cell("S1", "flight", "drift")
    chk.add("S1 flight wall (drift, quoted in section 1)", med(drift_f, "wall_s"),
            doc_number(results_md, r"runs \*\*([\d.]+) s at 35\.7 s\*\*"))
    drift_t = cell("S1", "trino", "drift")
    chk.add("S1 trino drift (0.4% flat)", med(drift_t, "wall_s"),
            doc_number(results_md, r"\(44\.70 → ([\d.]+) s\)"))

    # S1m -- bytes/row is the mechanism that shows bounded and unbounded paths cost the same.
    f, t = cell("S1m", "flight"), cell("S1m", "trino")
    chk.add("S1m flight wall", med(f, "wall_s"),
            doc_number(results_md, r"SoftClient4ES, Arrow Flight SQL\*\* \| \*\*([\d.]+) s\*\*"))
    chk.add("S1m trino wall", med(t, "wall_s"),
            doc_number(results_md, r"\| Trino, documented client \| ([\d.]+) s"))
    chk.add("S1m flight bytes/row", es_bytes(f) / 1_000_000,
            doc_number(results_md, r"\*\*([\d]+) bytes per\nrow off the cluster"), tol=1.0)

    # S3 -- push-down, the largest claim in the document. Published rounded, so the check
    # is that the rounding is honest.
    f, t = cell("S3", "flight"), cell("S3", "trino")
    ours, theirs = es_bytes(f), es_bytes(t)
    chk.add("S3 flight ES wire (KB)", ours / 1_000,
            doc_number(results_md, r"\| ES wire \| \*\*([\d.]+) KB\*\*"), tol=0.5)
    chk.add("S3 trino ES wire (GB)", theirs / 1_000_000_000,
            doc_number(results_md, r"\| ES wire \| \*\*[\d.]+ KB\*\* \| ([\d.]+) GB"), tol=0.02)
    # The "factor of roughly 52,000" phrasing was REMOVED on review (2026-08-21,
    # point 2.5): a byte ratio beside a time narrative reads as a speed factor.
    # The byte pair itself (26.5 KB vs 1.39 GB) stays and is checked above.
    chk.add("S3 trino wall", med(t, "wall_s"),
            doc_number(results_md, r"\| Wall median \| \*\*0\.043 s\*\* \[[^\]]+\] \| ([\d.]+) s"))

    # Floors -- BOTH arms. The artifact-building one is the like-for-like comparison; the
    # count-and-discard one does a cheaper task and is labelled as such.
    chk.add("S0 wall (count-and-discard)", med(cell("S0", "es-raw"), "wall_s"),
            doc_number(results_md, r"one reader, count and discard \| ([\d.]+) s"))
    chk.add("S0p wall (count-and-discard)", med(cell("S0p", "es-raw"), "wall_s"),
            doc_number(results_md, r"6 slices, count and discard \| ([\d.]+) s"))
    chk.add("S0p wall (building Arrow)", med(cell("S0p", "es-raw", "arrow"), "wall_s"),
            doc_number(results_md, r"6 slices, building Arrow\*\* \| \*\*([\d.]+) s\*\* \[[^\]]+\]"))
    chk.add("S0p client CPU (building Arrow)", med(cell("S0p", "es-raw", "arrow"), "cpu_s"),
            doc_number(results_md, r"6 slices, building Arrow\*\* \| \*\*[\d.]+ s\*\* \[[^\]]+\] \| \*\*([\d.]+) s\*\*"))
    return chk


def verify_paging_ab(session, results_md, chk):
    """Section 6.1 -- sliced vs sequential, the attribution for section 6's widening gap."""
    runs = load_runs(f"{session}-sliced-ab")

    def arm(sc, variant):
        return runs.get((sc, "flight", variant), [])

    seq, sli = arm("S1", "sequential"), arm("S1", "sliced")
    if not seq or not sli:
        chk.rows.append(("6.1 A/B runs present", None, None, False))
        return chk
    chk.add("6.1 S1 sequential wall", med(seq, "wall_s"),
            doc_number(results_md, r"\*\*S1\*\* — extract 10M rows \| ([\d.]+) s \["))
    chk.add("6.1 S1 sliced wall", med(sli, "wall_s"),
            doc_number(results_md, r"\*\*S1\*\* — extract 10M rows \| [\d.]+ s \[[^\]]+\] \| \*\*([\d.]+) s\*\*"))
    chk.add("6.1 S1 gain", med(seq, "wall_s") / med(sli, "wall_s"),
            doc_number(results_md, r"\*\*S1\*\* — extract 10M rows \|[^|]*\|[^|]*\| \*\*([\d.]+)×\*\*"), tol=0.01)
    # The controls are the reason the A/B is readable at all: if they ever move, the arms
    # differed by something other than the slice ceiling.
    for sc in ("S3", "S4"):
        a, b = arm(sc, "sequential"), arm(sc, "sliced")
        if a and b:
            chk.add(f"6.1 {sc} control unmoved", abs(med(a, "wall_s") - med(b, "wall_s")), 0.0, tol=0.01)
    # Elasticsearch CPU FALLING is the evidence that slicing is less work, not more overlap.
    chk.add("6.1 sequential ES CPU", med(seq, "server_cpu.elasticsearch_cpu_s") if False else
            statistics.median([r["server_cpu"]["elasticsearch_cpu_s"] for r in seq
                               if isinstance(r.get("server_cpu"), dict)]),
            doc_number(results_md, r"S1 \*\*Elasticsearch CPU\*\* \| ([\d.]+) s"), tol=0.5)
    return chk


def verify_topology(session, results_md, chk):
    """Section 6 -- the 1-shard arm, and the scaling factors quoted from it."""
    # The re-run after the host-sleep abort carries a "b" suffix on the base name.
    name = f"{session}-1shard"
    if not (HERE / "results" / name).is_dir():
        name = f"{session.replace('-v030', 'b-v030', 1)}-1shard"
    runs = load_runs(name)

    def cell(sc, stack):
        return runs.get((sc, stack, "1shard"), [])

    f1, t1 = cell("S1", "flight"), cell("S1", "trino")
    if not f1 or not t1:
        chk.rows.append(("section 6 1-shard runs present", None, None, False))
        return chk
    chk.add("6 flight 1-shard wall", med(f1, "wall_s"),
            doc_number(results_md, r"\| S1 wall \| ([\d.]+) s"))
    chk.add("6 trino 1-shard wall", med(t1, "wall_s"),
            doc_number(results_md,
                       r"\| S1 wall \| [\d.]+ s \[[^\]]+\] \| \*\*[\d.]+ s\*\* \[[^\]]+\] \| ([\d.]+) s"))

    # The 6-shard reference is the HEADLINE cell (first block), not the drift bound.
    warm = load_runs(session)
    f6 = warm.get(("S1", "flight", ""), [])
    t6 = warm.get(("S1", "trino", ""), [])
    if f6 and t6:
        chk.add("6 flight shard scaling", med(f1, "wall_s") / med(f6, "wall_s"),
                doc_number(results_md, r"SoftClient4ES goes \*\*([\d.]+)× faster\*\* with six shards"), tol=0.02)
        chk.add("6 trino shard scaling", med(t1, "wall_s") / med(t6, "wall_s"),
                doc_number(results_md, r"Trino goes \*\*([\d.]+)× faster\*\*"), tol=0.02)
    return chk


def verify_joins(session, results_md, chk):
    """J0-J2 medians AND the pairwise counts, which are the ordering evidence."""
    import itertools
    base = HERE / "results" / f"join-{session}"
    for sc in ("J0", "J1", "J2"):
        w = {}
        for st in ("flight", "trino"):
            w[st] = sorted(json.loads(f.read_text())["wall_s"]
                           for f in base.glob(f"{st}-{sc}-run*.json"))
        if not w["flight"] or not w["trino"]:
            chk.rows.append((f"{sc} runs present", None, None, False))
            continue
        chk.add(f"{sc} flight wall", statistics.median(w["flight"]),
                doc_number(results_md, rf"\| \*\*{sc}\*\*[^|]*\| \*?\*?([\d.]+) s"))
        wins = sum((a < b) + 0.5 * (a == b) for a, b in itertools.product(w["flight"], w["trino"]))
        # Two columns between the scenario and the win count (min-max per engine), not
        # three: the "Gap" column was dropped when the margins moved into the table above.
        published = doc_number(results_md, rf"\| {sc} \|[^|]*\|[^|]*\| \*\*\w+ ([\d]+) / 25\*\*")
        derived = wins if wins >= 12.5 else 25 - wins        # whichever engine the doc names
        chk.add(f"{sc} pairings won", derived, published, tol=0.5)
    return chk


# ── report vs RESULTS, BY LABEL — never by bare token (see module docstring) ────
# Each entry: a human label, how to find the value in RESULTS, how to find it in the report.
CELL_PAIRS = [
    ("glance S0 floor",
     r"\| \*\*S0\*\* \|[^|]*\| ([\d.]+) s to read 10M rows",
     r"reference floor\?</td><td>([\d.]+) s to read 10M rows"),
    ("glance S4 parity",
     r"\| \*\*S4\*\* \|[^|]*\| Parity — ([\d]+) ms",
     r"Parity</span> ([\d]+) ms"),
    ("S1 client CPU (ours)",
     r"\| Client CPU \| \*\*([\d.]+) s\*\* \| [\d.]+ s \| 8\.5× less",
     r'Client CPU</td><td class="num win">([\d.]+) s</td><td class="num">[\d.]+ s</td><td class="num">8\.5× less'),
    ("S1 client CPU (Trino)",
     r"\| Client CPU \| \*\*[\d.]+ s\*\* \| ([\d.]+) s \| 8\.5× less",
     r'Client CPU</td><td class="num win">[\d.]+ s</td><td class="num">([\d.]+) s</td><td class="num">8\.5× less'),
    ("S1 wall (ours)",
     r"\| Wall median \| \*\*([\d.]+) s\*\* \[",
     r'Wall median</td><td class="num win">([\d.]+) s <span class="small">\['),
    ("S1 Elasticsearch CPU (ours)",
     r"\| Elasticsearch CPU \| ([\d.]+) s \| \*\*[\d.]+ s\*\*",
     r'Elasticsearch CPU</td><td class="num">([\d.]+) s</td><td class="num win">[\d.]+ s'),
    ("S2 wall (ours)",
     r"\| Wall median \| \*\*([\d.]+) s\*\* \[[^\]]+\] \| [\d.]+ s \[[^\]]+\] \| \*\*5\.1× faster",
     r'Wall median</td><td class="num win">([\d.]+) s <span class="small">\[[^\]]+\]</span></td>'
     r'<td class="num">[\d.]+ s <span class="small">\[[^\]]+\]</span></td><td class="num"><b>5\.1× faster'),
    ("S1m ours",
     r"SoftClient4ES, Arrow Flight SQL\*\* \| \*\*([\d.]+) s\*\*",
     r'SoftClient4ES, Arrow Flight SQL</td><td class="num win">([\d.]+) s'),
    ("S1m Trino",
     r"\| Trino, documented client \| ([\d.]+) s",
     r'Trino, documented client</td><td class="num">([\d.]+) s'),
    ("J0 ours",
     r"\| \*\*J0\*\* plain join \| ([\d.]+) s",
     r'J0 — plain join</td><td class="num">([\d.]+) s'),
    ("J2 Trino",
     r"\| \*\*J2\*\* \+ `GROUP BY` \| [\d.]+ s \| \*\*([\d.]+) s\*\*",
     r'J2 — \+ <code>GROUP BY</code></td><td class="num">[\d.]+ s</td><td class="num win">([\d.]+) s'),
    ("1-shard S1 ours",
     r"\| S1 wall \| [\d.]+ s \[[^\]]+\] \| \*\*([\d.]+) s\*\*",
     r'S1 wall</td><td class="num">[\d.]+ s <span class="small">\[[^\]]+\]</span></td>'
     r'<td class="num win">([\d.]+) s'),
]


# ── image provenance ────────────────────────────────────────────────────────
# The cells above are checked by LABEL, which means the version string is invisible
# to them: every figure can agree perfectly while the page footer attributes them to
# a different build. That is not hypothetical -- it shipped. `report/build.mjs` kept
# "measured on the released 0.2.5 build" in the running footer of all 21 pages and in
# the PDF's /Subject metadata after everything else moved to 0.2.5.1, so the artifact
# contradicted its own cover and credited the numbers to the build whose defect caused
# the republication. Nothing in the harness could see it: not the figure checks (the
# figures were right), not the cell diff (no label), not a token scan.
#
# So the tag and digest become checked facts with a single source of truth -- the
# CURRENT session's own `sidecar-image.txt`, written by the harness from the running
# container, not typed by anyone.
IMAGE_SITES = [
    (".env.example", r"^SIDECAR_TAG=(\S+)\s*$", "the tag a fresh clone measures"),
    ("docker-compose.yml", r"arrow-flight-sql:\$\{SIDECAR_TAG:-([^}]+)\}",
     "the compose default when no .env is present"),
    ("RESULTS.md", r"arrow-flight-sql:([\d.]+)`", "RESULTS section 1, environment"),
    ("FINDINGS.md", r"measured on the released `([\d.]+)` build",
     "FINDINGS, what the fixes were measured on"),
    ("report/report.html", r"arrow-flight-sql:([\d.]+)</code>", "report body, environment"),
    ("report/cover.html", r"<dd>([\d.]+)<small>Arrow Flight SQL sidecar</small>",
     "report cover"),
    ("report/build.mjs", r"measured on the released ([\d.]+) build</span>",
     "PDF running footer, every page"),
    ("report/build.mjs", r"measured on the released ([\d.]+) build',",
     "PDF /Subject metadata"),
]
# Sites that print the image digest (truncated for the page, so compare the prefix).
DIGEST_SITES = [
    ("RESULTS.md", r"digest `sha256:([0-9a-f]+)"),
    ("report/report.html", r"digest <code>sha256:([0-9a-f]+)"),
]
# Version strings that are DELIBERATELY not the measured one. Each is a historical
# statement, and each must stay readable as history -- keyed by a phrase from the line
# rather than a line number, so an edit that changes the meaning breaks the key.
VERSION_ALLOWLIST = [
    ("docker-compose.yml", "is NOT a cosmetic bump",
     "names the old tag on purpose, to say why it must not be used"),
    ("docker-compose.yml", "arrow#141, fixed in 0.2.5-SNAPSHOT",
     "when a fix landed, not what was measured"),
    ("docker-compose.yml", "bring-up on 0.2.5 and has NOT been repeated",
     "an honest attribution of a check to the build that ran it"),
    ("runners/orchestrate.py", "0.2.5-SNAPSHOT was republished",
     "the incident that made provenance recording necessary"),
    ("runners/orchestrate_concurrent.py", "the report's \"every figure was measured on image 0.2.5",
     "quotes the sentence that turned out to be unverifiable"),
    ("runners/orchestrate_join.py", "the 0.2.5-SNAPSHOT tag was republished",
     "same incident"),
    ("runners/run_join.py", "the 0.2.5-SNAPSHOT tag was",
     "same incident"),
    ("results/README.md", "measured **sidecar 0.2.5 or 0.2.5.1**",
     "the RETIRED sessions really were measured on those"),
    ("results/README.md", "on 0.2.5 the Flight schema probe",
     "explains why a retired figure legitimately differs"),
    ("results/README.md", "on 0.2.5.1 extraction paged Elasticsearch sequentially",
     "explains the other reason a retired figure differs: pre-#238 paging"),
    ("results/README.md", "CONTAMINATED-join-20260818T230041-v0251",
     "names a discarded session by its directory name"),
    ("results/README.md", "ABORTED-20260818T213431-v0251",
     "names a discarded session by its directory name"),
    ("RESULTS.md", "release measured 34.6 s on this same cluster",
     "the previous release's figure, which the sequential A/B arm reproduces"),
    ("FINDINGS.md", "fixed in core 0.21.0, shipped in",
     "names where a fix landed, not what was measured"),
    # The paging A/B and the compose knob both describe the behaviour of the PREVIOUS
    # release by name -- that is the whole point of the arm, so the mention is load-bearing.
    ("docker-compose.yml", "sequential reader that every session up to and including",
     "names the release whose behaviour ELASTIC_SCROLL_MAX_SLICES=1 reproduces"),
    ("runners/run_sequential_ab.sh", "Every session up to and including sidecar",
     "names the release the sequential arm exists to reproduce"),
    ("docker-compose.yml", "is a bigger break than that",
     "names the previous tag to say why figures must not be blended across it"),
    ("docker-compose.yml", "Do not blend figures across",
     "same warning, naming both tags on purpose"),
    # Third-party pins that collide with OUR family. Listed by exact line rather than
    # excluded by file, so a sidecar tag typed into requirements.txt would still be a
    # stray -- this is the most confusable token in the tree now that we ship a 0.3.0.
    ("requirements.txt", "dbc==0.3.0",
     "the ADBC driver's own 0.3.0, nothing to do with the sidecar tag"),
]
# Sidecar tag families this scan polices. This was hardcoded `0\.2\.\d+` until the 0.3.0
# bump, at which point it would have gone BLIND to exactly the tokens it now has to
# police: every new "0.3.0" typed into a doc would have matched nothing, been in no
# allowlist, and passed silently. A scan whose pattern tracks the version it is checking
# can only ever catch the PREVIOUS mistake.
#
# Widening it to a bare `0\.\d+\.\d+` is the WRONG repair: third-party pins share the
# shape (trino 0.338.0, adbc 0.4.5, distlib 0.3.9), so the scan fills with noise and a
# noisy gate gets ignored -- which is a worse failure than the blind spot, because it
# looks green-adjacent. Shape cannot discriminate; the family list must be explicit.
#
# It is kept HONEST by check_family_covered() below: the measured tag's own family must
# appear here or provenance FAILS. The next bump therefore cannot silently go blind --
# it goes red until someone adds the family.
PRODUCT_FAMILIES = ("0.2", "0.3")
VERSION_TOKEN = re.compile(
    r"\b(?:" + "|".join(re.escape(f) for f in PRODUCT_FAMILIES) + r")\.\d+"
    r"(?:\.\d+)?(?:-SNAPSHOT)?\b")


def tag_family(tag):
    """'0.2.5.1' -> '0.2'. The first two components are what names a release line."""
    parts = tag.split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else tag
SCAN_SUFFIXES = {".md", ".py", ".yml", ".yaml", ".html", ".mjs", ".sh", ".example", ".txt"}


def session_image(session):
    """Tag and digest of the image the session actually ran, from the harness record."""
    rec = HERE / "results" / session / "sidecar-image.txt"
    if not rec.exists():
        return None, None
    text = rec.read_text()
    tag = re.search(r"^config_image=\S+:(\S+)$", text, re.M)
    dig = re.search(r"^image_id=sha256:([0-9a-f]+)$", text, re.M)
    return (tag.group(1) if tag else None), (dig.group(1) if dig else None)


def verify_image_provenance(session):
    """Every place that names the measured build must name the one that ran."""
    print("\n=== image provenance: every version string vs the session's own record ===")
    tag, digest = session_image(session)
    if not tag or not digest:
        print(f"  FAIL no readable results/{session}/sidecar-image.txt — "
              "provenance cannot be checked, so it is not assumed")
        return False
    print(f"  session ran {tag} (sha256:{digest[:12]}…), per the harness's own record")
    bad = 0
    for path, pattern, why in IMAGE_SITES:
        text = (HERE / path).read_text()
        m = re.search(pattern, text, re.M)
        if not m:
            print(f"  ANCHOR LOST {path} — {why} (pattern no longer matches; fix the "
                  "pattern, do not drop the site)")
            bad += 1
        elif m.group(1) != tag:
            print(f"  MISMATCH {path} says {m.group(1)}, session ran {tag} — {why}")
            bad += 1
    for path, pattern in DIGEST_SITES:
        m = re.search(pattern, (HERE / path).read_text())
        if not m:
            print(f"  ANCHOR LOST {path} digest")
            bad += 1
        elif not digest.startswith(m.group(1)):
            print(f"  MISMATCH {path} digest {m.group(1)}… is not a prefix of {digest}")
            bad += 1
    # Fail CLOSED if the measured tag's release line is not one the scan below knows how
    # to see. Without this, a bump to a new family would quietly disarm the stray scan --
    # the single failure mode this whole function exists to prevent.
    if tag_family(tag) not in PRODUCT_FAMILIES:
        print(f"  FAIL measured tag {tag} is in family {tag_family(tag)}, which the "
              f"stray scan does not police (PRODUCT_FAMILIES={PRODUCT_FAMILIES}). "
              "Add it, or every stray naming that family passes unseen.")
        bad += 1
    # The other half: a version string nobody listed. Catches the next footer.
    allowed = {(p, needle) for p, needle, _ in VERSION_ALLOWLIST}
    strays = 0
    for f in sorted(HERE.rglob("*")):
        if not f.is_file() or f.suffix not in SCAN_SUFFIXES:
            continue
        rel = f.relative_to(HERE).as_posix()
        # What is deliberately NOT scanned, and why:
        #   .venv / .git      third-party and VCS internals -- polars ships a "0.2.5"
        #                     in its own version table, which says nothing about us
        #   results/*         a measured session RECORDS the image it ran; the retired
        #                     ones legitimately name 0.2.5, and that is the evidence,
        #                     not a claim (results/README.md IS scanned -- it makes
        #                     claims about those sessions)
        #   this file         it is the registry: the allowlist and the docstring name
        #                     old versions by construction
        # Matched on path COMPONENTS, not on the prefix. `rel.startswith(".venv/")` only
        # ever excluded a virtualenv at the REPO ROOT, so report/.venv -- a second, real
        # virtualenv -- was scanned all along. Under the old narrow pattern that was
        # survivable by luck; pip vendors distlib 0.3.9 and an SPDX table full of 0.3.x,
        # so from the 0.3.0 bump onward it would have produced a wall of strays.
        parts = set(pathlib.PurePosixPath(rel).parts)
        if parts & {".venv", ".venv-latest", ".git", "node_modules"}:
            continue
        if rel.startswith("results/") and rel != "results/README.md":
            continue
        if f.resolve() == pathlib.Path(__file__).resolve():
            continue
        for n, line in enumerate(f.read_text(errors="ignore").splitlines(), 1):
            for tok in VERSION_TOKEN.findall(line):
                if tok == tag or any(rel == p and needle in line
                                     for p, needle in allowed):
                    continue
                print(f"  STRAY {rel}:{n} names {tok}, not the measured {tag} — "
                      "allowlist it with a reason if it is deliberate history")
                strays += 1
    total = len(IMAGE_SITES) + len(DIGEST_SITES)
    print(f"  {total - bad}/{total} provenance sites agree; {strays} unlisted "
          "version strings")
    return bad == 0 and strays == 0


def compare_report_cells(results_md, report_html):
    """Label-matched diff. A value is compared to the value under the SAME label."""
    print("\n=== report vs RESULTS, cell by cell ===")
    bad = 0
    for label, md_pat, html_pat in CELL_PAIRS:
        a, b = doc_number(results_md, md_pat), doc_number(report_html, html_pat)
        if a is None or b is None:
            print(f"  ANCHOR LOST {label}: RESULTS={a}, report={b} "
                  "(the pattern no longer matches — fix the pattern, do not ignore it)")
            bad += 1
        elif abs(a - b) > 1e-9:
            print(f"  MISMATCH {label}: RESULTS={a}, report={b}")
            bad += 1
    print(f"  {len(CELL_PAIRS) - bad}/{len(CELL_PAIRS)} cells agree")
    return bad == 0


# --------------------------------------------------------------------------
# 4. A DOCUMENT THAT DESCRIBES A DIFFERENT SESSION THAN THE ONE IT PUBLISHES.
#    Cells are matched by label and the image by digest, so nothing above sees
#    the PROSE around them. The 0.3.0 report shipped a provenance paragraph
#    inherited from the 0.2.5.1 campaign: "the single-shard extraction matrix
#    ... 165 measured runs", three sections describing the main index as
#    1-shard, a table headed "5 shards" over 6-shard numbers, and a limitation
#    ("that setup favours SoftClient4ES") that had become false. Every figure
#    in it verified. So this checks the things a figure check cannot see: how
#    many runs the session actually holds, what the host was doing during
#    them, and whether the documents describe the topology they measured.
# --------------------------------------------------------------------------
def verify_session_state(session, results_md, report_html):
    d = HERE / "results" / session
    runs = [json.loads(f.read_text()) for f in sorted(d.glob("*-run*.json"))]
    runs = [r for r in runs if isinstance(r, dict) and "wall_s" in r]
    n = len(runs)

    def after(r):
        return (r.get("host_load_after") or {}).get("loadavg_1m")

    def mp(r, key):
        return (r.get("mem_pressure") or {}).get(key)

    loads = [v for v in (after(r) for r in runs) if v is not None]
    avail = [v for v in (mp(r, "available_mb") for r in runs) if v is not None]
    swap = [v for v in (mp(r, "swap_used_mb") for r in runs) if v is not None]
    levels = {mp(r, "pressure_level") for r in runs}

    chk = Check()
    # Run count -- both documents, against a live count of the directory.
    # Anchored on "measured runs, one gate" -- a bare "(\d+) measured runs"
    # matches the environment table's "5 measured runs per cell" instead.
    chk.add("RESULTS run count", n,
            doc_number(results_md, r"(\d+) measured runs, one gate"), tol=0)
    chk.add("report run count", n,
            doc_number(report_html, r"(\d+)\s*\n?\s*measured runs, one gate"), tol=0)
    chk.add("report run count (fairness rules)", n,
            doc_number(report_html, r"one host state, ([\d,]+) measured runs\)"), tol=0)
    chk.add("report 'all N runs' (pressure)", n,
            doc_number(report_html, r"pressure level 1 \(normal\) on all (\d+) runs"), tol=0)
    chk.add("RESULTS 'all N runs' (pressure)", n,
            doc_number(results_md, r"on all (\d+) runs\*\*"), tol=0)

    # Host envelope -- re-derived, never transcribed. The published figures were
    # a stale transcription: max load 11.4 against an actual 13.10, and 17.3 GB
    # available against an actual 11.84, with 16 runs outside the stated band.
    if loads:
        chk.add("host load median", statistics.median(loads),
                doc_number(results_md, r"median of ([\d.]+) and a maximum"), tol=0.05)
        chk.add("host load max", max(loads),
                doc_number(results_md, r"a maximum of ([\d.]+) against"), tol=0.05)
        chk.add("host load max (report)", max(loads),
                doc_number(report_html, r"maximum of ([\d.]+)\s+against 16"), tol=0.05)
        chk.add("uncommitted cores at peak", 16 - max(loads),
                doc_number(results_md, r"([\d.]+) cores were uncommitted"), tol=0.05)
    if avail:
        chk.add("min available GB", min(avail) / 1024,
                doc_number(results_md, r"\*\*([\d.]+) GB available"), tol=0.05)
        chk.add("min available GB (report)", min(avail) / 1024,
                doc_number(report_html, r"on all \d+ runs, with\s+([\d.]+) GB"), tol=0.05)
    if swap:
        # Two-block story: the overnight matrix's swap vs the morning addendum's.
        lo = [v for v in swap if v < 6000]
        hi = [v for v in swap if v >= 6000]
        if lo:
            chk.add("overnight swap (flat)", max(lo) - min(lo), 0, tol=2)
            chk.add("overnight swap level MB", statistics.median(lo),
                    doc_number(results_md, r"flat to\s*\n?\s*the megabyte at ([\d,]+) MB"), tol=2)
        if hi:
            chk.add("addendum swap min MB", min(hi),
                    doc_number(results_md, r"([\d,]+) MB at the minimum"), tol=2)
            chk.add("addendum swap max MB", max(hi),
                    doc_number(results_md, r"([\d,]+) MB at the maximum"), tol=2)

    # The VM's size, from the session's own docker-info.txt -- the report shipped
    # "10 CPU / 15.6 GiB" (a previous campaign's VM) while RESULTS said 13/31.3,
    # and an external reviewer built a whole contention argument on the stale row.
    info = d / "docker-info.txt"
    if info.exists():
        mcpu = re.search(r"CPUs:\s*(\d+)", info.read_text())
        mmem = re.search(r"Total Memory:\s*([\d.]+)GiB", info.read_text())
        if mcpu:
            ncpu = int(mcpu.group(1))
            for label, text in (("RESULTS", results_md), ("report", report_html)):
                for m in re.finditer(r"VM\s*\*{0,2}(\d+) CPU", text):
                    chk.add("%s says the VM has %s CPUs" % (label, m.group(1)),
                            ncpu, float(m.group(1)), tol=0)
        if mmem:
            # The user allocates in GB (32); Docker reports GiB (31.29). The docs
            # print BOTH -- the allocation as configured, the reported value in
            # parentheses -- and the gate checks the reported one against the
            # session's own record.
            gib = round(float(mmem.group(1)), 1)
            for label, text in (("RESULTS", results_md), ("report", report_html)):
                m = re.search(r"reports ([\d.]+) GiB", text)
                chk.add("%s VM memory as reported (GiB)" % label,
                        gib, float(m.group(1)) if m else None, tol=0.05)

    ok = chk.report("session state: run count and host envelope, re-derived")
    if levels != {1}:
        print(f"  FAIL memory pressure was not level 1 on every run: {sorted(levels)}")
        ok = False

    # Topology wording. The main matrix is 6-shard; prose that says otherwise is
    # a survivor from the previous campaign, and no figure check can see it.
    banned = [
        ("single-shard extraction matrix", "the main matrix is 6-shard"),
        ("single-shard matrix", "the main matrix is 6-shard"),
        ("measured on a single-shard index", "the main matrix is 6-shard"),
        ("Single primary shard in the main results", "no longer true of this campaign"),
        ('<th class="num">5 shards</th>', "the index has 6 primary shards"),
    ]
    print("\n=== topology wording: does the prose describe the index measured? ===")
    # Every published document, not only the two that carry figures: METHODOLOGY
    # states the same fairness rule and drifted with them.
    docs = [("RESULTS", results_md), ("report", report_html)]
    for extra in ("METHODOLOGY.md", "README.md", "FINDINGS.md"):
        f = HERE / extra
        if f.exists():
            docs.append((extra, f.read_text()))
    hits = 0
    for phrase, why in banned:
        for label, text in docs:
            if phrase in text:
                print(f"  FAIL {label} still says {phrase!r} — {why}")
                hits += 1
    if not hits:
        print("  clean: no stale single-shard/5-shard prose in either document")
    return ok and not hits


# --------------------------------------------------------------------------
# 5. STALE PROSE. The checks above match a figure to a LABEL, so they see only
#    the cells someone remembered to wire. Everything else -- the at-a-glance
#    table and every "Outcome" box -- was unguarded, and in the 0.3.0 build it
#    had gone stale wholesale: S2 published "1.65x faster" against a table
#    saying 5.2x, S4 "37 ms vs 56 ms" against a body saying 69, S6's own table
#    carried 38.0 s / 39.9 s / 51.0 s from a previous campaign, and the JOIN row
#    claimed a win on the plain join that section 6 gives to Trino.
#
#    So this is a COVERAGE check rather than another list of anchors: every
#    number carrying a unit inside those blocks must be reproducible from a
#    session record, or be explicitly waived with a reason. A rewrite that
#    invents a figure fails here until someone either derives it or waives it.
# --------------------------------------------------------------------------
PROSE_SESSIONS = (
    "20260821T041841-v030-prewarm", "20260821T041841b-v030-prewarm-1shard",
    "20260821T041841-v030-prewarm-sliced-ab", "join-20260821T041841-v030-prewarm",
    "capped-20260821T041841-v030-prewarm", "capped-cx-20260821T041841-v030-prewarm",
    "concurrent-20260821T041841-v030-prewarm", "concurrent-cx-20260821T041841-v030-prewarm",
)

# Numbers that are inputs or deliberate references to quarantined records, not
# measurements of this session. Each needs a reason; an empty reason is a bug.
WAIVED = {
    ("2", "GB"): "S5 container cap -- a configured input",
    ("3", "GB"): "S5 container cap -- a configured input",
    ("4", "GB"): "S5 container cap -- a configured input",
    ("6", "GB"): "S5 container cap -- a configured input",
    ("8", "GB"): "S5/S6 budget -- a configured input",
    ("19.2", "s"): "the 5-slice floor, quarantined in void-5slice-floors/ and named there as void",
}


def _round_forms(x, unit, out, src=""):
    for d in (0, 1, 2, 3):
        out.setdefault("%s|%s" % (("%%.%df" % d) % x, unit), src or True)
        out.setdefault("%s|%s" % ("{:,.0f}".format(x), unit), src or True)


def derive_universe(sessions):
    """Every quantity the documents may legitimately print, as printed strings."""
    u = {}
    for name in sessions:
        d = HERE / "results" / name
        if not d.is_dir():
            continue
        arms = defaultdict(list)
        for f in sorted(d.glob("*.json")):
            if f.name.startswith("warmup"):
                continue
            try:
                r = json.loads(f.read_text())
            except Exception:
                continue
            if not isinstance(r, dict):
                continue
            # S6: one record per concurrency level
            if "total_wall_s" in r:
                _round_forms(r["total_wall_s"], "s", u)
                if r.get("cap_mb"):
                    _round_forms(r["cap_mb"], "MB", u)
                    _round_forms(r["cap_mb"] / 1024.0, "GB", u)
            # S5: one record per (engine, mode, cap), payload-nested
            pay = r.get("payload") if isinstance(r.get("payload"), dict) else None
            if pay and "wall_s" in pay:
                _round_forms(pay["wall_s"], "s", u)
                for k, unit in (("cpu_s", "s"), ("peak_rss_mb", "MB"),
                                ("peak_footprint_mb", "MB")):
                    if pay.get(k) is not None:
                        _round_forms(pay[k], unit, u)
            if "wall_s" in r:
                arms[(r.get("scenario"), r.get("stack"), r.get("variant") or "")].append(r)

        stats = {}
        for k, rs in arms.items():
            w = med(rs, "wall_s")
            c = med(rs, "cpu_s")
            m = med(rs, "peak_footprint_mb")
            sc = [r["server_cpu"] for r in rs if isinstance(r.get("server_cpu"), dict)]
            ev = [x["engine_cpu_s"] for x in sc if x.get("engine_cpu_s") is not None]
            sv = [x["elasticsearch_cpu_s"] for x in sc
                  if x.get("elasticsearch_cpu_s") is not None]
            ecpu = statistics.median(ev) if ev else None
            escpu = statistics.median(sv) if sv else None
            stats[k] = (w, c, m, ecpu, escpu)
            _round_forms(w, "s", u)
            _round_forms(w * 1000.0, "ms", u)
            # min-max spreads: tables print "7.87-7.90 s (0.4%)" per arm
            walls = sorted(r["wall_s"] for r in rs)
            _round_forms(walls[0], "s", u)
            _round_forms(walls[-1], "s", u)
            if w:
                _round_forms((walls[-1] - walls[0]) / w * 100.0, "%", u)
            for v, unit in ((c, "s"), (m, "MB")):
                if v is not None:
                    _round_forms(v, unit, u)
            cs = med(rs, "connect_s")
            if cs is not None:
                _round_forms(cs * 1000.0, "ms", u)
            for key in ("engine_cpu_s", "elasticsearch_cpu_s", "total_cpu_s"):
                vals = [r["server_cpu"][key] for r in rs
                        if isinstance(r.get("server_cpu"), dict)
                        and r["server_cpu"].get(key) is not None]
                if vals:
                    # Server CPU is recorded on EVERY run but published at one
                    # decimal; emitting 3-decimal forms of idle-noise medians
                    # (0.056 s) made small stale values unfalsifiable.
                    v = statistics.median(vals)
                    u.setdefault("%.0f|s" % v, "servercpu")
                    u.setdefault("%.1f|s" % v, "servercpu")
            b = es_bytes(rs)
            if b:
                for div, unit in ((1e9, "GB"), (1e6, "MB"), (1e3, "KB")):
                    _round_forms(b / div, unit, u)
                # per-row bytes: prose prints "253 bytes per row"
                rows = med(rs, "rows")
                if rows:
                    _round_forms(b / rows, "bytes", u)
            # The engine's own network legs (S1 publishes the downstream leg):
            # summed stack tx/rx, plus the derived internal share (rx minus the
            # ES wire) and the derived client leg (tx minus that share).
            tx = statistics.median([r["net"]["tx_bytes"] for r in rs
                                    if isinstance(r.get("net"), dict)
                                    and r["net"].get("tx_bytes") is not None] or [0])
            rx = statistics.median([r["net"]["rx_bytes"] for r in rs
                                    if isinstance(r.get("net"), dict)
                                    and r["net"].get("rx_bytes") is not None] or [0])
            for v in (tx, rx):
                if v:
                    for div, unit in ((1e9, "GB"), (1e6, "MB")):
                        _round_forms(v / div, unit, u)
            if tx and rx and b and rx > b:
                internal = rx - b
                for v in (internal, tx - internal):
                    if v > 0:
                        for div, unit in ((1e9, "GB"), (1e6, "MB")):
                            _round_forms(v / div, unit, u)
            # Since this session the downstream leg is measured PER CONTAINER
            # rather than inferred, so the coordinator's own egress -- the bytes
            # that actually reach the client, and the number the prose quotes --
            # has to be part of the universe on every route.
            for svc_key in ("net_per_service",):
                svcs = [r[svc_key] for r in rs if isinstance(r.get(svc_key), dict)]
                if not svcs:
                    continue
                names = {n for d_ in svcs for n in d_}
                for n in names:
                    vals = [d_[n].get("tx_bytes") for d_ in svcs
                            if isinstance(d_.get(n), dict)
                            and d_[n].get("tx_bytes") is not None]
                    if vals:
                        v = statistics.median(vals)
                        for div, unit in ((1e9, "GB"), (1e6, "MB")):
                            _round_forms(v / div, unit, u)

        # The four-layer connect probe is its own record, quoted in section 4
        # and the appendix -- derive its layer medians so both must agree with it.
        probe = d / "connect-probe.json"
        if probe.exists():
            try:
                layers = json.loads(probe.read_text()).get("layers", {})
                meds = []
                for l in layers.values():
                    samp = sorted(l.get("samples_ms", []))
                    if samp:
                        meds.append(statistics.median(samp))
                        _round_forms(meds[-1], "ms", u)
                for x in meds:
                    for y in meds:
                        if x != y:
                            _round_forms(abs(y - x), "ms", u)
            except Exception:
                pass

        u.setdefault("__stats__", {})
        u["__stats__"][name] = stats

    # Ratios, percentage gaps and marginal costs -- across sessions too, since
    # the topology table compares S1 in one session with S1 in another. The
    # pairing is CONSTRAINED: the same scenario (across stacks, variants and
    # topologies), the same stack+variant (J0 vs J2 marginal cost), or a floor
    # (es-raw exists only to be compared against the engines). An unrestricted
    # cross product makes small values unfalsifiable -- a 0.056 s falsification
    # probe PASSED under one, which is disqualifying.
    allstats = [(name, k) + v
                for name, stats in u.get("__stats__", {}).items()
                for k, v in stats.items()]
    for _, ka, wa, ca, ma, ea, sa in allstats:
        for _, kb, wb, cb, mb, eb, sb in allstats:
            if ka == kb:
                continue
            # Floors are only ever compared against the 10M extraction (S1) --
            # "10.6x our client CPU on S1", "1.58x faster". A floor-vs-anything
            # wildcard re-densifies the universe and small values stop failing.
            floor_vs_s1 = (("es-raw" in (ka[1], kb[1]))
                           and ("S1" in (ka[0], kb[0])))
            comparable = (ka[0] == kb[0]
                          or (ka[1], ka[2]) == (kb[1], kb[2])
                          or floor_vs_s1)
            if not comparable:
                continue
            for x, y in ((wa, wb), (ca, cb), (ma, mb), (ea, eb), (sa, sb)):
                # Server CPU is recorded on every run but PUBLISHED only for
                # the floors, S1 and the paging A/B's S1/S2 rows. Pairing it on
                # other scenarios turns background ES drift (S1r variants
                # differ by 0.5-1.3 s of idle escpu) into universe coverage,
                # and small stale values stop failing. Publish-scope only.
                if (x, y) in ((ea, eb), (sa, sb)):
                    if not (ka[0] in ("S0", "S0p", "S1", "S2")
                            and kb[0] in ("S0", "S0p", "S1", "S2")):
                        continue
                    if x and y and min(x, y) < 1.0:
                        continue
                if x and y:
                    src = "pair %s ~ %s" % (ka, kb)
                    _round_forms(y / x, "×", u, src)
                    _round_forms(abs(y - x) / x * 100.0, "%", u, src)
                    # marginal costs: prose prints "0.74 s faster than its own J0"
                    _round_forms(abs(y - x), "s", u, src)
                    _round_forms(abs(y - x) * 1000.0, "ms", u, src)
    return u


_UNIT_TOKEN = re.compile(r"(\d[\d,]*\.?\d*)\s*(×|%|s\b|ms\b|MB\b|GB\b|KB\b|bytes\b)")


def _prose_blocks(report_html):
    """Every block whose numbers must trace to a run: outcome boxes, EVERY
    table, and every list item. The environment/library tables are excluded --
    they state configuration (versions, allocations), not measurements."""
    out = []
    body = report_html
    env = re.search(r'<h2><span class="n">2</span>.*?<h2><span class="n">3</span>',
                    body, re.S)
    if env:
        body = body.replace(env.group(0), "")
    # Appendix B narrates issues found while benchmarking; its figures quote
    # those issues' own investigations (other sessions, other harnesses), so
    # they cannot -- and should not -- trace to this campaign's records.
    appb = re.search(r'<h2><span class="n">B</span>.*', body, re.S)
    if appb:
        body = body.replace(appb.group(0), "")
    for i, mm in enumerate(re.finditer(r"<table>(.*?)</table>", body, re.S)):
        out.append(("table %d" % (i + 1), mm.group(1)))
    for i, mm in enumerate(re.finditer(r'<div class="outcome">(.*?)</div>',
                                       body, re.S)):
        out.append(("outcome box %d" % (i + 1), mm.group(1)))
    for i, mm in enumerate(re.finditer(r"<li>(.*?)</li>", body, re.S)):
        out.append(("list item %d" % (i + 1), mm.group(1)))
    return out


def verify_report_prose(report_html):
    u = derive_universe(PROSE_SESSIONS)
    blocks = _prose_blocks(report_html)
    print("\n=== report prose: every figure in the glance table and the "
          "Outcome boxes, traced to a run ===")
    if not blocks:
        print("  FAIL could not locate the glance table or any Outcome box "
              "(the anchors moved -- fix them, do not ignore this)")
        return False
    bad, seen = [], 0
    for name, raw in blocks:
        text = re.sub(r"<[^>]+>", " ", raw)
        for ent, ch in (("&nbsp;", " "), ("&amp;", "&"), ("&times;", "×"),
                        ("&minus;", "-"), ("&#8212;", "—")):
            text = text.replace(ent, ch)
        for val, unit in _UNIT_TOKEN.findall(text):
            seen += 1
            if "%s|%s" % (val, unit) in u:
                continue
            if (val, unit) in WAIVED:
                continue
            bad.append((name, val, unit))
    print("  %d/%d figures reproduce from a session record "
          "(%d waived kinds)" % (seen - len(bad), seen, len(WAIVED)))
    for name, val, unit in bad:
        print("  FAIL %s: %s %s matches no run in %s"
              % (name, val, unit, ", ".join(PROSE_SESSIONS[:2]) + ", …"))
    return not bad



def verify_derived_prose(results_md, report_html):
    """Every derived quantity against every sentence that prints it.

    The cell layer proves a cell is live. This proves the SENTENCE is: each
    multiple, percentage and sum is recomputed from the artifacts by derive.py and
    matched where the document prints it, so a re-measure fails at the sentence
    rather than passing on a token collision. See runners/derived_sites.py.
    """
    import derive
    from derived_sites import DERIVED_SITES

    d, _ = derive.build()
    docs = {"R": ("RESULTS.md", results_md), "H": ("report.html", report_html)}
    print("\n=== derived quantities recomputed, and matched to the sentence "
          "that prints them ===")
    bad, total = [], 0
    for name, sites in DERIVED_SITES.items():
        if name not in d:
            bad.append((name, "-", "no formula in derive.py", "-"))
            continue
        val, prec = d[name]["value"], d[name]["precision"]
        want = round(val, prec)
        tol = 0.5 * (10 ** -prec) + 1e-9
        for doc, pattern in sites:
            total += 1
            fname, text = docs[doc]
            m = re.search(pattern, text)
            if not m:
                bad.append((name, fname, "anchor not found", "%.*f" % (prec, want)))
                continue
            got = next((g for g in m.groups() if g), None)
            if got is None or abs(float(got) - want) > tol:
                bad.append((name, fname, got, "%.*f" % (prec, want)))
    print("  %d/%d prose sites carry the value their formula gives"
          % (total - len(bad), total))
    for name, fname, got, want in bad:
        print("  FAIL %s in %s: document says %s, formula gives %s"
              % (name, fname, got, want))
    return not bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", default="20260821T041841-v030-prewarm")
    a = ap.parse_args()

    results_md = RESULTS.read_text()
    report_html = REPORT.read_text()

    chk = Check()
    verify_extraction(a.session, results_md, chk)
    verify_joins(a.session, results_md, chk)
    verify_paging_ab(a.session, results_md, chk)
    verify_topology(a.session, results_md, chk)
    ok_claims = chk.report("published figures re-derived from artifacts")
    ok_cells = compare_report_cells(results_md, report_html)
    ok_image = verify_image_provenance(a.session)
    ok_state = verify_session_state(a.session, results_md, report_html)
    ok_prose = verify_report_prose(report_html)
    ok_derived = verify_derived_prose(results_md, report_html)

    if ok_claims and ok_cells and ok_image and ok_state and ok_prose and ok_derived:
        print("\nALL CHECKS PASS")
        return 0
    print("\nFAILED — a published figure is not supported by the artifacts, the "
          "report and RESULTS disagree, or a document names the wrong build.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

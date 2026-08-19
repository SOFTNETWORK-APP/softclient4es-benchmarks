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
    """The single-shard matrix: wall, client CPU, peak memory and ES egress."""
    runs = load_runs(session)

    def cell(sc, stack, variant=""):
        return runs.get((sc, stack, variant), [])

    # S1 — the headline, checked on all four metrics it is quoted with.
    f, t = cell("S1", "flight"), cell("S1", "trino")
    chk.add("S1 flight wall", med(f, "wall_s"),
            doc_number(results_md, r"\| Wall median \| \*\*([\d.]+) s\*\* \(spread [\d.]+%\) \| [\d.]+ s"))
    chk.add("S1 trino wall", med(t, "wall_s"),
            doc_number(results_md, r"\| Wall median \| \*\*[\d.]+ s\*\* \(spread [\d.]+%\) \| ([\d.]+) s"))
    chk.add("S1 flight cpu", med(f, "cpu_s"),
            doc_number(results_md, r"\| Client CPU \| \*\*([\d.]+) s\*\* \| [\d.]+ s \| 4\.7× less"))
    chk.add("S1 trino cpu", med(t, "cpu_s"),
            doc_number(results_md, r"\| Client CPU \| \*\*[\d.]+ s\*\* \| ([\d.]+) s \| 4\.7× less"))
    chk.add("S1 flight mem", med(f, "peak_footprint_mb"),
            doc_number(results_md, r"\| Peak client memory \| \*\*([\d,]+) MB\*\* \| [\d,]+ MB \| 4\.9× less"), tol=1.0)
    chk.add("S1 trino mem", med(t, "peak_footprint_mb"),
            doc_number(results_md, r"\| Peak client memory \| \*\*[\d,]+ MB\*\* \| ([\d,]+) MB \| 4\.9× less"), tol=1.0)
    d_ratio = med(t, "wall_s") / med(f, "wall_s")
    chk.add("S1 ratio", d_ratio, doc_number(results_md, r"\*\*([\d.]+)× faster\*\* \|"), tol=0.005)

    # S1m — the cell the release moved. Bytes/row is the mechanism, so it is checked too.
    f, t = cell("S1m", "flight"), cell("S1m", "trino")
    chk.add("S1m flight wall", med(f, "wall_s"),
            doc_number(results_md, r"SoftClient4ES, Arrow Flight SQL\*\* \| \*\*([\d.]+) s\*\*"))
    chk.add("S1m trino wall", med(t, "wall_s"),
            doc_number(results_md, r"\| Trino, stock client \| ([\d.]+) s"))
    chk.add("S1m flight bytes/row", es_bytes(f) / 1_000_000,
            doc_number(results_md, r"\*\*([\d]+) bytes per\nrow off the cluster"), tol=1.0)

    # S3 — push-down, the largest claim in the document. RESULTS publishes it ROUNDED
    # ("24 KB", "1,394 MB"), so the check is that the rounding is honest, not that the
    # document carries raw bytes: derived/1e3 and /1e6 against the printed figures, each
    # within half a printed unit. The factor is checked too, since that is what is quoted.
    f, t = cell("S3", "flight"), cell("S3", "trino")
    ours, theirs = es_bytes(f), es_bytes(t)
    chk.add("S3 flight ES wire (KB)", ours / 1_000,
            doc_number(results_md, r"\| ES wire \| \*\*([\d]+) KB\*\*"), tol=0.5)
    chk.add("S3 trino ES wire (MB)", theirs / 1_000_000,
            doc_number(results_md, r"\| ES wire \| \*\*[\d]+ KB\*\* \| ([\d,]+) MB"), tol=0.5)
    # The factor is quoted to two significant figures ("a factor of 57,000"), so the test
    # is that the rounding is honest: the derived 57,294 must round to what is printed.
    chk.add("S3 push-down factor", theirs / ours,
            doc_number(results_md, r"a factor of ([\d,]+)"), tol=500)

    # Floors — the pair the report's glance table got wrong.
    chk.add("S0 wall", med(cell("S0", "es-raw"), "wall_s"),
            doc_number(results_md, r"one process, one scroll \| ([\d.]+) s"))
    chk.add("S0p wall", med(cell("S0p", "es-raw"), "wall_s"),
            doc_number(results_md, r"5 slices, 5 processes \| \*\*([\d.]+) s\*\*"))

    # Drift: published as a percentage, so re-derive the percentage.
    base, drift = cell("S1", "flight"), cell("S1", "flight", "drift")
    if base and drift:
        pct = (med(drift, "wall_s") - med(base, "wall_s")) / med(base, "wall_s") * 100
        chk.add("drift %", pct, doc_number(results_md, r"\*\*\+([\d.]+)% against a run-to-run"), tol=0.05)
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
        published = doc_number(results_md, rf"\| {sc} \|[^|]*\|[^|]*\|[^|]*\| \*\*\w+ ([\d]+) / 25\*\*")
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
     r"\| Client CPU \| \*\*([\d.]+) s\*\* \| [\d.]+ s \| 4\.7× less",
     r'Client CPU</td><td class="num win">([\d.]+) s</td><td class="num">[\d.]+ s</td><td class="num">4\.7× less'),
    ("S1 client CPU (Trino)",
     r"\| Client CPU \| \*\*[\d.]+ s\*\* \| ([\d.]+) s \| 4\.7× less",
     r'Client CPU</td><td class="num win">[\d.]+ s</td><td class="num">([\d.]+) s</td><td class="num">4\.7× less'),
    ("S1 wall (ours)",
     r"\| Wall median \| \*\*([\d.]+) s\*\* \(spread",
     r'Wall median</td><td class="num win">([\d.]+) s <span class="small">\(spread'),
    ("S2 wall (ours)",
     r"\| Wall median \| \*\*([\d.]+) s\*\* \| [\d.]+ s \| \*\*1\.65× faster",
     r'Wall median</td><td class="num win">([\d.]+) s</td><td class="num">[\d.]+ s</td><td class="num"><b>1\.65× faster'),
    ("S1m ours",
     r"SoftClient4ES, Arrow Flight SQL\*\* \| \*\*([\d.]+) s\*\*",
     r'SoftClient4ES, Arrow Flight SQL</td><td class="num win">([\d.]+) s'),
    ("S1m Trino",
     r"\| Trino, stock client \| ([\d.]+) s",
     r'Trino, stock client</td><td class="num">([\d.]+) s'),
    ("J0 ours",
     r"\| \*\*J0\*\* plain join \| \*\*([\d.]+) s\*\*",
     r'J0 — plain join</td><td class="num win">([\d.]+) s'),
    ("5-shard S1 ours",
     r"\| S1 wall \| [\d.]+ s \| \*\*([\d.]+) s\*\*",
     r'S1 wall</td><td class="num">[\d.]+ s</td><td class="num win">([\d.]+) s'),
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
    ("results/README.md", "measured **sidecar 0.2.5**",
     "the RETIRED sessions really were measured on it"),
    ("results/README.md", "on 0.2.5 the Flight schema probe",
     "explains why a retired figure legitimately differs"),
]
VERSION_TOKEN = re.compile(r"\b0\.2\.\d+(?:\.\d+)?(?:-SNAPSHOT)?\b")
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
        if rel.startswith((".venv/", ".git/", "node_modules/")):
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", default="20260818T230041-v0251")
    a = ap.parse_args()

    results_md = RESULTS.read_text()
    report_html = REPORT.read_text()

    chk = Check()
    verify_extraction(a.session, results_md, chk)
    verify_joins(a.session, results_md, chk)
    ok_claims = chk.report("published figures re-derived from artifacts")
    ok_cells = compare_report_cells(results_md, report_html)
    ok_image = verify_image_provenance(a.session)

    if ok_claims and ok_cells and ok_image:
        print("\nALL CHECKS PASS")
        return 0
    print("\nFAILED — a published figure is not supported by the artifacts, the "
          "report and RESULTS disagree, or a document names the wrong build.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

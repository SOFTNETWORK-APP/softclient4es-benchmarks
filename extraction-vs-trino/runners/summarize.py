#!/usr/bin/env python3.12
"""results/<session>/*.json -> medians + spread -> markdown for RESULTS.md.

One session = one orchestrate.py invocation (its timestamped subdir). Defaults to
the NEWEST session so two benchmark sessions can never be mixed into one table.

This is also the cross-stack correctness gate: every scenario must have all the
stacks it requires, and they must have returned identical row counts. A scenario
where one stack produced no files at all FAILS here rather than printing one-sided
medians that look complete.

    python runners/summarize.py                      # newest session
    python runners/summarize.py results/2026...      # a specific session
"""
import collections
import json
import pathlib
import statistics
import sys

from scenarios import REQUIRED_STACKS

RESULTS = pathlib.Path(__file__).resolve().parent.parent / "results"
MIN_RUNS = 5
# Above this relative spread a median stops being a fair summary of the runs.
VARIANCE_WARN = 0.25
METRICS = [("wall_s", "wall-clock median (s)", 2),
           ("cpu_s", "client CPU median (s)", 2)]
# The comparison each scenario's table is built around.
BASELINE = {"S1": ("flight", "trino"), "S1m": ("flight", "trino"),
            "S2": ("flight", "trino"),
            "S3": ("flight", "trino"), "S4": ("flight", "trino")}
# ES|QL can only enter S1m/S3/S4 -- everywhere else it is absent BY PRODUCT LIMIT
# (1,000,000-row ceiling), which RESULTS states rather than leaving as a blank.
# The table below does not filter on that list: it prints what was measured, so a
# scenario ES|QL should not have been able to run would be visible rather than
# silently dropped.


def median_of(runs, key):
    return statistics.median([r[key] for r in runs])


def median_mem(runs):
    """Median client memory, preferring the footprint metric (arrow#150).

    `peak_footprint_mb` is pressure-immune (macOS kernel lifetime footprint);
    `peak_rss_mb` is the compression-eroded legacy metric kept for old
    sessions. Never mix the two in one median: if any run in the group lacks
    a usable footprint, the whole group reports the legacy metric, labelled.
    Returns (value, label).
    """
    fp = [r.get("peak_footprint_mb") for r in runs]
    if all(v is not None for v in fp):
        return statistics.median(fp), "footprint"
    return median_of(runs, "peak_rss_mb"), "rss(legacy)"


def main():
    if len(sys.argv) > 1:
        session = pathlib.Path(sys.argv[1])
    else:
        dirs = sorted(d for d in RESULTS.iterdir() if d.is_dir()) if RESULTS.is_dir() else []
        if not dirs:
            sys.exit("no sessions under results/ -- run orchestrate.py first")
        session = dirs[-1]
    if not session.is_dir():
        sys.exit(f"no such session dir: {session}")

    groups = collections.defaultdict(list)
    # `*-run<N>.json`, NOT `*run*.json`. The loose glob matched
    # esql-truncation-probe.json -- "t-run-cation" contains "run" -- and ingested a
    # PROBE as a measured run. It crashed on the missing `rows` key only because the
    # probe happens to call its count `rows_returned`; with a matching name it would
    # have merged silently into a scenario's medians instead. Measured 2026-08-18.
    for f in sorted(session.glob("*-run[0-9]*.json")):
        r = json.loads(f.read_text())
        # variant is part of the key: a 4-shard S1b run must never merge into S1's
        # headline medians just because it shares a scenario name.
        groups[(r["scenario"], r["stack"], r.get("variant") or "")].append(r)
    if not groups:
        sys.exit(f"no run files in {session}")

    # ---- correctness gates -------------------------------------------------
    by_scenario = collections.defaultdict(dict)
    warnings = []
    for (scenario, stack, variant), runs in sorted(groups.items()):
        counts = {r["rows"] for r in runs}
        assert len(counts) == 1, (
            f"{scenario}/{stack}/{variant or 'base'}: inconsistent row counts {counts}")
        by_scenario[(scenario, variant)][stack] = counts.pop()
        if len(runs) < MIN_RUNS:
            warnings.append(f"{scenario}/{stack}/{variant or 'base'} has {len(runs)} "
                            f"runs, below the required {MIN_RUNS} -- not publishable")
        wall = sorted(r["wall_s"] for r in runs)
        med = statistics.median(wall)
        if med and (wall[-1] - wall[0]) / med > VARIANCE_WARN:
            warnings.append(
                f"{scenario}/{stack}/{variant or 'base'} spread is "
                f"{(wall[-1] - wall[0]) / med:.0%} of the median "
                f"({wall[0]:.2f}-{wall[-1]:.2f}s) -- the median understates the noise")
    for (scenario, variant), stacks in sorted(by_scenario.items()):
        missing = REQUIRED_STACKS.get(scenario, set()) - set(stacks)
        if variant:
            # Sensitivity arms are single-stack BY CONSTRUCTION -- a Trino-only
            # client route, an ES|QL wire route, a Flight dial, a drift block --
            # so requiring the full stack set here would make every variant
            # unsummarizable. Report the absence; gate only the base variant,
            # which is where the published headline lives.
            if missing:
                warnings.append(f"{scenario}/{variant}: variant arm covers only "
                                f"{sorted(stacks)} (absent: {sorted(missing)})")
        else:
            assert not missing, (
                f"{scenario}/base: missing stack(s) {sorted(missing)} -- "
                "refusing to print one-sided medians")
        # Row-count equality is a gate everywhere: two stacks that answered
        # differently are not comparable, variant or not.
        assert len(set(stacks.values())) == 1, (
            f"{scenario}/{variant or 'base'}: row-count mismatch across stacks {stacks}")

    print(f"<!-- session: {session.name} -->")
    for w in warnings:
        print(f"<!-- WARNING: {w} -->")

    # ---- raw table ---------------------------------------------------------
    print("\n### Raw medians\n")
    print("| Scenario | Variant | Stack | Runs | Wall median (s) | Wall min-max (s) "
          "| CPU median (s) | Peak client memory median (MB) | Rows |")
    print("|---|---|---|---|---|---|---|---|---|")
    for key in sorted(groups):
        scenario, stack, variant = key
        runs = groups[key]
        wall = sorted(r["wall_s"] for r in runs)
        mem, mem_label = median_mem(runs)
        print(f"| {scenario} | {variant or '-'} | {stack} | {len(runs)} "
              f"| {median_of(runs, 'wall_s'):,.2f} "
              f"| {wall[0]:,.2f}-{wall[-1]:,.2f} "
              f"| {median_of(runs, 'cpu_s'):,.2f} "
              f"| {mem:,.0f} ({mem_label}) "
              f"| {runs[0]['rows']:,} |")

    # ---- the floors --------------------------------------------------------
    s0 = groups.get(("S0", "es-raw", ""))
    s0p = groups.get(("S0p", "es-raw", ""))
    if s0 or s0p:
        print(f"\n### S0 / S0p -- the floors both stacks pay\n")
        print("| Floor | Wall median (s) | Client CPU median (s) | Peak client memory (MB) | Runs |")
        print("|---|---|---|---|---|")
        for label, runs in (("S0 single-process scroll", s0),
                            ("S0p sliced scroll (parallel)", s0p)):
            if not runs:
                continue
            mem, mem_label = median_mem(runs)
            slices = runs[0].get("slices", 1)
            print(f"| {label}"
                  + (f", {slices} slices" if slices and slices > 1 else "")
                  + f" | {median_of(runs, 'wall_s'):,.2f} "
                    f"| {median_of(runs, 'cpu_s'):,.2f} "
                    f"| {mem:,.0f} ({mem_label}) | {len(runs)} |")
        if s0 and s0p:
            # The sliced floor buys wall clock by spending CPU across processes.
            # Both halves are published; a floor quoted on wall alone would be the
            # same selective reading this benchmark refuses elsewhere.
            print(f"\n<!-- S0p is {median_of(s0, 'wall_s') / median_of(s0p, 'wall_s'):,.2f}x "
                  f"S0's wall clock for "
                  f"{median_of(s0p, 'cpu_s') / median_of(s0, 'cpu_s'):,.2f}x its client CPU, "
                  f"across {s0p[0].get('slices')} processes. -->")

    # ---- per-scenario comparison blocks, RESULTS section 2 shape -----------
    for (scenario, variant) in sorted(by_scenario):
        pair = BASELINE.get(scenario)
        if not pair:
            continue
        left, right = pair
        l_runs = groups.get((scenario, left, variant))
        r_runs = groups.get((scenario, right, variant))
        if not (l_runs and r_runs):
            continue
        label = f"{scenario}" + (f" ({variant})" if variant else "")
        print(f"\n### {label}\n")
        print("| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | "
              "Ratio (Trino / SC4ES) |")
        print("|---|---|---|---|---|")
        for key, name, places in METRICS:
            lv, rv = median_of(l_runs, key), median_of(r_runs, key)
            ratio = f"{rv / lv:,.1f}x" if lv else "n/a"
            print(f"| {scenario} | {name} | {lv:,.{places}f} | {rv:,.{places}f} "
                  f"| {ratio} |")
        lm, l_label = median_mem(l_runs)
        rm, r_label = median_mem(r_runs)
        # A ratio across different metrics (one side footprint, one side
        # legacy RSS) would compare incomparable quantities -- refuse it.
        if l_label != r_label:
            mem_ratio = "n/a (mixed metrics)"
        else:
            mem_ratio = f"{rm / lm:,.1f}x" if lm else "n/a"
        print(f"| {scenario} | peak client memory median (MB) | {lm:,.0f} ({l_label}) "
              f"| {rm:,.0f} ({r_label}) | {mem_ratio} |")
        lw = sorted(r["wall_s"] for r in l_runs)
        rw = sorted(r["wall_s"] for r in r_runs)
        print(f"| {scenario} | wall-clock min-max (s) | {lw[0]:,.2f}-{lw[-1]:,.2f} "
              f"| {rw[0]:,.2f}-{rw[-1]:,.2f} | - |")
        print(f"| {scenario} | connect-only median (s) | "
              f"{median_of(l_runs, 'connect_s'):,.3f} | "
              f"{median_of(r_runs, 'connect_s'):,.3f} | - |")
        lnet = [r["net"]["tx_bytes"] for r in l_runs if r.get("net")]
        rnet = [r["net"]["tx_bytes"] for r in r_runs if r.get("net")]
        if lnet and rnet:
            lb, rb = statistics.median(lnet), statistics.median(rnet)
            ratio = f"{rb / lb:,.1f}x" if lb else "n/a"
            print(f"| {scenario} | bytes to client, median (MB) | {lb / 1e6:,.0f} "
                  f"| {rb / 1e6:,.0f} | {ratio} |")
        print(f"| {scenario} | rows returned (gate: identical) "
              f"| {l_runs[0]['rows']:,} | {r_runs[0]['rows']:,} | - |")

    # ---- S1 vs S1r: wire versus representation -----------------------------
    s1 = groups.get(("S1", "flight", ""))
    s1r = groups.get(("S1r", "flight", ""))
    s1t = groups.get(("S1", "trino", ""))
    if s1 and s1r and s1t:
        a, b, c = (median_of(s1, "wall_s"), median_of(s1r, "wall_s"),
                   median_of(s1t, "wall_s"))
        print("\n### S1 / S1r / Trino -- decomposing the gap\n")
        print(f"| Path | Wall median (s) |")
        print(f"|---|---|")
        print(f"| Arrow wire, Arrow table (S1) | {a:,.2f} |")
        print(f"| Arrow wire, Python rows (S1r) | {b:,.2f} |")
        print(f"| Trino wire, Python rows (S1) | {c:,.2f} |")
        print(f"\n<!-- Of the {c - a:,.2f}s gap, {c - b:,.2f}s survives when both "
              f"sides build Python rows (attributable to the wire and the server "
              f"side), and {b - a:,.2f}s is the cost of building row objects at all "
              f"(attributable to the columnar client representation). -->")

    # ---- ES|QL, the third stack --------------------------------------------
    # Published where it can run, absent where the product forbids it -- and the
    # absence is stated, because a blank cell reads as "not tested" when it is in
    # fact a 1,000,000-row ceiling.
    # Driven by the RECORDED `route` field, not by a variant string. A variant is a
    # label two layers can both write; a run that names its own wire format cannot
    # be dropped from this table by a naming change. (It was: a doubled `arrow-arrow`
    # tag silently omitted every Arrow run from the first Phase-2 session.)
    esql_rows_out = []
    for (scenario, stack, variant), runs in sorted(groups.items()):
        if stack != "esql":
            continue
        mem, mem_label = median_mem(runs)
        # The variant is its own column. Labelling by scenario + wire alone would
        # print a drift block (or any sensitivity arm) as a second, identical-looking
        # row with a different median -- a published table cannot have two rows that
        # claim to be the same measurement.
        esql_rows_out.append(
            (scenario, runs[0].get("route") or "json", variant or "base",
             median_of(runs, "wall_s"),
             median_of(runs, "cpu_s"), mem, mem_label, runs[0]["rows"], len(runs)))
    if esql_rows_out:
        print("\n### ES|QL -- Elasticsearch's own query language\n")
        print("| Scenario | Wire | Arm | Wall median (s) | Client CPU median (s) "
              "| Peak client memory (MB) | Rows | Runs |")
        print("|---|---|---|---|---|---|---|---|")
        for sc, wire, arm, wall, cpu, mem, lbl, rows, n in esql_rows_out:
            print(f"| {sc} | {wire} | {arm} | {wall:,.3f} | {cpu:,.3f} "
                  f"| {mem:,.0f} ({lbl}) | {rows:,} | {n} |")
        cap = next((r.get("esql_result_truncation_max_size")
                    for k, rs in groups.items() if k[1] == "esql" for r in rs), None)
        print(f"\n<!-- ES|QL ran with esql.query.result_truncation_max_size={cap}; "
              "its product maximum is 1,000,000, which is why S1/S2/S5/S6 have no "
              "ES|QL column. -->")

    # ---- drift control: the same block, re-run at the end of the session ----
    for (scenario, stack, variant), runs in sorted(groups.items()):
        if not variant.endswith("drift"):
            continue
        base = groups.get((scenario, stack, variant[:-len("-drift")].rstrip("-")))
        if not base:
            continue
        a_med, b_med = median_of(base, "wall_s"), median_of(runs, "wall_s")
        a_sorted = sorted(r["wall_s"] for r in base)
        spread = (a_sorted[-1] - a_sorted[0]) / a_med if a_med else 0
        drift = (b_med - a_med) / a_med if a_med else 0
        verdict = ("inside" if abs(drift) <= spread else "OUTSIDE")
        print(f"\n### Drift control -- {scenario}/{stack}\n")
        print(f"| Block | Wall median (s) | Runs |")
        print(f"|---|---|---|")
        print(f"| first (A) | {a_med:,.2f} | {len(base)} |")
        print(f"| repeat at session end (A') | {b_med:,.2f} | {len(runs)} |")
        print(f"\nSession drift {drift:+.1%}, {verdict} A's own run-to-run spread "
              f"of {spread:.1%}." + ("" if verdict == "inside" else
              " The blocks are NOT interchangeable: this session's cross-stack "
              "comparison carries a drift larger than its noise and must say so."))

    # ---- host load: bounding the "one machine" confound ---------------------
    loads = [r["host_load_after"]["loadavg_1m"]
             for rs in groups.values() for r in rs
             if isinstance(r.get("host_load_after"), dict)
             and r["host_load_after"].get("loadavg_1m") is not None]
    cpus = next((r["host_load_after"].get("logical_cpus")
                 for rs in groups.values() for r in rs
                 if isinstance(r.get("host_load_after"), dict)), None)
    if loads and cpus:
        print("\n### Host load during the session\n")
        print(f"1-minute load average sampled after every run: median "
              f"{statistics.median(loads):,.1f}, max {max(loads):,.1f}, against "
              f"{cpus} logical cores. The measured client runs on the host, so this "
              f"is what bounds host contention as an explanation of the wall-clock "
              f"figures: at the observed maximum, "
              f"{max(0.0, cpus - max(loads)):,.1f} cores were not committed.")

    # ---- the disclosed Flight metadata-column leak, METHODOLOGY section 6 --
    if s1:
        cols = s1[0].get("col_names")
        print(f"\n<!-- METHODOLOGY section 6: S1 Flight returned {s1[0].get('cols')} "
              f"columns ({s1[0].get('extra_cols')} beyond the 8 selected). -->")
        if cols:
            print(f"<!-- observed column list: {', '.join(cols)} -->")


if __name__ == "__main__":
    main()

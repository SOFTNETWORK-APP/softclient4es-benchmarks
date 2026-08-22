#!/usr/bin/env python3.12
"""Derived quantities bound to the exact sentences that print them.

WHY THIS FILE EXISTS. `verify_claims.py`'s cell checks match a LABEL to its value,
so a stale cell cannot survive a re-measure. They say nothing about what the prose
DOES with those cells -- the multiples, percentages, sums and per-core rates that
sentences carry. Those were checked by scanning for tokens, and a scanner cannot
tell a stale "2.83x" from a live one: any number that also appears somewhere
legitimate passes. That residue let a whole join table survive a re-measure with
medians sitting OUTSIDE their own published min-max intervals -- arithmetically
impossible, invisible to a scanner, and caught only by a human reader.

So `derive.py` owns the FORMULA and this file binds it to the sentence. A re-measure
moves the cell, the formula moves with it, and the check fails AT THE SENTENCE,
naming the value it should now carry. Adding a derived number to the prose means
adding it here, or it is unverified.

    name -> [(document, regex with one capture group), ...]      R = RESULTS.md
                                                                 H = report/report.html
"""

DERIVED_SITES = {
    "s1.mult.documented": [
        ("R", r"a factor of 1\.23 to ([\d.]+)"),
        ("R", r"\| Wall median \| \*\*11\.91 s\*\*[^|]*\|[^|]*\| \*\*([\d.]+)× faster\*\* \|"),
        ("H", r"<div class=\"v\">1\.23–([\d.]+)×"),
        ("H", r"<b>1\.23× faster</b> than Trino's fastest client, <b>([\d.]+)×</b>"),
    ],
    "s1.mult.cx": [
        ("R", r"a factor of ([\d.]+) to 3\.75"),
        ("R", r"\*\*([\d.]+)× against the fastest of"),
        ("H", r"lead against every Trino client \(([\d.]+)× vs the fastest\)"),
    ],
    "s1.cpu.mult.documented": [
        ("R", r"\| Client CPU \| \*\*2\.83 s\*\* \| 24\.05 s \| ([\d.]+)× less \|"),
    ],
    "s1.cpu.mult.cx": [
        ("R", r"while still spending ([\d.]+)×\s*\n?\s*our client CPU"),
        ("H", r"while still spending ([\d.]+)× our client CPU"),
    ],
    "s1.mem.mult.documented": [
        ("R", r"\| Peak client memory \| \*\*921 MB\*\* \| 4,455 MB \| ([\d.]+)× less \|"),
    ],
    "s1.escpu.mult": [
        ("R", r"\| Elasticsearch CPU \| 42\.2 s \| \*\*30\.4 s\*\* \| ([\d.]+)× \*more\* \|"),
    ],

    # --- the system-cost claim, per route: quoting only the slowest was the defect
    "s1.system.ours": [
        ("R", r"total system CPU is \*\*73\.8 s against our\s*\n?\s*([\d.]+) s"),
        ("H", r"total system CPU is <b>73\.8 s against our\s*\n?\s*([\d.]+) s"),
    ],
    "s1.system.trino.documented": [
        ("R", r"comes to \*\*([\d.]+) s against our 71\.0 s\*\*"),
        ("H", r"Elasticsearch\) comes to <b>([\d.]+) s against our 71\.0 s</b>"),
    ],
    "s1.system.trino.cx": [
        ("R", r"total system CPU is \*\*([\d.]+) s against our"),
        ("H", r"total system CPU is <b>([\d.]+) s against our"),
    ],
    "s1.cores.trino.documented": [
        ("R", r"average \*\*about ([\d.]+) of its 6 CPUs\*\*"),
        ("H", r"average <b>about ([\d.]+) of its 6 CPUs</b>"),
    ],
    "s1.cores.trino.cx": [
        ("R", r"the same cluster averages \*\*([\d.]+) of\s*\n?\s*its 6 CPUs\*\*"),
        ("H", r"the same cluster averages <b>([\d.]+) of its 6\s*\n?\s*CPUs</b>"),
    ],
    "s1.cores.ours": [
        ("R", r"against our ([\d.]+) of 4"),
        ("H", r"against our ([\d.]+) of 4"),
    ],
    "s1.escores.ours": [
        ("R", r"which over the run is \*\*([\d.]+) of the cluster's 6 CPUs against"),
        ("H", r"which over the run is <b>([\d.]+) of the cluster's 6 CPUs against"),
    ],
    "s1.escores.trino.documented": [
        ("R", r"of the cluster's 6 CPUs against\s*\n?\s*([\d.]+)\*\*"),
        ("H", r"of the cluster's 6 CPUs against ([\d.]+)</b>"),
    ],
    "s1.escores.trino.cx": [
        ("R", r"\(documented route; ([\d.]+) on connectorx\)"),
        ("H", r"\(documented route; ([\d.]+) on\s*\n?\s*connectorx\)"),
    ],
    "s1.tuned.gain.pct": [
        ("R", r"44\.70 s to \*\*44\.41 s\*\*, \*\*([\d.]+)%\*\*"),
        ("H", r"wall clock from 44\.70 s to <b>44\.41 s</b> — <b>([\d.]+)%</b>"),
    ],
    "s1.wire.ours.bpr": [
        ("R", r"where we read \*\*(\d+)\*\*"),
        ("H", r"off the cluster where we read\s*\n?\s*<b>(\d+) bytes</b>"),
    ],
    "s1.wire.trino.bpr": [
        ("R", r"Trino reads \*\*(\d+) bytes per row\*\*"),
        ("H", r"Trino reads <b>(\d+) bytes per row</b>"),
    ],

    # --- the floor: 1.16x is published, 1.37x is disclosed and NOT used
    "floor.margin.firstblock": [
        ("R", r"we are \*\*([\d.]+)× faster\*\*\. The"),
        ("H", r"we are <b>([\d.]+)×\s*\n?\s*faster</b>, and 1\.16× is the number this report uses"),
    ],
    "floor.margin.settled": [
        ("R", r"the same floor would read ([\d.]+)×"),
        ("H", r"floor would read ([\d.]+)×"),
    ],
    "floor.cpu.mult": [
        ("R", r"\*\*([\d.]+)× our client CPU\*\* \(24\.13 s against 2\.83 s\)"),
        ("H", r"<b>([\d.]+)× our client CPU</b> \(24\.13 s against 2\.83 s\)"),
    ],
    "floor.mem.mult": [
        ("R", r"\*\*([\d.]+)× our client memory\*\* \(3,669 MB against"),
        ("H", r"<b>([\d.]+)× our client memory</b>\s*\n?\s*\(3,669 MB against 921 MB\)"),
    ],

    # --- S1r / S2 were measured settled, so BOTH ends of the position band are checked
    "s1r.pandas.mult.cx": [
        ("R", r"against its fastest route, \*\*([\d.]+)× faster on 1\.9× less memory\*\*"),
        ("H", r"against its fastest route, <b>([\d.]+)× faster on 1\.9× less memory</b>"),
    ],
    "s1r.pandas.mult.cx.firstblock": [
        ("R", r"read \*\*4\.7×\*\* and\s*\n?\s*\*\*([\d.]+)×\*\*"),
        ("H", r"the same comparisons read\s*\n?\s*<b>4\.7×</b> and <b>([\d.]+)×</b>"),
    ],
    "s1r.pandas.mult.documented": [
        ("R", r"The honest statements are therefore \*\*4\.7–([\d.]+)×\*\*"),
        ("H", r"<b>4\.7–([\d.]+)×</b>\s*\n?\s*faster than Trino's documented route on pandas"),
    ],
    "s1r.pandas.mult.documented.firstblock": [
        ("R", r"are therefore \*\*([\d.]+)–5\.6×\*\*"),
        ("H", r"<b>([\d.]+)–5\.6×</b>\s*\n?\s*faster than Trino's documented route on pandas"),
    ],
    "s1r.pandas.mem.mult.cx": [
        ("R", r"fastest route, \*\*1\.6× faster on ([\d.]+)× less memory\*\*"),
        ("H", r"fastest route, <b>1\.6× faster on ([\d.]+)× less memory</b>"),
    ],
    "s1r.dtype.mem.saved": [
        ("R", r"the Arrow-backed one simply saves (\d+) MB"),
        ("H", r"\(9\.95 s against 9\.97 s\) and save\s*\n?\s*(\d+) MB"),
    ],
    "s1r.polars.mult.cx": [
        ("R", r"\*\*1\.2–([\d.]+)×\*\* faster than its\s*\n?\s*fastest"),
        ("H", r"<b>1\.2–([\d.]+)×</b> \(polars\) faster than its fastest route"),
    ],
    "s1r.polars.mult.documented": [
        ("R", r"and \*\*4\.3–([\d.]+)×\*\* faster than Trino's documented routes"),
        ("H", r"<b>4\.3–([\d.]+)×</b> on polars"),
    ],
    "s2.mult": [
        ("R", r"\*\*4\.3× to ([\d.]+)×\*\*, depending on where our cell sits"),
        ("H", r"<b>4\.3× to ([\d.]+)×</b> depending on where our cell sits"),
    ],
    "s2.mult.firstblock": [
        ("R", r"\*\*([\d.]+)× to 5\.1×\*\*, depending on where our cell sits"),
        ("H", r"<b>([\d.]+)× to 5\.1×</b> depending on where our cell sits"),
    ],
    "s2.mem.mult": [
        ("R", r"largest memory advantage in the benchmark\s*\n?\s*\(([\d.]+)×\)"),
        ("H", r"largest memory advantage in the\s*\n?\s*benchmark \(([\d.]+)×\)"),
    ],

    # --- topology
    "topo.ours": [
        ("R", r"goes \*\*([\d.]+)× faster\*\* with six shards"),
        ("H", r"goes <b>([\d.]+)× faster</b> with six shards"),
    ],
    "topo.trino": [
        ("R", r"Trino goes \*\*([\d.]+)× faster\*\* \(55\.11 s"),
        ("H", r"Trino goes <b>([\d.]+)× faster</b> \(55\.11 s"),
    ],
    "topo.gap.oneshard": [
        ("R", r"\*\*widens from ([\d.]+)× to 3\.75×\*\*"),
        ("H", r"widens from <b>([\d.]+)× to 3\.75×</b>"),
    ],

    # --- the paging A/B, and how far its absolutes sit from the matrix
    "ab.mult.s1": [
        ("R", r"the feature is worth \*\*([\d.]+)×\*\*"),
        ("H", r"the feature is\s*\n?\s*worth <b>([\d.]+)×</b>"),
    ],
    "ab.escpu.drop.pct": [
        ("R", r"Elasticsearch CPU \*\*falls (\d+)%\*\*"),
        ("H", r"Elasticsearch CPU <b>falls (\d+)%</b>"),
    ],
    "ab.s1.vs.main.pct": [
        ("R", r"the sliced S1 arm lands \*\*(\d+)% below\*\*"),
        ("H", r"the sliced S1 arm lands <b>(\d+)% below</b>"),
    ],
    "ab.s2.vs.main.pct": [
        ("R", r"the sliced S2 arm lands \*\*(\d+)% above\*\*"),
        ("H", r"while the sliced S2 arm lands <b>(\d+)% above</b>"),
    ],
    "ab.s2.escpu.vs.main.pct": [
        ("R", r"carries (\d+)% more\s*\n?\s*Elasticsearch CPU"),
        ("H", r"carrying (\d+)% more\s*\n?\s*Elasticsearch CPU"),
    ],
    "warmin.trino.pct": [
        ("R", r"while \*\*Trino\s*\n?\s*moves ([\d.]+)%\*\*"),
        ("H", r"while <b>Trino moves ([\d.]+)%</b>"),
    ],

    # --- the joins: Trino wins all three, and the summary under-declared it
    "join.j0.pct": [
        ("R", r"\| \*\*J0\*\* plain join \| 7\.99 s \| \*\*7\.43 s\*\* \| \*\*([\d.]+)%"),
        ("R", r"\*\*J0 by ([\d.]+)%\*\*"),
        ("H", r"7\.43 s <span class=\"small\">\(([\d.]+)% faster\)"),
        ("H", r"By <b>([\d.]+)%</b> on the plain join"),
        ("H", r"<b>J0 by ([\d.]+)%</b>"),
    ],
    "join.j1.pct": [
        ("R", r"\| \*\*J1\*\* \+ `WHERE` \| 3\.93 s \| \*\*3\.44 s\*\* \| \*\*([\d.]+)%"),
        ("R", r"\*\*J1 by ([\d.]+)%\*\*"),
        ("H", r"3\.44 s <span class=\"small\">\(([\d.]+)% faster\)"),
        ("H", r"<b>J1 by ([\d.]+)%</b>"),
    ],
    "join.j2.pct": [
        ("R", r"\| \*\*J2\*\* \+ `GROUP BY` \| 8\.56 s \| \*\*6\.66 s\*\* \| \*\*([\d.]+)%"),
        ("R", r"\*\*J2 by ([\d.]+)%\*\*"),
        ("H", r"6\.66 s <span class=\"small\">\(([\d.]+)% faster\)"),
        ("H", r"<b>J2 by ([\d.]+)%</b>"),
    ],
    "s1m.mult": [
        ("R", r"between the two SQL engines we are ([\d.]+)× faster"),
        ("H", r"between the two SQL engines we are ([\d.]+)× faster"),
    ],
}

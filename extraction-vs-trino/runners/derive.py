#!/usr/bin/env python3.12
"""Every DERIVED quantity the documents print, recomputed from the run artifacts.

    python3.12 runners/derive.py            # table of every derived quantity
    python3.12 runners/derive.py --json     # same, machine-readable

WHY THIS EXISTS. `verify_claims.py` checks CELLS: it matches a label to its value, so a
stale cell cannot survive. It does not check what the prose does with those cells -- the
multiples, percentages, sums and per-core rates that sentences carry. Those were verified
by scanning for tokens, and a token check cannot distinguish a stale "2.83x" from a live
one: any number that also appears somewhere legitimate passes. That residue let a whole
join table survive a re-measure with medians sitting OUTSIDE their own published min-max
intervals -- arithmetically impossible, and invisible to a scanner.

So each entry here owns a FORMULA over cells, and `verify_claims.py` binds it to the exact
prose sites that print it. A re-measure moves the cell; the formula moves with it; the site
check fails at the sentence, naming the value the sentence should now carry. Adding a
number to the prose means adding it here, or it is unverified.
"""
import argparse
import json
import pathlib
import statistics
import sys

HERE = pathlib.Path(__file__).resolve().parent.parent
RESULTS = HERE / "results"

MAIN = "20260821T041841-v030-prewarm"
ONESHARD = "20260821T041841b-v030-prewarm-1shard"
AB = "20260821T041841-v030-prewarm-sliced-ab"
JOIN = "join-20260821T041841-v030-prewarm"


class Cell:
    """One measured cell: the runs of a (session, scenario, stack, variant, dest, dtype)."""

    def __init__(self, key, runs):
        self.key, self.runs, self.n = key, runs, len(runs)

    def _med(self, *path):
        vals = []
        for r in self.runs:
            x = r
            for k in path:
                x = x.get(k) if isinstance(x, dict) else None
            if isinstance(x, (int, float)):
                vals.append(x)
        return statistics.median(vals) if vals else None

    def _all(self, key):
        return [r[key] for r in self.runs if isinstance(r.get(key), (int, float))]

    @property
    def wall(self):
        return self._med("wall_s")

    @property
    def wall_min(self):
        return min(self._all("wall_s"))

    @property
    def wall_max(self):
        return max(self._all("wall_s"))

    @property
    def spread_pct(self):
        """(max-min)/median, in percent -- the dispersion the tables print."""
        return 100.0 * (self.wall_max - self.wall_min) / self.wall

    @property
    def cpu(self):
        return self._med("cpu_s")

    @property
    def mem(self):
        v = self._med("peak_footprint_mb")
        return v if v is not None else self._med("peak_rss_mb")

    @property
    def engine_cpu(self):
        return self._med("server_cpu", "engine_cpu_s")

    @property
    def es_cpu(self):
        return self._med("server_cpu", "elasticsearch_cpu_s")

    @property
    def es_tx_gb(self):
        return (self._med("net_es", "tx_bytes") or 0) / 1e9

    @property
    def rows(self):
        return self.runs[0].get("rows") or 0


def load(session):
    by = {}
    for f in sorted((RESULTS / session).glob("*-run*.json")):
        r = json.loads(f.read_text())
        k = (r.get("scenario"), r.get("stack"), r.get("variant") or "",
             r.get("dest") or "", r.get("dtype_backend") or "")
        by.setdefault(k, []).append(r)
    return {k: Cell(k, runs) for k, runs in by.items()}


def cell(session_cells, scenario, stack, variant="", dest="", dtype=""):
    c = session_cells.get((scenario, stack, variant, dest, dtype))
    if c is None:
        raise KeyError(f"no cell {scenario}/{stack}/{variant or '-'}/{dest or '-'}/{dtype or '-'}")
    return c


def build():
    """(derived, cells). Formula strings are documentation, never evaluated."""
    m, o, ab, j = load(MAIN), load(ONESHARD), load(AB), load(JOIN)

    S1f = cell(m, "S1", "flight")
    S1fd = cell(m, "S1", "flight", "drift")
    S1t = cell(m, "S1", "trino")
    S1cx = cell(m, "S1", "trino", "arrowcx")
    S1adbc = cell(m, "S1", "trino", "arrowadbc")
    S1tuned = cell(m, "S1", "trino", "tuned")
    S1td = cell(m, "S1", "trino", "drift")
    S2f, S2t = cell(m, "S2", "flight"), cell(m, "S2", "trino")
    S3f, S3t = cell(m, "S3", "flight"), cell(m, "S3", "trino")
    S3q, S3qa = cell(m, "S3", "esql"), cell(m, "S3", "esql", "arrow")
    S4f, S4t = cell(m, "S4", "flight"), cell(m, "S4", "trino")
    S4q, S4qa = cell(m, "S4", "esql"), cell(m, "S4", "esql", "arrow")
    S1mf, S1mt = cell(m, "S1m", "flight"), cell(m, "S1m", "trino")
    S1mq, S1mqa = cell(m, "S1m", "esql"), cell(m, "S1m", "esql", "arrow")
    F0 = cell(m, "S0p", "es-raw")                 # count-and-discard, 6 slices
    F0a = cell(m, "S0p", "es-raw", "arrow")       # same, building the Arrow table
    F0seq = cell(m, "S0", "es-raw")
    F0seqa = cell(m, "S0", "es-raw", "arrow")
    S1rf = cell(m, "S1r", "flight", "", "", "default")
    S1rfa = cell(m, "S1r", "flight", "arrowdtype", "", "pyarrow")
    S1rfp = cell(m, "S1r", "flight", "polars")
    S1rt = cell(m, "S1r", "trino", "", "", "default")
    S1rta = cell(m, "S1r", "trino", "arrowdtype", "", "pyarrow")
    S1rtcx = cell(m, "S1r", "trino", "pandascx")
    S1rtadbc = cell(m, "S1r", "trino", "pandasadbc")
    S1rtp = cell(m, "S1r", "trino", "polars")
    S1rtpcx = cell(m, "S1r", "trino", "polarscx")
    S1rtpadbc = cell(m, "S1r", "trino", "polarsadbc")
    O1f, O1t = cell(o, "S1", "flight", "1shard"), cell(o, "S1", "trino", "1shard")
    O3f, O3t = cell(o, "S3", "flight", "1shard"), cell(o, "S3", "trino", "1shard")
    ABs1q, ABs1s = cell(ab, "S1", "flight", "sequential"), cell(ab, "S1", "flight", "sliced")
    ABs2q, ABs2s = cell(ab, "S2", "flight", "sequential"), cell(ab, "S2", "flight", "sliced")
    ABs3q, ABs3s = cell(ab, "S3", "flight", "sequential"), cell(ab, "S3", "flight", "sliced")
    ABs4q, ABs4s = cell(ab, "S4", "flight", "sequential"), cell(ab, "S4", "flight", "sliced")
    J = {n: (cell(j, n, "flight"), cell(j, n, "trino")) for n in ("J0", "J1", "J2")}

    d = {}

    def add(name, value, formula, prec=2):
        d[name] = {"value": value, "formula": formula, "precision": prec}

    warm = S1f.wall / S1fd.wall

    # ---- the warm-in: the position-in-sequence effect, measured on S1 -------------
    add("warmin.factor", warm, "S1 first block / S1 drift block, our side", 3)
    add("warmin.pct", 100 * (warm - 1), "how much slower our first timed block is", 0)
    add("warmin.trino.pct", 100 * abs(S1t.wall / S1td.wall - 1), "the same on Trino: the control", 1)

    # ---- S1, the headline --------------------------------------------------------
    add("s1.mult.documented", S1t.wall / S1f.wall, "trino S1 wall / flight S1 wall")
    add("s1.mult.cx", S1cx.wall / S1f.wall, "trino connectorx wall / flight S1 wall")
    add("s1.mult.adbc", S1adbc.wall / S1f.wall, "trino adbc wall / flight S1 wall")
    add("s1.cpu.mult.documented", S1t.cpu / S1f.cpu, "client CPU, documented route", 1)
    add("s1.cpu.mult.cx", S1cx.cpu / S1f.cpu, "client CPU, connectorx route", 1)
    add("s1.mem.mult.documented", S1t.mem / S1f.mem, "peak client memory, documented route", 1)
    add("s1.engine.mult", S1t.engine_cpu / S1f.engine_cpu, "engine CPU, documented route", 1)
    add("s1.escpu.mult", S1f.es_cpu / S1t.es_cpu, "our ES CPU / Trino's -- we cost the cluster more")
    # The published components are printed to one decimal, so the sum a reader can check
    # is the sum of the PRINTED cells, not of the raw medians (85.7 vs 85.8).
    def syscpu(c):
        return round(c.es_cpu, 1) + round(c.engine_cpu, 1)

    add("s1.system.ours", syscpu(S1f), "our printed ES CPU + printed engine CPU", 1)
    add("s1.system.trino.documented", syscpu(S1t),
        "Trino printed ES + engine CPU, documented route", 1)
    add("s1.system.trino.cx", syscpu(S1cx),
        "Trino printed ES + engine CPU, connectorx route", 1)
    add("s1.cores.ours", S1f.engine_cpu / S1f.wall, "our engine CPU / wall: mean cores of 4", 1)
    add("s1.cores.trino.documented", S1t.engine_cpu / S1t.wall,
        "Trino engine CPU / wall: mean cores of 6, documented route", 1)
    add("s1.cores.trino.cx", S1cx.engine_cpu / S1cx.wall,
        "Trino engine CPU / wall: mean cores of 6, connectorx route", 1)
    add("s1.escores.ours", S1f.es_cpu / S1f.wall, "our ES CPU / wall: mean ES cores of 6", 1)
    add("s1.escores.trino.documented", S1t.es_cpu / S1t.wall,
        "Trino ES CPU / wall: mean ES cores of 6, documented route", 2)
    add("s1.escores.trino.cx", S1cx.es_cpu / S1cx.wall, "the same, connectorx route", 1)
    add("s1.tuned.gain.pct", 100 * (1 - S1tuned.wall / S1t.wall),
        "what the tuned scroll-size catalog buys Trino", 1)
    add("s1.wire.ours.bpr", S1f.es_tx_gb * 1e9 / S1f.rows, "ES egress bytes/row, ours", 0)
    add("s1.wire.trino.bpr", S1t.es_tx_gb * 1e9 / S1t.rows, "ES egress bytes/row, Trino", 0)
    add("s1.throughput.ours", S1f.rows / S1f.wall / 1e6, "million rows per second, ours", 2)
    add("s1.sec.per.million", S1f.wall / (S1f.rows / 1e6), "seconds per million rows, ours", 2)

    # ---- the floor ---------------------------------------------------------------
    add("floor.margin.firstblock", F0a.wall / S1f.wall,
        "Arrow-building floor / our FIRST block: the published margin")
    add("floor.margin.settled", F0a.wall / S1fd.wall,
        "the same against our settled block: the favourable bound")
    add("floor.cpu.mult", F0a.cpu / S1f.cpu, "floor client CPU / ours", 1)
    add("floor.mem.mult", F0a.mem / S1f.mem, "floor client memory / ours", 1)

    # ---- S1r: the destination matrix ---------------------------------------------
    add("s1r.pandas.mult.cx", S1rtcx.wall / S1rf.wall, "Trino's fastest pandas route / ours", 1)
    add("s1r.pandas.mult.cx.firstblock", S1rtcx.wall / (S1rf.wall * warm),
        "the same, with the S1-measured position effect applied to our cell", 1)
    add("s1r.pandas.mult.documented", S1rt.wall / S1rf.wall, "documented pandas route / ours", 1)
    add("s1r.pandas.mult.documented.firstblock", S1rt.wall / (S1rf.wall * warm),
        "the same, position-adjusted", 1)
    add("s1r.pandas.mem.mult.cx", S1rtcx.mem / S1rf.mem, "peak memory, ours vs connectorx", 1)
    add("s1r.dtype.mem.saved", S1rf.mem - S1rfa.mem, "MB the pyarrow dtype backend saves us", 0)
    add("s1r.polars.mult.cx", S1rtpcx.wall / S1rfp.wall, "polars: Trino connectorx / ours", 1)
    add("s1r.polars.mult.cx.firstblock", S1rtpcx.wall / (S1rfp.wall * warm),
        "the same, position-adjusted", 1)
    add("s1r.polars.mult.documented", S1rtp.wall / S1rfp.wall, "polars: documented route / ours", 1)
    add("s1r.polars.mult.documented.firstblock", S1rtp.wall / (S1rfp.wall * warm),
        "the same, position-adjusted", 1)
    add("s1r.pandas.cpu.mult.documented", S1rt.cpu / S1rf.cpu,
        "pandas client CPU: documented route / ours", 1)
    add("s1r.pandas.mem.mult.documented", S1rt.mem / S1rf.mem,
        "pandas peak memory: documented route / ours", 1)

    # ---- S2 ----------------------------------------------------------------------
    add("s2.mult", S2t.wall / S2f.wall, "S2 (DuckDB) Trino / ours, settled state", 1)
    add("s2.mult.firstblock", S2t.wall / (S2f.wall * warm), "the same, position-adjusted", 1)
    add("s2.mem.mult", S2t.mem / S2f.mem, "S2 peak client memory ratio", 1)

    # ---- S3, the structural result ------------------------------------------------
    add("s3.bytes.mult", (S3t.es_tx_gb * 1e9) / max(S3f.es_tx_gb * 1e9, 1),
        "ES egress: Trino / ours on the pushed-down aggregation", 0)
    add("s3.escpu.mult", S3t.es_cpu / S3f.es_cpu, "cluster CPU: Trino / ours", 0)
    add("s3.wall.mult", S3t.wall / S3f.wall, "wall: Trino / ours", 0)
    add("s3.flight.spread", S3f.spread_pct, "our S3 cell's own min-max spread", 0)
    add("s3.esql.spread", S3q.spread_pct, "ES|QL's S3 spread, json route", 0)
    add("s3.esql.arrow.spread", S3qa.spread_pct, "ES|QL's S3 spread, arrow route", 0)
    add("s3.oneshard.mult", O3t.wall / O3f.wall, "the same comparison at 1 shard", 0)

    # ---- topology ----------------------------------------------------------------
    add("topo.ours", O1f.wall / S1f.wall, "1 shard / 6 shards, our side")
    add("topo.trino", O1t.wall / S1t.wall, "1 shard / 6 shards, Trino")
    add("topo.gap.oneshard", O1t.wall / O1f.wall, "the S1 multiple at 1 shard")

    # ---- the #238 A/B ------------------------------------------------------------
    add("ab.mult.s1", ABs1q.wall / ABs1s.wall, "sequential / sliced, S1, same build")
    add("ab.mult.s2", ABs2q.wall / ABs2s.wall, "sequential / sliced, S2, same build")
    add("ab.escpu.drop.pct", 100 * (1 - ABs1s.es_cpu / ABs1q.es_cpu),
        "ES CPU saved by sliced paging, S1", 0)
    add("ab.s2.escpu.vs.main.pct", 100 * (ABs2s.es_cpu / S2f.es_cpu - 1),
        "extra ES CPU the A/B's S2 sliced arm carried vs the matrix cell", 0)
    add("ab.s1.vs.main.pct", 100 * (1 - ABs1s.wall / S1f.wall),
        "A/B sliced S1 against the matrix cell: faster by", 0)
    add("ab.s2.vs.main.pct", 100 * (ABs2s.wall / S2f.wall - 1),
        "A/B sliced S2 against the matrix cell: slower by", 0)

    # ---- joins: Trino wins all three ---------------------------------------------
    for n, (fc, tc) in J.items():
        add(f"join.{n.lower()}.pct", 100 * (1 - tc.wall / fc.wall), f"{n}: Trino faster by", 1)

    # ---- ES|QL calibration -------------------------------------------------------
    add("esql.s1m.mult.json", S1mf.wall / S1mq.wall, "S1m: ours / ES|QL json route", 1)
    add("esql.s1m.mult.arrow", S1mf.wall / S1mqa.wall, "S1m: ours / ES|QL arrow route", 0)
    add("s1m.mult", S1mt.wall / S1mf.wall, "S1m: Trino / ours")

    cells = {
        "S1.flight": S1f, "S1.flight.drift": S1fd, "S1.trino": S1t, "S1.trino.cx": S1cx,
        "S1.trino.adbc": S1adbc, "S1.trino.tuned": S1tuned, "S1.trino.drift": S1td,
        "S2.flight": S2f, "S2.trino": S2t, "S3.flight": S3f, "S3.trino": S3t,
        "S3.esql": S3q, "S3.esql.arrow": S3qa, "S4.flight": S4f, "S4.trino": S4t,
        "S4.esql": S4q, "S4.esql.arrow": S4qa,
        "S1m.flight": S1mf, "S1m.trino": S1mt, "S1m.esql": S1mq, "S1m.esql.arrow": S1mqa,
        "S0p.floor": F0, "S0p.floor.arrow": F0a, "S0.floor": F0seq, "S0.floor.arrow": F0seqa,
        "S1r.flight.pandas": S1rf, "S1r.flight.arrowdtype": S1rfa, "S1r.flight.polars": S1rfp,
        "S1r.trino.pandas": S1rt, "S1r.trino.arrowdtype": S1rta, "S1r.trino.pandascx": S1rtcx,
        "S1r.trino.pandasadbc": S1rtadbc, "S1r.trino.polars": S1rtp,
        "S1r.trino.polarscx": S1rtpcx, "S1r.trino.polarsadbc": S1rtpadbc,
        "1shard.S1.flight": O1f, "1shard.S1.trino": O1t,
        "1shard.S3.flight": O3f, "1shard.S3.trino": O3t,
        "ab.S1.sequential": ABs1q, "ab.S1.sliced": ABs1s,
        "ab.S2.sequential": ABs2q, "ab.S2.sliced": ABs2s,
        "ab.S3.sequential": ABs3q, "ab.S3.sliced": ABs3s,
        "ab.S4.sequential": ABs4q, "ab.S4.sliced": ABs4s,
        "J0.flight": J["J0"][0], "J0.trino": J["J0"][1],
        "J1.flight": J["J1"][0], "J1.trino": J["J1"][1],
        "J2.flight": J["J2"][0], "J2.trino": J["J2"][1],
    }
    return d, cells


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    derived, cells = build()
    if a.json:
        print(json.dumps({
            "derived": {k: v["value"] for k, v in derived.items()},
            "cells": {k: {"n": c.n, "wall": c.wall, "min": c.wall_min, "max": c.wall_max,
                          "spread_pct": c.spread_pct, "cpu": c.cpu, "mem": c.mem,
                          "engine_cpu": c.engine_cpu, "es_cpu": c.es_cpu,
                          "es_tx_gb": c.es_tx_gb, "rows": c.rows}
                      for k, c in cells.items()},
        }, indent=2))
        return 0
    print(f"{'CELL':<26}{'n':>3}{'wall':>9}{'min':>9}{'max':>9}{'spr%':>7}"
          f"{'cliCPU':>8}{'cliMB':>8}{'engCPU':>8}{'esCPU':>7}")
    for k, c in cells.items():
        print(f"{k:<26}{c.n:>3}{c.wall:>9.3f}{c.wall_min:>9.3f}{c.wall_max:>9.3f}"
              f"{c.spread_pct:>7.1f}{(c.cpu or 0):>8.2f}{(c.mem or 0):>8.0f}"
              f"{(c.engine_cpu or 0):>8.1f}{(c.es_cpu or 0):>7.1f}")
    print(f"\n{'DERIVED':<40}{'value':>10}   formula")
    for k, v in derived.items():
        print(f"{k:<40}{v['value']:>10.{v['precision']}f}   {v['formula']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

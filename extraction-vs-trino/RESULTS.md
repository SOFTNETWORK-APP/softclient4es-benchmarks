# Results — SoftClient4ES (Arrow Flight SQL) vs Trino Elasticsearch connector

This benchmark measures how efficiently each system extracts data **out of Elasticsearch** into a
Python data-science client (pandas, polars, DuckDB). Both run against the same 3-node Elasticsearch
cluster, the same 10-million-row index across 6 primary shards, on the same host, back to back.

**Who this is for:** data teams pulling large result sets out of Elasticsearch into Python. It is
**not** a distributed-analytics, federation, or SQL-coverage comparison — for those, Trino is the
right tool and this benchmark deliberately does not exercise its strengths.

**Summary.** For work Elasticsearch can compute, SoftClient4ES pushes it into the cluster and moves
almost no data off it — an aggregation returning 100 rows moves **27.1 KB against 1.39 GB**, and
costs **0.1 s of cluster CPU against 21.3 s**. That number leads this document deliberately: it is
its one **structural** result *against Trino* — unchanged in kind at 1 shard and at 6 (25.9 KB and
27.1 KB leave the cluster; the 10M rows never do), grounded in the
connector's own documentation, exact by Elasticsearch's own error bounds — where every wall-clock
multiple below is competitive and can erode. It is not a property nobody else has: Elasticsearch's
own ES|QL pushes the same aggregation down and ties us on that cell (section 3). What is structural
is the distance from a connector that pushes no aggregation down at all. For large extractions it runs in far less client
memory: it lands 10M rows in a **2 GB** container where Trino's documented client is OOM-killed even
with 8 GB, and completes **five concurrent extractions in an 8 GB budget where that client completes
none and Trino's fastest client completes two**. It is also faster — **11.91 s against
14.66 s for Trino's fastest client and 44.70 s for its documented one, a factor of 1.23 to 3.75
depending on the route** — because it delivers Arrow columnar batches the client consumes directly,
where Trino's Python clients materialise or re-parse rows. The low end is the serious comparison:
nobody extracts 10M rows through the documented client on purpose. That shows up most starkly on the
client: **2.83 s of client CPU against 24.05 s**.

Trino runs throughout as a **3-node cluster allocated 1.5× our CPU and 2× our memory** — 6 CPUs
and 8 GB against the sidecar's 4 and 4, neither side's server counted against the client budgets of
section 4. What the handicap buys is fairness, not speed, and the CPU counters say how much of it is
used — **which depends on the route, so both are stated**. Driven by its documented client, Trino's
55.4 s of engine CPU over a 44.70 s run average **about 1.2 of its 6 CPUs** against our 2.4 of 4,
and total system CPU (engine plus Elasticsearch) comes to **85.8 s against our 71.0 s**. Driven by
connectorx — the route every multiple here is quoted against — the same cluster averages **2.8 of
its 6 CPUs**, 47% of its allocation against our 60%, and total system CPU is **73.8 s against our
71.0 s: parity**. The wide system-cost gap belongs to the documented client, not to Trino. Its
bottleneck is its own row processing rather than its allocation — which is also what the flat
`scroll-size` knob (section 7), its topology-flat Elasticsearch CPU (section 6), and the **per-run
throttling counters** say: across the whole matrix no container of either stack was throttled by
its CPU limit (0 periods on S1/S2/S3/S4; Trino's one blip is 0.8 s during the 4.4 s S1m burst).
The one place the accounting favours Trino on *every* route is Elasticsearch itself — we cost the
cluster **42.2 s of CPU against its 30.4 s** on that first-measured cell (in the settled state,
35.7 — see *the warm-in* below), which over the run is **3.5 of the cluster's 6 CPUs against
0.68** (documented route; 2.2 on connectorx). Where that cluster also serves search traffic, that is a real operational trade-off, and
the limitations in `METHODOLOGY.md` carry it as one.

**Read the fastest-client row before quoting any multiple.** Trino has three Python clients and the
documented one is its slowest. Against **connectorx**, its fastest, the same extraction is 14.66 s
against our 11.91 s — **1.23×, not 3.75×** — at 10.52 s of client CPU against our 2.83 s. Connectorx
also uses **less client memory than we do**: 617 MB against 921 MB. Every multiple in this document
is stated against a named route, and section 7 collects the cells Trino wins.

**Shard parallelism.** Resharding the same corpus from 6 primary shards to 1 costs us **3.50×**
(11.91 s → 41.69 s) and costs Trino **1.23×** (44.70 s → 55.11 s). On a single shard the two engines
are close (1.32×); the 3.75× is shard parallelism, and section 6.1 isolates it on our own side
alone, with concurrent paging switched on and off on the same build: disabled, the identical build
takes **35.34 s**, so the feature is worth **3.37×** on this cluster. **Read that as a lead, not a
moat**: sliced paging uses Elasticsearch's public `slice` API, available to any engine — Trino's
connector already plans one split per shard. What no pagination strategy changes are the
architectural results: the push-down (S3) and the client cost profile.

**Calibration — two reference points, not alternatives.** Two things here extract faster than
either SQL engine. Neither is a candidate for the workload this benchmark is about, and section 7
says why; they are published because a reader who knows Elasticsearch will ask, and an unmeasured
question reads as an avoided one.

*Elasticsearch's own query language.* Up to and including one million rows, ES|QL extracts faster
than either engine — **0.98 s against our 3.99 s**, and **0.32 s** over its Arrow wire format. It
cannot return more than 1,000,000 rows, which is why the ten-million-row scenarios have no ES|QL
column. On the pushed-down aggregation the two are level (0.034 s against our 0.043 s — their
five-run intervals overlap; see section 3, S3).

*A hand-written sliced scroll.* Six processes taking one Elasticsearch slice each — one per primary
shard — **building the same Arrow table we deliver**, extract the same 10M rows in **13.76 s against
our 11.91 s**. Stripped down to merely counting the rows and throwing them away, which is a cheaper
task than either engine performs, it manages 12.46 s — still, narrowly, slower than us. Concurrent
paging, new in the release measured here, closed that gap: the sliced floor is no longer the fastest
extraction in this document (section 6.1), though the margin is now thin and this document says so.

It remains the reference floor and is worth reading as one. Producing the artifact costs it **8.5×
our client CPU** (24.13 s against 2.83 s) and **4.0× our client memory** (3,669 MB against 921 MB),
which puts it among the clients that cannot complete in the 2 GB container where we do (section 4).
And it **is not written in SQL**.

This benchmark is for data engineers, analysts and scientists moving Elasticsearch data into
pandas, polars, DuckDB or a BI tool. For that reader, the floor's query is not a query at all: it is
a hand-maintained Elasticsearch Query DSL document — projection, filter, sort, slice and page size
as JSON — and every change to the question is an edit to that JSON. The floor also cannot do two
things this audience takes for granted:

- **Aggregate without re-reading everything.** The `GROUP BY` of S3 costs us 0.043 s and 27.1 KB off
  the cluster; a script that compiles nothing must pull all 10M rows back and count them client-side.
- **Join at all.** Elasticsearch has no join, so the cross-index scenarios (J0–J2) are not slower on
  this floor — they are absent. Reproducing them means hand-coding a hash join over two scrolls,
  which is neither a floor nor two hundred lines.

So the scope is stated rather than implied: this compares **SQL engines over Elasticsearch**, for
people who work in SQL and dataframes.

**What the floor means for a decision.** It is not an argument against using a SQL engine over
Elasticsearch. It says that *if* a workload is one fixed extraction, written once as Query DSL and
maintained as JSON, with no joins and no aggregation push-down, then a script is a reasonable way to
move those rows — and that is published here rather than hidden. The moment the workload is a
**query language** — users writing SQL, joining indices, aggregating, landing results in pandas,
polars, DuckDB or a BI tool, or running anywhere memory is bounded — the floor stops being an option
at all, and the comparison that decides anything is the one between the SQL engines (sections 3
onward).

**Where Trino is stronger.** It wins every cross-index join measured here — by **7.0%, 12.4% and
22.3%** on J0, J1 and J2 — offers a client that uses less memory than ours, and provides distributed
execution, spill and connector breadth that this benchmark does not exercise. Section 7 collects
these rather than scattering them.

All figures below were measured on the **released** sidecar image
`softnetwork/softclient4es8-arrow-flight-sql:0.3.0`
(digest `sha256:9dd80b77f62e…`), which carries core 0.21.0 and extensions 0.3.0, against Trino 483
and Elasticsearch 8.18.3.

---

## 1. Environment

| Item | Value |
|---|---|
| SoftClient4ES sidecar | `softclient4es8-arrow-flight-sql:0.3.0` (Arrow Flight SQL, gRPC :32010) — core 0.21.0, extensions 0.3.0 |
| Trino | 483 (official image), Elasticsearch connector — a **3-node cluster in every scenario**: a dedicated coordinator plus two workers, `node-scheduler.include-coordinator=false` |
| Elasticsearch | 8.18.3 — a **3-node cluster**, every node data + master-eligible, quorum of 2 |
| Index | `bench_events_10m` — 10,000,000 documents, flat mapping, **6 primary shards**, 0 replicas, force-merged (974 MB) |
| Host | Apple M4 Max (Mac16,6), macOS (Darwin arm64), 16 logical cores; Docker Desktop VM **13 CPU / 32 GB allocated** (reports 31.3 GiB) |
| Resources, and the handicap they encode | **One** SoftClient4ES sidecar: 4 CPU / 4 GB. **Trino: a 3-node cluster totalling 6 CPU / 8 GB** — 1.5× the CPU and 2× the memory, in every scenario. Elasticsearch: 3 nodes × 2 CPU / 4 GB (heap 2 GB each) |
| Sensitivity topology (§6) | `bench_events_10m_s1` — same corpus, same seed, **1 primary shard**. The engines are unchanged: only the index topology differs. Only one 10M index is open at a time, so neither shares the other's page cache |
| Paging (§6.1) | `elastic.scroll.max-slices` is left at its shipped default of 8, which resolves to `min(6 shards, 8) = 6` concurrent PIT slices. Section 6.1 measures the same build with it set to 1 |
| ES\|QL | Elasticsearch's own query language, measured as a third stack wherever it can run. Its cells were taken with `esql.query.result_truncation_max_size` raised from its 10,000 default to 1,000,000 — the product maximum; every ES\|QL run records the effective value. |
| Runs | 2 warm-ups, then 5 measured runs per cell; each run in a fresh client process. Every wall-clock cell in sections 3 and 6 prints the median with the **min–max** of its runs; section 5's joins publish theirs in a companion table, with all 25 cross-pairings. The section 4 sweeps are **one run per cell** by design — the outcome there is binary (completes / OOM-killed) rather than a timing — so they carry no dispersion. |

**Client libraries** (identical versions on both sides where a library is shared):

| Library | Version | Used by |
|---|---|---|
| CPython | 3.12.12 (arm64) | all |
| pyarrow | 25.0.0 | SoftClient4ES (Arrow) |
| adbc-driver-flightsql | 1.12.0 | SoftClient4ES (Flight SQL) |
| adbc-driver-manager | 1.12.0 | ADBC drivers |
| pandas | 3.0.5 (numpy 2.5.1) | both |
| polars | 1.43.2 | both (polars variant) |
| duckdb | 1.5.5 | both (S2) |
| trino | 0.338.0 | Trino (stock client) |
| sqlalchemy | 2.0.51 | Trino (`pandas.read_sql` / `pl.read_database`) |
| connectorx | 0.4.5 | Trino (fast columnar client) |
| ADBC Trino driver | 0.5.1 | Trino (ADBC client) |

**Measurement provenance.** The 6-shard extraction matrix — floors, S1, S1m, S1r, S2, S3, S4, both
ES\|QL wire formats, Trino's connectorx and ADBC routes, every S1r destination, the tuned-Trino arm,
the hostname-dial arm and the drift controls — is **one session,
`results/20260821T041841-v030-prewarm`**: 180 measured runs, one gate, one host state, and — new to
this session — **the corpus warmed to a verified steady state before the first timed block**
(`warm-in.json`; see *the warm-in* below). Four groups cannot share it because each rebuilds its own
container topology or index, so each is its own session, run the same morning on the same host and
the same image: S5 (`capped-20260821T041841-v030-prewarm`, `capped-cx-…`), S6
(`concurrent-20260821T041841-v030-prewarm`, `concurrent-cx-…`), the joins
(`join-20260821T041841-v030-prewarm`), and the 1-shard sensitivity arm
(`20260821T041841b-v030-prewarm-1shard`). Section 6.1's paging A/B recreates the sidecar between
arms, so it is its own session too (`20260821T041841-v030-prewarm-sliced-ab`). Every table names the
session it draws on; no table mixes two.

One session is **quarantined whole**, with a README saying why, rather than deleted:
`ABORTED-sleep-…-1shard`, whose Trino leg died because macOS put the host to sleep mid-block
(Trino killed the stalled client's queries as `ABANDONED_QUERY`); the phase was re-run under
`caffeinate`, which every phase now uses. Nothing in it is used by any table.

- *The warm-in — this document's one moving measurement, published against ourselves.* The session
  opens by extracting the full corpus, untimed, until Elasticsearch's own CPU per pass stabilises
  (<5% change twice in a row — it took 4 passes, 45.4 → 36.7 → 35.9 → 36.4 s, archived in
  `warm-in.json`), so **no timed block starts cold** and a cold page cache is excluded by
  construction. Even so, the first timed S1 block measures **11.91 s at 42.2 s of Elasticsearch
  CPU**, and the same cell re-measured at the **end** of the session — for both engines, because a
  drift arm for one proves nothing about the other — runs **10.04 s at 35.7 s**, while **Trino
  moves 0.4%** (44.70 → 44.52 s) in the same windows. The extra cost is entirely server-side (the
  sidecar also declines, 28.8 → 24.3 s of CPU — consistent with JVM warm-up; client CPU is flat),
  it decays over repeated identical reads, and it **re-arms**: the scroll floors, run between the
  warm-in and the first block, pushed the sliced path's Elasticsearch cost back from ~36 to 42 s.
  The mechanism is **not established**; what is established is that it only affects our sliced
  path, on a build and cluster that are otherwise metronome-stable. **This document publishes the
  first-measured block (11.91 s) as the headline** — the state a user meets on a warm cluster —
  and prints the end-of-session 10.04 s as the favourable bound this document deliberately does
  not use. Cells measured later inside the same block sequence (S1r, S2) sit in the settled state,
  which is why S1r's 9.97 s is *faster* than S1's 11.91 s for strictly more work — the order of
  measurement, not the workload, separates them.
- *Host load, and the throttling counters.* The client process runs on the host while the engines
  run in the Docker VM, so host contention could in principle inflate the client's wall clock. The
  1-minute load average sampled with every run had a **median of 5.5 and a maximum of 13.2 against
  16 logical cores** — 2.8 cores were uncommitted at the worst moment; the peaks belong to the arms
  that run several client processes at once, not to the headline block, and the harness refuses to
  measure above a configured load ceiling. The
  VM's 13 CPUs are oversubscribed by the declared limits (16) with asymmetric headroom per arm —
  ours claims 10 of 13, Trino's 12 of 13 — so this session publishes what the load average cannot
  see: the cgroup throttling counters (`nr_throttled`/`throttled_usec`, from the same `cpu.stat`
  file the CPU figures come from), **per run, for every stack**. The verdict is in section 3's
  tables and it is unambiguous: **zero throttled periods for either engine and for Elasticsearch on
  every 10M-row cell**; the one non-zero reading anywhere is Trino's 0.8 s during the 4.4 s S1m
  burst. Neither engine's limit ever bound a headline measurement.
- *Host memory pressure.* Peak client memory is the axis on which the two clients differ most, so
  the host's own memory state is sampled with every run. macOS reported **pressure level 1 (normal)
  on all 180 runs**, with **16.7 GB available at the tightest moment**. Swap was not empty, and it
  tells the session's shape honestly: across the 180 runs of the overnight matrix it sat **flat to
  the megabyte at 3,810 MB** — memory swapped out before the session began, not paging during it —
  and the ten-run Arrow-floor addendum, measured later that morning after ordinary desktop use, sat
  equally flat at ~9.0 GB (8,922 MB at the minimum and 9,066 MB at the maximum). A host paging
  *under* measurement would show the band growing within a block; neither block does. The harness
  refuses to measure a host that does not qualify — it refused twice earlier in the week at
  pressure level 2, and one prior attempt at this very session is quarantined whole because the
  host *slept* through its Trino block.
- *Engine quiescence.* S5 and S6 kill their clients on purpose — that is the scenario — and a Trino
  query whose client has gone keeps executing, and then "finishing", on the cluster for minutes
  afterwards. Every block therefore starts behind a gate that waits until Trino reports no query in
  flight and the sidecar's CPU has returned to idle, so no block is ever timed against the previous
  one's unfinished work.

**Metrics.** *Wall* is end-to-end time including connection. *Client CPU* is `time.process_time()`
in the client process. *Peak client memory* is the process's peak physical footprint
(`ri_lifetime_max_phys_footprint`), which is immune to the macOS memory compressor. **One caution
when reading across sections**: S5/S6 measure their clients *inside* Linux containers, where the
instrument is `ru_maxrss` — within every scenario both engines share one instrument, so every
comparison stands, but a memory figure from section 3 and one from section 4 are not on one scale. *Engine CPU* and
*Elasticsearch CPU* are exact cumulative counters read from each container's cgroup
(`cpu.stat`/`usage_usec`), not sampled. *ES wire* is the bytes that left the Elasticsearch cluster,
computed as the sum of the three nodes' `eth0` transmit counters **minus** the inter-node transport
traffic they report — so replication and coordination between nodes are not counted as data
delivered to a client. Every run asserts the exact expected row count before its timing is recorded;
a run that returns the wrong number of rows is discarded, not reported.

---

## 2. Scenarios at a glance

| ID | Question it answers | Outcome |
|---|---|---|
| **S0** | What does a single-process Python scroll client cost as a reference floor? | 37.0 s to read 10M rows |
| **S0p** | And a *sliced* scroll — 6 slices, 6 processes, one per shard: the floor a client that parallelises gets? | **13.76 s** building the same Arrow table, for **8.5×** SoftClient4ES's client CPU on S1 |
| **S1** | Extract 10M rows into a client-side columnar table | SoftClient4ES **1.23× faster** than Trino's fastest client, **3.75×** than its documented one; 3.7× and 8.5× less client CPU by route |
| **S1m** | Extract **1,000,000** rows — the only scale all three stacks can reach | **ES\|QL 0.32 s**, SoftClient4ES 3.99 s, Trino 4.42 s |
| **S1r** | Extract 10M rows into a **pandas / polars DataFrame** (what an analyst builds) | SoftClient4ES **4.7–5.6×** (pandas) and **4.3–5.1×** (polars) faster than the documented client; **1.3–1.6×** and **1.2–1.4×** faster than its fastest route (band = position in the block sequence) |
| **S2** | Extract 10M rows and compute an aggregate in DuckDB | SoftClient4ES **4.3–5.1× faster**, 8.0× less memory |
| **S3** | `GROUP BY` returning 100 rows | SoftClient4ES **0.043 s vs 5.50 s**, and moves **27.1 KB against 1.39 GB** off the cluster |
| **S4** | Fetch 100 rows (`LIMIT 100`) | Parity — 37 ms vs 65 ms; ES\|QL takes it at 6 ms |
| **S5** | Does the extraction fit in a constrained-memory container? | SoftClient4ES fits **2 GB**; Trino's documented client never fits (OOM at 8 GB), its fastest client fits 3 GB |
| **S6** | How many concurrent extractions fit in an 8 GB budget? | SoftClient4ES **5** (50M rows); Trino **2** with its fastest client, **0** with its documented one |
| **J0–J2** | Cross-index JOIN landed as a DataFrame | **Trino faster on all three** — by 7.0%, 12.4% and 22.3% |
| **§6** | Do the results survive resharding, and how much of the gap is shard parallelism? | 6 shards → 1 shard costs us **3.50×** and Trino **1.23×**; the gap narrows from **3.75× to 1.32×** |
| **§6.1** | What is concurrent paging worth, measured on the same build with the feature on and off? | **3.37×** on S1 (35.34 s → 10.48 s), with 30% *less* Elasticsearch CPU and unchanged controls |

---


## 3. Extraction scenarios

*(Session `20260821T041841-v030-prewarm` throughout this section.)*

### S0 / S0p — the reference floors

A plain Python client that scrolls Elasticsearch and parses each JSON hit. Two shapes: one process
with one point-in-time reader, and six processes each taking one Elasticsearch slice — **one per
primary shard**, which is the parallelism a competent engineer would choose and the one Elasticsearch
is happiest to serve.

Each shape is measured twice, because a floor that does less work than the engines is not a floor.
Counting rows and discarding them is a **cheaper task** than producing a columnar table; so both
shapes also run **building the real deliverable** — one Arrow table, 10M × 8, the same artifact S1
produces, assembled per page and concatenated once. The server side is instrumented at the same
time, from cgroup `usage_usec`, which is an exact counter rather than a sampled percentage.

| Floor (6 shards) | Wall | Client CPU | **Elasticsearch CPU** | Peak client memory |
|---|---|---|---|---|
| S0 — one reader, count and discard | 37.00 s [36.76–37.14] | 14.49 s | 45.5 s | 27 MB |
| S0 — one reader, **building Arrow** | 40.07 s [39.98–40.41] | 17.54 s | 46.7 s | 3,455 MB |
| S0p — 6 slices, count and discard | 12.46 s [12.21–12.94] | 15.97 s | 34.6 s | 165 MB |
| **S0p — 6 slices, building Arrow** | **13.76 s** [13.42–14.25] | **24.13 s** | 33.7 s | **3,669 MB** |
| SoftClient4ES S1, for comparison | **11.91 s** [11.73–12.01] | **2.83 s** | 42.2 s + 28.8 s sidecar | **921 MB** |
| Trino S1 (connectorx), for comparison | 14.66 s [14.51–14.71] | 10.52 s | 32.9 s + 40.9 s Trino | 617 MB |

**Outcome — the sliced floor does not win, and the margin is honest about being thin.** Building the
same artifact, it takes **13.76 s against our 11.91 s**: we are **1.16× faster**. The
count-and-discard arm — which produces nothing and is therefore not a comparison at all — lands at
12.46 s, a half-second behind us. Two things frame that margin. First, the warm-in (section 1): our
11.91 s carries the early-session state while the floors, measured right after the warm gate, sit
nearer the settled one — against our settled-state block the same floor would read 1.37×, which is
the favourable bound section 1 discloses and this document does not publish as the result. **1.16×
is the number it uses.** Second, section 6.1: with concurrent paging disabled we take 35.34 s, and the floor beats us
comfortably — the margin exists because of that feature, and only because of it.

**Note the slice count, because it decides the number.** The floor is measured at **6 slices on a
6-shard index**. A run in a previous campaign used 5 — a literal left over from an older topology —
which hands one slice two shards and makes the wall clock the slowest slice's: it measured 19.2 s, a
31% handicap **in our favour**, and those records are quarantined in `void-5slice-floors/` in that
campaign's session rather than deleted. A floor measured at the wrong parallelism flatters whatever
it is compared against, and this one was compared against us.

**What separates it from an engine, measured rather than asserted.** Producing the artifact costs it
**8.5× our client CPU** (24.13 s against 2.83 s) and **4.0× our client memory** (3,669 MB against
921 MB) — enough that it joins the clients OOM-killed in the 2 GB container where we complete
(section 4). Its Elasticsearch-side cost (33.7 s) is actually *below* our first-block 42.2 s — the
floor ran in the settled state — so this is not a case of buying wall-clock with cluster CPU;
slicing to the shard count is simply an efficient way to read an index.

And it answers exactly one query shape. The `GROUP BY` of S3 costs us 0.043 s and costs it a full
re-scan, because there is nothing in it to push anything down with. That is the scope boundary this
document states on page 1 rather than leaving a reader to find.


### S1 — extract 10M rows into a columnar client table

The full 10-million-row result set materialised in the client. SoftClient4ES fetches an Arrow table;
Trino's documented client fetches rows.

```python
# SoftClient4ES — Arrow Flight SQL (adbc_driver_flightsql)
cur.execute("SELECT id, event_ts, amount, qty, status, country, category, name "
            "FROM bench_events_10m")
table = cur.fetch_arrow_table()          # Arrow columnar batches

# Trino — documented client (trino.dbapi)
cur.execute(same_sql)
rows = cur.fetchall()                    # list of Python tuples
```

| Metric | SoftClient4ES | Trino (documented) | Ratio |
|---|---|---|---|
| Wall median | **11.91 s** [11.73–12.01] | 44.70 s [44.46–44.97] | **3.75× faster** |
| Client CPU | **2.83 s** | 24.05 s | 8.5× less |
| Peak client memory | **921 MB** | 4,455 MB | 4.8× less |
| Engine CPU | **28.8 s** | 55.4 s | 1.9× less |
| Elasticsearch CPU | 42.2 s | **30.4 s** | 1.39× *more* |
| Throttled (engine / ES) | **0 / 0 periods** | **0 / 0 periods** | — |
| ES wire | 2.54 GB | 2.92 GB | see below |
| Rows / columns | 10,000,000 / 8 | 10,000,000 / 8 | ✓ |

**Outcome.** SoftClient4ES is faster and far lighter on the client. The reason is client
representation: the client consumes Arrow batches without ever building 10 million Python objects,
which is what dominates Trino's client CPU and memory. The one column that goes the other way is
Elasticsearch's own CPU — we cost the cluster 39% more on this first-measured cell, because six
concurrent readers drive it harder to finish in a quarter of the time; in the settled state the
same cell costs 35.7 s (section 1, *the warm-in*), and this table deliberately publishes the
un-settled one.

**The upstream wire gap is systematic and unexplained.** For the same 8 columns at the same page
size, Trino reads **292 bytes per row** off the cluster where we read **254** — a 15% difference,
reproduced at S1m (308 against 253), so it is behaviour, not noise. Candidate causes — `_source`
filtering, per-hit metadata, the scroll-versus-PIT response envelope — have not been separated;
**a capture of each engine's `_search` requests is queued**, and until then this document notes the
gap rather than explaining it.

**The downstream leg, measured — per container, directly.** The bytes each engine sends its client
are read at each container's own interface. The sidecar's egress for the 10M rows is **0.71 GB** of
Arrow batches; **Trino's coordinator sends 0.25 GB** — its HTTP responses are compressed (the
other 0.72 GB of its stack's egress is internal worker-coordinator exchange). Read it plainly:
**on the documented route Trino ships fewer bytes to the client than we do — and on connectorx it
ships more** (0.85 GB against our 0.71 GB, compression declined), so the ordering inverts with the
client. Which is the point: on neither route is the byte count the explanation. The gap is not the byte count; it
is what the client must do with the bytes — 24.05 s of CPU turning JSON pages into Python objects
against 2.83 s consuming Arrow. (connectorx declines the compression: the same coordinator sends it
0.85 GB.) This also bounds the client-boundary objection (the client runs outside the Docker VM):
the leg that crosses that boundary is *smaller* for Trino, and the in-container scenarios (S5, S6),
where no such boundary exists, reproduce the same ordering.

**Fairness — measured against Trino's fastest clients.** Trino's documented client is not its only
option, and it is its slowest. [connectorx](https://github.com/sfu-db/connector-x) (a Rust engine
that parses Trino's result pages straight into columnar buffers) and the
[ADBC Trino driver](https://adbc-drivers.org/drivers/trino/) both return an Arrow table. Measured on
the same query, in the same session:

| Route | Wall | Spread | Client CPU | Peak memory |
|---|---|---|---|---|
| **SoftClient4ES** (Arrow Flight SQL) | **11.91 s** [11.73–12.01] | 2.3% | **2.83 s** | 921 MB |
| Trino — connectorx | 14.66 s [14.51–14.71] | 1.4% | 10.52 s | **617 MB** |
| Trino — ADBC driver | 26.97 s [26.87–27.01] | 0.5% | 18.64 s | 1,963 MB |
| Trino — documented client | 44.70 s [44.46–44.97] | 1.2% | 24.05 s | 4,455 MB |

SoftClient4ES keeps the wall-clock lead against every Trino client — **1.23× against the fastest of
them**, 3.75× against the documented one. **Quote the route with the multiple**: a bare "3.8× faster"
compares us with Trino's slowest client and is not the number a reader evaluating Trino should use.
And note what the warm-in note implies here: our 11.91 s carries the early-session state; measured
where connectorx was (mid-session, settled), the same fetch runs ~10.0 s.

On client cost the picture is genuinely mixed. Connectorx, building contiguous buffers in Rust,
reaches a **lower peak memory than we do** — 617 MB against our 921 MB — while still spending 3.7×
our client CPU. Our durable advantages here are wall-clock and CPU, not peak memory. Every route is
tight this session (spreads 0.5–2.3%), so the medians rest on solid ground.

### S1r — extract 10M rows into a DataFrame

The artifact a data scientist actually builds. Each side uses its most idiomatic route to a
`pandas.DataFrame`, and separately to a `polars.DataFrame`.

```python
# SoftClient4ES
df = cur.fetch_arrow_table().to_pandas()        # pandas
df = pl.from_arrow(cur.fetch_arrow_table())     # polars

# Trino
df = pandas.read_sql(sql, sqlalchemy_conn)      # pandas, via the trino SQLAlchemy dialect
df = pl.read_database(sql, sqlalchemy_conn)     # polars
```

**To a pandas DataFrame:**

| Route | Wall | Client CPU | Peak memory |
|---|---|---|---|
| **SoftClient4ES** (`to_pandas()`) | **9.97 s** [9.76–10.01] | **2.67 s** | 1,481 MB |
| SoftClient4ES (`types_mapper=pd.ArrowDtype`) | 9.95 s [9.94–10.17] | 2.63 s | **919 MB** |
| Trino — connectorx | 15.52 s [14.98–15.71] | 11.54 s | 2,824 MB |
| Trino — ADBC driver | 27.29 s [27.08–27.42] | 18.88 s | 2,542 MB |
| Trino — SQLAlchemy (documented) | 56.13 s [55.86–56.18] | 35.30 s | 8,079 MB |

Against Trino's documented route that is **5.6× faster on 13.2× less CPU and 5.5× less memory**;
against its fastest route, **1.6× faster on 1.9× less memory**.

**Read those two multiples as a band.** Unlike S1, this cell was measured in the settled state — it
ran later in the block sequence — so it carries the favourable end of the warm-in described in
section 1. Place our cell where S1's first block sat and the same comparisons read **4.7×** and
**1.3×**; Trino's own position effect across the session is 0.4%, so the band is ours, not theirs.
The honest statements are therefore **4.7–5.6×** against the documented route and **1.3–1.6×**
against the fastest one. Both of our dtype backends are published, and this session they cost the
same wall clock — the Arrow-backed one simply saves 562 MB, which makes it the route a memory-bound
analyst should take.

**To a polars DataFrame:**

| Route | Wall | Client CPU | Peak memory |
|---|---|---|---|
| **SoftClient4ES** (`pl.from_arrow`) | **10.94 s** [10.52–11.25] | 11.31 s | 2,484 MB |
| Trino — connectorx | 15.34 s [14.89–15.45] | 11.64 s | **2,186 MB** |
| Trino — ADBC driver | 28.20 s [27.96–28.82] | 28.17 s | 3,551 MB |
| Trino — SQLAlchemy (documented) | 56.04 s [55.85–56.34] | 34.95 s | 11,047 MB |

**Outcome.** SoftClient4ES lands a DataFrame faster on every route and destination — **4.7–5.6×**
and **4.3–5.1×** faster than Trino's documented routes for pandas and polars, and **1.3–1.6×** and
**1.2–1.4×** faster than its fastest; each band runs from our cell placed where S1's first block sat
to the settled state it was actually measured in. On the polars destination both sides re-encode Arrow strings into polars' native format,
which raises our client CPU sharply (11.31 s against 2.67 s for pandas); connectorx does the same
re-encoding in Rust and matches us on CPU there while using **less memory than we do**. The durable
advantage across every destination is wall-clock. (Note that both our S1r walls sit *below* our S1:
these cells ran later in the block sequence and carry the settled state — section 1, *the
warm-in*.)


### S2 — extract 10M rows and aggregate in DuckDB

The same fetch, landed in DuckDB and aggregated. SoftClient4ES registers the Arrow table zero-copy;
Trino builds a DataFrame from rows and registers that.

```python
# SoftClient4ES
con.register("events", cur.fetch_arrow_table())          # zero-copy
con.execute("SELECT category, AVG(amount) FROM events GROUP BY category")

# Trino
df = pandas.DataFrame(cur.fetchall(), columns=cols)
con.register("events", df)
con.execute(same_agg)
```

| Metric | SoftClient4ES | Trino | Ratio |
|---|---|---|---|
| Wall median | **10.32 s** [10.25–10.48] | 52.95 s [52.86–53.39] | **5.1× faster** |
| Client CPU | **3.70 s** | 32.45 s | 8.8× less |
| Peak client memory | **944 MB** | 7,510 MB | **8.0× less** |

**Outcome.** The zero-copy Arrow hand-off to DuckDB is the largest memory advantage in the benchmark
(8.0×): the columnar result is scanned in place instead of being copied through Python objects. Note
that landing 10M rows in DuckDB costs us only 23 MB over the bare Arrow table of S1 (944 MB against
921 MB), because nothing is copied; the same step costs Trino 3.1 GB over its own S1 figure.

Like S1r, this cell was measured in the settled state, so the wall-clock multiple is a band —
**4.3× to 5.1×**, depending on where our cell sits in the block sequence (section 1, *the
warm-in*). The memory ratio is unaffected: peak memory does not drift.

### S3 — `GROUP BY` returning 100 rows

`SELECT category, COUNT(*), AVG(amount) FROM bench_events_10m GROUP BY category`, returning 100 groups.

| Metric | SoftClient4ES | Trino | ES\|QL |
|---|---|---|---|
| Wall median | **0.043 s** [0.036–0.046] | 5.50 s [5.42–5.61] | **0.034 s** [0.033–0.035] |
| ES wire | **27.1 KB** | 1.39 GB | 45.2 KB |
| Elasticsearch CPU | **0.1 s** | 21.3 s | — |
| Rows | 100 | 100 | 100 |

**The same answer, not merely the same row count.** A `terms` aggregation is approximate when its
bucket size is too small, so "100 groups" is not by itself evidence that the pushed-down result
matches a full scan. Every session gates on the values: all 100 `(category, COUNT, AVG(amount))`
triples are compared across the three stacks — identical counts, averages equal to within 1e-9 — and
Elasticsearch reports `doc_count_error_upper_bound = 0` and `sum_other_doc_count = 0` for the
aggregation, which is the cluster's own statement that no bucket is missing and no document fell
outside the returned buckets. A push-down that returned a different answer faster would not be a
result.

**Outcome.** SoftClient4ES compiles the `GROUP BY` into an Elasticsearch `terms` aggregation: the
cluster computes the 100 groups and returns only those 100 rows — **27.1 KB of aggregation response
against the 1.39 GB Trino reads to compute the same answer**, at **0.1 s of cluster CPU against
21.3 s** and 0.043 s of wall against 5.50 s. The byte ratio is spectacular but it is a ratio of
bytes, not of time — the time columns are the claim. Trino's Elasticsearch connector performs predicate
push-down only, so it scans all 10M rows into Trino and aggregates there. That is the one
load-bearing statement in this document that rests on a source rather than on a session artifact,
so the source is named: the connector documentation for the version measured
(<https://trino.io/docs/483/connector/elasticsearch.html>, consulted 2026-08-22) carries a
*Predicate push down* section and no aggregation push-down of any kind — and the 1.39 GB it reads
off the cluster for a 100-row answer is this benchmark's measurement of exactly that.
This is the clearest architectural difference in the benchmark — for work Elasticsearch can do,
SoftClient4ES does not move the data at all.

**This is the one durable number in this document, and it should be read differently from every
other.** The extraction multiples are competitive results: they can erode with a Trino release, a
tuning pass, different hardware — section 6.1 shows our own headline moving 3.4× with one setting.
This cell is a **property**: it follows from what Trino's connector documentation says the
connector does not do (push aggregations down), it is invariant to topology in the quantity that
carries the claim — the bytes that leave the cluster, 25.9 KB at one shard and 27.1 KB at six
against Trino's 1.39 GB either way (section 6; the wall-clock cells, 0.038 s and 0.043 s, are
milliseconds and move with the session), and it is validated answer-by-answer with Elasticsearch's own
`doc_count_error_upper_bound = 0`. It moves only if the connector itself changes — and if that
ships, this document's successor will say so. A reader who takes one number from this benchmark
should take this one, not the 3.75×.

**Against ES|QL this cell is a tie, not a win.** Elasticsearch's own language answers in 0.034 s
against our 0.043 s — the same median on both of its wire formats, so best-route discipline costs
nothing to apply here. Both are small enough that the two five-run intervals overlap (ES|QL
0.033–0.062 s over `format=json`, 0.033–0.035 s over `format=arrow`, against our 0.036–0.046 s), so
the honest reading is parity, and section 7 records it as one.

### S1m — extract 1,000,000 rows: the only scale all three stacks can reach

Elasticsearch's own query language cannot return more than 1,000,000 rows (see below), so this is
the largest result set on which SoftClient4ES, Trino and ES|QL can be compared like for like. Same
eight columns, same index, `LIMIT 1000000` on every stack.

| Route | Wall | Client CPU | Peak client memory | Bytes off the cluster |
|---|---|---|---|---|
| **ES\|QL, `format=arrow`** | **0.32 s** [0.28–0.34] | 0.05 s | 166 MB | 72 MB |
| ES\|QL, `format=json` | 0.98 s [0.95–1.07] | 0.50 s | 666 MB | 86 MB |
| **SoftClient4ES, Arrow Flight SQL** | **3.99 s** [3.97–4.05] | **0.19 s** | 176 MB | 253 MB |
| Trino, documented client | 4.42 s [4.40–4.46] | 2.23 s | 475 MB | 308 MB |

**Outcome — ES|QL wins this scale; between the two SQL engines we are 1.11× faster.** ES|QL is an
order of magnitude faster than anything else measured here, and that is the honest headline of this
cell. Against Trino, SoftClient4ES lands the same million rows in 3.99 s against 4.42 s, on **11.7×
less client CPU** (0.19 s against 2.23 s) and 2.7× less client memory.

The reason ES|QL is fast is not the wire: it reads `doc_values` — columnar on disk — while both
engines read `_source` and pay a JSON parse per document. That is a genuine architectural advantage,
and it is bounded by the ceiling in the next section rather than by anything either engine does.

**The wire explains the ordering.** For the same million rows SoftClient4ES moves **253 bytes per
row off the cluster against Trino's 308 bytes**, and it does so at the same cost per byte as its own
ten-million-row run, which moves **254 bytes per row**. The bounded and unbounded paths through the
product cost the same per row, so the million-row figure is the ten-million-row behaviour at one
tenth the scale, not a different regime.

**Note what does *not* scale down: concurrent paging.** At 10M rows sliced paging is worth 3.37×
(section 6.1); at 1M rows against Trino the margin is 1.11×. A `LIMIT`ed query does not take the
sliced path at all — it pages sequentially — which is most of why this cell is close while S1 is not.


### ES|QL — Elasticsearch's own query language, and where it stops

A benchmark of two SQL engines over Elasticsearch that never measures what Elasticsearch itself can
do invites an obvious question. So it is measured, over both of its wire formats: the row-shaped
`format=json` every ES|QL client speaks, and `format=arrow`, which returns an Apache Arrow IPC
stream. All four columns below are the same session as the sections above.

| Scenario | ES\|QL (arrow) | ES\|QL (json) | SoftClient4ES | Trino |
|---|---|---|---|---|
| S1m — 1,000,000 rows | **0.32 s** [0.28–0.34] | 0.98 s [0.95–1.07] | 3.99 s [3.97–4.05] | 4.42 s [4.40–4.46] |
| S3 — `GROUP BY`, 100 rows | **0.034 s** [0.033–0.035] | **0.034 s** [0.033–0.062] | 0.043 s [0.036–0.046] | 5.50 s [5.42–5.61] |
| S4 — `LIMIT 100` | **0.006 s** [0.006–0.007] | **0.006 s** [0.006–0.006] | 0.037 s [0.031–0.040] | 0.065 s [0.056–0.067] |
| S1, S2, S5, S6 — 10,000,000 rows | *cannot run* | *cannot run* | ✓ | ✓ |

**Where it wins.** Below its ceiling ES|QL is the fastest way to get rows out of Elasticsearch that
we measured: on a 100-row fetch its whole round trip (6 ms) costs less than SoftClient4ES's
connection handshake alone (13.0 ms). Trino's handshake is cheaper still (1.6 ms); its
65 ms total is spent elsewhere. On the million-row extraction it is **12.5× faster than we are**.

**Where it does not.** On the pushed-down aggregation the result is a **tie between ES|QL and us**:
0.034 s for ES|QL against our 0.043 s, both moving kilobytes rather than gigabytes off the cluster
(27.1 KB for us, 45.2 KB for ES|QL). At these magnitudes — run-to-run spreads run 20–90% of the
median — the difference is not distinguishable. The work is identical; the difference is what the
result travels over.

**Why it is absent from four scenarios.** `esql.query.result_truncation_max_size` defaults to 10,000
rows and is declared with a **hard maximum of 1,000,000**
(`Setting.intSetting("esql.query.result_truncation_max_size", 10000, 1, 1000000, …)` in
`EsqlPlugin.java`, v8.18.3). Elasticsearch refuses a higher value outright. Ask for ten million rows
with the setting at its maximum and the response is **1,000,000 rows, HTTP 200, and no `Warning`
header** — a truncated answer that looks like a complete one. (Recorded as
`esql-truncation-probe.json` in the session directory; the cross-index JOIN refusal is recorded
beside it as `esql-join-probe.json`.) This is not a licence gate: the cluster measured here runs a
`basic` licence and reports `esql.available: true`.

That ceiling is the whole reason the ten-million-row scenarios have no ES|QL column, and it is worth
stating plainly rather than as a footnote: for result sets under a million rows, ES|QL is an
excellent answer and this benchmark is not the argument for anything else. What changes above a
million rows is not that ES|QL becomes slow — it becomes unavailable.

**The second boundary is the join.** ES|QL's `LOOKUP JOIN` does not join two fact indices: it
resolves only against an index in `lookup` mode, and such an index is **restricted to a single
shard**, i.e. it is a dimension table. Asked for this benchmark's cross-index join it answers
`400 — invalid [bench_1m] resolution in lookup mode to an index in [standard] mode` (recorded as
`esql-join-probe.json`). So the join scenarios in section 5 have no ES|QL column either, and for the
same kind of reason as the extraction ones: not that it is slower, but that the shape is outside
what the feature accepts. A dimension lookup that fits in one shard is what it is built for.

### S4 — fetch 100 rows (`LIMIT 100`)

The control: when the result is small, the client representation stops mattering.

| Metric | SoftClient4ES | Trino |
|---|---|---|
| Wall median | 0.037 s [0.031–0.040] | 0.065 s [0.056–0.067] |

**Outcome.** Near parity, as expected. The extraction advantage only appears at scale, or when work
can be pushed into Elasticsearch.

**What the 28 ms actually is, and a control that removes it.** At this size the whole scenario is
connection setup: SoftClient4ES's ADBC Flight SQL handshake takes **13.0 ms** of the 37, Trino's
**1.6 ms** of the 65. Both clients dial an IP literal here, deliberately — because when
SoftClient4ES is pointed at a hostname instead, its connect cost rises to **30.5 ms** and the S4
median moves to **0.054 s**, closing most of the distance to Trino's 0.065 s. The margin on this
control is therefore worth about as much as a name lookup.

The cause is client-side and measured. A four-layer probe (`connect-probe.json`, 30 repeats)
separates a bare TCP connect (**0.07 ms**) from the Flight C++ layer (**1.64 ms**), the Go ADBC
layer dialling an IP (**3.01 ms**) and the same layer dialling a name (**16.07 ms**). grpc-go
performs its own resolution rather than using the OS resolver, which is where the ~13 ms goes. No
other scenario in this document is affected — a name lookup is invisible against 12 seconds — but
on a 100-row fetch it is most of the result.

### Correctness gate — run before any of the timings above

Row counts alone do not establish that a pushed-down
aggregation returns the same answer as a full scan, so each session first runs a data-equivalence
gate, and a session whose gate does not pass is not measured at all. **All three stacks take part in
both halves:**

- On the push-down itself — every one of S3's 100 `(category, cnt, AVG(amount))` triples, key by key
  and value by value. They agreed exactly on the counts and to within 1e-9 on the averages.
- On `COUNT`, `SUM(id)`, `SUM(qty)` and `SUM(amount)` over the whole index — the check that the
  extraction paths see the same 10,000,000 documents, not merely the same number of them. All three
  agreed on all four.

Elasticsearch's own error bounds for the `terms` aggregation were `doc_count_error_upper_bound = 0`
and `sum_other_doc_count = 0` — i.e. the aggregation is exact at this shard count and bucket size,
not merely close. The gate's verdict is recorded as `equivalence-gate.json` in each session
directory, and section 6's topology run has its own.

**A gate must be able to fail.** The first attempt at this session recorded a pass while Trino was
not running: every Trino leg was skipped, the gate graded itself `PASS_WITH_SKIPS`, and the guard
that read it matched on a prefix. That session was discarded and the gate rebuilt — engines are now
started and proven healthy *before* the gate runs, and a skip naming either timed engine is fatal
rather than cosmetic. The verdict recorded for the published session is `PASS` with **zero skips**.


---

## 4. Constrained-memory and concurrency

### S5 — does the extraction fit in a small container?

*(Sessions `capped-20260821T041841-v030-prewarm` and `capped-cx-…` — the whole table from one sweep per client.)*

The client runs inside a container with a hard memory cap (`docker run --memory`, swap pinned equal
so the kernel kills rather than pages). The question is binary: does landing 10M rows as a DataFrame
complete, or is the process killed?

**Whole result set as one DataFrame:**

| Container cap | SoftClient4ES | Trino (documented) | Trino (connectorx, fastest) |
|---|---|---|---|
| 8 GB | ✅ 10.9 s · 1,537 MB | ❌ OOM-killed | ✅ 13.1 s · 2,911 MB |
| 6 GB | ✅ 11.3 s · 1,535 MB | ❌ OOM-killed | ✅ 12.7 s · 2,914 MB |
| 4 GB | ✅ 10.7 s · 1,533 MB | ❌ OOM-killed | ✅ 12.6 s · 2,912 MB |
| 3 GB | ✅ 10.7 s · 1,541 MB | ❌ OOM-killed | ✅ 13.4 s · 2,909 MB |
| **2 GB** | ✅ **11.4 s · 1,531 MB** | ❌ OOM-killed | ❌ **OOM-killed** |

*"OOM-killed" is the kernel killing the client — exit 137 — in every cell above, verified per cell.*

**Streaming, where neither side holds the whole result** — SoftClient4ES via
`fetch_record_batch()`, Trino via `pandas.read_sql(chunksize=…)`. This is the workflow a competent
Trino user reaches for first, so it is a table rather than a footnote:

| Container cap | SoftClient4ES | Trino (documented) |
|---|---|---|
| 8 GB | ✅ 10.9 s · 156 MB | ✅ 58.5 s · 312 MB |
| 6 GB | ✅ 10.8 s · 153 MB | ✅ 58.5 s · 313 MB |
| 4 GB | ✅ 10.8 s · 154 MB | ✅ 58.8 s · 314 MB |
| 3 GB | ✅ 10.7 s · 155 MB | ✅ 59.2 s · 315 MB |
| **2 GB** | ✅ **10.8 s · 153 MB** | ✅ **58.6 s · 312 MB** |

Both complete at every cap. SoftClient4ES is **5.4–5.5× faster on 2.0× less memory** — the streaming
comparison is the one that applies whenever the workflow does not need the whole result set in
memory at once. (The previous campaign's noisy streaming band, 10.9–15.4 s, does not reproduce:
this session's arm is flat at 10.7–10.9 s across a 4× cap range.)

**Outcome.** Landing the whole 10M-row DataFrame, SoftClient4ES fits in a **2 GB** container. Trino's
documented client is killed at every cap measured, 8 GB included; its fastest client (connectorx)
fits at 3 GB but is killed at 2 GB — the cap where SoftClient4ES still completes. Both requirements
are independent of the cap (peak memory moves by under 1% across a 4× range), so they reflect the
data, not memory thrashing.

The binary framing is the harder claim, and it should be read next to the streaming table above: it
answers "can I materialise this result in a small container", not "can this pipeline run at all".
Streaming, both engines run everywhere.

### S6 — concurrent extractions in a fixed memory budget

*(Sessions `concurrent-20260821T041841-v030-prewarm` and `concurrent-cx-…`.)*

An 8 GB total client budget, split evenly across N clients launched simultaneously, each extracting
10M rows. How many complete with the correct row count?

Measured against **both** of Trino's clients: the documented one it ships, and connectorx — the
fastest client it has, and the one S1 already grants it. Reporting only the documented client here
would abandon three scenarios later the fairness rule S1 sets.

| Engine / client | N | Cap each | Completed | Total wall |
|---|---|---|---|---|
| SoftClient4ES | 1 | 8,192 MB | 1/1 | 11.2 s |
| SoftClient4ES | 2 | 4,096 MB | 2/2 | 17.7 s |
| SoftClient4ES | 3 | 2,730 MB | 3/3 | 25.0 s |
| SoftClient4ES | 4 | 2,048 MB | 4/4 | 34.4 s |
| **SoftClient4ES** | **5** | **1,638 MB** | **5/5** | **42.8 s** |
| Trino — connectorx | 1 | 8,192 MB | 1/1 | 13.6 s |
| **Trino — connectorx** | **2** | **4,096 MB** | **2/2** | **21.0 s** |
| Trino — connectorx | 3 | 2,730 MB | 0/3 | killed |
| Trino — documented client | 1 | 8,192 MB | 0/1 | killed |

**Outcome.** In an 8 GB budget, SoftClient4ES completes **five** concurrent extractions — 50 million
rows — against **two** for Trino's fastest client and none for its documented one. The ratio is
2.5×, not the five-versus-nothing the documented client alone would suggest, and it follows directly
from what each client holds per extraction.

**Concurrency is not free, and the reason is ours.** Five concurrent extractions take 42.8 s
against 11.2 s for one — 3.8× the time for 5× the work. Each client opens six concurrent
point-in-time slices (section 6.1), so five of them put **thirty readers on a six-CPU Elasticsearch
cluster**: sliced paging spends cluster budget, and at N=5 it is the cluster rather than the client
that sets the pace. That is a real property of the feature, not an artifact — the claim here is
capacity, not scaling. This measures client-side capacity; it does not measure
server-side concurrency, which is a separate question this benchmark does not address.

---


## 5. Cross-index JOIN (J0–J2)

*(Session `join-20260821T041841-v030-prewarm`; medians of 5 runs, as everywhere else, behind the
engine-quiescence gate described in section 1.)*

A join between a 1M-row index (`bench_1m`) and the 10M-row index, landed as a pandas DataFrame on
both sides. Both engines execute the join server-side (SoftClient4ES in an embedded DuckDB, Trino in
its own engine).

```sql
-- J0  plain join                    (1,000,000 rows out)
SELECT a.id, b.amount FROM bench_1m a JOIN bench_events_10m b ON a.id = b.id
-- J1  + predicate on the large leg  (125,044 rows out)
SELECT a.id, b.amount FROM bench_1m a JOIN bench_events_10m b ON a.id = b.id WHERE b.status = 'paid'
-- J2  + GROUP BY                     (100 rows out)
SELECT b.category, COUNT(*), AVG(b.amount) FROM bench_1m a JOIN bench_events_10m b ON a.id = b.id GROUP BY b.category
```

**Trino wins all three.** This is the clearest loss in the document and it is not close on two of them.

| Scenario | SoftClient4ES wall | Trino wall | Trino faster by | Client CPU (SC4ES / Trino) |
|---|---|---|---|---|
| **J0** plain join | 7.99 s | **7.43 s** | **7.0%** | 0.05 s / 1.16 s |
| **J1** + `WHERE` | 3.93 s | **3.44 s** | **12.4%** | 0.02 s / 0.19 s |
| **J2** + `GROUP BY` | 8.56 s | **6.66 s** | **22.3%** | 0.01 s / 0.05 s |

The spread belongs next to the margins, and here it removes any doubt about the ordering:

| Scenario | SoftClient4ES min–max | Trino min–max | Runs won, of the 25 pairings |
|---|---|---|---|
| J0 | 7.85–8.16 s (3.9%) | 7.41–7.63 s (2.9%) | **Trino 25 / 25** |
| J1 | 3.91–3.95 s (1.1%) | 3.39–3.50 s (3.1%) | **Trino 25 / 25** |
| J2 | 8.53–8.62 s (1.0%) | 6.52–7.03 s (7.6%) | **Trino 25 / 25** |

Each engine's 5 runs are compared against the other's 5 — 25 pairings per scenario. Trino wins
**every pairing in every scenario**; the ranges do not touch anywhere.

**Outcome.** Trino is the better cross-index join engine on this corpus, by 7% on the plain join and
by 12% and 22% once a predicate or an aggregation is added. Its advantage *grows* with the work
added to the join: the marginal cost of adding the `GROUP BY` of J2 is **negative** for Trino
(J2 is 0.77 s faster than its own J0, because the aggregation collapses the result before it is
returned) against **+0.57 s** for us.

The one axis that runs our way is client cost — SoftClient4ES does **5–23× less client-side work**,
because the result arrives as Arrow rather than as Python objects — but on a join returning 100 rows
that is a small absolute saving, and it does not offset the wall clock.

**Provenance.** All three scenarios come from **one counter-balanced block** in which both engines
were measured together with both join legs pre-warmed, behind the engine-quiescence gate. (A
previous campaign's first join block, in which Trino's J2 degraded 37% because `bench_1m` was cold,
is quarantined in that campaign's `void-order-effect/` — the lesson is why this block warms both
legs first.)

---


## 6. Index topology sensitivity — 6 shards vs 1

*(Session `20260821T041841b-v030-prewarm-1shard`, with its own equivalence gate against the 1-shard index.)*

Everything above was measured on a **6-shard** index. Trino's Elasticsearch connector creates one
split per shard, so the natural question is how much of the gap is shard parallelism — and whether
either engine actually exploits it.

Both do. They do not exploit it equally.

**Setup.** Same host, same engines, same corpus regenerated from the same seed into a **1-shard**
index. **Only the shard count changes**: the engines, their resources and the 1.5×-CPU handicap in
Trino's favour are the ones stated in section 1, unchanged. Only one 10M index is open at a time, so
neither topology sits in the other's page cache, and the index is warmed before the first timed
block so that neither engine pays the first-touch cost described in section 1. Medians of 5 runs
after 2 warm-ups.

The parallelism was verified rather than assumed, on both topologies, from a live probe of
`system.runtime.tasks` during the scan:

| Index | Trino scan splits | Distribution |
|---|---|---|
| `bench_events_10m` (6 shards) | **6** | 3 on each worker, none on the coordinator |
| `bench_events_10m_s1` (1 shard) | **1** | a single reader on one worker |

| Metric | SoftClient4ES 1 shard | **6 shards** | Trino 1 shard | **6 shards** |
|---|---|---|---|---|
| S1 wall | 41.69 s [40.93–65.60] | **11.91 s** [11.73–12.01] | 55.11 s [54.18–56.17] | **44.70 s** [44.46–44.97] |
| S1 client CPU | 3.56 s | 2.83 s | 24.29 s | 24.05 s |
| S1 peak client memory | 922 MB | 921 MB | 4,473 MB | 4,455 MB |
| S1 Elasticsearch CPU | 25.4 s | 42.2 s | 28.7 s | 30.4 s |
| S1 ES wire | 2.53 GB | 2.54 GB | 2.96 GB | 2.92 GB |
| S3 wall (`GROUP BY`, 100 rows) | 0.038 s [0.033–0.045] | 0.043 s [0.036–0.046] | 28.54 s [28.42–29.73] | **5.50 s** [5.42–5.61] |
| S3 ES wire | 25.9 KB | **27.1 KB** | 1.43 GB | 1.39 GB |

**Outcome — extraction.** SoftClient4ES goes **3.50× faster** with six shards (41.69 s → 11.91 s).
Trino goes **1.23× faster** (55.11 s → 44.70 s), with six real splits spread across both workers.
The S1 ratio therefore **widens from 1.32× to 3.75×**. (One of our five 1-shard runs is an outlier
at 65.6 s — the min–max shows it; the median is robust to it, and the record stays.)

On one shard the two engines are close. Nearly all of the headline gap is shard parallelism — which
is precisely what section 6.1 isolates, by switching concurrent paging off on the same build rather
than by comparing builds.

**Why Trino gains so little from six splits.** Not because it fails to parallelise — the probe shows
six splits on two workers. Its wall clock is bound by its own row processing rather than by how fast
Elasticsearch can serve pages, and the server-side counters say so: Trino's Elasticsearch CPU barely
moves between topologies (28.7 s → 30.4 s) while its engine CPU stays near 56 s in both. Ours moves
the other way — Elasticsearch CPU **rises** 25.4 s → 42.2 s, because six concurrent readers drive
the cluster harder to finish in a quarter of the time. The same asymmetry explains why we are
sensitive to the cluster's warm-in state in section 1 and Trino is not.

**What this establishes, and what it does not.** Elasticsearch here is a **real 3-node cluster** (3 ×
2 CPU), not one container wearing three hats, so the six splits and six slices really are served by
three independent nodes — the result is about shard parallelism, not about a single node being the
bottleneck. What remains untested is a **larger** cluster — more nodes, more shards, and shards per
node above two. A reader operating one should treat the scaling *slope* as measured on three nodes
rather than as a general law.

**Outcome — client memory is a property of the protocol, not the topology; client CPU is not.**
Peak client memory moves by **0.2% on our side and 0.3% on Trino's**: the client is one process
consuming one wire format however many shards or workers serve it. Client **CPU** does move on our
side — **3.56 s on one shard against 2.83 s on six**, a 1.3× difference — while Trino's is flat
(24.29 s → 24.05 s). Six slices deliver the same bytes in more, smaller, concurrently-assembled
batches, so per-batch work overlaps the wait instead of queueing behind a single reader; the wire
format is unchanged, the scheduling is not. The claim this column supports is about **memory**, and
it is stated that way. Section 4's constrained-container result was **not** re-measured on the 1-shard index —
this arm ran S1 and S3 only — but since peak client memory moves 0.2% between topologies, the 2 GB
outcome is expected to be unchanged. That is an inference from a measured quantity, not a
measurement, and it is stated as one.

**Outcome — push-down is architectural.** The `GROUP BY` still moves **27.1 KB off the cluster
against 1.39 GB** — the 100-row answer itself. Six shards do
not change this — the Trino cluster is identical in both columns — because the connector does not
push aggregations down: it reads all 10 million rows whatever the topology, and merely reads them
faster.

**Where Trino gains most.** Its aggregation wall-clock improves **5.2×** (28.54 s → 5.50 s), the
largest single topology gain measured here for either engine. Scan parallelism is exactly what a
multi-shard index gives its workers, and on aggregate-heavy work it converts directly into
wall-clock — it simply starts from a number that push-down never has to pay.

---

### 6.1 What the parallelism is worth — sliced paging measured against itself

*(Session `20260821T041841-v030-prewarm-sliced-ab`.)*

Section 6 shows the gap widening with shards. This subsection attributes it, by holding **everything**
constant except one setting on our own side.

Core 0.21.0 pages a no-`ORDER BY`, no-`LIMIT` extraction with `min(primary shards, max-slices)`
concurrent point-in-time slices; before it, one sequential reader paged the whole result. The
shipped default `max-slices = 8` resolves to 6 on this index; setting it to **1** restores exactly
the earlier behaviour. Same image, same cluster, same corpus, minutes apart.

Which branch each arm actually took was **observed, not assumed** — the sidecar reports its slice
count per extraction, and each arm asserts on it before measuring:

```
sequential arm:  PIT search_after completed (1 slice(s))
sliced arm:      PIT search_after completed (6 slice(s))
```

| Cell | Sequential (1 slice) | Sliced (6 slices) | Gain |
|---|---|---|---|
| **S1** — extract 10M rows | 35.34 s [35.09–39.42] | **10.48 s** [10.14–10.58] | **3.37×** |
| **S2** — 10M rows into DuckDB | 36.97 s [36.06–38.76] | **14.49 s** [13.31–15.43] | 2.55× |
| S3 — `GROUP BY` (control) | 0.03 s | 0.03 s | — |
| S4 — `LIMIT 100` (control) | 0.02 s | 0.02 s | — |
| S1 engine CPU | 21.4 s | 25.7 s | +20% |
| S1 **Elasticsearch CPU** | 53.1 s | **37.4 s** | **−30%** |

**Read this table across, not against section 3.** Each arm is measured minutes after its pair, on
a cluster that pair has just worked hard, so the absolute values here do not line up with the
matrix — and they miss it in *both* directions: the sliced S1 arm lands **12% below** section 3's
11.91 s, while the sliced S2 arm lands **40% above** section 3's 10.32 s and carries 44% more
Elasticsearch CPU than the matrix cell did (51.0 s against 35.5 s), inheriting the state its own
sequential arm left behind. The claim of this section is a *ratio within a row* — same image, same
corpus, one setting different — not a wall-clock figure to be quoted beside section 3's.

**The controls are the point of the table.** S3 and S4 do not page — one is an aggregation, the
other returns 100 rows — and both are identical to the hundredth of a second *across the two arms*
(0.03 s and 0.02 s), which is the comparison this section makes. They are not identical to section
3's 0.043 s for the same S3 fetch, for the same session reason as above. Whatever moved S1 and S2
did not move them, so the arms differ by the paging strategy and nothing else.

**It is less work, not just more overlap.** Elasticsearch CPU **falls 30%** while our engine CPU rises
20%. Sequential paging issues ~10,000 round trips that each fan out to all six shards and merge —
roughly 60,000 shard operations; sliced paging sends each page to one shard. So the 3.37× is not
parallelism hiding latency behind more total work: the cluster does measurably less of it.

**Scope.** Slicing applies to extractions without `ORDER BY` or `LIMIT`, which is the shape of every
large extraction in this document. An ordered or bounded query still pages sequentially, and a
1-shard index cannot slice at all — `min(1, 8) = 1` — which is the other reason section 6's 1-shard
column is what it is.


## 7. Calibration, and where Trino is stronger

### Calibration — reference points, not alternatives

Two things in this document extract faster than either SQL engine. Neither is a candidate for the
workload this benchmark is about: one stops at a million rows and cannot join two fact indices, the
other is a hand-maintained Query DSL document. They are measured and published because a reader who
knows Elasticsearch will ask about them, and an unmeasured question reads as an avoided one. They
calibrate the scale; they do not compete on it.

- **Up to one million rows, Elasticsearch itself extracts faster than either engine** — ES|QL
  returns 1M rows in **0.32 s over Arrow** and 0.98 s over JSON, against our 3.99 s and Trino's
  4.42 s, and a 100-row fetch in **6 ms** against our 37 ms and Trino's 65 ms (section 3). Its
  1,000,000-row ceiling and its refusal to join two fact indices are why it is absent from the rest
  of this document. On the segment it does cover — result sets under a million rows, no joins, no
  SQL — it is a credible answer, and this document is not the argument against it.
- **ES|QL ties us on the pushed-down aggregation.** The two are level — **0.034 s for ES|QL against
  our 0.043 s** — a difference well inside the noise band this document flags for cells that small
  (run-to-run spreads run 20–90% of the median there). The honest statement is a tie.
- **What the extraction gap bounds.** ES|QL reads columnar `doc_values`; both SQL engines read
  `_source` and pay a JSON parse per document. The whole S1m gap between ES|QL's Arrow route and
  ours is **3.67 s per million rows** (3.99 s against 0.32 s) — an upper bound on what that leg can
  be worth, since the two routes also differ in where they run and what they cross. It is the
  largest identified item left on our extraction path: a roadmap line, not a defeat.
- **A hand-written sliced scroll is the extraction floor** — six processes, one slice per shard,
  building the same Arrow table: **13.76 s against our 11.91 s** (section 3). It answers exactly one
  query shape, written in JSON rather than SQL, at 8.5× our client CPU and 4.0× our client memory.

### Where Trino is stronger

A fair benchmark names the other system's strengths.
- **Aggregation over a sharded index** — given shards to parallelise over, Trino's `GROUP BY`
  wall-clock improves **5.2×** (28.54 s on one shard to 5.50 s on six), the largest single topology
  gain either engine showed here. It still moves 1.39 GB off the cluster to get there.
- **Cross-index joins — Trino wins all three**, and by clear margins: **J0 by 7.0%** (7.43 s against our
  7.99 s), **J1 by 12.4%** (3.44 s against 3.93 s) and **J2 by 22.3%** (6.66 s against 8.56 s). It is
  also the more efficient at aggregating over a join — adding the `GROUP BY` of J2 costs Trino
  **less than nothing** (J2 is 0.77 s *faster* than its own J0) against **+0.57 s** for us.
  All three come from one counter-balanced block in which both engines were measured together, both
  join legs pre-warmed.
- **A 100-row fetch is a tie once a name lookup is involved** — our 37 ms against Trino's 65 ms is
  worth about one hostname resolution in our gRPC client: dialling a name instead of an IP costs us
  **17 ms** (37 ms → 54 ms), because grpc-go resolves names itself rather than through the host
  resolver. On any scenario larger than this one it is invisible.
- **Its page size is not tuned in these results, and tuning it barely helps** — raising
  `elasticsearch.scroll-size` from its default 1000 to 5000 moves Trino's S1 wall clock from
  44.70 s to **44.41 s**, **0.7%**. On this cluster the knob has almost nothing left to give, which
  is consistent with Trino being bound by its own row processing rather than by paging (section 6).
- **A lower-memory Arrow client exists, and it is a real win** — via connectorx, Trino lands an
  Arrow table in **617 MB against our 921 MB**, using **33% less client memory than we do** on the
  headline scenario, and it is the closest client to us on wall clock (14.66 s against 11.91 s).
  Anyone quoting a multiple from this document should quote it against this route, not the
  documented one. (It does not extend to every destination — see S5, where connectorx needs a 3 GB
  container and we complete in 2 GB.)
- **Distributed execution beyond three nodes, spill-to-disk, and fault tolerance** — Trino scales a
  single query across a cluster far larger than the 3 nodes it runs on here, and spills to disk;
  this benchmark exercises none of that.
- **Connector breadth** — Trino federates 40+ data sources; SoftClient4ES is Elasticsearch-focused.
- **Licensing** — Trino is Apache-2.0 throughout; SoftClient4ES gates result-set size by licence
  tier (see Reproduction).


## 8. Reproduction

Prerequisites: Docker, Python 3.12. Bring up the stack (`docker compose up -d`), then run the
scenarios from `runners/`. The whole 6-shard matrix is one script —
`/bin/bash runners/run_full_session.sh` — with `run_full_session_phase2.sh` for S5/S6/joins,
`run_full_session_phase3.sh` for the 1-shard topology sensitivity, and
`run_sequential_ab.sh` for section 6.1's paging A/B. Full instructions are in
[README.md](README.md); fairness rules and metric definitions are in [METHODOLOGY.md](METHODOLOGY.md).

**Invoke the scripts with `/bin/bash`, not `bash`.** On a Mac with Homebrew's Intel build first on
`PATH`, `bash` runs under Rosetta and translates every child process, including the Python whose
CPU time is a headline metric. The runners detect this and refuse rather than report a translated
number, which is what the absolute path avoids.

**Two cluster-level preconditions are not in `docker-compose.yml`,** because they are cluster
settings rather than container settings, and they do not survive `docker compose down -v`:

- `esql.query.result_truncation_max_size` must be raised to 1,000,000 or every ES|QL cell above
  1,000,000 rows silently truncates (HTTP 200, no warning header). `runners/ensure_engines.py`
  now asserts and sets it as part of the pre-flight, after a session aborted 71 minutes in because
  it had reverted to its 10,000 default.
- The benchmark index must exist with the topology the tables claim. `generator/` builds it;
  `generator/select_topology.py` opens exactly one of the two 10M indices so neither sits in the
  other's page cache.

**Licensing.** Extracting the full 10M rows (S1/S2) requires a licence tier whose result-set quota
exceeds 10M; the licence changes a quota only, not the data path, the batch size or the wire format.
With no licence the harness runs on the Community tier, which reproduces every scenario's shape at
reduced (`--limit`) scale — and the runners assert exact row counts, so a quota-truncated run aborts
rather than reporting a fast, wrong number.

**To reproduce the full 10M-row scenarios**: a time-limited Enterprise evaluation licence for
deeper testing is available on request, with no contractual obligation attached — write to
**sales@softclient4es.com**.

⚠️ **From extensions 0.3.0 onward a self-signed licence no longer verifies.** Licence signatures are
checked against a trust root compiled into the artifact, and `SOFTCLIENT4ES_LICENSE_PUBLIC_KEY` is
ignored. A licence that does not verify degrades to Community silently — the row-count assert is what
turns that into a visible failure rather than a truncated result.

**Evidence.** Every measured run of every session is published under `results/<session>/` — one JSON
per run with its wall, CPU, memory, wire bytes, host load and memory-pressure reading, plus the
equivalence gate, the ES|QL truncation and join probes, the Trino splits probes for both topologies,
the connect probe, the engine-quiescence records and the resolved sidecar image digest. Records that
were measured but are **not** used by any table are quarantined in place, each with a README stating
why: `void-5slice-floors/` (a floor at the wrong parallelism), `void-order-effect/` (joins measured
cold), plus `COLD-CACHE-NOTE.md` and `JOIN-PASS-NOTE.md` for the two substitutions this document
makes. A third party who cannot re-run the matrix can still audit the dispersion behind every median,
the gates that had to pass before a number was kept, and what was discarded and on what grounds.

**Verification.** `runners/verify_claims.py` re-derives every figure in this document from those
JSON records and fails if a number here is not supported by them. It also checks provenance: the
image tag and digest printed in this document must match the ones the session actually ran, read
from `results/<session>/sidecar-image.txt`, and any other version string in the repository must be
on an explicit allowlist with a stated reason.

*This document was measured on session `20260821T041841-v030-prewarm` and its companions, listed in
section 1.*

# Results — SoftClient4ES (Arrow Flight SQL) vs Trino Elasticsearch connector

This benchmark measures how efficiently each system extracts data **out of Elasticsearch** into a
Python data-science client (pandas, polars, DuckDB). Both run against the same single-node
Elasticsearch, the same 10-million-row index, on the same host, back to back.

**Who this is for:** data teams pulling large result sets out of Elasticsearch into Python. It is
**not** a distributed-analytics, federation, or SQL-coverage comparison — for those, Trino is the
right tool and this benchmark deliberately does not exercise its strengths.

**Summary.** For work Elasticsearch can compute, SoftClient4ES pushes it into the cluster and moves
almost no data off it — an aggregation returning 100 rows moves **24 KB against 1.4 GB**. For large
extractions it runs in far less client memory: it lands 10M rows in a **2 GB** container where
Trino's client needs more, and completes **five concurrent extractions in an 8 GB budget where
Trino's stock client completes none and its fastest client completes two**. It is also faster,
because it delivers Arrow columnar batches the client consumes directly, where Trino's Python client
materialises rows into Python objects. These results hold on a **5-shard index read by a 3-node
Trino cluster given 1.5× the CPU** — where the extraction gap widens rather than closes (section 6).

**Where this does not apply.** Below one million rows, Elasticsearch's own query language extracts
faster than either engine here and can hand back Arrow directly — section 3 publishes those cells,
including the ones we lose. (On the pushed-down aggregation it is the other way round: SoftClient4ES
answers S3 in 0.04 s against ES|QL's 0.16 s.) ES|QL cannot return more than 1,000,000 rows, which is
why the ten-million-row scenarios have no ES|QL column. Trino is ahead on cross-index joins — by
about 3% on two of the three, and by nothing at all on the third — is faster on the one-million-row
extraction, gains the most from shard parallelism on aggregate-heavy work, and offers capabilities
(distributed execution, spill, connector breadth) this benchmark does not exercise.

All figures below were measured on the **released** sidecar image
`softnetwork/softclient4es8-arrow-flight-sql:0.2.5`
(digest `sha256:d4fb6950f09f…`), Trino 483, Elasticsearch 8.18.3.

---

## 1. Environment

| Item | Value |
|---|---|
| SoftClient4ES sidecar | `softclient4es8-arrow-flight-sql:0.2.5` (Arrow Flight SQL, gRPC :32010) |
| Trino | 483 (official image), Elasticsearch connector |
| Elasticsearch | 8.18.3, single node |
| Index | `bench_events_10m` — 10,000,000 documents, flat mapping, 1 primary shard, 0 replicas, force-merged (916 MB) |
| Host | Apple M4 Max, macOS (Darwin arm64), Docker Desktop VM 10 CPU / 15.6 GiB |
| Container limits | Elasticsearch, sidecar, Trino each capped at 4 CPU / 4 GB (heap 2 GB) |
| Sensitivity topology (§6) | `bench_events_10m_s5` — same corpus, same seed, **5 primary shards**; Trino as a 3-node cluster (coordinator + 2 workers, 6 CPU / 8 GB total) |
| ES\|QL | Elasticsearch's own query language, measured as a third stack wherever it can run. Its cells were taken with `esql.query.result_truncation_max_size` raised from its 10,000 default to 1,000,000 — the product maximum; every ES\|QL run records the effective value. |
| Runs | 2 warm-ups, then 5 measured runs per cell; each run in a fresh client process. Tables show the median and the min–max spread. |

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

**Correctness gate, run before the timings.** Row counts alone do not establish that a pushed-down
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

**Measurement provenance.** The single-shard extraction matrix — floors, S1, S1m, S1r, S2, S3, S4,
both ES|QL wire formats, the tuned-Trino arm, the hostname-dial arm and the drift control — is
**one session, `results/20260817T223632`**: 165 measured runs, one gate, one host state. Three
scenarios cannot share it because each rebuilds its own container topology, so each is its own
session, run the same night on the same host and the same image: S5 (`capped-20260818T005133`,
`capped-cx-…`), S6 (`concurrent-20260818T005133`, `concurrent-cx-…`) and the joins
(`join-20260818T005133`). Section 6 regenerates the corpus into a 5-shard index and reconfigures
Trino into a cluster, so it is its own session too (`20260818T013834-5shard`). Every table names
the session it draws on; no table mixes two.

- *Drift.* Runs are blocked by stack, so the first stack's block and the second's are separated in
  time. To bound the drift that would otherwise be confounded with engine identity, the first
  stack's S1 block is re-run at the **end** of the session: 35.04 s at the start against 35.23 s at
  the end, **+0.5% against a run-to-run spread of 1.0% within the block**. The drift is smaller than
  the block's own noise, and two orders of magnitude smaller than the S1 gap it could bias.
  (This session's S1 ratio, 1.46×, reproduces the 1.43× and 1.41× measured on 2026-08-13 and
  2026-08-16 on the same image.)
- *Host load.* The client process runs on the host while the engines run in the Docker VM, so host
  contention could in principle inflate the client's wall clock. The 1-minute load average sampled
  with every run had a median of 5.7 and a maximum of 12.0 against 16 logical cores: at the worst
  moment of the session, 4 cores were uncommitted. The harness refuses to measure above a
  configured load ceiling rather than measuring and hoping.
- *Host memory pressure.* Peak client memory is the axis on which the two clients differ most, so
  the host's own memory state is sampled with every run too. macOS reported **pressure level 1
  (normal) on all 165 runs**, with **0 MB of swap in use throughout**, so no figure here was taken
  from a host that was paging.

**Metrics.** *Wall* is end-to-end time including connection. *Client CPU* is `time.process_time()`
in the client process. *Peak client memory* is the process's peak physical footprint
(`ri_lifetime_max_phys_footprint`), which is immune to the macOS memory compressor. *ES wire* is the
bytes that left Elasticsearch, measured at **the Elasticsearch container itself** in every table in
this document, section 6 included — so the figure never depends on how many containers the engine is
made of. Every run asserts the exact expected row count before its timing is recorded; a run that
returns the wrong number of rows is discarded, not reported.

---

## 2. Scenarios at a glance

| ID | Question it answers | Outcome |
|---|---|---|
| **S0** | What does a single-process Python scroll client cost as a reference floor? | 45.7 s to read 10M rows |
| **S0p** | And a *sliced* scroll — 5 slices, 5 processes, the floor a client that parallelises gets? | **23.2 s**, for 2.9× SoftClient4ES's client CPU on S1 |
| **S1** | Extract 10M rows into a client-side columnar table | SoftClient4ES **1.46× faster**, 4.7× less CPU, 4.9× less memory |
| **S1m** | Extract **1,000,000** rows — the only scale all three stacks can reach | **ES\|QL 0.60 s**, Trino 5.25 s, SoftClient4ES 7.64 s |
| **S1r** | Extract 10M rows into a **pandas / polars DataFrame** (what an analyst builds) | SoftClient4ES **40–43% faster** on far less CPU and memory |
| **S2** | Extract 10M rows and compute an aggregate in DuckDB | SoftClient4ES **1.68× faster**, 7.9× less memory |
| **S3** | `GROUP BY` returning 100 rows | SoftClient4ES **0.04 s vs 25 s**, and moves **24 KB against 1.4 GB** off the cluster |
| **S4** | Fetch 100 rows (`LIMIT 100`) | Parity — 38 ms vs 56 ms |
| **S5** | Does the extraction fit in a constrained-memory container? | SoftClient4ES fits **2 GB**; Trino's stock client never fits, its fastest client fits 3 GB |
| **S6** | How many concurrent extractions fit in an 8 GB budget? | SoftClient4ES **5** (50M rows); Trino **2** with its fastest client, **0** with its stock one |
| **J0–J2** | Cross-index JOIN landed as a DataFrame | Trino faster by ~3% on two of three, a dead heat on the plain join; SoftClient4ES does 7–18× less client work |
| **§6** | Do the results survive a 5-shard index read by a 3-node Trino? | Yes — the S1 gap **widens to 1.51×**; client cost and pushdown unchanged; Trino's `GROUP BY` gains 4.3× |

---

## 3. Extraction scenarios

*(Session `20260817T223632` throughout this section.)*

### S0 / S0p — the reference floors

A plain Python client that scrolls Elasticsearch and parses each JSON hit, counting the rows and
throwing them away. Neither floor builds a usable artifact — no Arrow table, no DataFrame — which is
what makes them floors rather than contenders.

| Floor | Wall | Client CPU | Peak client memory |
|---|---|---|---|
| **S0** — one process, one scroll | 45.7 s | 14.5 s | 26 MB |
| **S0p** — 5 slices, 5 processes | **23.2 s** | 14.8 s (summed over the processes) | 130 MB (summed) |

**Outcome.** Scrolling single-threaded is the simplest approach, not the fastest one available to a
client that is willing to work for it: a sliced scroll halves the wall clock for almost the same
total CPU, and at 23.2 s it is **faster than either engine's S1** on this index. It costs **2.9× the
client CPU SoftClient4ES spends on the same ten million rows in the same session** (14.8 s against
5.2 s) and returns nothing you can compute on — but a reader who knows Elasticsearch would supply
this comparison themselves, so it is measured here rather than left out. On this single-shard index
the gain comes from overlapping request round-trips rather than from shard parallelism — the summed
CPU barely moves — so a multi-shard index would be expected to help this floor further, exactly as
it helps Trino. That variant was not run, and no figure for it is published here.

### S1 — extract 10M rows into a columnar client table

The full 10-million-row result set materialised in the client. SoftClient4ES fetches an Arrow table;
Trino's stock client fetches rows.

```python
# SoftClient4ES — Arrow Flight SQL (adbc_driver_flightsql)
cur.execute("SELECT id, event_ts, amount, qty, status, country, category, name "
            "FROM bench_events_10m")
table = cur.fetch_arrow_table()          # Arrow columnar batches

# Trino — stock client (trino.dbapi)
cur.execute(same_sql)
rows = cur.fetchall()                    # list of Python tuples
```

| Metric | SoftClient4ES | Trino | Ratio |
|---|---|---|---|
| Wall median | **35.0 s** (spread 1.0%) | 51.2 s (spread 2.7%) | **1.46× faster** |
| Client CPU | **5.2 s** | 24.2 s | 4.7× less |
| Peak client memory | **918 MB** | 4,462 MB | 4.9× less |
| ES wire | 2,492 MB | 2,923 MB | — |
| Rows / columns | 10,000,000 / 8 | 10,000,000 / 8 | ✓ |

**Outcome.** SoftClient4ES is faster and far lighter. The reason is client representation: the client
consumes Arrow batches without ever building 10 million Python objects, which is what dominates
Trino's client CPU and memory.

**Fairness — measured against Trino's fastest clients.** Trino's stock client is not its only option.
[connectorx](https://github.com/sfu-db/connector-x) (a Rust engine that parses Trino's result pages
straight into columnar buffers) and the [ADBC Trino driver](https://adbc-drivers.org/drivers/trino/)
both return an Arrow table. Measured on the same query, in the same session:

| Route | Wall | Client CPU | Peak memory |
|---|---|---|---|
| **SoftClient4ES** (Arrow Flight SQL) | **35.0 s** | 5.2 s | 918 MB |
| Trino — ADBC driver | 50.9 s | 19.1 s | 1,944 MB |
| Trino — stock client | 51.2 s | 24.2 s | 4,462 MB |
| Trino — connectorx | 51.3 s | 11.4 s | **698 MB** |

SoftClient4ES keeps the wall-clock lead against every Trino client (1.45× against the fastest of
them). On client cost the picture is honest: connectorx, building contiguous buffers in Rust,
reaches a lower peak memory (698 MB) than our chunked Arrow batches. The wall-clock advantage comes
from the server and the wire, not from any client library. Note that 698 MB is a bare Arrow *table*:
when the same workflow materialises the whole result as a DataFrame (scenario S5), connectorx needs
2.9 GB where SoftClient4ES needs 1.5 GB — so this memory advantage does not carry to the DataFrame
an analyst actually keeps.

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
| **SoftClient4ES** (`to_pandas()`) | **35.8 s** | 5.4 s | 1,476 MB |
| Trino — connectorx | 50.0 s | 11.9 s | 2,824 MB |
| Trino — ADBC driver | 50.2 s | 19.5 s | 2,539 MB |
| Trino — SQLAlchemy (stock) | 62.7 s | 35.5 s | 7,968 MB |

Against Trino's stock route that is **43% faster on 6.6× less CPU and 5.4× less memory**; against its
fastest route, 28% faster. With Arrow-backed pandas dtypes (`types_mapper=pd.ArrowDtype`),
SoftClient4ES needs only **915 MB** (35.5 s), versus 7,799 MB for the stock Trino route.

**To a polars DataFrame:**

| Route | Wall | Client CPU | Peak memory |
|---|---|---|---|
| **SoftClient4ES** (`pl.from_arrow`) | **37.1 s** | 14.4 s | 2,481 MB |
| Trino — connectorx | 49.9 s | 11.7 s | **2,225 MB** |
| Trino — ADBC driver | 51.0 s | 28.2 s | 3,539 MB |
| Trino — SQLAlchemy (stock) | 61.6 s | 35.0 s | 11,045 MB |

**Outcome.** SoftClient4ES lands a DataFrame faster on every route and destination — 40% faster than
Trino's stock route on polars, 43% on pandas, and 26–28% faster than its fastest route on either. On
the polars destination both sides re-encode Arrow strings into polars' native format, which raises
our client CPU; connectorx does the same re-encoding in Rust and comes in slightly lower on CPU and
memory. The durable advantage, again, is wall-clock, held on every route.

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
| Wall median | **35.3 s** | 59.3 s | **1.68× faster** |
| Client CPU | **6.3 s** | 32.5 s | 5.2× less |
| Peak client memory | **946 MB** | 7,506 MB | 7.9× less |

**Outcome.** The zero-copy Arrow hand-off to DuckDB is the largest memory advantage in the benchmark
(7.9×): the columnar result is scanned in place instead of being copied through Python objects.

### S3 — `GROUP BY` returning 100 rows

`SELECT category, COUNT(*), AVG(amount) FROM bench_events_10m GROUP BY category`, returning 100 groups.

| Metric | SoftClient4ES | Trino |
|---|---|---|
| Wall median | **0.04 s** | 25.3 s |
| ES wire | **24 KB** | 1,394 MB |
| Rows | 100 | 100 |

**The same answer, not merely the same row count.** A `terms` aggregation is approximate when its
bucket size is too small, so "100 groups" is not by itself evidence that the pushed-down result
matches a full scan. Every session gates on the values: all 100 `(category, COUNT, AVG(amount))`
triples are compared across the three stacks — identical counts, averages equal to within 1e-9 — and
Elasticsearch reports `doc_count_error_upper_bound = 0` and `sum_other_doc_count = 0` for the
aggregation, which is the cluster's own statement that no bucket is missing and no document fell
outside the returned buckets. A push-down that returned a different answer faster would not be a
result.

**Outcome.** SoftClient4ES compiles the `GROUP BY` into an Elasticsearch `terms` aggregation: the
cluster computes the 100 groups and returns only those 100 rows — 24 KB of aggregation response
against the 1,394 MB Trino reads to compute the same answer, a factor of 57,000. Trino's Elasticsearch connector
performs predicate push-down only (per its documentation), so it scans all 10M rows into Trino and
aggregates there. This is the clearest architectural difference in the benchmark — for work
Elasticsearch can do, SoftClient4ES does not move the data at all.

### S1m — extract 1,000,000 rows: the only scale all three stacks can reach

Elasticsearch's own query language cannot return more than 1,000,000 rows (see below), so this is
the largest result set on which SoftClient4ES, Trino and ES|QL can be compared like for like. Same
eight columns, same index, `LIMIT 1000000` on every stack.

| Route | Wall | Client CPU | Peak client memory | Bytes off the cluster |
|---|---|---|---|---|
| **ES\|QL, `format=arrow`** | **0.60 s** | 0.09 s | 166 MB | 72 MB |
| ES\|QL, `format=json` | 1.30 s | 0.59 s | 663 MB | 86 MB |
| Trino, stock client | 5.25 s | 2.25 s | 473 MB | 291 MB |
| SoftClient4ES, Arrow Flight SQL | 7.64 s | **0.23 s** | 177 MB | 495 MB |

**Outcome — we lose this one, on wall clock, to both.** ES|QL is an order of magnitude faster than
anything else measured here, and Trino is 1.45× faster than SoftClient4ES. Three things are worth
separating from that.

First, the client cost still splits the way the rest of the benchmark predicts: SoftClient4ES spends
0.23 s of client CPU against Trino's 2.25 s, because the client is consuming Arrow batches rather
than building a million Python tuples.

Second, the reason ES|QL is fast is not the wire: it reads `doc_values` — columnar on disk — while
both engines read `_source` and pay a JSON parse per document. That is a genuine architectural
advantage, and it is bounded by the ceiling in the next section rather than by anything either
engine does.

Third — and this is the part that belongs to us — **the loss against Trino is a wire-volume loss,
and the volume is ours to explain.** For the same 1,000,000 rows SoftClient4ES moved **495 bytes per
row off the cluster against 291 for Trino**, and against **249 bytes per row on its own
ten-million-row run** in the same session. Its throughput barely moves between the two — 64.7 MB/s
here against 71.1 MB/s on S1, within 9% — so the time is going into bytes, not into the client: at
the rate it actually sustains, the ~249 MB the result needs would take **3.8 s**, and the measured
**7.64 s** is what moving 495 MB costs. The two runs take different paths inside SoftClient4ES: an
unbounded `SELECT` streams through the scroll pager, while an explicit `LIMIT` above the index's
result window takes the bounded paging path. Only the bounded one shows the doubling. It is filed
for investigation and is a defect on our side, not a property of the comparison — but until it is
fixed the number stands as measured, and Trino wins this scale.

### ES|QL — Elasticsearch's own query language, and where it stops

A benchmark of two SQL engines over Elasticsearch that never measures what Elasticsearch itself can
do invites an obvious question. So it is measured, over both of its wire formats: the row-shaped
`format=json` every ES|QL client speaks, and `format=arrow`, which returns an Apache Arrow IPC
stream. All four columns below are the same session as the sections above.

| Scenario | ES\|QL (arrow) | ES\|QL (json) | SoftClient4ES | Trino |
|---|---|---|---|---|
| S1m — 1,000,000 rows | **0.60 s** | 1.30 s | 7.64 s | 5.25 s |
| S3 — `GROUP BY`, 100 rows | 0.22 s | 0.16 s | **0.04 s** | 25.31 s |
| S4 — `LIMIT 100` | 0.006 s | **0.004 s** | 0.038 s | 0.056 s |
| S1, S2, S5, S6 — 10,000,000 rows | *cannot run* | *cannot run* | ✓ | ✓ |

**Where it wins.** Below its ceiling ES|QL is the fastest way to get rows out of Elasticsearch that
we measured: on a 100-row fetch its whole round trip (4 ms) costs less than SoftClient4ES's
connection handshake alone (13.0 ms). Trino's handshake is cheaper still (1.7 ms); its 56 ms total
is spent elsewhere.

**Where it does not.** On the pushed-down aggregation SoftClient4ES is 3.8× faster (0.04 s against
0.16 s) while both move kilobytes rather than gigabytes off the cluster — 24 KB for us, 41 KB for
ES|QL. The work is identical; the difference is what the result travels over.

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
excellent answer and this benchmark is not the argument for anything else.

### S4 — fetch 100 rows (`LIMIT 100`)

The control: when the result is small, the client representation stops mattering.

| Metric | SoftClient4ES | Trino |
|---|---|---|
| Wall median | 0.038 s | 0.056 s |

**Outcome.** Near parity, as expected. The extraction advantage only appears at scale, or when work
can be pushed into Elasticsearch.

**What the 18 ms actually is, and a control that removes it.** At this size the whole scenario is
connection setup: SoftClient4ES's ADBC Flight SQL handshake takes 13.0 ms of the 38, Trino's 1.7 ms
of the 56. Both clients dial an IP literal here, deliberately — because when SoftClient4ES is
pointed at `localhost` instead, its connect cost rises by ~15 ms and the S4 median moves to
**0.056 s, exactly Trino's**. The margin on this control is therefore worth about as much as a name
lookup, and it disappears entirely if the client resolves one. The cause is client-side and
measured: a four-layer probe (`connect-probe.json`) separates a bare TCP connect (0.07 ms) from the
Flight C++ layer (1.6 ms), the Go ADBC layer dialling an IP (2.8 ms) and the same layer dialling a
name (18.1 ms). grpc-go performs its own resolution rather than using the OS resolver, which is
where the 15 ms goes; Trino's Python clients use the OS resolver and pay 0.06 ms for the same name.
It is filed on our side. No other scenario in this document is affected — 15 ms is invisible against
35 seconds — but on a 100-row fetch it is the whole result.

---

## 4. Constrained-memory and concurrency

### S5 — does the extraction fit in a small container?

*(Sessions `capped-20260818T005133` and `capped-cx-20260818T005133` — the whole table from one sweep
per client, rather than the three sessions the first published version drew on.)*

The client runs inside a container with a hard memory cap (`docker run --memory`, swap pinned equal
so the kernel kills rather than pages). The question is binary: does landing 10M rows as a DataFrame
complete, or is the process killed?

**Whole result set as one DataFrame:**

| Container cap | SoftClient4ES | Trino (stock) | Trino (connectorx, fastest) |
|---|---|---|---|
| 8 GB | ✅ 32.5 s · 1,533 MB | ❌ OOM-killed | ✅ 51.9 s · 2,902 MB |
| 6 GB | ✅ 35.7 s · 1,531 MB | ❌ OOM-killed | ✅ 49.7 s · 2,903 MB |
| 4 GB | ✅ 36.5 s · 1,527 MB | ❌ OOM-killed | ✅ 51.5 s · 2,913 MB |
| 3 GB | ✅ 36.1 s · 1,535 MB | ❌ OOM-killed | ✅ 49.3 s · 2,909 MB |
| **2 GB** | ✅ **36.1 s · 1,531 MB** | ❌ OOM-killed | ❌ **OOM-killed** |

*"OOM-killed" is the kernel killing the client — exit 137 — in every cell above, verified per cell.*

**Streaming, where neither side holds the whole result** — SoftClient4ES via
`fetch_record_batch()`, Trino via `pandas.read_sql(chunksize=…)`. This is the workflow a competent
Trino user reaches for first, so it is a table rather than a footnote:

| Container cap | SoftClient4ES | Trino (stock) |
|---|---|---|
| 8 GB | ✅ 36.9 s · 148 MB | ✅ 58.2 s · 302 MB |
| 6 GB | ✅ 36.9 s · 150 MB | ✅ 58.3 s · 304 MB |
| 4 GB | ✅ 37.1 s · 150 MB | ✅ 58.5 s · 302 MB |
| 3 GB | ✅ 37.2 s · 147 MB | ✅ 58.7 s · 302 MB |
| **2 GB** | ✅ **36.9 s · 149 MB** | ✅ **58.3 s · 299 MB** |

Both complete at every cap. SoftClient4ES is **1.57–1.58× faster on 2.0× less memory** — a smaller
and more representative difference than the binary one above, and the one that applies whenever the
workflow does not need the whole result set in memory at once.

**Outcome.** Landing the whole 10M-row DataFrame, SoftClient4ES fits in a **2 GB** container. Trino's
stock client is killed at every cap measured, 8 GB included; its fastest client (connectorx) fits at
3 GB but is killed at 2 GB — the cap where SoftClient4ES still completes. Both requirements are
independent of the cap (peak memory moves by under 1% across a 4× range), so they reflect the data,
not memory thrashing.

The binary framing is the harder claim, and it should be read next to the streaming table above: it
answers "can I materialise this result in a small container", not "can this pipeline run at all".
Streaming, both engines run everywhere, and the difference narrows to 1.58×.

### S6 — concurrent extractions in a fixed memory budget

*(Sessions `concurrent-20260818T005133` and `concurrent-cx-20260818T005133`.)*

An 8 GB total client budget, split evenly across N clients launched simultaneously, each extracting
10M rows. How many complete with the correct row count?

Measured against **both** of Trino's clients: the stock one it ships, and connectorx — the fastest
client it has, and the one S1 already grants it. Reporting only the stock client here would abandon
three scenarios later the fairness rule S1 sets.

| Engine / client | N | Cap each | Completed | Wall |
|---|---|---|---|---|
| SoftClient4ES | 1 | 8,192 MB | 1/1 | 36.5 s |
| SoftClient4ES | 2 | 4,096 MB | 2/2 | 36.8 s |
| SoftClient4ES | 3 | 2,730 MB | 3/3 | 37.3 s |
| SoftClient4ES | 4 | 2,048 MB | 4/4 | 38.4 s |
| **SoftClient4ES** | **5** | **1,638 MB** | **5/5** | **40.3 s** |
| Trino — connectorx | 1 | 8,192 MB | 1/1 | 50.3 s |
| **Trino — connectorx** | **2** | **4,096 MB** | **2/2** | **51.9 s** |
| Trino — connectorx | 3 | 2,730 MB | 0/3 | killed |
| Trino — stock client | 1 | 8,192 MB | 0/1 | killed |

**Outcome.** In an 8 GB budget, SoftClient4ES completes **five** concurrent extractions — 50 million
rows for 5× the work at **+10% wall time** — against **two** for Trino's fastest client and none for
its stock one. The ratio is 2.5×, not the 5-versus-nothing the stock client alone would suggest, and
it follows directly from what each client holds per extraction. This measures client-side capacity;
it does not measure server-side concurrency, which is a separate question this benchmark does not
address.

---

## 5. Cross-index JOIN (J0–J2)

*(Session `join-20260818T005133`; medians of 5 runs, as everywhere else.)*

A join between a 1M-row index (`bench_1m`) and the 10M-row index, landed as a pandas DataFrame on
both sides. Both engines execute the join server-side (SoftClient4ES in an embedded DuckDB, Trino in
its own engine).

```sql
-- J0  plain join                    (1,000,000 rows out)
SELECT a.id, b.amount FROM bench_1m a JOIN bench_events_10m b ON a.id = b.id
-- J1  + predicate on the large leg  (125,361 rows out)
SELECT a.id, b.amount FROM bench_1m a JOIN bench_events_10m b ON a.id = b.id WHERE b.status = 'paid'
-- J2  + GROUP BY                     (100 rows out)
SELECT b.category, COUNT(*), AVG(b.amount) FROM bench_1m a JOIN bench_events_10m b ON a.id = b.id GROUP BY b.category
```

| Scenario | SoftClient4ES wall | Trino wall | Client CPU (SC4ES / Trino) |
|---|---|---|---|
| **J0** plain join | 26.78 s | 26.77 s (**0.0%**) | 0.07 s / 1.21 s |
| **J1** + `WHERE` | 10.64 s | 10.30 s (3.3% faster) | 0.02 s / 0.21 s |
| **J2** + `GROUP BY` | 28.80 s | 27.90 s (3.2% faster) | 0.02 s / 0.10 s |

These are small differences, so the spread belongs next to them:

| Scenario | SoftClient4ES min–max | Trino min–max | Gap | Do the ranges overlap? |
|---|---|---|---|---|
| J0 | 26.32–27.14 s (3.0%) | 26.28–27.73 s (5.4%) | 0.0% | yes, almost entirely |
| J1 | 10.59–10.67 s (0.8%) | 10.26–10.31 s (0.5%) | 3.3% | **no** |
| J2 | 28.67–28.82 s (0.5%) | 27.39–27.92 s (1.9%) | 3.2% | **no** |

**Outcome.** Trino is faster on the two joins where a difference is measurable — J1 and J2, by 3.3%
and 3.2%, with ranges that do not overlap — and **the plain join J0 is a dead heat**: the medians
differ by 7 milliseconds in 27 seconds, and the ranges overlap almost completely. SoftClient4ES does
7–18× less client-side work in every case. The marginal cost of adding a `GROUP BY` to the join is
higher for SoftClient4ES (+2.0 s vs +1.1 s), so on join-side aggregation Trino is the more efficient
engine, and that is the clearest of the three results.

---

## 6. Index topology sensitivity — 5 shards, read by a 3-node Trino

*(Session `20260818T013834-5shard`, with its own equivalence gate against the 5-shard index.)*

Everything above was measured on a single-shard index. Trino's Elasticsearch connector creates one
split per shard, so a single-shard index gives it a single reader — which invites the fair question
of whether these results survive an index that lets Trino parallelise, read by a Trino cluster
rather than one node.

They do, and the extraction gap widens.

**Setup.** Same host, same corpus regenerated from the same seed into a **5-shard** index, one
segment per shard. Trino runs as a real cluster — a dedicated coordinator plus two workers, with
`node-scheduler.include-coordinator=false` so the coordinator plans and serves results while the
workers scan. Trino is deliberately given **6 CPU / 8 GB against SoftClient4ES's 4 CPU / 4 GB**:
1.5× the CPU and 2× the memory. Medians of 5 runs after 2 warm-ups, as everywhere else. The
parallelism under test was verified rather than assumed: a live probe of `system.runtime.tasks`
during the scan recorded **5 splits, 3 on one worker and 2 on the other, none on the coordinator**
(`trino-splits-probe.json`).

| Metric | SoftClient4ES 1 shard | **5 shards** | Trino 1 shard | **5 shards** |
|---|---|---|---|---|
| S1 wall | 35.0 s | **29.8 s** | 51.2 s | **45.2 s** |
| S1 client CPU | 5.2 s | 4.7 s | 24.2 s | 24.1 s |
| S1 peak client memory | 918 MB | 919 MB | 4,462 MB | 4,463 MB |
| S1 ES wire | 2,492 MB | 2,551 MB | 2,923 MB | 2,949 MB |
| S3 wall (`GROUP BY`, 100 rows) | 0.04 s | 0.04 s | 25.3 s | **5.9 s** |
| S3 ES wire | 24 KB | **24 KB** | 1,394 MB | 1,419 MB |
| S5 — 10M rows in a 2 GB container | completes | completes | killed | killed |

**Outcome — extraction.** Both engines benefit from the extra shards, and SoftClient4ES benefits
slightly more: 15% faster against Trino's 12%. The S1 ratio therefore **widens from 1.46× to 1.51×**,
on a topology chosen to favour Trino and with Trino holding half again our CPU. The widening is
small; what the section establishes is that the gap does not close.

**Outcome — client cost is a property of the protocol, not the topology.** Peak client memory moves
by **0.02% on our side and 0.01% on Trino's**; client CPU is flat to slightly lower for both.
Sharding the index and adding Trino nodes does not change what the client pays, because the client
is one process consuming one wire format however large the cluster is. The same holds for the
constrained-container result: 10M rows still land as a DataFrame in 2 GB on one side and are still
OOM-killed on the other.

**Outcome — pushdown is architectural.** The `GROUP BY` still moves **24 KB off the cluster against
1,419 MB** — the 100-row answer itself, and a factor of 58,000. Five shards and two extra Trino nodes do not change this, because the connector
does not push aggregations down: it reads all 10 million rows whatever the topology, and merely
reads them faster.

**Where Trino gains most.** Its aggregation wall-clock improves **4.3×** (25.3 s → 5.9 s), by far the
largest single improvement measured in this benchmark for either engine. Scan parallelism is exactly
what a multi-shard index gives Trino, and on aggregate-heavy work it converts directly into
wall-clock.

## 7. Where Trino is stronger

A fair benchmark names the other system's strengths.

- **Below one million rows, Elasticsearch itself extracts faster than we do** — ES|QL returns 1M
  rows in 0.60 s over Arrow against our 7.64 s, and a 100-row fetch in 4 ms against our 38 ms
  (section 3). It does not beat us on the pushed-down aggregation, where we are 3.8× faster, and its
  1,000,000-row ceiling is why it is absent from the rest of this document.
- **Aggregation over a sharded index** — given shards to parallelise over, Trino's `GROUP BY`
  wall-clock improves 4.3× (25.3 s to 5.9 s on a 5-shard index), the largest single gain either
  engine showed in this benchmark. It still moves 1.4 GB off the cluster to get there.
- **Cross-index joins** — Trino is faster on the two joins where the difference is measurable (J1,
  J2, ~3% with non-overlapping ranges) and more efficient at aggregating over a join. The plain join
  is a dead heat.
- **One-million-row extraction** — Trino lands 1M rows in 5.25 s against SoftClient4ES's 7.64 s
  (S1m), 1.45× faster. The advantage does not survive to ten million rows, where the client cost it
  pays becomes the constraint, but at this scale it is real. The gap is a wire-volume gap and the
  volume is ours: 495 bytes per row against Trino's 291, on a code path — bounded `LIMIT` — that
  moves twice what the same product's unbounded path moves. It is filed as a defect, and reported
  here as measured until it is fixed.
- **A 100-row fetch is a tie once a name lookup is involved** — our 18 ms margin on S4 is worth
  about one hostname resolution in our gRPC client, and vanishes when the client dials `localhost`
  instead of an IP (section 3, S4). Filed on our side.
- **Its page size is not tuned in these results, and tuning it helps a little** — raising
  `elasticsearch.scroll-size` from its default 1000 to 5000 improves Trino's S1 wall clock from
  51.2 s to **50.7 s** (1.0%). The headline table pairs product default against product default;
  this is what the other setting is worth on this corpus.
- **A lower-memory Arrow client exists** — via connectorx, Trino can land an Arrow *table* in less
  client memory than SoftClient4ES (698 MB vs 918 MB on S1), and with less CPU and memory on the
  polars destination. (This does not extend to the full DataFrame, where SoftClient4ES uses less —
  S5.)
- **Distributed execution, spill-to-disk, and fault tolerance** — Trino scales a single query across
  a cluster and spills to disk; this single-node benchmark does not exercise any of it.
- **Connector breadth** — Trino federates 40+ data sources; SoftClient4ES is Elasticsearch-focused.
- **Licensing** — Trino is Apache-2.0 throughout; SoftClient4ES gates result-set size by licence
  tier (see Reproduction).

## 8. Reproduction

Prerequisites: Docker, Python 3.12. Bring up the stack (`docker compose up -d`), then run the
scenarios from `runners/`. The whole single-shard matrix is one script —
`/bin/bash runners/run_full_session.sh` — with `run_full_session_phase2.sh` for S5/S6/joins and
`run_full_session_phase3.sh` for the topology sensitivity. Full instructions are in
[README.md](README.md); fairness rules and metric definitions are in [METHODOLOGY.md](METHODOLOGY.md).

Extracting the full 10M rows (S1/S2) requires a licence tier whose result-set quota exceeds 10M; the
licence changes a quota only, not the data path. With no licence the harness runs on the Community
tier, which reproduces every scenario's shape at reduced (`--limit`) scale.

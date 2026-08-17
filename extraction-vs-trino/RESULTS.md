# Results — SoftClient4ES (Arrow Flight SQL) vs Trino Elasticsearch connector

This benchmark measures how efficiently each system extracts data **out of Elasticsearch** into a
Python data-science client (pandas, polars, DuckDB). Both run against the same single-node
Elasticsearch, the same 10-million-row index, on the same host, back to back.

**Who this is for:** data teams pulling large result sets out of Elasticsearch into Python. It is
**not** a distributed-analytics, federation, or SQL-coverage comparison — for those, Trino is the
right tool and this benchmark deliberately does not exercise its strengths.

**Summary.** For work Elasticsearch can compute, SoftClient4ES pushes it into the cluster and moves
almost no data off it — an aggregation returning 100 rows moves **0 vs 1.4 GB**. For large
extractions it runs in far less client memory: it lands 10M rows in a **2 GB** container where
Trino's client needs more, and completes **five concurrent extractions in an 8 GB budget where
Trino's stock client completes none and its fastest client completes two**. It is also faster,
because it delivers Arrow columnar batches the client consumes directly, where Trino's Python client
materialises rows into Python objects. These results hold on a **5-shard index read by a 3-node
Trino cluster given 1.5x the CPU** — where the extraction gap widens rather than closes (section 6).

**Where this does not apply.** Below one million rows, Elasticsearch's own query language extracts
faster than either engine here and can hand back Arrow directly — section 3 publishes those cells,
including the ones we lose. (On the pushed-down aggregation it is the other way round: SoftClient4ES
answers S3 in 0.03 s against ES|QL's 0.16 s.) ES|QL cannot return more than 1,000,000 rows, which is why the
ten-million-row scenarios have no ES|QL column. Trino is ahead on cross-index joins — unambiguously on the join
with a `GROUP BY`, within the noise on the other two — is faster on the one-million-row extraction, gains the most from shard parallelism on aggregate-heavy
work, and offers capabilities (distributed execution, spill, connector breadth) this benchmark does
not exercise.

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
| Runs | ≥2 warm-ups, then ≥5 measured runs per cell; each run in a fresh client process. Tables show the median and the min–max spread. |

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
gate. Its two halves have different reach, and the difference is stated rather than blurred:

- **All three stacks** are compared on the push-down itself — every one of S3's 100
  `(category, cnt, AVG(amount))` triples, key by key and value by value. They agreed exactly on the
  counts and to within 1e-9 on the averages.
- **SoftClient4ES and Trino** are additionally compared on `COUNT`, `SUM(id)`, `SUM(qty)` and
  `SUM(amount)` over the whole index — the check that the two extraction paths see the same
  10,000,000 documents, not merely the same number of them. In the sessions published here ES|QL
  does not take part in this half; the gate gained an ES|QL leg for it on 2026-08-17, after these
  measurements were taken.

Elasticsearch's own error bounds for the `terms` aggregation were `doc_count_error_upper_bound = 0`
and `sum_other_doc_count = 0` — i.e. the aggregation is exact at this shard count and bucket size,
not merely close. The gate's verdict is recorded as `equivalence-gate.json` in the session
directory.

**Measurement provenance.** Figures come from four sessions, and each table names the one it draws
on. The extraction matrix — S1, S1r, S2, S3, S4 — and the JOIN scenarios are the **2026-08-13**
session. The floors (S0/S0p), S1m, the ES|QL cells, the drift control, the tuned-Trino arm and S6
are the **2026-08-16** re-measurement, which also re-ran S1 and S3; where a figure from that re-run
is quoted inside a 2026-08-13 table — the ES-wire rows, which it is the first session to measure at
the cluster — the table says so. S5 draws on three sessions and its own note attributes each row.
Section 6's topology sensitivity names its own pair.

Where a control was measured twice, both values appear rather than one quietly replacing the other.
The single exception is the S5 8 GB row, where the later repeat supersedes the earlier pair outright,
for the reason that table's note gives.

- *Drift.* Runs are blocked by stack, so the first stack's block and the second's are separated in
  time. To bound the drift that would otherwise be confounded with engine identity, the first
  stack's S1 block is re-run at the **end** of the 2026-08-16 session: 36.86 s at the start against
  36.06 s at the end, a **−2.2% drift against a run-to-run spread of 0.4% within the block**. The
  drift is therefore larger than the block's own noise and is reported rather than assumed away; it
  is an order of magnitude smaller than the S1 gap it could bias. (That session's S1 medians —
  36.86 s against Trino's 51.81 s, a ratio of 1.41× — reproduce the published 1.43× measured three
  days earlier on the same image.)
- *Host load.* The client process runs on the host while the engines run in the Docker VM, so host
  contention could in principle inflate the client's wall clock. The 1-minute load average sampled
  after every run had a median of 4.0 and a maximum of 9.3 against 16 logical cores: at the worst
  moment of the session, 6.7 cores were uncommitted.
- *Host memory pressure.* Peak client memory is the axis on which the two clients differ most, so
  the host's own memory state is sampled with every run too. macOS reported **pressure level 1
  (normal) on every recorded run** of the session, so no figure here was taken from a host that was
  thrashing.

**Metrics.** *Wall* is end-to-end time including connection. *Client CPU* is `time.process_time()`
in the client process. *Peak client memory* is the process's peak physical footprint
(`ri_lifetime_max_phys_footprint`), which is immune to the macOS memory compressor. *ES wire* is the
bytes that left Elasticsearch, measured from container network counters — sampled at the
Elasticsearch container itself, so the figure does not depend on how many containers the engine is
made of. (Sessions before 2026-08-16 sampled the *engine* container's received bytes instead. The
two agree within 0.1% on this single-node topology — 2,492 MB against 2,494 for SoftClient4ES,
2,923 against 2,926 for Trino — but they are different metrics, so the ES-wire figures below are the
Elasticsearch-side ones, with one exception named where it occurs: the 5-shard SoftClient4ES column
in section 6, which is the sidecar's received bytes one hop from the cluster.) Every run asserts the
exact expected row count before its timing is recorded; a run that returns the wrong number of rows
is discarded, not reported.

---

## 2. Scenarios at a glance

| ID | Question it answers | Outcome |
|---|---|---|
| **S0** | What does a single-process Python scroll client cost as a reference floor? | 45.3 s to read 10M rows |
| **S0p** | And a *sliced* scroll — 5 slices, 5 processes, the floor a client that parallelises gets? | **23.3 s**, for 3.9× SoftClient4ES's client CPU on S1 |
| **S1** | Extract 10M rows into a client-side columnar table | SoftClient4ES **1.43× faster**, 5.5× less CPU, 5.0× less memory |
| **S1m** | Extract **1,000,000** rows — the only scale all three stacks can reach | **ES\|QL 0.51 s**, Trino 5.30 s, SoftClient4ES 7.63 s |
| **S1r** | Extract 10M rows into a **pandas / polars DataFrame** (what an analyst builds) | SoftClient4ES **42–44% faster** on far less CPU and memory |
| **S2** | Extract 10M rows and compute an aggregate in DuckDB | SoftClient4ES **1.73× faster**, 8.4× less memory |
| **S3** | `GROUP BY` returning 100 rows | SoftClient4ES **0.04 s vs 27 s**, and moves **0 vs 1.4 GB** off the cluster |
| **S4** | Fetch 100 rows (`LIMIT 100`) | Parity — 40 ms vs 57 ms |
| **S5** | Does the extraction fit in a constrained-memory container? | SoftClient4ES fits **2 GB**; Trino's stock client needs >8 GB, its fastest client 3 GB |
| **S6** | How many concurrent extractions fit in an 8 GB budget? | SoftClient4ES **5** (50M rows); Trino **2** with its fastest client, **0** with its stock one |
| **J0–J2** | Cross-index JOIN landed as a DataFrame | Trino faster on wall — clean on J2 (8.9%), at the noise floor on J0/J1; SoftClient4ES does 5–20× less client work |
| **§6** | Do the results survive a 5-shard index read by a 3-node Trino? | Yes — the S1 gap **widens to 1.60×**; client cost and pushdown unchanged; Trino's `GROUP BY` gains 4.7× |

---

## 3. Extraction scenarios

### S0 / S0p — the reference floors

A plain Python client that scrolls Elasticsearch and parses each JSON hit, counting the rows and
throwing them away. Neither floor builds a usable artifact — no Arrow table, no DataFrame — which is
what makes them floors rather than contenders.

| Floor | Wall | Client CPU | Peak client memory |
|---|---|---|---|
| **S0** — one process, one scroll | 45.3 s | 14.7 s | 27 MB |
| **S0p** — 5 slices, 5 processes | **23.3 s** | 15.4 s (summed over the processes) | 135 MB (summed) |

**Outcome.** Scrolling single-threaded is the simplest approach, not the fastest one available to a
client that is willing to work for it: a sliced scroll halves the wall clock for almost the same
total CPU, and at 23.3 s it is **faster than either engine's S1** on this index. It costs **3.9× the
client CPU SoftClient4ES spends on the same ten million rows in the same session** (15.4 s against
3.9 s; 3.4× against the 4.6 s of the published S1 block, which is a different session) and returns
nothing you can compute on — but a reader who knows
Elasticsearch would supply this comparison themselves, so it is measured here rather than left out.
On this single-shard index the gain comes from overlapping request round-trips rather than from
shard parallelism — the summed CPU barely moves — so a multi-shard index would be expected to help
this floor further, exactly as it helps Trino. That variant was not run, and no figure for it is
published here.

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
| Wall median | **37.1 s** (spread 2.1%) | 53.1 s (spread 6.2%) | **1.43× faster** |
| Client CPU | **4.6 s** | 25.1 s | 5.5× less |
| Peak client memory | **900 MB** | 4,472 MB | 5.0× less |
| ES wire | 2,492 MB | 2,923 MB | — |
| Rows / columns | 10,000,000 / 8 | 10,000,000 / 8 | ✓ |

*(Wall, CPU and memory are the 2026-08-13 session. ES wire is the 2026-08-16 re-measurement, which
is the first to read the counter at the Elasticsearch container; that session's wall medians were
36.86 s and 51.81 s.)*

**Outcome.** SoftClient4ES is faster and far lighter. The reason is client representation: the client
consumes Arrow batches without ever building 10 million Python objects, which is what dominates
Trino's client CPU and memory.

**Fairness — measured against Trino's fastest clients.** Trino's stock client is not its only option.
[connectorx](https://github.com/sfu-db/connector-x) (a Rust engine that parses Trino's result pages
straight into columnar buffers) and the [ADBC Trino driver](https://adbc-drivers.org/drivers/trino/)
both return an Arrow table. Measured on the same query:

| Route | Wall | Client CPU | Peak memory |
|---|---|---|---|
| **SoftClient4ES** (Arrow Flight SQL) | **37.1 s** | 4.6 s | 900 MB |
| Trino — connectorx | 49.0 s | 9.9 s | **609 MB** |
| Trino — ADBC driver | 49.6 s | 18.9 s | 1,933 MB |
| Trino — stock client | 53.1 s | 25.1 s | 4,472 MB |

SoftClient4ES keeps the wall-clock lead against every Trino client (1.32× vs the fastest). On
client cost the picture is honest: connectorx, building contiguous buffers in Rust, reaches a lower
peak memory (609 MB) than our chunked Arrow batches, and lower CPU than the ADBC driver. The
wall-clock advantage comes from the server and the wire, not from any client library. Note that
609 MB is a bare Arrow *table*: when the same workflow materialises the whole result as a DataFrame
(scenario S5), connectorx needs 2.8 GB where SoftClient4ES needs 1.5 GB — so this memory advantage
does not carry to the DataFrame an analyst actually keeps.

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

| Route | SoftClient4ES | Trino (stock) | SoftClient4ES win |
|---|---|---|---|
| Wall | **36.9 s** | 63.7 s | **42% faster** |
| Client CPU | **4.8 s** | 36.1 s | 7.5× less |
| Peak memory | **1,466 MB** | 8,317 MB | 5.7× less |

With Arrow-backed pandas dtypes (`types_mapper=pd.ArrowDtype`), SoftClient4ES needs only 900 MB
(38.2 s), versus 7,984 MB for Trino.

**To a polars DataFrame:**

| Route | Wall | Client CPU | Peak memory |
|---|---|---|---|
| **SoftClient4ES** (`pl.from_arrow`) | **37.7 s** | 14.9 s | 2,487 MB |
| Trino — connectorx | 51.4 s | 10.9 s | 2,394 MB |
| Trino — ADBC driver | 51.6 s | 28.7 s | 3,533 MB |
| Trino — SQLAlchemy | 66.4 s | 38.1 s | 11,052 MB |

**Outcome.** SoftClient4ES lands a DataFrame faster on every route and destination. On the polars
destination both sides re-encode Arrow strings into polars' native format, which raises our client
CPU; connectorx does the same re-encoding in Rust and comes in slightly lower on CPU and memory. The
durable advantage, again, is wall-clock, held on every route.

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
| Wall median | **36.6 s** | 63.4 s | **1.73× faster** |
| Client CPU | **5.8 s** | 34.6 s | 5.9× less |
| Peak client memory | **930 MB** | 7,779 MB | 8.4× less |

**Outcome.** The zero-copy Arrow hand-off to DuckDB is the largest memory advantage in the benchmark
(8.4×): the columnar result is scanned in place instead of being copied through Python objects.

### S3 — `GROUP BY` returning 100 rows

`SELECT category, AVG(amount) FROM bench_events_10m GROUP BY category`, returning 100 groups.

| Metric | SoftClient4ES | Trino |
|---|---|---|
| Wall median | **0.04 s** | 27.0 s |
| ES wire | **0 MB** | 1,394 MB |
| Rows | 100 | 100 |

**The same answer, not merely the same row count.** A `terms` aggregation is approximate when its
bucket size is too small, so "100 groups" is not by itself evidence that the pushed-down result
matches a full scan. Every session gates on the values: all 100 `(category, AVG(amount))` pairs are
compared across the stacks — identical counts, averages equal to within 1e-9 — and Elasticsearch
reports `doc_count_error_upper_bound = 0` and `sum_other_doc_count = 0` for the aggregation, which
is the cluster's own statement that no bucket is missing and no document fell outside the returned
buckets. A push-down that returned a different answer faster would not be a result.

**Outcome.** SoftClient4ES compiles the `GROUP BY` into an Elasticsearch `terms` aggregation: the
cluster computes the 100 groups and returns only those 100 rows. Trino's Elasticsearch connector
performs predicate push-down only (per its documentation), so it scans all 10M rows into Trino and
aggregates there. This is the clearest architectural difference in the benchmark — for work
Elasticsearch can do, SoftClient4ES does not move the data at all.

### S1m — extract 1,000,000 rows: the only scale all three stacks can reach

Elasticsearch's own query language cannot return more than 1,000,000 rows (see below), so this is
the largest result set on which SoftClient4ES, Trino and ES|QL can be compared like for like. Same
eight columns, same index, `LIMIT 1000000` on every stack.

| Route | Wall | Client CPU | Peak client memory | Bytes off the cluster |
|---|---|---|---|---|
| **ES\|QL, `format=arrow`** | **0.51 s** | 0.05 s | 166 MB | 72 MB |
| ES\|QL, `format=json` | 1.17 s | 0.51 s | 669 MB | 86 MB |
| Trino, stock client | 5.30 s | 2.22 s | 474 MB | 291 MB |
| SoftClient4ES, Arrow Flight SQL | 7.63 s | **0.19 s** | 176 MB | 495 MB |

**Outcome — we lose this one, on wall clock, to both.** ES|QL is an order of magnitude faster than
anything else measured here, and Trino is 1.4× faster than SoftClient4ES. Three things are worth
separating from that.

First, the client cost still splits the way the rest of the benchmark predicts: SoftClient4ES spends
0.19 s of client CPU against Trino's 2.22 s, because the client is consuming Arrow batches rather
than building a million Python tuples.

Second, the reason ES|QL is fast is not the wire: it reads `doc_values` — columnar on disk — while
both engines read `_source` and pay a JSON parse per document. That is a genuine architectural
advantage, and it is bounded by the ceiling in the next section rather than by anything either
engine does.

Third — and this is the part that belongs to us — **the loss against Trino is a wire-volume loss,
and the volume is ours to explain.** For the same 1,000,000 rows SoftClient4ES moved **495 bytes per
row off the cluster against 291 for Trino**, and against **249 bytes per row on its own
ten-million-row run** in the same session. Its throughput barely moves between the two — 64.8 MB/s
here against 67.6 MB/s on S1, within 4% — so the time is going into bytes, not into the client: at
that rate the ~249 MB the result actually needs would take **3.8 s**, and the measured **7.63 s** is
what moving 495 MB costs. The two runs take different paths inside SoftClient4ES: an unbounded
`SELECT` streams through the scroll pager, while an explicit `LIMIT` above the index's result window
takes the bounded paging path. Only the bounded one shows the doubling. It is filed for
investigation and is a defect on our side, not a property of the comparison — but until it is fixed
the number stands as measured, and Trino wins this scale.

### ES|QL — Elasticsearch's own query language, and where it stops

A benchmark of two SQL engines over Elasticsearch that never measures what Elasticsearch itself can
do invites an obvious question. So it is measured, over both of its wire formats: the row-shaped
`format=json` every ES|QL client speaks, and `format=arrow`, which returns an Apache Arrow IPC
stream.

| Scenario | ES\|QL (arrow) | ES\|QL (json) | SoftClient4ES | Trino |
|---|---|---|---|---|
| S1m — 1,000,000 rows | **0.51 s** | 1.17 s | 7.63 s | 5.30 s |
| S3 — `GROUP BY`, 100 rows | 0.21 s | 0.16 s | **0.03 s** | 26.0 s |
| S4 — `LIMIT 100` | 0.007 s | **0.005 s** | 0.027 s | 0.031 s |
| S1, S2, S5, S6 — 10,000,000 rows | *cannot run* | *cannot run* | ✓ | ✓ |

*(The SoftClient4ES and Trino columns here are the same session's re-measurement, quoted so that all
four columns of one table come from one session. They are **not** the figures published in the
sections above, and on the two sub-100 ms controls they are faster: S3 0.029 s against 0.04 s,
S4 0.027 s against 0.040 s, both outside the earlier runs' min–max. At this scale a control drifts
between sessions by more than its own spread; the ratios it supports — three orders of magnitude on
S3, near-parity on S4 — are unchanged.)*

**Where it wins.** Below its ceiling ES|QL is the fastest way to get rows out of Elasticsearch that
we measured: on a 100-row fetch its whole round trip (5 ms) costs less than SoftClient4ES's
connection handshake alone (12.7 ms). Trino's handshake is cheaper still (1.8 ms); its 31 ms total is spent elsewhere.

**Where it does not.** On the pushed-down aggregation SoftClient4ES is 5× faster (0.03 s against
0.16 s) while both move 0 bytes off the cluster — the work is identical, the difference is what the
result travels over.

**Why it is absent from four scenarios.** `esql.query.result_truncation_max_size` defaults to 10,000
rows and is declared with a **hard maximum of 1,000,000**
(`Setting.intSetting("esql.query.result_truncation_max_size", 10000, 1, 1000000, …)` in
`EsqlPlugin.java`, v8.18.3). Elasticsearch refuses a higher value outright. Ask for ten million rows
with the setting at its maximum and the response is **1,000,000 rows, HTTP 200, and no `Warning`
header** — a truncated answer that looks like a complete one. (Recorded as
`esql-truncation-probe.json` in the session directory; the cross-index JOIN refusal is recorded
beside it.) This is not a licence gate: the
cluster measured here runs a `basic` licence and reports `esql.available: true`.

That ceiling is the whole reason the ten-million-row scenarios have no ES|QL column, and it is worth
stating plainly rather than as a footnote: for result sets under a million rows, ES|QL is an
excellent answer and this benchmark is not the argument for anything else.

### S4 — fetch 100 rows (`LIMIT 100`)

The control: when the result is small, the client representation stops mattering.

| Metric | SoftClient4ES | Trino |
|---|---|---|
| Wall median | 0.040 s | 0.057 s |

**Outcome.** Near parity, as expected. The extraction advantage only appears at scale, or when work
can be pushed into Elasticsearch.

---

## 4. Constrained-memory and concurrency

### S5 — does the extraction fit in a small container?

The client runs inside a container with a hard memory cap (`docker run --memory`, swap pinned equal
so the kernel kills rather than pages). The question is binary: does landing 10M rows as a DataFrame
complete, or is the process killed?

**Whole result set as one DataFrame:**

| Container cap | SoftClient4ES | Trino (stock) | Trino (connectorx, fastest) |
|---|---|---|---|
| 8 GB | ✅ 36.9 s · 1,528 MB | ❌ OOM-killed | ✅ 49.7 s · 2,908 MB |
| 6 GB | ✅ 36.7 s · 1,529 MB | ❌ OOM-killed | ✅ 49.3 s · 2,909 MB |
| 4 GB | ✅ 36.1 s · 1,535 MB | ❌ OOM-killed | ✅ 49.9 s · 2,907 MB |
| 3 GB | ✅ 37.1 s · 1,535 MB | ❌ OOM-killed | ✅ 48.8 s · 2,910 MB |
| **2 GB** | ✅ **36.4 s · 1,529 MB** | ❌ OOM-killed | ❌ **OOM-killed** |

*Provenance: the 8, 4 and 2 GB rows are the 2026-08-16 run; 6 and 3 GB were measured on 2026-08-13
and not repeated; the connectorx column is the 2026-08-14 run. "OOM-killed" is the kernel killing
the client — exit 137 — in every cell above, verified per cell. One earlier cell resolved
differently and is worth recording: on 2026-08-13 the stock client at 8 GB did not reach exit 137,
it stalled long enough for Trino to abandon the query server-side. Same conclusion, different
mechanism; the 2026-08-16 repeat is a clean kill at every cap, and that is what the table now
quotes.*

**Streaming, where neither side holds the whole result** — SoftClient4ES via
`fetch_record_batch()`, Trino via `pandas.read_sql(chunksize=…)`. This is the workflow a competent
Trino user reaches for first, so it is a table rather than a footnote:

| Container cap | SoftClient4ES | Trino (stock) |
|---|---|---|
| 8 GB | ✅ 36.8 s · 144 MB | ✅ 57.9 s · 302 MB |
| 4 GB | ✅ 37.1 s · 144 MB | ✅ 57.9 s · 304 MB |
| **2 GB** | ✅ **37.4 s · 147 MB** | ✅ **57.8 s · 301 MB** |

Both complete at every cap. SoftClient4ES is **1.55–1.57× faster on 2.1× less memory** — a smaller and
more representative difference than the binary one above, and the one that applies whenever the
workflow does not need the whole result set in memory at once.

**Outcome.** Landing the whole 10M-row DataFrame, SoftClient4ES fits in a **2 GB** container. Trino's
stock client is killed even at 8 GB; its fastest client (connectorx) fits at 3 GB but is killed at
2 GB — the cap where SoftClient4ES still completes. Both requirements are independent of the cap
(peak memory barely moves across a 4× range), so they reflect the data, not memory thrashing.

The binary framing is the harder claim, and it should be read next to the streaming table above: it
answers "can I materialise this result in a small container", not "can this pipeline run at all".
Streaming, both engines run everywhere, and the difference narrows to 1.57×.

### S6 — concurrent extractions in a fixed memory budget

An 8 GB total client budget, split evenly across N clients launched simultaneously, each extracting
10M rows. How many complete with the correct row count?

Measured against **both** of Trino's clients: the stock one it ships, and connectorx — the fastest
client it has, and the one S1 already grants it. Reporting only the stock client here would abandon
three scenarios later the fairness rule S1 sets.

| Engine / client | N | Cap each | Completed | Wall |
|---|---|---|---|---|
| SoftClient4ES | 1 | 8,192 MB | 1/1 | 34.9 s |
| SoftClient4ES | 2 | 4,096 MB | 2/2 | 36.8 s |
| SoftClient4ES | 3 | 2,730 MB | 3/3 | 38.0 s |
| SoftClient4ES | 4 | 2,048 MB | 4/4 | 39.1 s |
| **SoftClient4ES** | **5** | **1,638 MB** | **5/5** | **41.0 s** |
| Trino — connectorx | 1 | 8,192 MB | 1/1 | 51.7 s |
| **Trino — connectorx** | **2** | **4,096 MB** | **2/2** | **53.7 s** |
| Trino — connectorx | 3 | 2,730 MB | 0/3 | killed |
| Trino — stock client | 1 | 8,192 MB | 0/1 | killed |

**Outcome.** In an 8 GB budget, SoftClient4ES completes **five** concurrent extractions — 50 million
rows for 5× the work at +17% wall time — against **two** for Trino's fastest client and none for its
stock one. The ratio is 2.5×, not the 5-versus-nothing the stock client alone would suggest, and it
follows directly from what each client holds per extraction. This measures client-side capacity; it
does not measure server-side concurrency, which is a separate question this benchmark does not
address.

---

## 5. Cross-index JOIN (J0–J2)

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
| **J0** plain join | 27.6 s | 26.3 s (1.05× faster) | 0.06 s / 1.17 s |
| **J1** + `WHERE` | 11.9 s | 11.1 s (1.07× faster) | 0.03 s / 0.21 s |
| **J2** + `GROUP BY` | 29.7 s | 27.3 s (1.09× faster) | 0.02 s / 0.09 s |

(J0 and J2 are medians of 12 runs; J1 of 5.) These are small differences, so the spread belongs
next to them:

| Scenario | SoftClient4ES min–max | Trino min–max | Gap | Do the ranges overlap? |
|---|---|---|---|---|
| J0 | 26.49–28.57 s (7.5%) | 25.88–26.67 s (3.0%) | 4.6% | yes, marginally |
| J1 | 11.04–12.98 s (16.4%) | 10.94–11.25 s (2.9%) | 7.3% | yes |
| J2 | 29.05–30.26 s (4.1%) | 26.66–28.07 s (5.2%) | 8.9% | **no** |

**Outcome.** Trino is faster on wall clock across all three joins. On **J2 the result is clean** —
the two ranges do not overlap, and 8.9% is a real difference. On J0 and J1 the medians favour Trino
by 4.6% and 7.3% while the run-to-run ranges still overlap, so the precise percentage there is at
the noise floor of this measurement; we report the direction rather than defend the digits.
SoftClient4ES does 5–20× less client-side work in every case. The marginal cost of adding a
`GROUP BY` to the join is higher for SoftClient4ES (+2.1 s vs +0.9 s), so on join-side aggregation
Trino is the more efficient engine.

---

## 6. Index topology sensitivity — 5 shards, read by a 3-node Trino

Everything above was measured on a single-shard index. Trino's Elasticsearch connector creates one
split per shard, so a single-shard index gives it a single reader — which invites the fair question
of whether these results survive an index that lets Trino parallelise, read by a Trino cluster
rather than one node.

They do, and the extraction gap widens.

**Setup.** Same host, same corpus regenerated from the same seed into a **5-shard** index, one
segment per shard. Trino runs as a real cluster — a dedicated coordinator plus two workers, with
`node-scheduler.include-coordinator=false` so the coordinator plans and serves results while the
workers scan. Trino is deliberately given **6 CPU / 8 GB against SoftClient4ES's 4 CPU / 4 GB**:
1.5× the CPU and 2× the memory. Medians of 5 runs after 2 warm-ups, as everywhere else. Trino's
query plan used **5 scan splits spread across its two workers**, read back from
`system.runtime.tasks`, so the parallelism under test was genuinely exercised.

| Metric | SoftClient4ES 1 shard | **5 shards** | Trino 1 shard | **5 shards** |
|---|---|---|---|---|
| S1 wall | 37.1 s | **27.8 s** | 53.1 s | **44.7 s** |
| S1 client CPU | 4.6 s | 3.8 s | 25.1 s | 24.1 s |
| S1 peak client memory | 900 MB | 919 MB | 4,472 MB | 4,466 MB |
| S1 ES wire | 2,494 MB | 2,553 MB | 2,926 MB | 2,949 MB |
| S3 wall (`GROUP BY`, 100 rows) | 0.04 s | 0.04 s | 27.0 s | **5.8 s** |
| S3 ES wire | 0 MB | **0 MB** | 1,394 MB | 1,419 MB |
| S5 — 10M rows in a 2 GB container | completes | completes | killed | killed |

**Outcome — extraction.** Both engines benefit from the extra shards, and SoftClient4ES benefits
more: 25% faster against Trino's 16%. The S1 ratio therefore **widens from 1.43× to 1.60×**, on a
topology chosen to favour Trino and with Trino holding half again our CPU.

**Outcome — client cost is a property of the protocol, not the topology.** Peak client memory moves
by 2% on our side and 0.1% on Trino's; client CPU is flat to slightly lower for both. Sharding the
index and adding Trino nodes does not change what the client pays, because the client is one
process consuming one wire format however large the cluster is. The same holds for the
constrained-container result: 10M rows still land as a DataFrame in 2 GB on one side and are still
OOM-killed on the other.

**Outcome — pushdown is architectural.** The `GROUP BY` still moves **0 bytes off the cluster
against 1,419 MB**. Five shards and two extra Trino nodes do not change this, because the connector
does not push aggregations down: it reads all 10 million rows whatever the topology, and merely
reads them faster.

**Where Trino gains most.** Its aggregation wall-clock improves **4.7×** (27.0 s → 5.8 s), by far
the largest single improvement measured in this benchmark for either engine. Scan parallelism is
exactly what a multi-shard index gives Trino, and on aggregate-heavy work it converts directly into
wall-clock.

*ES wire for Trino in this section is measured at the Elasticsearch container itself — bytes that
left the cluster — because with a three-node Trino the coordinator's own counter sees only a
fraction of the scan (721 MB of the 2,949 MB Elasticsearch actually sent). That is why the topology
run was repeated with an Elasticsearch-side counter before anything was published. The
SoftClient4ES figure is the sidecar's received bytes: it is one hop from the cluster, and the two
counters agree within 0.1% wherever both were recorded.*

## 7. Where Trino is stronger

A fair benchmark names the other system's strengths.

- **Below one million rows, Elasticsearch itself extracts faster than we do** — ES|QL returns 1M
  rows in 0.51 s over Arrow against our 7.63 s, and a 100-row fetch in 5 ms against our 27 ms
  (section 3). It does not beat us on the pushed-down aggregation, where we are 5× faster, and its
  1,000,000-row ceiling is why it is absent from the rest of this document.
- **Aggregation over a sharded index** — given shards to parallelise over, Trino's `GROUP BY`
  wall-clock improves 4.7x (27.0 s to 5.8 s on a 5-shard index), the largest single gain either
  engine showed in this benchmark. It still moves 1.4 GB off the cluster to get there.
- **Cross-index joins** — Trino is faster on wall clock (J0–J2), unambiguously so on the join with a
  `GROUP BY`, and more efficient at aggregating over a join.
- **One-million-row extraction** — Trino lands 1M rows in 5.30 s against SoftClient4ES's 7.63 s
  (S1m), 1.44× faster. The advantage does not survive to ten million rows, where the client cost it
  pays becomes the constraint, but at this scale it is real. The gap is a wire-volume gap and the
  volume is ours: 495 bytes per row against Trino's 291, on a code path — bounded `LIMIT` — that
  moves twice what the same product's unbounded path moves. It is filed as a defect, and reported
  here as measured until it is fixed.
- **Its page size is not tuned in these results, and tuning it helps** — raising
  `elasticsearch.scroll-size` from its default 1000 to 5000 improves Trino's S1 wall clock from
  51.8 s to **49.7 s** (4%). The headline table pairs product default against product default; this
  is what the other setting is worth.
- **A lower-memory Arrow client exists** — via connectorx, Trino can land an Arrow *table* in less
  client memory than SoftClient4ES (609 MB vs 900 MB on S1), and with less CPU on the polars
  destination. (This does not extend to the full DataFrame, where SoftClient4ES uses less — S5.)
- **Distributed execution, spill-to-disk, and fault tolerance** — Trino scales a single query across
  a cluster and spills to disk; this single-node benchmark does not exercise any of it.
- **Connector breadth** — Trino federates 40+ data sources; SoftClient4ES is Elasticsearch-focused.
- **Licensing** — Trino is Apache-2.0 throughout; SoftClient4ES gates result-set size by licence
  tier (see Reproduction).

## 8. Reproduction

Prerequisites: Docker, Python 3.12. Bring up the stack (`docker compose up -d`), then run the
scenarios from `runners/`. Full instructions are in [README.md](README.md); fairness rules and
metric definitions are in [METHODOLOGY.md](METHODOLOGY.md).

Extracting the full 10M rows (S1/S2) requires a licence tier whose result-set quota exceeds 10M; the
licence changes a quota only, not the data path. With no licence the harness runs on the Community
tier, which reproduces every scenario's shape at reduced (`--limit`) scale.

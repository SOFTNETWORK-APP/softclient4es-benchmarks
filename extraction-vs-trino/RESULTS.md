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
Trino's client needs more, and completes **five concurrent extractions in an 8 GB budget where Trino
completes none**. It is also faster, because it delivers Arrow columnar batches the client consumes
directly, where Trino's Python client materialises rows into Python objects. Trino remains marginally
ahead on cross-index joins and offers capabilities (distributed execution, spill, connector breadth)
this single-node extraction benchmark does not exercise.

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

**Metrics.** *Wall* is end-to-end time including connection. *Client CPU* is `time.process_time()`
in the client process. *Peak client memory* is the process's peak physical footprint
(`ri_lifetime_max_phys_footprint`), which is immune to the macOS memory compressor. *ES wire* is the
bytes read from Elasticsearch, measured from the container network counters. Every run asserts the
exact expected row count before its timing is recorded; a run that returns the wrong number of rows
is discarded, not reported.

---

## 2. Scenarios at a glance

| ID | Question it answers | Outcome |
|---|---|---|
| **S0** | What does a naïve Python scroll client cost as a reference floor? | 46.5 s to read 10M rows |
| **S1** | Extract 10M rows into a client-side columnar table | SoftClient4ES **1.43× faster**, 5.5× less CPU, 5.0× less memory |
| **S1r** | Extract 10M rows into a **pandas / polars DataFrame** (what an analyst builds) | SoftClient4ES **42–44% faster** on far less CPU and memory |
| **S2** | Extract 10M rows and compute an aggregate in DuckDB | SoftClient4ES **1.73× faster**, 8.4× less memory |
| **S3** | `GROUP BY` returning 100 rows | SoftClient4ES **0.04 s vs 27 s**, and moves **0 vs 1.4 GB** off the cluster |
| **S4** | Fetch 100 rows (`LIMIT 100`) | Parity — 40 ms vs 57 ms |
| **S5** | Does the extraction fit in a constrained-memory container? | SoftClient4ES fits **2 GB**; Trino's stock client needs >8 GB, its fastest client 3 GB |
| **S6** | How many concurrent extractions fit in an 8 GB budget? | SoftClient4ES **5** (50M rows); Trino **0** |
| **J0–J2** | Cross-index JOIN landed as a DataFrame | Trino marginally faster on wall (5–9%); SoftClient4ES does 5–20× less client work |

---

## 3. Extraction scenarios

### S0 — reference floor: a naïve Python scroll client

A plain Python client that scrolls Elasticsearch and parses each JSON hit. This is not a competitor;
it is the cost of the most obvious approach, and it anchors the other numbers.

**Result:** 46.5 s to read 10M rows (spread 2.8%). Both engines below finish faster than this,
because they parse in the JVM and hand the client a columnar buffer rather than JSON.

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
| ES wire | 2,494 MB | 2,926 MB | — |
| Rows / columns | 10,000,000 / 8 | 10,000,000 / 8 | ✓ |

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

**Outcome.** SoftClient4ES compiles the `GROUP BY` into an Elasticsearch `terms` aggregation: the
cluster computes the 100 groups and returns only those 100 rows. Trino's Elasticsearch connector
performs predicate push-down only (per its documentation), so it scans all 10M rows into Trino and
aggregates there. This is the clearest architectural difference in the benchmark — for work
Elasticsearch can do, SoftClient4ES does not move the data at all.

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
| 8 GB | ✅ 41 s · 1,531 MB | ❌ killed | ✅ 50 s · 2,908 MB |
| 6 GB | ✅ 37 s · 1,529 MB | ❌ **killed** | ✅ 2,909 MB |
| 4 GB | ✅ 37 s · 1,535 MB | ❌ killed | ✅ 2,907 MB |
| 3 GB | ✅ 37 s · 1,535 MB | ❌ killed | ✅ 2,910 MB |
| **2 GB** | ✅ **37 s · 1,533 MB** | ❌ killed | ❌ **killed** |

**Streaming** (neither side holds the whole result — SoftClient4ES via `fetch_record_batch()`, Trino
via `pandas.read_sql(chunksize=…)`): both complete at every cap, SoftClient4ES ≈ 38 s / 146 MB,
Trino ≈ 59 s / 300 MB.

**Outcome.** Landing the whole 10M-row DataFrame, SoftClient4ES fits in a **2 GB** container. Trino's
stock client is killed even at 8 GB; its fastest client (connectorx) fits at 3 GB but is killed at
2 GB — the cap where SoftClient4ES still completes. Both requirements are independent of the cap
(peak memory barely moves across a 4× range), so they reflect the data, not memory thrashing. If the
workflow streams instead of materialising, both fit and SoftClient4ES is ~1.6× faster on ~2× less
memory.

### S6 — concurrent extractions in a fixed memory budget

An 8 GB total client budget, split evenly across N clients launched simultaneously, each extracting
10M rows. How many complete with the correct row count?

| Engine | N | Cap each | Completed | Wall |
|---|---|---|---|---|
| SoftClient4ES | 1 | 8,192 MB | 1/1 | 37.9 s |
| SoftClient4ES | 2 | 4,096 MB | 2/2 | 37.8 s |
| SoftClient4ES | 3 | 2,730 MB | 3/3 | 39.9 s |
| SoftClient4ES | 4 | 2,048 MB | 4/4 | 45.1 s |
| **SoftClient4ES** | **5** | **1,638 MB** | **5/5** | **47.6 s** |
| Trino (stock) | 1 | 8,192 MB | **0/1** | killed |

**Outcome.** In an 8 GB budget, SoftClient4ES completes five concurrent extractions — 50 million
rows for 5× the work at +26% wall time — while a single Trino stock client cannot complete one. This
measures client-side capacity; it does not measure server-side concurrency, which is a separate
question this benchmark does not address.

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

(J0 and J2 are medians of 12 runs; J1 of 5.)

**Outcome.** Trino is marginally faster on wall clock across all three joins (5–9%), reached on a
single-shard index that does not exercise its split parallelism. SoftClient4ES does 5–20× less
client-side work in every case. The marginal cost of adding a `GROUP BY` to the join is higher for
SoftClient4ES (+2.1 s vs +0.9 s), so on join-side aggregation Trino is the more efficient engine.

---

## 6. Where Trino is stronger

A fair benchmark names the other system's strengths.

- **Cross-index joins** — Trino is marginally faster on wall clock (J0–J2), and more efficient at
  aggregating over a join.
- **A lower-memory Arrow client exists** — via connectorx, Trino can land an Arrow *table* in less
  client memory than SoftClient4ES (609 MB vs 900 MB on S1), and with less CPU on the polars
  destination. (This does not extend to the full DataFrame, where SoftClient4ES uses less — S5.)
- **Distributed execution, spill-to-disk, and fault tolerance** — Trino scales a single query across
  a cluster and spills to disk; this single-node benchmark does not exercise any of it.
- **Connector breadth** — Trino federates 40+ data sources; SoftClient4ES is Elasticsearch-focused.
- **Licensing** — Trino is Apache-2.0 throughout; SoftClient4ES gates result-set size by licence
  tier (see Reproduction).

## 7. Reproduction

Prerequisites: Docker, Python 3.12. Bring up the stack (`docker compose up -d`), then run the
scenarios from `runners/`. Full instructions are in [README.md](README.md); fairness rules and
metric definitions are in [METHODOLOGY.md](METHODOLOGY.md).

Extracting the full 10M rows (S1/S2) requires a licence tier whose result-set quota exceeds 10M; the
licence changes a quota only, not the data path. With no licence the harness runs on the Community
tier, which reproduces every scenario's shape at reduced (`--limit`) scale.

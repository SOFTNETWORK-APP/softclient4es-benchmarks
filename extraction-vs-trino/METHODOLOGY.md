# Methodology

## 1. What this benchmark measures — and what it does not

The data path under test:

```text
Elasticsearch → row-serialized documents out of the cluster (the _search / _sql path
                both engines read; ES|QL's columnar Arrow output is a third path,
                measured separately and capped at 1,000,000 rows)
             → SoftClient4ES: converted to Arrow ONCE at the sidecar, streamed as
               Arrow batches, consumed by the client with no further deserialization
             → Trino: parsed into Trino's columnar pages, re-serialized to Trino's
               row-oriented client protocol, re-parsed value-by-value in the client
```

**Every client pays an Elasticsearch serialization leg.** Nothing leaves the cluster unserialized,
and this benchmark never claims to avoid that step.

For the SQL and JDBC path it is about, that serialization is **row-shaped**: `_search` returns JSON,
and the SQL endpoint offers CSV, TSV, YAML and the binary CBOR and Smile — every one of them a
row-serialized document encoding. Elastic's own JDBC driver is a client of that endpoint.

**One Elasticsearch format is columnar**, and it is measured here rather than glossed over: ES|QL's
`format=arrow` returns an Apache Arrow IPC stream (`content-type:
application/vnd.apache.arrow.stream`, verified on Elasticsearch 8.18.3). It is fast: below its
ceiling it *extracts* faster than either engine in this benchmark — though not on the pushed-down
aggregation, where SoftClient4ES is 5× faster. The ES|QL section of RESULTS publishes both.

That ceiling is the reason it appears in three scenarios and not in the rest: `esql.query.result_truncation_max_size` defaults to 10,000 rows and
is declared with a hard maximum of 1,000,000, so ES|QL cannot return a ten-million-row result at
all — and above the configured ceiling it truncates with HTTP 200 and no `Warning` header.

What this benchmark measures is what happens *after* the serialization leg, on the path a SQL client
takes: SoftClient4ES serializes to Arrow once and the client consumes that columnar buffer directly,
while Trino re-serializes to a row protocol the client must parse again.

So this benchmark measures **large result-set extraction and client-side consumption** — wall-clock,
client CPU, peak client memory — which is where the client representation dominates. It does **not**
measure Elasticsearch-side query execution (I/O, scoring), where the wire format is irrelevant.

No number is published that did not come out of a run of this harness.

## 2. Fairness rules

- **Same host, same session, same index.** Every stack measured in a scenario — SoftClient4ES,
  Trino, and ES|QL where it can run — reads `bench_events_10m` back to back. The rule is honoured at
  the level of the whole matrix, not just the scenario: **the entire single-shard matrix is one
  session** (`run_full_session.sh` — one gate, one host state, 165 measured runs). Three scenarios
  cannot join it because each rebuilds its own container topology — S5 caps the client container,
  S6 launches N of them, and the joins use a second index — so each is its own session, run the
  same night on the same host and the same image, and RESULTS names the session behind every table.
- **One product version per publication.** Every figure RESULTS publishes comes from a single
  sidecar image, never blended across releases. When a release changes the data path, the whole
  matrix is re-measured rather than patched — a single cell can move by 2× and reverse, which a
  mixed table would hide.
- **Blocks are separated by an engine-quiescence gate, not by the previous block's exit.** S5 and S6
  kill their clients deliberately, and a Trino query whose client has gone keeps executing and
  "finishing" on the cluster for minutes afterwards (`ABANDONED_QUERY` and `ABANDONED_TASK` states,
  observed running for hundreds of seconds). Timing the next block against that leftover work
  measures neither engine: `wait_engines_idle.py` blocks until Trino reports no query in flight and
  the sidecar's CPU has returned to idle, and refuses rather than proceeding if that does not
  happen within its timeout.
- **Container limits, and the handicap they encode:** Elasticsearch and the SoftClient4ES sidecar
  each get 4 CPU / 4 GB. **Trino gets more, deliberately** — it runs as a 3-node cluster
  (coordinator 2 CPU / 2 GB, two workers 2 CPU / 3 GB each) totalling **6 CPU / 8 GB**, i.e. 1.5×
  the CPU and 2× the memory, in *every* scenario rather than only the multi-shard one. Trino is
  the system that benefits from more hardware, so giving it more makes every result here a
  conservative statement of the gap. Elasticsearch heap is pinned at 2 GB; Trino's official image
  auto-sizes its heap from container memory.
- **Page-size symmetry:** the sidecar's `ARROW_BATCH_SIZE=1000` equals Trino's
  `elasticsearch.scroll-size=1000`. Both are product defaults, pinned explicitly so the setting can
  be checked.
- **Authentication disabled on both paths**, so authentication cost is zero and identical on both
  sides.
- **Both clients are used the way their projects document them.** SoftClient4ES is driven with the
  standard `adbc_driver_flightsql` Arrow Flight SQL driver; Trino with its own `trino` Python
  package, and — for the fairness comparison in RESULTS S1/S1r — with its fastest available clients
  (connectorx and the ADBC Trino driver). No SoftClient4ES-specific client code is used.
- **Trino uses its default type mapping.** `legacy_primitive_types` would return unparsed strings,
  deferring the parse rather than avoiding it, and is not used.
- **Warm cache, fresh process.** Each scenario's warm-ups run immediately before its measured runs,
  so both systems measure against a warm Elasticsearch page cache. Every measured run is a fresh OS
  process, so peak memory cannot leak between runs. Because SoftClient4ES runs first in each
  scenario, Trino meets a slightly warmer cache — an effect in Trino's favour.
- **Correctness before timing.** Each run asserts the exact expected row count; a run that returns
  the wrong count is discarded, never reported. Python is never run with `-O`, so the asserts
  always execute.
- The exact configuration lives alongside this file, and the data generator is deterministic
  (fixed seed).

## 3. Metrics

- **Wall-clock:** `time.perf_counter()` around connect → materialized result. Connection setup is
  included because it is time the user waits.

  **Every stack dials an IP literal**, so no arm resolves a name. That is a fairness rule, and it
  was not always obeyed: until 2026-08-17 the Flight runner dialled `127.0.0.1` while every Trino
  route dialled `localhost`.

  It costs Trino almost nothing to dial a name: its Python clients resolve through the OS
  resolver, which answers from `/etc/hosts` in microseconds — Trino's measured connect in this
  session is **1.58 ms** (S1) whichever it dials. The stock ADBC Flight SQL driver is Go and uses grpc-go's own resolver, which does
  not consult `/etc/hosts`. A four-layer connect probe (`probe_connect.py`, artifact
  `connect-probe.json`) separates the cost by layer: bare TCP 0.09 ms, the Flight C++ layer 1.6 ms,
  the Go ADBC layer dialling an IP 3.0 ms, and **the same layer dialling a name 17.7 ms** — a ≈15 ms
  resolver tax that belongs to the driver, not to the protocol (the host's own resolver answers the
  same name in 0.20 ms). Behind it sits a 5 s per-query DNS
  timeout that produced 5.2 s and 10.1 s outliers in earlier sessions; that cause was established
  and closed in softclient4es-arrow#151.

  So the two dials are not two settings of one experiment. Dialling names everywhere would place a
  third-party Go resolver inside the comparison; dialling literals everywhere removes name
  resolution from every arm. **Where it matters is a question of scale, and the answer differs by
  scenario:** ≈15 ms is 0.04% of a ten-million-row extraction and changes nothing, while on the S4
  control — a 100-row fetch — it is the whole result. Both dials are therefore measured and both are
  published: dialled by IP, S4 is 0.037 s against Trino's 0.056 s; dialled by name it is **0.053 s**,
  which at these spreads (~20%) is no longer distinguishable from Trino's. The name lookup does not
  invert that control, it **erases** it, and RESULTS says so where the control appears. The driver's cost is published as a deployment note, not as a
  scenario: with the Go driver, dial an address or reuse the connection.
- **Client CPU:** `time.process_time()` for the client process — the CPU spent turning the wire
  format into usable values.
- **Peak client memory:** the process's peak physical footprint. On macOS this is the kernel's
  `ri_lifetime_max_phys_footprint`, which includes compressed pages and is therefore immune to the
  macOS memory compressor (plain RSS under-reports under memory pressure). On Linux the harness
  falls back to `ru_maxrss`.
- **ES wire bytes:** read from the container network counters, so a wire-volume difference is
  measured, not asserted. Since 2026-08-16 the counter is read **at the Elasticsearch container**
  (bytes it transmitted), so the figure cannot depend on how many containers the engine is made of.
  Earlier sessions recorded only the *engine* container's received bytes, which is not the same
  metric — and with Trino running as three containers it is not even the same quantity. Reading at
  Elasticsearch removes that ambiguity: the figure counts what left the cluster, however many
  containers the engine is made of. **Every ES-wire figure RESULTS publishes is the
  Elasticsearch-side one**, section 6 included.
- **Server-side cost is not measured.** Neither the Elasticsearch container, the sidecar, nor Trino
  has its CPU or memory recorded per run. Every resource figure published is a **client** figure.
  This is a real limitation of the harness rather than a claim: it means an aggregation push-down is
  credited with the bytes it does not move, not with the cluster CPU it consumes, and the sidecar's
  own JVM cost sits outside the comparison exactly as Trino's does.

## 4. Licence disclosure

SoftClient4ES enforces a result-set quota by licence tier (Community 10,000 / Pro 1,000,000 /
Enterprise unlimited). The 10-million-row scenarios therefore run under an Enterprise-tier licence.
**The licence lifts a row quota only — it does not change the batch size, the serialization, or the
data path.** The runners assert exactly 10,000,000 rows, so a quota-capped run aborts rather than
being reported; a truncated result can never be published by accident. With no licence the harness
runs on Community and reproduces every scenario at reduced (`--limit`) scale.

## 5. Interpretation guardrails

- **S1 / S1r / S2** differences are attributable to the wire format and client-side representation.
- **S3** differences are attributable to **aggregation pushdown**, not the wire format: SoftClient4ES
  compiles the `GROUP BY` into an Elasticsearch aggregation, while Trino's connector performs
  predicate pushdown only and scans all rows. S3 is never a wire-format result.
- **S4** is the control: with a small result the wire format stops mattering, and near-parity there
  is a sign the benchmark is honest, not a weakness.
- **Where Trino is stronger** is published alongside the results (RESULTS section 7).

## 6. Known limitations

- **One Elasticsearch node, and a 3-node Trino.** The cluster Trino runs on here is real but tiny,
  and nothing exercises its spill-to-disk or fault tolerance — real strengths of Trino that an
  extraction benchmark at this scale cannot show.
- **One flat mapping, no nested fields or arrays.** This avoids Trino's documented gaps in those
  types, so the connector is not handicapped; it also means the benchmark says nothing about SQL
  coverage, only about extraction efficiency.
- **One column shape: eight columns over five types** (`long`, `date`, `double`, `integer`,
  `keyword`)**.** The client-side cost of a wire format is a
  function of column count and type mix, and only one point on that curve is measured. Nothing here
  establishes how the gap moves for two columns or eighty.
- **Only the client's half of the cost is measured** (see section 3). A reading of these results as
  "total system cost" is not supported by anything in this harness.
- **Warm cache only, by construction.** Each scenario's warm-ups run immediately before its measured
  runs. No cold-cache arm exists, so nothing here describes a first query after a restart.
- **The constrained-memory and concurrency scenarios (S5, S6) run their client inside a Linux
  container**, while S0–S4 run it natively on macOS. Wall-clock is therefore comparable *within*
  each group and only indicative across them; the peak-memory metric also changes accordingly
  (`ru_maxrss` in the container, physical footprint on the host).
- **The ten-million-row scenarios are not reproducible on the Community tier.** S1/S2/S5/S6 require a
  licence whose result quota exceeds 10M (section 4). A third party can reproduce every scenario's
  shape at reduced scale, and can reproduce S1m/S3/S4 exactly, but cannot independently re-run the
  headline extraction without one.
- **Single primary shard in the main results — and that setup favours SoftClient4ES on
  wall-clock.** Trino's connector creates one split per shard, so a single-shard index gives it one
  reader and its scan parallelism is not exercised. Note what bounds this: the *shard* count, not
  the node count — additional Trino workers cannot split a single reader.

  This is no longer left as a caveat. **RESULTS section 6 publishes the measured sensitivity run:**
  the same corpus regenerated from the same seed into a 5-shard index, read by a real 3-node Trino
  cluster (dedicated coordinator + 2 workers, `node-scheduler.include-coordinator=false`) given
  6 CPU / 8 GB against SoftClient4ES's 4 CPU / 4 GB. Trino's plan used 5 scan splits — 3 on one
  worker, 2 on the other, none on the coordinator — captured live from `system.runtime.tasks` by
  `probe_trino_splits.py`, because those rows are dropped the moment a query finishes and cannot be
  recovered afterwards. Both engines get faster; the S1 gap widens from **1.48× to 1.52×**; client
  CPU, peak client memory (0.01% ours, 0.04% Trino's) and the 2 GB container threshold do not move; the `GROUP BY`
  still moves 24 KB against 1.4 GB. Trino's own largest gain in the whole benchmark appears there
  too — its aggregation wall-clock improves **4.4×** — and is published as such.

  The distinction the run establishes: **wall-clock is topology-sensitive; client cost and pushdown
  are not.** The client is one process consuming one wire format however large the cluster, and the
  connector pushes no aggregation down at any shard count. That is now measured rather than argued.
- **Trino's spooling protocol was not benchmarked separately.** The optional "S1-spooled" variant
  (Trino's `json+zstd` spooling client) is superseded by the S1/S1r fairness comparison against
  connectorx and the ADBC Trino driver, which measure Trino's fastest *columnar* clients directly —
  a stronger fairness position than the spooling protocol alone.

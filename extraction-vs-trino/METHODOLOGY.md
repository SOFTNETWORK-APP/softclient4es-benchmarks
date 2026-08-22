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
  the level of the whole matrix, not just the scenario: **the entire 6-shard matrix is one
  session** (`run_full_session.sh`, warmed to steady state before the first timed block — one gate, one host state, 180 measured runs). Four groups
  cannot join it because each rebuilds its own container topology or index — S5 caps the client
  container, S6 launches N of them, the joins use a second index, and the topology-sensitivity arm
  regenerates the corpus into 1 shard — so each is its own session, run the same night on the same
  host and the same image, and RESULTS names the session behind every table.
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
  scenario, Trino meets a warmer cluster. RESULTS section 1 measures what that is worth on the
  headline cell and it is **not** slight: re-running S1 for both engines at the end of the session
  moved SoftClient4ES 25.1% (13.07 s → 9.78 s) and Trino 0.3%. The mechanism of that warm-in is
  **not established** (a cold page cache is excluded — the warm-ups read the full index before the
  first timed run); the effect runs in Trino's favour, and the published pair is the warm one for
  both engines rather than an early arm against a late one. The fix is ordering — warm the index
  before the first timed block — which the topology arm does and the queued re-measurement of the
  main matrix adopts.
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
- **Server-side cost: measured on every cell of the main session, published where it carries a
  claim.** Engine and Elasticsearch CPU are read per run from each container's cgroup
  (`cpu.stat`/`usage_usec` — an exact cumulative counter, not a sampled percentage; the throttling
  counters come from the same file) for the **whole 6-shard matrix** — floors, S1, S1m, S1r, S2, S3,
  S4 — and for the topology and paging arms of RESULTS sections 6 and 6.1. The tables print it where
  it carries a claim (floors, S1, S3, sections 6 and 6.1); on the remaining cells it lives in the
  session records, which is where section 6.1's cross-reference to S2's 35.5 s of Elasticsearch CPU
  comes from. **Three groups are the exception:** S5, S6 and the joins rebuild container topologies
  mid-run and stay **client-side only**, so reading *those* rows as total system cost is
  unsupported. The push-down is not credited with bytes alone — S3 publishes its cluster CPU (0.1 s
  against 21.3 s) beside them. The sidecar's own JVM cost sits inside the comparison, exactly as
  Trino's does.

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
- **Server-side cost is a recent instrument, and three groups still lack it.** Until 2026-08-19 this
  harness measured the client alone, which left two claims resting on nothing: "Trino holds half
  again our CPU" (an *allocation*, being read as a *consumption*) and the floor comparison, where a
  sliced scroll pays everything in its client processes while SoftClient4ES pays a smaller client
  bill plus an uncounted sidecar JVM. Both are now measured. The instrument is cgroup v2's
  `usage_usec`, a monotonic counter read inside each container before and after a run — the engine's
  containers summed (Trino is three) and Elasticsearch separately. It is exact for the interval, not
  a sampled percentage integrated over it. **What it covers in this campaign:** every cell of the
  main session `20260821T041841-v030-prewarm` (floors, S1, S1m, S1r, S2, S3, S4), plus the topology
  arm and the paging A/B. **What it does not:** S5, S6 and the joins, whose harnesses rebuild
  container topologies mid-run. A reading of *those* scenarios as "total system cost" remains
  unsupported.
- **Warm cache only, by construction.** Each scenario's warm-ups run immediately before its measured
  runs. No cold-cache arm exists, so nothing here describes a first query after a restart.
- **The constrained-memory and concurrency scenarios (S5, S6) run their client inside a Linux
  container**, while S0–S4 run it natively on macOS. Wall-clock is therefore comparable *within*
  each group and only indicative across them; the peak-memory metric also changes accordingly
  (`ru_maxrss` in the container, physical footprint on the host).
- **The ten-million-row scenarios are not reproducible on the Community tier.** S1/S2/S5/S6 require a
  licence whose result quota exceeds 10M (section 4). A third party can reproduce every scenario's
  shape at reduced scale, and can reproduce S1m/S3/S4 exactly, but cannot independently re-run the
  headline extraction without one. What is available instead of a re-run is the evidence: **every measured run of every session is published in this repository** under `results/<session>/`, warmed to steady state before the first timed block — one JSON per run with its wall, CPU, memory, wire bytes, host load and memory-pressure reading, plus the equivalence gate, the ES|QL truncation and join probes, the connect probe, the engine-quiescence records and the sidecar image digest. A third party who cannot re-run the headline extraction can still audit the dispersion behind every median, the gates that had to pass before a number was kept, and the state of the host while it was measured. It is not reproduction, and it is not nothing.
- **Six primary shards in the main results, and the topology is measured both ways.** Trino's
  connector creates one split per shard, so shard count — not node count — bounds its scan
  parallelism: additional workers cannot split a single reader.

  This is not left as a caveat. **RESULTS section 6 publishes the measured sensitivity run** on the
  same corpus regenerated from the same seed into a **1-shard** index, and the parallelism is
  verified on *both* topologies rather than assumed. Captured live from `system.runtime.tasks` by
  `probe_trino_splits.py` — those rows are dropped the moment a query finishes and cannot be
  recovered afterwards — Trino's plan used **6 scan splits, 3 on each worker, none on the
  coordinator** against the 6-shard index, and **1 split on one worker** against the 1-shard index.

  Both engines get faster with shards, and very unequally: SoftClient4ES **4.32×**, Trino
  **1.27×**, so the S1 gap widens from **1.34× to 4.58×**. Client CPU, peak client memory (0.2%
  ours, 0.3% Trino's) and the 2 GB container threshold do not move; the `GROUP BY` still moves
  26.5 KB against 1.39 GB. Trino's own largest gain in the whole benchmark appears here too — its
  aggregation wall-clock improves **5.2×** — and is published as such.

  **Section 6.1 then attributes our share of it** rather than leaving it to the architecture in
  general: the same build with concurrent paging disabled (`ELASTIC_SCROLL_MAX_SLICES=1`) takes
  35.55 s against 10.06 s, so the feature is worth **3.53×**. The controls in that A/B — an
  aggregation and a 100-row fetch, neither of which pages — are identical across arms, which is
  what makes the attribution readable.

  The distinction the runs establish: **wall-clock is topology-sensitive; client cost and pushdown
  are not.** The client is one process consuming one wire format however large the cluster, and the
  connector pushes no aggregation down at any shard count. That is measured rather than argued.

  ⚠️ Elasticsearch here is a **real 3-node cluster** (3 × 2 CPU), so the splits and slices are
  served by independent nodes. What remains untested is a *larger* cluster — more nodes, more
  shards, more shards per node. The scaling slope is measured on three nodes, not established as a
  general law.
- **Queued: where our 921 MB goes.** connectorx lands the same Arrow table in 617 MB against our
  921 MB (S1), attributed in the text to chunked batches against contiguous Rust buffers. Chunking
  alone does not obviously account for 300 MB, and the shape of the gap is consistent with a
  transient double buffer at `fetch_arrow_table()` — the batch list and the concatenated table alive
  at the same moment. Measuring `fetch_record_batch()` with an explicit concatenation against the
  one-shot call would separate the two, and is queued. Until it is measured the report publishes the
  loss as measured, without an explanation it cannot support.

- **Trino's spooling protocol was not measured, and the connectorx comparison does not substitute
  for it.** connectorx parses the *same* row-oriented client protocol, faster, in Rust; the
  spooling protocol changes the protocol itself — the leg this report identifies as Trino's
  bottleneck. It is the unmeasured variant most likely to narrow the S1 gap, and it is queued.
  Until then, Trino has been benchmarked on its fastest *client*, not necessarily its fastest
  *path*.
- **Dispersion is published per repeated cell; two groups have none by construction.** Every
  wall-clock cell of the extraction matrix, the topology arm and the paging A/B prints its median
  with the min-max of its five runs; the joins publish theirs in a companion table with all 25
  cross-pairings. The S5 caps and the S6
  concurrency levels are **one run per cell** — their outcome is binary (completes / OOM-killed, or
  how many clients finish with the right row count), not a timing — so no dispersion exists to
  publish, and the repeated cells bound run-to-run noise at <= 6% anyway.

- **Sliced paging buys wall clock with cluster CPU, and the peak is what a shared cluster feels.**
  The headline cell costs Elasticsearch 42.2 s of CPU over an 11.91 s run — an average of **3.5 of
  the cluster's 6 CPUs** for its duration, against 0.68 for Trino's documented client and 2.2 for
  connectorx. Nothing here measures what that does to a search workload running on the same cluster;
  S6 measures only what it does to our own concurrency (five clients put thirty readers on six
  Elasticsearch CPUs, and the cluster becomes the limit). The counterweight is published in
  RESULTS section 6.1: over a whole extraction, sliced paging costs the cluster **30% less** total
  CPU than sequential paging — higher peak, lower integral — and `elastic.scroll.max-slices = 1`
  turns it off.

- **The index is force-merged and carries no replicas.** One segment per shard, no replica competing
  for page cache, nothing writing to the index during a run. That removes a real source of variance
  and flatters both engines equally, but nothing here describes either one on a live index under
  ingest, or on a cluster where replicas serve part of the read.

- **Slicing is measured at one point on its own curve.** The shipped default `max-slices = 8` is
  never reached: this index has 6 primary shards, so every sliced run uses 6. What happens past the
  default — 12 or 24 shards against a cap of 8 — is unmeasured, so the 3.37× of section 6.1 must not
  be extrapolated beyond the point where the default saturates.

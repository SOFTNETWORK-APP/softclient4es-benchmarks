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

- **Same host, same session, same index.** Both systems read `bench_events_10m` back to back.
- **Identical container limits:** Elasticsearch, the sidecar, and Trino each get 4 CPU / 4 GB.
  Elasticsearch heap is pinned at 2 GB; Trino's official image auto-sizes its heap from container
  memory.
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
  included because it is time the user waits. (The Flight runner dials the sidecar by IP literal to
  avoid a Go-driver DNS lookup that would otherwise add a fixed per-connection latency; real
  deployments should reuse connections.)
- **Client CPU:** `time.process_time()` for the client process — the CPU spent turning the wire
  format into usable values.
- **Peak client memory:** the process's peak physical footprint. On macOS this is the kernel's
  `ri_lifetime_max_phys_footprint`, which includes compressed pages and is therefore immune to the
  macOS memory compressor (plain RSS under-reports under memory pressure). On Linux the harness
  falls back to `ru_maxrss`.
- **ES wire bytes:** read from the container network counters, so a wire-volume difference is
  measured, not asserted.

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
- **Where Trino is stronger** is published alongside the results (RESULTS section 6).

## 6. Known limitations

- **Single node throughout.** This does not exercise Trino's distributed execution, spill-to-disk,
  or fault tolerance — real strengths of Trino that a single-node extraction benchmark cannot show.
- **One flat mapping, no nested fields or arrays.** This avoids Trino's documented gaps in those
  types, so the connector is not handicapped; it also means the benchmark says nothing about SQL
  coverage, only about extraction efficiency.
- **Single primary shard in the main results — and that setup favours SoftClient4ES on
  wall-clock.** Trino's connector creates one split per shard, so a single-shard index gives it one
  reader and its scan parallelism is not exercised. Note what bounds this: the *shard* count, not
  the node count — additional Trino workers cannot split a single reader.

  This is no longer left as a caveat. **RESULTS section 6 publishes the measured sensitivity run:**
  the same corpus regenerated from the same seed into a 5-shard index, read by a real 3-node Trino
  cluster (dedicated coordinator + 2 workers, `node-scheduler.include-coordinator=false`) given
  6 CPU / 8 GB against SoftClient4ES's 4 CPU / 4 GB. Trino's plan used 5 scan splits across its two
  workers, read back from `system.runtime.tasks`. Both engines get faster; the S1 gap widens from
  1.43× to 1.60×; client CPU, peak client memory and the 2 GB container threshold do not move; the
  `GROUP BY` still moves 0 bytes against 1.4 GB. Trino's own largest gain in the whole benchmark
  appears there too — its aggregation wall-clock improves 4.7× — and is published as such.

  The distinction the run establishes: **wall-clock is topology-sensitive; client cost and pushdown
  are not.** The client is one process consuming one wire format however large the cluster, and the
  connector pushes no aggregation down at any shard count. That is now measured rather than argued.
- **Trino's spooling protocol was not benchmarked separately.** The optional "S1-spooled" variant
  (Trino's `json+zstd` spooling client) is superseded by the S1/S1r fairness comparison against
  connectorx and the ADBC Trino driver, which measure Trino's fastest *columnar* clients directly —
  a stronger fairness position than the spooling protocol alone.

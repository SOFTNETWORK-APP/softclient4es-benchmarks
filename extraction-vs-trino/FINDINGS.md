# Findings — issues discovered and fixed during benchmarking

Building this benchmark surfaced several issues in SoftClient4ES. All were fixed, and the results in
[RESULTS.md](RESULTS.md) were measured on the released `0.3.0` build that contains the fixes. This
document is the summary of what was found and how it was resolved.

## Extraction performance

Four independent causes were making large extractions slower than they needed to be:

- **Sequential paging over a sharded index** (fixed in core 0.21.0, shipped in `0.3.0`). An
  extraction with no `ORDER BY` and no `LIMIT` paged Elasticsearch with **one reader**, so a
  10M-row scan was ~10,000 round trips that each fanned out to every shard and were merged by the
  coordinating node — roughly 60,000 shard operations on a 6-shard index. Wall-clock therefore did
  not improve when the index was resharded. It now pages with `min(primary shards, max-slices)`
  concurrent point-in-time slices. Measured against itself on identical hardware, this is worth
  **3.53× on S1** (35.55 s → 10.06 s), and it is **less total work, not just more overlap**:
  Elasticsearch CPU falls 29% (49.8 s → 35.6 s) because each page now goes to one shard instead of
  all of them. RESULTS section 6.1 is the A/B.

- **Deep-paging sort regression (Elasticsearch 8+).** For a query with no `ORDER BY`, the streaming
  pager injected `_shard_doc` as the primary sort. From Elasticsearch 8 / Lucene 9 onward this
  defeats the document-id skip optimisation, so each page re-scanned the whole index instead of
  seeking to the next position — about **60 ms/page instead of 8 ms/page** on the 10M-row index, an
  effect that grows with index size. The fix uses `_doc` as the sort under a point-in-time context
  (which Elasticsearch turns into a total order via an automatic tiebreaker), restoring efficient
  paging while keeping results complete. A multi-shard completeness test guards against regression.

- **Per-row conversion cost.** The row-to-Arrow path did redundant per-row work (repeated map
  normalization and re-parsing of each response page). This was reduced to a single parse per page
  and a single-pass row conversion, which is the main reason the client-CPU figures in S1/S2 are as
  low as they are.

- **The schema probe re-ran bounded statements.** To advertise a result's schema the Flight endpoint
  executed the statement itself, and when the statement already carried a `LIMIT` it executed it
  *in full* — so a bounded extraction read every row off Elasticsearch **twice**. It is visible in
  the wire rather than only the clock: S1m now moves **253 bytes per row**, against 254 on the
  unbounded ten-million-row run. The probe now reads at most 100 rows and never more than the
  statement asked for; aggregation-shaped statements, where a `LIMIT` is a bucket count rather than
  a row count, are left untouched.

Together these fixes are what let SoftClient4ES page and convert 10M rows efficiently in the results
above.

## Result completeness

A family of cases silently returned incomplete results and were corrected:

- **`GROUP BY` without `LIMIT`** returned only the first ~10 groups (the default aggregation size).
- **Window `PARTITION BY`** truncated to the first ~10 partitions.
- **`SELECT` without `LIMIT`** on the non-scroll path returned only the first ~10 rows.
- **An explicit `LIMIT` above `index.max_result_window`** failed while the same query with no `LIMIT`
  succeeded.

Each now returns the complete result set, and each is covered by a multi-shard completeness test.
These matter for a benchmark because a silently truncated result would otherwise look like a fast,
correct run. The same reasoning drove the sliced-paging work above: its completeness test asserts
row counts **and distinct ids** on a multi-shard index, because the failure mode of a paging change
is silence, not an error.

## Result shape

Extraction rows previously carried Elasticsearch hit-metadata columns (`_id`, `_index`, `_score`)
in addition to the selected columns. Rows now contain exactly the columns the SQL projects, so the
client receives — and pays for — only the requested data (8 columns for the S1 query).

## Licence verification

From extensions `0.3.0`, licence signatures are verified against a trust root compiled into the
artifact, and the `SOFTCLIENT4ES_LICENSE_PUBLIC_KEY` override is ignored. This closes an offline
tier bypass. It has a consequence for anyone reproducing this benchmark: a self-signed licence no
longer verifies and degrades to Community silently, so the harness's exact row-count asserts are
what turn that into a visible failure rather than a fast, truncated run. See RESULTS section 8.

## Environment note

The published sidecar image originally shipped without a logging configuration, which caused it to
log at DEBUG and write every Elasticsearch response byte to the container log — a large performance
and data-exposure problem. The image now ships a logging configuration that keeps wire-level logging
off by default; the benchmark runs the sidecar as shipped.

## Issues found in the benchmark itself

Not everything found was in the product. These were defects in the measurement harness, and each
would have published a wrong number:

- **The reference floor ran at the wrong parallelism.** `SLICES` was a literal left over from a
  5-shard topology, so the sliced floor took 5 slices over a 6-shard index — one slice carrying two
  shards, and the wall clock is the slowest slice. It measured 19.2 s instead of 13.2 s, a **31%
  handicap in our own favour**, on the number our extraction is compared against. The slice count is
  now derived from the index's actual shard count.
- **The equivalence gate could pass without running.** Its verdict is `PASS` / `PASS_WITH_SKIPS` /
  `FAIL`, and the guard reading it matched on a prefix — so a session in which Trino was not running,
  and every Trino leg was skipped, graded as a pass. Engines are now started and proven healthy
  before the gate, and a skip naming either timed engine is fatal.
- **The join oracles were hardcoded for a corpus that had been regenerated.** Two engines returning
  the same "wrong" row count is the signature of a bad oracle, not a bad engine. They are now read
  from Elasticsearch at run time, with the id window read from the small leg rather than assumed.
- **The first engine measured paid a cold-cache tax no later block paid** — worth 25% of our S1 wall
  clock, and invisible unless the drift arm runs for *both* engines. See RESULTS section 1.

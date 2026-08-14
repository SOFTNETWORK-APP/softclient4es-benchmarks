# Findings — issues discovered and fixed during benchmarking

Building this benchmark surfaced several issues in SoftClient4ES. All were fixed, and the results in
[RESULTS.md](RESULTS.md) were measured on the released `0.2.5` build that contains the fixes. This
document is the summary of what was found and how it was resolved.

## Extraction performance

Two independent causes were making large extractions slower than they needed to be:

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
correct run.

## Result shape

Extraction rows previously carried Elasticsearch hit-metadata columns (`_id`, `_index`, `_score`)
in addition to the selected columns. Rows now contain exactly the columns the SQL projects, so the
client receives — and pays for — only the requested data (8 columns for the S1 query).

## Environment note

The published sidecar image originally shipped without a logging configuration, which caused it to
log at DEBUG and write every Elasticsearch response byte to the container log — a large performance
and data-exposure problem. The image now ships a logging configuration that keeps wire-level logging
off by default; the benchmark runs the sidecar as shipped.

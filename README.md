# SoftClient4ES benchmarks

Reproducible benchmarks comparing SoftClient4ES against comparable stacks. Each subdirectory is one
self-contained benchmark: a Docker Compose stack, a deterministic data generator, stock clients, and
documented results.

| Benchmark | Compares | Status |
|---|---|---|
| [extraction-vs-trino](extraction-vs-trino/) | Extracting large result sets out of Elasticsearch into a Python client, vs Trino's Elasticsearch connector | Complete — see [its results](extraction-vs-trino/RESULTS.md) |

## Principles

A benchmark published by the vendor of one of the systems is read critically, and should be.
Every benchmark here follows the same rules:

1. **Every number comes from a run of the harness in this repository** — nothing is estimated or
   carried over from elsewhere.
2. **Correctness before timing.** Runners assert the exact expected row count and cross-check that
   both systems returned the same data; a run that returns the wrong result is discarded, not timed.
3. **Both systems use stock clients and documented defaults,** pinned explicitly. Where a default is
   changed, the reason is recorded in the benchmark's `METHODOLOGY.md` and applied to both sides.
4. **Biases are disclosed in the direction they run,** including the ones that count against
   SoftClient4ES.
5. **Every benchmark states where the other system is stronger,** with the same prominence as the
   rest of the results.
6. **Licence tiers are disclosed** wherever a quota affects what can be measured.

## Reproducing

Each benchmark's README lists its host requirements and run commands. All expect Docker and a native
CPython 3.12, and none depend on anything outside their own directory.

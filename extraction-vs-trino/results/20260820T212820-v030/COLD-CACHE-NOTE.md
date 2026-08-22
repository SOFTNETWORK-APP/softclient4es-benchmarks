# flight S1: publish the drift-arm (warm) value, not the matrix (cold) one

## What was measured

| cell | when | wall | client CPU | engine CPU | ES CPU |
|---|---|---|---|---|---|
| `flight-S1-run*`       | 21:36 | 13.07 s | 2.93 | 32.5 | **47.5** |
| `drift-flight-S1-run*` | 22:49 |  9.78 s | 2.47 | 24.0 | **34.9** |
| `trino-S1-run*`        | 21:40 | 44.92 s | 24.24 | 55.8 | 31.1 |
| `drift-trino-S1-run*`  | 23:00 | 44.79 s | 24.13 | 55.6 | 30.2 |

Flight gains **25.1 %** between the two blocks. Trino gains **0.3 %**.

## Why

Not JIT: each block is internally flat (flight cold 13.15→12.62, warm 9.56→9.89), so the
harness's two warmups had already done their work.

Not the cluster warming as a whole: Trino queries the SAME cluster in the same two eras
and gains nothing, which rules out a cluster-wide effect.

The cause is **first touch**. Flight's was the first engine block of the session, so it
paid the cold-page-cache cost alone. By the time Trino ran four minutes later, flight had
pulled 10,000,000 rows seven times and the cache was hot — Trino's "cold" block was never
cold. The ES-CPU column is the evidence: ours falls 47.5 → 34.9 and converges on Trino's
stable 30–31, which is where a warm full scan settles for either engine.

Trino is insensitive because at ~45 s it is bound by its own processing rather than by how
fast Elasticsearch can serve pages. Flight is now fast enough that page-cache state is
worth 25 % of its wall clock.

## Decision

Publish the **warm** pair. Comparing flight's cold 13.07 s against Trino's already-warm
44.92 s is not like-for-like, and it understates us: 3.44× where the warm-vs-warm figure
is **4.58×**. Both numbers are published and the sensitivity is disclosed; the cold value
is not deleted.

**Only this one cell is affected.** `flight-S1r` (ES CPU 35.2) and `flight-S2` (35.4)
already carry the warm signature, not the cold 47.5 — they ran after the cache was hot.

## Consequence for future sessions

The block order in `run_full_session.sh` gives the first engine measured a cold-cache
penalty that no later block pays. Either warm the index before block 2, or measure a drift
arm for BOTH engines (this session had one only for flight, and `summarize.py` warned about
exactly that: `S1/drift: variant arm covers only ['flight']`).

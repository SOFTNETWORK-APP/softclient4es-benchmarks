# Which sessions back which document

`results/` holds measured runs from several dates. Only some of them back the current
RESULTS.md, and a session that no longer does still *looks* authoritative — same file
names, same shape, different numbers. This file says which is which.

Verify rather than trust it: `python3.12 runners/verify_claims.py` re-derives every
published figure from the sessions listed as CURRENT and fails if the document disagrees.

## CURRENT — sidecar 0.3.0, digest `sha256:9dd80b77f62e`

RESULTS.md and `report/` are measured entirely from these. One night, one host, one image
(core 0.21.0, extensions 0.3.0).

| Session | Backs |
|---|---|
| `20260821T041841-v030-prewarm` | **the published campaign** — the 6-shard matrix re-measured under the corrected protocol: steady-state warm-in gate (`warm-in.json`), per-run throttling counters, per-container network legs, drift controls for both engines, floors incl. `--build arrow` |
| `20260821T041841b-v030-prewarm-1shard` | section 6 (1-shard), its own gate and splits probe; the `b` marks the re-run after the host-sleep abort |
| `20260821T041841-v030-prewarm-sliced-ab` | section 6.1, sliced-vs-sequential A/B |
| `capped-…`, `concurrent-…`, `join-20260821T041841-v030-prewarm` | S5, S6 and J0–J2 for the published campaign |
| `ABORTED-sleep-…-1shard` | quarantined whole: macOS slept mid-block, Trino killed the stalled client's queries |
| `20260820T212820-v030` | *(previous campaign)* the 6-shard matrix: floors (count-and-discard **and** `--build arrow`), S1, S1m, S1r and every destination, S2, S3, S4, both ES\|QL wire formats, Trino's connectorx and ADBC routes, the tuned-Trino and hostname-dial arms, the drift controls for **both** engines, and the probes (connect, ES\|QL truncation, ES\|QL join, Trino splits) |
| `20260820T212820-v030-1shard` | section 6, with its own equivalence gate against the 1-shard index and its own splits probe |
| `20260820T212820-v030-sliced-ab` | section 6.1, the sliced-vs-sequential paging A/B |
| `capped-20260820T212820-v030`, `capped-cx-…` | S5, the memory-cap sweep |
| `concurrent-20260820T212820-v030`, `concurrent-cx-…` | S6, concurrency within an 8 GB budget |
| `join-20260820T212820-v030` | J0–J2, measured behind the engine-quiescence gate |
| `idle-gate-*.json` | the gate's own record: what it waited for, and for how long |

## Quarantined inside the CURRENT sessions — measured, but backing nothing

These are real measurements of something other than what the table claims. They are kept in
place, each with a README, because a benchmark that deletes its inconvenient runs is asking
to be trusted rather than checked.

| Path | Why it backs no figure |
|---|---|
| `20260820T212820-v030/void-5slice-floors/` | the floor at 5 slices over a 6-shard index — one slice carries two shards, so the wall clock is the slowest slice. 19.2 s instead of 13.2 s, a 31% handicap **in our favour** |
| `20260820T212820-v030/COLD-CACHE-NOTE.md` | the cold flight S1 block: ours was the first engine measured and paid a page-cache cost Trino never paid. 13.07 s cold against 9.78 s warm, while Trino moved 0.3%. The warm pair is published; the cold block is retained |
| `join-20260820T212820-v030/void-order-effect/` | joins measured before both legs were warm. Trino's J2 degraded 37% across its own runs; quarantined for **both** engines. Discarding it made Trino's win *larger* |
| `join-20260820T212820-v030/JOIN-PASS-NOTE.md` | a third join pass of our side alone, agreeing with the published one within 5%. Not used: it would give one engine more evidence than the other |

## DISCARDED — kept deliberately, published nowhere

| Session | Why it was thrown away |
|---|---|
| `CONTAMINATED-join-20260818T230041-v0251` | timed on top of Trino's unfinished work from S6 (`ABANDONED_QUERY` with 174 s of "finishing"; `ABANDONED_TASK` still running after 1,000 s). **Both** engines' J2 degraded ~4× and Trino's J0 failed 5/5, on a host that was provably clean — i.e. it made the competitor look broken. Re-measured behind `wait_engines_idle.py`; see `CONTAMINATED.txt` inside |
| `ABORTED-20260818T213431-v0251` | the host left `pressure level 1` mid-session and the memory guard refused to continue, 100 runs in. Restarted from scratch on a clean VM rather than resumed |

## RETIRED — earlier sessions, superseded

Sessions dated 2026-08-13 through -19 measured **sidecar 0.2.5 or 0.2.5.1** and back no
current figure. They are kept rather than deleted so the repository shows what was measured
on the earlier builds — but a number from them will not match RESULTS.md, and the gap is not
an error. Two changes in the software account for most of it:

- on 0.2.5 the Flight schema probe re-ran any statement carrying a `LIMIT`, which is why
  S1m reads 7.6 s in the oldest sessions and 3.97 s now;
- on 0.2.5.1 extraction paged Elasticsearch sequentially, which is why S1 reads ~34 s there
  and 9.78 s here. RESULTS section 6.1 measures that difference directly rather than
  inferring it across sessions.

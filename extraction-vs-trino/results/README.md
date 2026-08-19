# Which sessions back which document

`results/` holds measured runs from several dates. Only some of them back the current
RESULTS.md, and a session that no longer does still *looks* authoritative — same file
names, same shape, different numbers. This file says which is which.

Verify rather than trust it: `python3.12 runners/verify_claims.py` re-derives every
published figure from the sessions listed as CURRENT and fails if the document disagrees.

## CURRENT — sidecar 0.2.5.1, digest `sha256:90f0cf405655`

RESULTS.md and `report/` are measured entirely from these. One night, one host, one image.

| Session | Backs |
|---|---|
| `20260818T230041-v0251` | the single-shard matrix: floors, S1, S1m, S1r, S2, S3, S4, both ES\|QL wire formats, the tuned-Trino and hostname-dial arms, the drift control, and the four probes (connect, OS resolver, ES\|QL truncation, ES\|QL join) |
| `20260818T230041-v0251-5shard` | section 6, with its own equivalence gate against the 5-shard index |
| `capped-20260818T230041-v0251`, `capped-cx-…` | S5, the memory-cap sweep |
| `concurrent-20260818T230041-v0251`, `concurrent-cx-…` | S6, concurrency within an 8 GB budget |
| `join-20260818T230041-v0251` | J0–J2, measured behind the engine-quiescence gate |
| `idle-gate-*.json` | the gate's own record: what it waited for, and for how long |

⚠️ `plan.json` inside the CURRENT sessions records only the **last** orchestrate invocation, not
all ~16 that make up the matrix: the harness overwrote the file per invocation. It is left exactly
as the harness wrote it rather than reconstructed after the fact — `session.log` is the authoritative
list of what ran, in order, with timestamps. Fixed forward: `orchestrate.py` now appends each
invocation and records the running `measured_runs` total, so later sessions describe themselves.

## DISCARDED — kept deliberately, published nowhere

Neither of these contributed a single figure. They are committed because a benchmark that
only keeps its good runs is asking to be trusted rather than checked.

| Session | Why it was thrown away |
|---|---|
| `CONTAMINATED-join-20260818T230041-v0251` | timed on top of Trino's unfinished work from S6 (`ABANDONED_QUERY` with 174 s of "finishing"; `ABANDONED_TASK` still running after 1,000 s). **Both** engines' J2 degraded ~4× and Trino's J0 failed 5/5, on a host that was provably clean — i.e. it made the competitor look broken. Re-measured behind `wait_engines_idle.py`; see `CONTAMINATED.txt` inside |
| `ABORTED-20260818T213431-v0251` | the host left `pressure level 1` mid-session and the memory guard refused to continue, 100 runs in. Restarted from scratch on a clean VM rather than resumed |

## RETIRED — earlier sessions, superseded

Sessions dated 2026-08-13, -14 and -16 measured **sidecar 0.2.5** and back no current
figure. They are the evidence for what the document said *then*, so they are kept, not
deleted — but a number from them will not match RESULTS.md today, and the gap is not an
error: on 0.2.5 the Flight schema probe re-ran any statement carrying a `LIMIT`, which is
why S1m reads 7.6 s there and 3.85 s here.

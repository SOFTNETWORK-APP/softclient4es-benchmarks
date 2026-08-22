# JOIN figures come from ONE paired block, both engines together

Three passes were run. The published one is the middle pass, in which flight AND trino
were measured in the same block, warm, with both join legs pre-read.

- **Pass A** (23:35) — quarantined in `void-order-effect/`. Trino's J2 degraded
  monotonically across its own five runs (7.66 → 10.50, +37%) and a later fresh warmup of
  the same cell ran 6.57 s, faster than any measured run. `bench_1m` is read by nothing
  except the join block, so it was genuinely cold. Quarantined for BOTH engines, not one.
- **Pass B** (00:44) — PUBLISHED for **J0 and J2**. Both engines, one block, both legs warmed
  first. Every arm flat: drift −0.1% to +1.8%.
- **J1** is published from its own paired block an hour earlier (flight 21:42:03–21:42:20Z,
  trino 21:42:28–21:42:43Z — 25 s apart, spreads 1.3% and 2.1%). It was re-measured warm after
  the oracle fix and was never part of pass A's quarantine, so it was not re-run with J0/J2.
  The pairing is symmetric; the block is not shared with J0/J2, and RESULTS says so.
- **Pass C** (01:1x) — flight only, dropped. It agreed with pass B to within 5%
  (J0 +0.5%, J1 +3.0%, J2 +5.2%), so it stands as a reproducibility check and nothing more.

**Why C is dropped rather than averaged:** it would give flight two passes against Trino's
one, and a figure must not be built from more evidence on one side than the other. Note the
pass-A quarantine moved the result AGAINST us — Trino's J2 improved 8.37 → 6.78 s once
measured cleanly, widening its win.

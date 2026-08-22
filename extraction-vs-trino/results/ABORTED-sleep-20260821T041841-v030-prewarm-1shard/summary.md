<!-- session: 20260821T041841-v030-prewarm-1shard -->
<!-- WARNING: S1/1shard: variant arm covers only ['flight'] (absent: ['trino']) -->

### Raw medians

| Scenario | Variant | Stack | Runs | Wall median (s) | Wall min-max (s) | CPU median (s) | Peak client memory median (MB) | Rows |
|---|---|---|---|---|---|---|---|---|
| S1 | 1shard | flight | 5 | 41.81 | 41.54-49.76 | 3.83 | 923 (footprint) | 10,000,000 |

### Host load during the session

1-minute load average sampled after every run: median 3.9, max 6.2, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 9.8 cores were not committed.

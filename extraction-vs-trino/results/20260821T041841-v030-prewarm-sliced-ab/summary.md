<!-- session: 20260821T041841-v030-prewarm-sliced-ab -->
<!-- WARNING: S1/sequential: variant arm covers only ['flight'] (absent: ['trino']) -->
<!-- WARNING: S1/sliced: variant arm covers only ['flight'] (absent: ['trino']) -->
<!-- WARNING: S2/sequential: variant arm covers only ['flight'] (absent: ['trino']) -->
<!-- WARNING: S2/sliced: variant arm covers only ['flight'] (absent: ['trino']) -->
<!-- WARNING: S3/sequential: variant arm covers only ['flight'] (absent: ['trino']) -->
<!-- WARNING: S3/sliced: variant arm covers only ['flight'] (absent: ['trino']) -->
<!-- WARNING: S4/sequential: variant arm covers only ['flight'] (absent: ['trino']) -->
<!-- WARNING: S4/sliced: variant arm covers only ['flight'] (absent: ['trino']) -->

### Raw medians

| Scenario | Variant | Stack | Runs | Wall median (s) | Wall min-max (s) | CPU median (s) | Peak client memory median (MB) | Rows |
|---|---|---|---|---|---|---|---|---|
| S1 | sequential | flight | 5 | 35.34 | 35.09-39.42 | 3.86 | 923 (footprint) | 10,000,000 |
| S1 | sliced | flight | 5 | 10.48 | 10.13-10.58 | 2.55 | 919 (footprint) | 10,000,000 |
| S2 | sequential | flight | 5 | 36.97 | 36.06-38.76 | 5.02 | 942 (footprint) | 10,000,000 |
| S2 | sliced | flight | 5 | 14.49 | 13.31-15.43 | 4.37 | 945 (footprint) | 10,000,000 |
| S3 | sequential | flight | 5 | 0.03 | 0.03-0.03 | 0.01 | 83 (footprint) | 100 |
| S3 | sliced | flight | 5 | 0.03 | 0.03-0.04 | 0.01 | 85 (footprint) | 100 |
| S4 | sequential | flight | 5 | 0.02 | 0.02-0.02 | 0.01 | 84 (footprint) | 100 |
| S4 | sliced | flight | 5 | 0.02 | 0.02-0.03 | 0.01 | 86 (footprint) | 100 |

### Host load during the session

1-minute load average sampled after every run: median 7.9, max 13.1, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 2.9 cores were not committed.

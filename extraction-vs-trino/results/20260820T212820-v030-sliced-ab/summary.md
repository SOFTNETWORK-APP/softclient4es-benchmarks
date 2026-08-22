<!-- session: 20260820T212820-v030-sliced-ab -->
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
| S1 | sequential | flight | 5 | 35.55 | 35.50-35.80 | 3.80 | 915 (footprint) | 10,000,000 |
| S1 | sliced | flight | 5 | 10.06 | 9.92-10.31 | 2.50 | 921 (footprint) | 10,000,000 |
| S2 | sequential | flight | 5 | 35.79 | 35.64-36.15 | 4.93 | 944 (footprint) | 10,000,000 |
| S2 | sliced | flight | 5 | 12.85 | 11.59-13.31 | 4.08 | 943 (footprint) | 10,000,000 |
| S3 | sequential | flight | 5 | 0.03 | 0.03-0.03 | 0.01 | 84 (footprint) | 100 |
| S3 | sliced | flight | 5 | 0.03 | 0.03-0.03 | 0.01 | 83 (footprint) | 100 |
| S4 | sequential | flight | 5 | 0.02 | 0.02-0.03 | 0.01 | 84 (footprint) | 100 |
| S4 | sliced | flight | 5 | 0.02 | 0.02-0.03 | 0.01 | 84 (footprint) | 100 |

### Host load during the session

1-minute load average sampled after every run: median 7.4, max 9.8, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 6.2 cores were not committed.

<!-- session: 20260818T230041-v0251-5shard -->
<!-- WARNING: S3/flight/5shard spread is 44% of the median (0.04-0.05s) -- the median understates the noise -->

### Raw medians

| Scenario | Variant | Stack | Runs | Wall median (s) | Wall min-max (s) | CPU median (s) | Peak client memory median (MB) | Rows |
|---|---|---|---|---|---|---|---|---|
| S1 | 5shard | flight | 5 | 30.08 | 29.71-30.35 | 4.60 | 916 (footprint) | 10,000,000 |
| S1 | 5shard | trino | 5 | 45.73 | 45.38-46.17 | 24.15 | 4,464 (footprint) | 10,000,000 |
| S3 | 5shard | flight | 5 | 0.04 | 0.04-0.05 | 0.01 | 81 (footprint) | 100 |
| S3 | 5shard | trino | 5 | 5.73 | 5.70-6.78 | 0.02 | 24 (footprint) | 100 |

### S1 (5shard)

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1 | wall-clock median (s) | 30.08 | 45.73 | 1.5x |
| S1 | client CPU median (s) | 4.60 | 24.15 | 5.2x |
| S1 | peak client memory median (MB) | 916 (footprint) | 4,464 (footprint) | 4.9x |
| S1 | wall-clock min-max (s) | 29.71-30.35 | 45.38-46.17 | - |
| S1 | connect-only median (s) | 0.012 | 0.003 | - |
| S1 | bytes to client, median (MB) | 709 | 976 | 1.4x |
| S1 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S3 (5shard)

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S3 | wall-clock median (s) | 0.04 | 5.73 | 147.3x |
| S3 | client CPU median (s) | 0.01 | 0.02 | 2.1x |
| S3 | peak client memory median (MB) | 81 (footprint) | 24 (footprint) | 0.3x |
| S3 | wall-clock min-max (s) | 0.04-0.05 | 5.70-6.78 | - |
| S3 | connect-only median (s) | 0.013 | 0.002 | - |
| S3 | bytes to client, median (MB) | 0 | 6 | 525.4x |
| S3 | rows returned (gate: identical) | 100 | 100 | - |

### Host load during the session

1-minute load average sampled after every run: median 3.9, max 5.1, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 10.9 cores were not committed.

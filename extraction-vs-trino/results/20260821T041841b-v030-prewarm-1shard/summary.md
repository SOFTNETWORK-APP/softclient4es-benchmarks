<!-- session: 20260821T041841b-v030-prewarm-1shard -->
<!-- WARNING: S1/flight/1shard spread is 59% of the median (40.93-65.60s) -- the median understates the noise -->
<!-- WARNING: S3/flight/1shard spread is 33% of the median (0.03-0.05s) -- the median understates the noise -->

### Raw medians

| Scenario | Variant | Stack | Runs | Wall median (s) | Wall min-max (s) | CPU median (s) | Peak client memory median (MB) | Rows |
|---|---|---|---|---|---|---|---|---|
| S1 | 1shard | flight | 5 | 41.69 | 40.93-65.60 | 3.56 | 922 (footprint) | 10,000,000 |
| S1 | 1shard | trino | 5 | 55.11 | 54.18-56.17 | 24.29 | 4,473 (footprint) | 10,000,000 |
| S3 | 1shard | flight | 5 | 0.04 | 0.03-0.05 | 0.01 | 84 (footprint) | 100 |
| S3 | 1shard | trino | 5 | 28.54 | 28.42-29.73 | 0.05 | 25 (footprint) | 100 |

### S1 (1shard)

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1 | wall-clock median (s) | 41.69 | 55.11 | 1.3x |
| S1 | client CPU median (s) | 3.56 | 24.29 | 6.8x |
| S1 | peak client memory median (MB) | 922 (footprint) | 4,473 (footprint) | 4.9x |
| S1 | wall-clock min-max (s) | 40.93-65.60 | 54.18-56.17 | - |
| S1 | connect-only median (s) | 0.014 | 0.003 | - |
| S1 | bytes to client, median (MB) | 703 | 973 | 1.4x |
| S1 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S3 (1shard)

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S3 | wall-clock median (s) | 0.04 | 28.54 | 754.8x |
| S3 | client CPU median (s) | 0.01 | 0.05 | 5.2x |
| S3 | peak client memory median (MB) | 84 (footprint) | 25 (footprint) | 0.3x |
| S3 | wall-clock min-max (s) | 0.03-0.05 | 28.42-29.73 | - |
| S3 | connect-only median (s) | 0.011 | 0.002 | - |
| S3 | bytes to client, median (MB) | 0 | 6 | 539.7x |
| S3 | rows returned (gate: identical) | 100 | 100 | - |

### Host load during the session

1-minute load average sampled after every run: median 3.7, max 11.8, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 4.2 cores were not committed.

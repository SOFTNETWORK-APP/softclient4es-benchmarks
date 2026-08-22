<!-- session: 20260820T212820-v030-1shard -->
<!-- WARNING: S3/flight/1shard spread is 27% of the median (0.04-0.05s) -- the median understates the noise -->

### Raw medians

| Scenario | Variant | Stack | Runs | Wall median (s) | Wall min-max (s) | CPU median (s) | Peak client memory median (MB) | Rows |
|---|---|---|---|---|---|---|---|---|
| S1 | 1shard | flight | 5 | 42.22 | 42.06-42.31 | 3.95 | 921 (footprint) | 10,000,000 |
| S1 | 1shard | trino | 5 | 56.76 | 56.05-56.95 | 24.33 | 4,472 (footprint) | 10,000,000 |
| S3 | 1shard | flight | 5 | 0.04 | 0.04-0.05 | 0.01 | 83 (footprint) | 100 |
| S3 | 1shard | trino | 5 | 28.16 | 28.04-28.48 | 0.06 | 25 (footprint) | 100 |

### S1 (1shard)

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1 | wall-clock median (s) | 42.22 | 56.76 | 1.3x |
| S1 | client CPU median (s) | 3.95 | 24.33 | 6.2x |
| S1 | peak client memory median (MB) | 921 (footprint) | 4,472 (footprint) | 4.9x |
| S1 | wall-clock min-max (s) | 42.06-42.31 | 56.05-56.95 | - |
| S1 | connect-only median (s) | 0.014 | 0.003 | - |
| S1 | bytes to client, median (MB) | 703 | 973 | 1.4x |
| S1 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S3 (1shard)

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S3 | wall-clock median (s) | 0.04 | 28.16 | 707.0x |
| S3 | client CPU median (s) | 0.01 | 0.06 | 5.7x |
| S3 | peak client memory median (MB) | 83 (footprint) | 25 (footprint) | 0.3x |
| S3 | wall-clock min-max (s) | 0.04-0.05 | 28.04-28.48 | - |
| S3 | connect-only median (s) | 0.013 | 0.001 | - |
| S3 | bytes to client, median (MB) | 0 | 6 | 543.7x |
| S3 | rows returned (gate: identical) | 100 | 100 | - |

### Host load during the session

1-minute load average sampled after every run: median 2.9, max 4.2, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 11.8 cores were not committed.

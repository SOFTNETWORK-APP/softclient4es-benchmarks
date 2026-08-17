<!-- session: 20260816T194233 -->
<!-- WARNING: S1/drift: variant arm covers only ['flight'] (absent: ['trino']) -->
<!-- WARNING: S1/tuned-tuned: variant arm covers only ['trino'] (absent: ['flight']) -->
<!-- WARNING: S1m/arrow-arrow: variant arm covers only ['esql'] (absent: ['flight', 'trino']) -->
<!-- WARNING: S3/arrow-arrow: variant arm covers only ['esql'] (absent: ['flight', 'trino']) -->
<!-- WARNING: S4/arrow-arrow: variant arm covers only ['esql'] (absent: ['flight', 'trino']) -->
<!-- WARNING: S4/dialhostname-dialhostname: variant arm covers only ['flight'] (absent: ['trino']) -->

### Raw medians

| Scenario | Variant | Stack | Runs | Wall median (s) | Wall min-max (s) | CPU median (s) | Peak client memory median (MB) | Rows |
|---|---|---|---|---|---|---|---|---|
| S0 | - | es-raw | 5 | 45.33 | 45.24-45.54 | 14.71 | 27 (footprint) | 10,000,000 |
| S0p | - | es-raw | 5 | 23.27 | 23.04-23.71 | 15.43 | 135 (footprint) | 10,000,000 |
| S1 | - | flight | 5 | 36.86 | 36.73-36.86 | 3.93 | 921 (footprint) | 10,000,000 |
| S1 | drift | flight | 5 | 36.06 | 35.73-36.30 | 3.89 | 918 (footprint) | 10,000,000 |
| S1 | - | trino | 5 | 51.81 | 51.67-52.18 | 24.02 | 4,463 (footprint) | 10,000,000 |
| S1 | tuned-tuned | trino | 5 | 49.70 | 49.10-50.57 | 24.28 | 4,466 (footprint) | 10,000,000 |
| S1m | - | esql | 5 | 1.17 | 1.16-1.20 | 0.51 | 669 (footprint) | 1,000,000 |
| S1m | arrow-arrow | esql | 5 | 0.51 | 0.50-0.52 | 0.05 | 166 (footprint) | 1,000,000 |
| S1m | - | flight | 5 | 7.63 | 7.62-7.66 | 0.18 | 176 (footprint) | 1,000,000 |
| S1m | - | trino | 5 | 5.30 | 5.27-5.40 | 2.22 | 474 (footprint) | 1,000,000 |
| S3 | - | esql | 5 | 0.15 | 0.15-0.16 | 0.00 | 16 (footprint) | 100 |
| S3 | arrow-arrow | esql | 5 | 0.21 | 0.20-0.21 | 0.00 | 28 (footprint) | 100 |
| S3 | - | flight | 5 | 0.03 | 0.03-0.03 | 0.01 | 82 (footprint) | 100 |
| S3 | - | trino | 5 | 25.99 | 25.88-26.42 | 0.04 | 24 (footprint) | 100 |
| S4 | - | esql | 5 | 0.01 | 0.00-0.01 | 0.00 | 16 (footprint) | 100 |
| S4 | arrow-arrow | esql | 5 | 0.01 | 0.01-0.01 | 0.00 | 28 (footprint) | 100 |
| S4 | - | flight | 5 | 0.03 | 0.02-0.03 | 0.01 | 83 (footprint) | 100 |
| S4 | dialhostname-dialhostname | flight | 5 | 0.06 | 0.05-0.06 | 0.01 | 82 (footprint) | 100 |
| S4 | - | trino | 5 | 0.03 | 0.03-0.03 | 0.01 | 23 (footprint) | 100 |

### S0 / S0p -- the floors both stacks pay

| Floor | Wall median (s) | Client CPU median (s) | Peak client memory (MB) | Runs |
|---|---|---|---|---|
| S0 single-process scroll | 45.33 | 14.71 | 27 (footprint) | 5 |
| S0p sliced scroll (parallel), 5 slices | 23.27 | 15.43 | 135 (footprint) | 5 |

<!-- S0p is 1.95x S0's wall clock for 1.05x its client CPU, across 5 processes. -->

### S1

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1 | wall-clock median (s) | 36.86 | 51.81 | 1.4x |
| S1 | client CPU median (s) | 3.93 | 24.02 | 6.1x |
| S1 | peak client memory median (MB) | 921 (footprint) | 4,463 (footprint) | 4.8x |
| S1 | wall-clock min-max (s) | 36.73-36.86 | 51.67-52.18 | - |
| S1 | connect-only median (s) | 0.012 | 0.002 | - |
| S1 | bytes to client, median (MB) | 703 | 973 | 1.4x |
| S1 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S1m

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1m | wall-clock median (s) | 7.63 | 5.30 | 0.7x |
| S1m | client CPU median (s) | 0.18 | 2.22 | 12.0x |
| S1m | peak client memory median (MB) | 176 (footprint) | 474 (footprint) | 2.7x |
| S1m | wall-clock min-max (s) | 7.62-7.66 | 5.27-5.40 | - |
| S1m | connect-only median (s) | 0.012 | 0.002 | - |
| S1m | bytes to client, median (MB) | 71 | 97 | 1.4x |
| S1m | rows returned (gate: identical) | 1,000,000 | 1,000,000 | - |

### S3

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S3 | wall-clock median (s) | 0.03 | 25.99 | 909.7x |
| S3 | client CPU median (s) | 0.01 | 0.04 | 3.7x |
| S3 | peak client memory median (MB) | 82 (footprint) | 24 (footprint) | 0.3x |
| S3 | wall-clock min-max (s) | 0.03-0.03 | 25.88-26.42 | - |
| S3 | connect-only median (s) | 0.011 | 0.002 | - |
| S3 | bytes to client, median (MB) | 0 | 6 | 540.9x |
| S3 | rows returned (gate: identical) | 100 | 100 | - |

### S4

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S4 | wall-clock median (s) | 0.03 | 0.03 | 1.2x |
| S4 | client CPU median (s) | 0.01 | 0.01 | 0.8x |
| S4 | peak client memory median (MB) | 83 (footprint) | 23 (footprint) | 0.3x |
| S4 | wall-clock min-max (s) | 0.02-0.03 | 0.03-0.03 | - |
| S4 | connect-only median (s) | 0.013 | 0.002 | - |
| S4 | bytes to client, median (MB) | 0 | 0 | 6.3x |
| S4 | rows returned (gate: identical) | 100 | 100 | - |

### ES|QL -- Elasticsearch's own query language

| Scenario | Wire | Arm | Wall median (s) | Client CPU median (s) | Peak client memory (MB) | Rows | Runs |
|---|---|---|---|---|---|---|---|
| S1m | json | base | 1.165 | 0.512 | 669 (footprint) | 1,000,000 | 5 |
| S1m | arrow | arrow-arrow | 0.509 | 0.050 | 166 (footprint) | 1,000,000 | 5 |
| S3 | json | base | 0.155 | 0.001 | 16 (footprint) | 100 | 5 |
| S3 | arrow | arrow-arrow | 0.208 | 0.001 | 28 (footprint) | 100 | 5 |
| S4 | json | base | 0.005 | 0.000 | 16 (footprint) | 100 | 5 |
| S4 | arrow | arrow-arrow | 0.007 | 0.001 | 28 (footprint) | 100 | 5 |

<!-- ES|QL ran with esql.query.result_truncation_max_size=1000000; its product maximum is 1,000,000, which is why S1/S2/S5/S6 have no ES|QL column. -->

### Drift control -- S1/flight

| Block | Wall median (s) | Runs |
|---|---|---|
| first (A) | 36.86 | 5 |
| repeat at session end (A') | 36.06 | 5 |

Session drift -2.2%, OUTSIDE A's own run-to-run spread of 0.4%. The blocks are NOT interchangeable: this session's cross-stack comparison carries a drift larger than its noise and must say so.

### Host load during the session

1-minute load average sampled after every run: median 4.0, max 9.3, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 6.7 cores were not committed.

<!-- METHODOLOGY section 6: S1 Flight returned 8 columns (0 beyond the 8 selected). -->
<!-- observed column list: id, event_ts, amount, qty, status, country, category, name -->

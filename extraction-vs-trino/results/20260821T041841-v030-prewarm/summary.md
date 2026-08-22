<!-- session: 20260821T041841-v030-prewarm -->
<!-- WARNING: S3/esql/base spread is 86% of the median (0.03-0.06s) -- the median understates the noise -->
<!-- WARNING: S4/flight/dialhostname spread is 25% of the median (0.05-0.06s) -- the median understates the noise -->
<!-- WARNING: S1/arrowadbc: variant arm covers only ['trino'] (absent: ['flight']) -->
<!-- WARNING: S1/arrowcx: variant arm covers only ['trino'] (absent: ['flight']) -->
<!-- WARNING: S1/drift: variant arm covers only ['flight'] (absent: ['trino']) -->
<!-- WARNING: S1/tuned: variant arm covers only ['trino'] (absent: ['flight']) -->
<!-- WARNING: S1m/arrow: variant arm covers only ['esql'] (absent: ['flight', 'trino']) -->
<!-- WARNING: S1r/pandasadbc: variant arm covers only ['trino'] (absent: ['flight']) -->
<!-- WARNING: S1r/pandascx: variant arm covers only ['trino'] (absent: ['flight']) -->
<!-- WARNING: S1r/polarsadbc: variant arm covers only ['trino'] (absent: ['flight']) -->
<!-- WARNING: S1r/polarscx: variant arm covers only ['trino'] (absent: ['flight']) -->
<!-- WARNING: S3/arrow: variant arm covers only ['esql'] (absent: ['flight', 'trino']) -->
<!-- WARNING: S4/arrow: variant arm covers only ['esql'] (absent: ['flight', 'trino']) -->
<!-- WARNING: S4/dialhostname: variant arm covers only ['flight'] (absent: ['trino']) -->

### Raw medians

| Scenario | Variant | Stack | Runs | Wall median (s) | Wall min-max (s) | CPU median (s) | Peak client memory median (MB) | Rows |
|---|---|---|---|---|---|---|---|---|
| S0 | - | es-raw | 5 | 37.00 | 36.76-37.14 | 14.49 | 27 (footprint) | 10,000,000 |
| S0p | - | es-raw | 5 | 12.46 | 12.21-12.94 | 15.97 | 165 (footprint) | 10,000,000 |
| S1 | - | flight | 5 | 11.91 | 11.73-12.01 | 2.83 | 921 (footprint) | 10,000,000 |
| S1 | drift | flight | 5 | 10.04 | 9.88-10.05 | 2.65 | 921 (footprint) | 10,000,000 |
| S1 | - | trino | 5 | 44.70 | 44.45-44.97 | 24.05 | 4,455 (footprint) | 10,000,000 |
| S1 | arrowadbc | trino | 5 | 26.97 | 26.87-27.01 | 18.64 | 1,963 (footprint) | 10,000,000 |
| S1 | arrowcx | trino | 5 | 14.66 | 14.51-14.71 | 10.52 | 617 (footprint) | 10,000,000 |
| S1 | tuned | trino | 5 | 44.41 | 44.32-44.50 | 23.82 | 4,465 (footprint) | 10,000,000 |
| S1m | - | esql | 5 | 0.98 | 0.95-1.07 | 0.50 | 666 (footprint) | 1,000,000 |
| S1m | arrow | esql | 5 | 0.32 | 0.28-0.34 | 0.05 | 166 (footprint) | 1,000,000 |
| S1m | - | flight | 5 | 3.99 | 3.97-4.05 | 0.19 | 176 (footprint) | 1,000,000 |
| S1m | - | trino | 5 | 4.42 | 4.40-4.46 | 2.23 | 475 (footprint) | 1,000,000 |
| S1r | - | flight | 5 | 9.97 | 9.76-10.01 | 2.67 | 1,481 (footprint) | 10,000,000 |
| S1r | arrowdtype | flight | 5 | 9.95 | 9.94-10.17 | 2.63 | 919 (footprint) | 10,000,000 |
| S1r | polars | flight | 5 | 10.94 | 10.52-11.25 | 11.31 | 2,484 (footprint) | 10,000,000 |
| S1r | - | trino | 5 | 56.13 | 55.86-56.18 | 35.30 | 8,079 (footprint) | 10,000,000 |
| S1r | arrowdtype | trino | 5 | 55.84 | 55.75-56.47 | 34.90 | 7,801 (footprint) | 10,000,000 |
| S1r | pandasadbc | trino | 5 | 27.29 | 27.08-27.42 | 18.88 | 2,542 (footprint) | 10,000,000 |
| S1r | pandascx | trino | 5 | 15.52 | 14.98-15.71 | 11.54 | 2,824 (footprint) | 10,000,000 |
| S1r | polars | trino | 5 | 56.04 | 55.85-56.34 | 34.95 | 11,047 (footprint) | 10,000,000 |
| S1r | polarsadbc | trino | 5 | 28.20 | 27.96-28.82 | 28.17 | 3,551 (footprint) | 10,000,000 |
| S1r | polarscx | trino | 5 | 15.34 | 14.89-15.45 | 11.64 | 2,186 (footprint) | 10,000,000 |
| S2 | - | flight | 5 | 10.32 | 10.25-10.48 | 3.70 | 944 (footprint) | 10,000,000 |
| S2 | - | trino | 5 | 52.95 | 52.86-53.39 | 32.45 | 7,510 (footprint) | 10,000,000 |
| S3 | - | esql | 5 | 0.03 | 0.03-0.06 | 0.00 | 16 (footprint) | 100 |
| S3 | arrow | esql | 5 | 0.03 | 0.03-0.03 | 0.00 | 29 (footprint) | 100 |
| S3 | - | flight | 5 | 0.04 | 0.04-0.05 | 0.01 | 84 (footprint) | 100 |
| S3 | - | trino | 5 | 5.50 | 5.42-5.61 | 0.02 | 25 (footprint) | 100 |
| S4 | - | esql | 5 | 0.01 | 0.01-0.01 | 0.00 | 16 (footprint) | 100 |
| S4 | arrow | esql | 5 | 0.01 | 0.01-0.01 | 0.00 | 29 (footprint) | 100 |
| S4 | - | flight | 5 | 0.04 | 0.03-0.04 | 0.01 | 84 (footprint) | 100 |
| S4 | dialhostname | flight | 5 | 0.05 | 0.05-0.06 | 0.01 | 84 (footprint) | 100 |
| S4 | - | trino | 5 | 0.07 | 0.06-0.07 | 0.01 | 24 (footprint) | 100 |

### S0 / S0p -- the floors both stacks pay

| Floor | Wall median (s) | Client CPU median (s) | Peak client memory (MB) | Runs |
|---|---|---|---|---|
| S0 single-process scroll | 37.00 | 14.49 | 27 (footprint) | 5 |
| S0p sliced scroll (parallel), 6 slices | 12.46 | 15.97 | 165 (footprint) | 5 |

<!-- S0p is 2.97x S0's wall clock for 1.10x its client CPU, across 6 processes. -->

### S1

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1 | wall-clock median (s) | 11.91 | 44.70 | 3.8x |
| S1 | client CPU median (s) | 2.83 | 24.05 | 8.5x |
| S1 | peak client memory median (MB) | 921 (footprint) | 4,455 (footprint) | 4.8x |
| S1 | wall-clock min-max (s) | 11.73-12.01 | 44.45-44.97 | - |
| S1 | connect-only median (s) | 0.012 | 0.002 | - |
| S1 | bytes to client, median (MB) | 710 | 977 | 1.4x |
| S1 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S1m

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1m | wall-clock median (s) | 3.99 | 4.42 | 1.1x |
| S1m | client CPU median (s) | 0.19 | 2.23 | 12.0x |
| S1m | peak client memory median (MB) | 176 (footprint) | 475 (footprint) | 2.7x |
| S1m | wall-clock min-max (s) | 3.97-4.05 | 4.40-4.46 | - |
| S1m | connect-only median (s) | 0.011 | 0.002 | - |
| S1m | bytes to client, median (MB) | 71 | 138 | 1.9x |
| S1m | rows returned (gate: identical) | 1,000,000 | 1,000,000 | - |

### S2

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S2 | wall-clock median (s) | 10.32 | 52.95 | 5.1x |
| S2 | client CPU median (s) | 3.70 | 32.45 | 8.8x |
| S2 | peak client memory median (MB) | 944 (footprint) | 7,510 (footprint) | 8.0x |
| S2 | wall-clock min-max (s) | 10.25-10.48 | 52.86-53.39 | - |
| S2 | connect-only median (s) | 0.011 | 0.002 | - |
| S2 | bytes to client, median (MB) | 710 | 977 | 1.4x |
| S2 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S3

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S3 | wall-clock median (s) | 0.04 | 5.50 | 128.7x |
| S3 | client CPU median (s) | 0.01 | 0.02 | 1.7x |
| S3 | peak client memory median (MB) | 84 (footprint) | 25 (footprint) | 0.3x |
| S3 | wall-clock min-max (s) | 0.04-0.05 | 5.42-5.61 | - |
| S3 | connect-only median (s) | 0.013 | 0.002 | - |
| S3 | bytes to client, median (MB) | 0 | 6 | 526.4x |
| S3 | rows returned (gate: identical) | 100 | 100 | - |

### S4

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S4 | wall-clock median (s) | 0.04 | 0.07 | 1.8x |
| S4 | client CPU median (s) | 0.01 | 0.01 | 0.9x |
| S4 | peak client memory median (MB) | 84 (footprint) | 24 (footprint) | 0.3x |
| S4 | wall-clock min-max (s) | 0.03-0.04 | 0.06-0.07 | - |
| S4 | connect-only median (s) | 0.013 | 0.002 | - |
| S4 | bytes to client, median (MB) | 0 | 0 | 9.6x |
| S4 | rows returned (gate: identical) | 100 | 100 | - |

### S1 / S1r / Trino -- decomposing the gap

| Path | Wall median (s) |
|---|---|
| Arrow wire, Arrow table (S1) | 11.91 |
| Arrow wire, Python rows (S1r) | 9.97 |
| Trino wire, Python rows (S1) | 44.70 |

<!-- Of the 32.80s gap, 34.73s survives when both sides build Python rows (attributable to the wire and the server side), and -1.94s is the cost of building row objects at all (attributable to the columnar client representation). -->

### ES|QL -- Elasticsearch's own query language

| Scenario | Wire | Arm | Wall median (s) | Client CPU median (s) | Peak client memory (MB) | Rows | Runs |
|---|---|---|---|---|---|---|---|
| S1m | json | base | 0.982 | 0.502 | 666 (footprint) | 1,000,000 | 5 |
| S1m | arrow | arrow | 0.321 | 0.046 | 166 (footprint) | 1,000,000 | 5 |
| S3 | json | base | 0.034 | 0.000 | 16 (footprint) | 100 | 5 |
| S3 | arrow | arrow | 0.034 | 0.001 | 29 (footprint) | 100 | 5 |
| S4 | json | base | 0.006 | 0.000 | 16 (footprint) | 100 | 5 |
| S4 | arrow | arrow | 0.006 | 0.001 | 29 (footprint) | 100 | 5 |

<!-- ES|QL ran with esql.query.result_truncation_max_size=1000000; its product maximum is 1,000,000, which is why S1/S2/S5/S6 have no ES|QL column. -->

### Drift control -- S1/flight

| Block | Wall median (s) | Runs |
|---|---|---|
| first (A) | 11.91 | 5 |
| repeat at session end (A') | 10.04 | 5 |

Session drift -15.7%, OUTSIDE A's own run-to-run spread of 2.3%. The blocks are NOT interchangeable: this session's cross-stack comparison carries a drift larger than its noise and must say so.

### Host load during the session

1-minute load average sampled after every run: median 5.5, max 13.2, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 2.8 cores were not committed.

<!-- METHODOLOGY section 6: S1 Flight returned 8 columns (0 beyond the 8 selected). -->
<!-- observed column list: id, event_ts, amount, qty, status, country, category, name -->

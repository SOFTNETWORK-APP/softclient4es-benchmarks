<!-- session: 20260820T212820-v030 -->
<!-- WARNING: S3/flight/base spread is 34% of the median (0.03-0.05s) -- the median understates the noise -->
<!-- WARNING: S4/esql/arrow spread is 48% of the median (0.01-0.01s) -- the median understates the noise -->
<!-- WARNING: S4/trino/base spread is 40% of the median (0.05-0.08s) -- the median understates the noise -->
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
| S0 | - | es-raw | 5 | 37.38 | 37.22-37.50 | 13.58 | 27 (footprint) | 10,000,000 |
| S0p | - | es-raw | 5 | 19.22 | 18.85-19.32 | 17.64 | 138 (footprint) | 10,000,000 |
| S1 | - | flight | 5 | 13.07 | 12.62-13.85 | 2.93 | 916 (footprint) | 10,000,000 |
| S1 | drift | flight | 5 | 9.78 | 9.56-9.89 | 2.47 | 919 (footprint) | 10,000,000 |
| S1 | - | trino | 5 | 44.92 | 44.75-45.07 | 24.24 | 4,458 (footprint) | 10,000,000 |
| S1 | arrowadbc | trino | 5 | 33.68 | 31.32-37.47 | 22.97 | 1,927 (footprint) | 10,000,000 |
| S1 | arrowcx | trino | 5 | 15.00 | 14.55-16.00 | 10.83 | 620 (footprint) | 10,000,000 |
| S1 | tuned | trino | 5 | 44.57 | 44.52-44.76 | 24.00 | 4,465 (footprint) | 10,000,000 |
| S1m | - | esql | 5 | 1.02 | 0.94-1.03 | 0.49 | 668 (footprint) | 1,000,000 |
| S1m | arrow | esql | 5 | 0.31 | 0.27-0.34 | 0.05 | 166 (footprint) | 1,000,000 |
| S1m | - | flight | 5 | 3.97 | 3.95-4.13 | 0.19 | 176 (footprint) | 1,000,000 |
| S1m | - | trino | 5 | 4.46 | 4.42-4.50 | 2.23 | 474 (footprint) | 1,000,000 |
| S1r | - | flight | 5 | 10.09 | 9.95-10.23 | 2.58 | 1,480 (footprint) | 10,000,000 |
| S1r | arrowdtype | flight | 5 | 14.37 | 14.20-14.60 | 3.23 | 922 (footprint) | 10,000,000 |
| S1r | polars | flight | 5 | 10.82 | 10.63-10.99 | 11.72 | 2,481 (footprint) | 10,000,000 |
| S1r | - | trino | 5 | 56.39 | 56.25-56.89 | 35.54 | 8,005 (footprint) | 10,000,000 |
| S1r | arrowdtype | trino | 5 | 56.35 | 56.23-56.52 | 35.49 | 7,802 (footprint) | 10,000,000 |
| S1r | pandasadbc | trino | 5 | 29.60 | 29.48-29.86 | 20.56 | 2,533 (footprint) | 10,000,000 |
| S1r | pandascx | trino | 5 | 18.72 | 18.31-18.97 | 14.31 | 2,824 (footprint) | 10,000,000 |
| S1r | polars | trino | 5 | 56.60 | 56.33-56.74 | 35.41 | 11,048 (footprint) | 10,000,000 |
| S1r | polarsadbc | trino | 5 | 28.38 | 28.03-28.90 | 28.24 | 3,532 (footprint) | 10,000,000 |
| S1r | polarscx | trino | 5 | 14.20 | 13.81-14.33 | 10.79 | 2,174 (footprint) | 10,000,000 |
| S2 | - | flight | 5 | 10.27 | 9.90-10.41 | 3.67 | 940 (footprint) | 10,000,000 |
| S2 | - | trino | 5 | 52.92 | 52.87-53.27 | 32.41 | 7,509 (footprint) | 10,000,000 |
| S3 | - | esql | 5 | 0.03 | 0.03-0.04 | 0.00 | 16 (footprint) | 100 |
| S3 | arrow | esql | 5 | 0.03 | 0.03-0.03 | 0.00 | 29 (footprint) | 100 |
| S3 | - | flight | 5 | 0.04 | 0.03-0.05 | 0.01 | 84 (footprint) | 100 |
| S3 | - | trino | 5 | 5.45 | 5.44-5.52 | 0.02 | 24 (footprint) | 100 |
| S4 | - | esql | 5 | 0.01 | 0.01-0.01 | 0.00 | 16 (footprint) | 100 |
| S4 | arrow | esql | 5 | 0.01 | 0.01-0.01 | 0.00 | 28 (footprint) | 100 |
| S4 | - | flight | 5 | 0.04 | 0.03-0.04 | 0.01 | 83 (footprint) | 100 |
| S4 | dialhostname | flight | 5 | 0.05 | 0.05-0.06 | 0.01 | 84 (footprint) | 100 |
| S4 | - | trino | 5 | 0.07 | 0.05-0.08 | 0.01 | 24 (footprint) | 100 |

### S0 / S0p -- the floors both stacks pay

| Floor | Wall median (s) | Client CPU median (s) | Peak client memory (MB) | Runs |
|---|---|---|---|---|
| S0 single-process scroll | 37.38 | 13.58 | 27 (footprint) | 5 |
| S0p sliced scroll (parallel), 5 slices | 19.22 | 17.64 | 138 (footprint) | 5 |

<!-- S0p is 1.94x S0's wall clock for 1.30x its client CPU, across 5 processes. -->

### S1

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1 | wall-clock median (s) | 13.07 | 44.92 | 3.4x |
| S1 | client CPU median (s) | 2.93 | 24.24 | 8.3x |
| S1 | peak client memory median (MB) | 916 (footprint) | 4,458 (footprint) | 4.9x |
| S1 | wall-clock min-max (s) | 12.62-13.85 | 44.75-45.07 | - |
| S1 | connect-only median (s) | 0.012 | 0.003 | - |
| S1 | bytes to client, median (MB) | 710 | 977 | 1.4x |
| S1 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S1m

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1m | wall-clock median (s) | 3.97 | 4.46 | 1.1x |
| S1m | client CPU median (s) | 0.19 | 2.23 | 11.9x |
| S1m | peak client memory median (MB) | 176 (footprint) | 474 (footprint) | 2.7x |
| S1m | wall-clock min-max (s) | 3.95-4.13 | 4.42-4.50 | - |
| S1m | connect-only median (s) | 0.011 | 0.001 | - |
| S1m | bytes to client, median (MB) | 71 | 136 | 1.9x |
| S1m | rows returned (gate: identical) | 1,000,000 | 1,000,000 | - |

### S2

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S2 | wall-clock median (s) | 10.27 | 52.92 | 5.2x |
| S2 | client CPU median (s) | 3.67 | 32.41 | 8.8x |
| S2 | peak client memory median (MB) | 940 (footprint) | 7,509 (footprint) | 8.0x |
| S2 | wall-clock min-max (s) | 9.90-10.41 | 52.87-53.27 | - |
| S2 | connect-only median (s) | 0.011 | 0.002 | - |
| S2 | bytes to client, median (MB) | 710 | 977 | 1.4x |
| S2 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S3

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S3 | wall-clock median (s) | 0.04 | 5.45 | 143.5x |
| S3 | client CPU median (s) | 0.01 | 0.02 | 1.9x |
| S3 | peak client memory median (MB) | 84 (footprint) | 24 (footprint) | 0.3x |
| S3 | wall-clock min-max (s) | 0.03-0.05 | 5.44-5.52 | - |
| S3 | connect-only median (s) | 0.013 | 0.002 | - |
| S3 | bytes to client, median (MB) | 0 | 6 | 526.4x |
| S3 | rows returned (gate: identical) | 100 | 100 | - |

### S4

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S4 | wall-clock median (s) | 0.04 | 0.07 | 1.9x |
| S4 | client CPU median (s) | 0.01 | 0.01 | 0.9x |
| S4 | peak client memory median (MB) | 83 (footprint) | 24 (footprint) | 0.3x |
| S4 | wall-clock min-max (s) | 0.03-0.04 | 0.05-0.08 | - |
| S4 | connect-only median (s) | 0.013 | 0.001 | - |
| S4 | bytes to client, median (MB) | 0 | 0 | 9.7x |
| S4 | rows returned (gate: identical) | 100 | 100 | - |

### S1 / S1r / Trino -- decomposing the gap

| Path | Wall median (s) |
|---|---|
| Arrow wire, Arrow table (S1) | 13.07 |
| Arrow wire, Python rows (S1r) | 10.09 |
| Trino wire, Python rows (S1) | 44.92 |

<!-- Of the 31.85s gap, 34.83s survives when both sides build Python rows (attributable to the wire and the server side), and -2.98s is the cost of building row objects at all (attributable to the columnar client representation). -->

### ES|QL -- Elasticsearch's own query language

| Scenario | Wire | Arm | Wall median (s) | Client CPU median (s) | Peak client memory (MB) | Rows | Runs |
|---|---|---|---|---|---|---|---|
| S1m | json | base | 1.015 | 0.485 | 668 (footprint) | 1,000,000 | 5 |
| S1m | arrow | arrow | 0.313 | 0.047 | 166 (footprint) | 1,000,000 | 5 |
| S3 | json | base | 0.033 | 0.000 | 16 (footprint) | 100 | 5 |
| S3 | arrow | arrow | 0.034 | 0.001 | 29 (footprint) | 100 | 5 |
| S4 | json | base | 0.005 | 0.000 | 16 (footprint) | 100 | 5 |
| S4 | arrow | arrow | 0.007 | 0.001 | 28 (footprint) | 100 | 5 |

<!-- ES|QL ran with esql.query.result_truncation_max_size=1000000; its product maximum is 1,000,000, which is why S1/S2/S5/S6 have no ES|QL column. -->

### Drift control -- S1/flight

| Block | Wall median (s) | Runs |
|---|---|---|
| first (A) | 13.07 | 5 |
| repeat at session end (A') | 9.78 | 5 |

Session drift -25.1%, OUTSIDE A's own run-to-run spread of 9.4%. The blocks are NOT interchangeable: this session's cross-stack comparison carries a drift larger than its noise and must say so.

### Host load during the session

1-minute load average sampled after every run: median 5.0, max 12.2, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 3.8 cores were not committed.

<!-- METHODOLOGY section 6: S1 Flight returned 8 columns (0 beyond the 8 selected). -->
<!-- observed column list: id, event_ts, amount, qty, status, country, category, name -->

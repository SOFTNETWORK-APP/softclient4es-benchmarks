<!-- session: 20260818T230041-v0251 -->
<!-- WARNING: S3/flight/base spread is 34% of the median (0.04-0.05s) -- the median understates the noise -->
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
| S0 | - | es-raw | 5 | 43.83 | 43.74-43.85 | 14.09 | 26 (footprint) | 10,000,000 |
| S0p | - | es-raw | 5 | 22.54 | 22.42-22.83 | 14.68 | 128 (footprint) | 10,000,000 |
| S1 | - | flight | 5 | 34.04 | 33.89-34.35 | 5.07 | 916 (footprint) | 10,000,000 |
| S1 | drift | flight | 5 | 34.67 | 34.54-34.99 | 5.37 | 918 (footprint) | 10,000,000 |
| S1 | - | trino | 5 | 50.39 | 49.67-50.42 | 24.02 | 4,462 (footprint) | 10,000,000 |
| S1 | arrowadbc | trino | 5 | 49.32 | 49.18-49.86 | 18.74 | 1,928 (footprint) | 10,000,000 |
| S1 | arrowcx | trino | 5 | 49.74 | 49.03-50.36 | 10.95 | 686 (footprint) | 10,000,000 |
| S1 | tuned | trino | 5 | 48.60 | 48.15-50.51 | 24.17 | 4,463 (footprint) | 10,000,000 |
| S1m | - | esql | 5 | 1.29 | 1.27-1.30 | 0.55 | 667 (footprint) | 1,000,000 |
| S1m | arrow | esql | 5 | 0.61 | 0.60-0.62 | 0.09 | 166 (footprint) | 1,000,000 |
| S1m | - | flight | 5 | 3.85 | 3.83-3.90 | 0.22 | 174 (footprint) | 1,000,000 |
| S1m | - | trino | 5 | 5.18 | 5.16-5.28 | 2.24 | 473 (footprint) | 1,000,000 |
| S1r | - | flight | 5 | 34.67 | 34.62-34.90 | 5.23 | 1,473 (footprint) | 10,000,000 |
| S1r | arrowdtype | flight | 5 | 34.33 | 34.23-34.36 | 4.56 | 916 (footprint) | 10,000,000 |
| S1r | polars | flight | 5 | 35.11 | 34.89-35.36 | 14.56 | 2,480 (footprint) | 10,000,000 |
| S1r | - | trino | 5 | 60.92 | 60.82-61.90 | 35.32 | 7,966 (footprint) | 10,000,000 |
| S1r | arrowdtype | trino | 5 | 60.94 | 59.78-62.47 | 35.28 | 7,800 (footprint) | 10,000,000 |
| S1r | pandasadbc | trino | 5 | 50.41 | 49.75-50.60 | 19.36 | 2,553 (footprint) | 10,000,000 |
| S1r | pandascx | trino | 5 | 49.55 | 49.44-50.22 | 11.72 | 2,823 (footprint) | 10,000,000 |
| S1r | polars | trino | 5 | 61.19 | 60.46-61.97 | 35.23 | 11,046 (footprint) | 10,000,000 |
| S1r | polarsadbc | trino | 5 | 50.79 | 50.69-51.01 | 28.40 | 3,558 (footprint) | 10,000,000 |
| S1r | polarscx | trino | 5 | 50.27 | 49.71-50.55 | 11.82 | 2,238 (footprint) | 10,000,000 |
| S2 | - | flight | 5 | 34.76 | 34.52-34.94 | 6.14 | 948 (footprint) | 10,000,000 |
| S2 | - | trino | 5 | 57.46 | 56.98-58.51 | 32.33 | 7,506 (footprint) | 10,000,000 |
| S3 | - | esql | 5 | 0.16 | 0.15-0.16 | 0.00 | 16 (footprint) | 100 |
| S3 | arrow | esql | 5 | 0.22 | 0.22-0.23 | 0.00 | 28 (footprint) | 100 |
| S3 | - | flight | 5 | 0.04 | 0.04-0.05 | 0.01 | 81 (footprint) | 100 |
| S3 | - | trino | 5 | 25.05 | 24.99-25.76 | 0.06 | 24 (footprint) | 100 |
| S4 | - | esql | 5 | 0.00 | 0.00-0.00 | 0.00 | 16 (footprint) | 100 |
| S4 | arrow | esql | 5 | 0.01 | 0.01-0.01 | 0.00 | 29 (footprint) | 100 |
| S4 | - | flight | 5 | 0.04 | 0.03-0.04 | 0.01 | 81 (footprint) | 100 |
| S4 | dialhostname | flight | 5 | 0.05 | 0.05-0.06 | 0.01 | 82 (footprint) | 100 |
| S4 | - | trino | 5 | 0.06 | 0.05-0.06 | 0.01 | 24 (footprint) | 100 |

### S0 / S0p -- the floors both stacks pay

| Floor | Wall median (s) | Client CPU median (s) | Peak client memory (MB) | Runs |
|---|---|---|---|---|
| S0 single-process scroll | 43.83 | 14.09 | 26 (footprint) | 5 |
| S0p sliced scroll (parallel), 5 slices | 22.54 | 14.68 | 128 (footprint) | 5 |

<!-- S0p is 1.94x S0's wall clock for 1.04x its client CPU, across 5 processes. -->

### S1

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1 | wall-clock median (s) | 34.04 | 50.39 | 1.5x |
| S1 | client CPU median (s) | 5.07 | 24.02 | 4.7x |
| S1 | peak client memory median (MB) | 916 (footprint) | 4,462 (footprint) | 4.9x |
| S1 | wall-clock min-max (s) | 33.89-34.35 | 49.67-50.42 | - |
| S1 | connect-only median (s) | 0.012 | 0.002 | - |
| S1 | bytes to client, median (MB) | 703 | 973 | 1.4x |
| S1 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S1m

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S1m | wall-clock median (s) | 3.85 | 5.18 | 1.3x |
| S1m | client CPU median (s) | 0.22 | 2.24 | 10.0x |
| S1m | peak client memory median (MB) | 174 (footprint) | 473 (footprint) | 2.7x |
| S1m | wall-clock min-max (s) | 3.83-3.90 | 5.16-5.28 | - |
| S1m | connect-only median (s) | 0.013 | 0.002 | - |
| S1m | bytes to client, median (MB) | 70 | 97 | 1.4x |
| S1m | rows returned (gate: identical) | 1,000,000 | 1,000,000 | - |

### S2

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S2 | wall-clock median (s) | 34.76 | 57.46 | 1.7x |
| S2 | client CPU median (s) | 6.14 | 32.33 | 5.3x |
| S2 | peak client memory median (MB) | 948 (footprint) | 7,506 (footprint) | 7.9x |
| S2 | wall-clock min-max (s) | 34.52-34.94 | 56.98-58.51 | - |
| S2 | connect-only median (s) | 0.013 | 0.003 | - |
| S2 | bytes to client, median (MB) | 703 | 973 | 1.4x |
| S2 | rows returned (gate: identical) | 10,000,000 | 10,000,000 | - |

### S3

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S3 | wall-clock median (s) | 0.04 | 25.05 | 667.6x |
| S3 | client CPU median (s) | 0.01 | 0.06 | 5.3x |
| S3 | peak client memory median (MB) | 81 (footprint) | 24 (footprint) | 0.3x |
| S3 | wall-clock min-max (s) | 0.04-0.05 | 24.99-25.76 | - |
| S3 | connect-only median (s) | 0.015 | 0.002 | - |
| S3 | bytes to client, median (MB) | 0 | 6 | 539.3x |
| S3 | rows returned (gate: identical) | 100 | 100 | - |

### S4

| Scenario | Metric | SoftClient4ES (Flight SQL) | Trino | Ratio (Trino / SC4ES) |
|---|---|---|---|---|
| S4 | wall-clock median (s) | 0.04 | 0.06 | 1.5x |
| S4 | client CPU median (s) | 0.01 | 0.01 | 1.0x |
| S4 | peak client memory median (MB) | 81 (footprint) | 24 (footprint) | 0.3x |
| S4 | wall-clock min-max (s) | 0.03-0.04 | 0.05-0.06 | - |
| S4 | connect-only median (s) | 0.013 | 0.002 | - |
| S4 | bytes to client, median (MB) | 0 | 0 | 6.4x |
| S4 | rows returned (gate: identical) | 100 | 100 | - |

### S1 / S1r / Trino -- decomposing the gap

| Path | Wall median (s) |
|---|---|
| Arrow wire, Arrow table (S1) | 34.04 |
| Arrow wire, Python rows (S1r) | 34.67 |
| Trino wire, Python rows (S1) | 50.39 |

<!-- Of the 16.35s gap, 15.73s survives when both sides build Python rows (attributable to the wire and the server side), and 0.62s is the cost of building row objects at all (attributable to the columnar client representation). -->

### ES|QL -- Elasticsearch's own query language

| Scenario | Wire | Arm | Wall median (s) | Client CPU median (s) | Peak client memory (MB) | Rows | Runs |
|---|---|---|---|---|---|---|---|
| S1m | json | base | 1.290 | 0.547 | 667 (footprint) | 1,000,000 | 5 |
| S1m | arrow | arrow | 0.609 | 0.091 | 166 (footprint) | 1,000,000 | 5 |
| S3 | json | base | 0.156 | 0.001 | 16 (footprint) | 100 | 5 |
| S3 | arrow | arrow | 0.223 | 0.001 | 28 (footprint) | 100 | 5 |
| S4 | json | base | 0.005 | 0.000 | 16 (footprint) | 100 | 5 |
| S4 | arrow | arrow | 0.006 | 0.001 | 29 (footprint) | 100 | 5 |

<!-- ES|QL ran with esql.query.result_truncation_max_size=1000000; its product maximum is 1,000,000, which is why S1/S2/S5/S6 have no ES|QL column. -->

### Drift control -- S1/flight

| Block | Wall median (s) | Runs |
|---|---|---|
| first (A) | 34.04 | 5 |
| repeat at session end (A') | 34.67 | 5 |

Session drift +1.8%, OUTSIDE A's own run-to-run spread of 1.4%. The blocks are NOT interchangeable: this session's cross-stack comparison carries a drift larger than its noise and must say so.

### Host load during the session

1-minute load average sampled after every run: median 4.3, max 9.8, against 16 logical cores. The measured client runs on the host, so this is what bounds host contention as an explanation of the wall-clock figures: at the observed maximum, 6.2 cores were not committed.

<!-- METHODOLOGY section 6: S1 Flight returned 8 columns (0 beyond the 8 selected). -->
<!-- observed column list: id, event_ts, amount, qty, status, country, category, name -->

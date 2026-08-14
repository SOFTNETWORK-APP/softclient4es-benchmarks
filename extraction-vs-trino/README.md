# Extraction benchmark — SoftClient4ES (Arrow Flight SQL) vs Trino's Elasticsearch connector

A reproducible benchmark comparing how efficiently **SoftClient4ES** (via Arrow Flight SQL) and
**Trino** (via its Elasticsearch connector) extract data out of the same Elasticsearch index into a
Python data-science client.

- **[RESULTS.md](RESULTS.md)** — the measured figures, one section per scenario, with client code
  and outcomes.
- **[METHODOLOGY.md](METHODOLOGY.md)** — what is measured, the fairness rules, and the metric
  definitions. Read this before interpreting any number.
- **[FINDINGS.md](FINDINGS.md)** — issues discovered while building the benchmark, and how they were
  fixed.

## Scenarios

All scenarios run against one 10-million-row index (`bench_events_10m`); the JOIN scenarios also use
a 1-million-row index (`bench_1m`).

| ID | What it measures | Why it exists |
|---|---|---|
| **S0** | A naïve Python scroll client reading 10M rows | Reference floor — the cost of the obvious approach |
| **S1** | Extract 10M rows into a client-side columnar table | The core extraction comparison |
| **S1r** | Extract 10M rows into a pandas / polars DataFrame | The artifact a data scientist actually builds |
| **S2** | Extract 10M rows and aggregate them in DuckDB | End-to-end "fetch and compute" |
| **S3** | `GROUP BY` returning 100 rows | Aggregation pushdown — does the engine move the data or push the work down? |
| **S4** | Fetch 100 rows (`LIMIT 100`) | Control — the wire format should stop mattering for small results |
| **S5** | The extraction under a hard container memory cap | Does it fit in a 2 / 4 / 8 GB container? |
| **S6** | Concurrent extractions within an 8 GB budget | How many analysts can one machine serve? |
| **J0–J2** | Cross-index JOIN landed as a DataFrame (plain / with `WHERE` / with `GROUP BY`) | Extraction plus a join |

S1 and S1r additionally compare SoftClient4ES against Trino's fastest available clients
(connectorx and the ADBC Trino driver), not only Trino's stock client — see RESULTS.

## Requirements

- Docker with a VM of **≥ 12 GB RAM / ≥ 12 CPUs** (the engine containers are 4 GB each).
- **≥ 32 GB host RAM** — the client runs on the host, and Trino's stock client materialises 10M rows
  into Python objects, which needs ~8–15 GB.
- A **native** CPython 3.12 (on Apple Silicon, an arm64 build — the runners refuse to start under
  Rosetta translation, which would misreport client CPU).

## Running the benchmark

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Bring up Elasticsearch and load the data (deterministic; ~3 minutes)
docker compose up -d elasticsearch
.venv/bin/python generator/generate_and_load.py

# Bring up both engines
docker compose up -d flight-sql trino

# Extraction matrix S0–S4 (medians of 5 runs after 2 warm-ups)
.venv/bin/python runners/orchestrate.py --stop-idle-engine

# Constrained-memory (S5) and concurrency (S6)
.venv/bin/python runners/orchestrate_capped.py
.venv/bin/python runners/orchestrate_concurrent.py --budget 8

# Cross-index JOIN (J0–J2)
.venv/bin/python runners/orchestrate_join.py
```

Results are written under `results/<session>/`, one JSON per run, with the environment and the
sidecar image digest captured automatically.

**Licence.** The full 10M-row scenarios (S1/S2) require a SoftClient4ES licence tier whose result
quota exceeds 10M; the licence changes a quota only, not the data path. With no licence configured
the harness runs on the Community tier, which reproduces every scenario at reduced (`--limit`)
scale. Enterprise licence provisioning for full-scale runs is local only and is not part of this
repository — see the internal benchmark runbook.

## Notes

- **Do not `docker compose down`** while the index is loaded unless you intend to discard it and
  reload (the data lives in a named volume; `down -v` removes it). `docker compose stop` preserves
  it.
- Each measured run happens in a fresh process so peak memory cannot leak between runs; every run
  asserts its exact expected row count before its timing counts.

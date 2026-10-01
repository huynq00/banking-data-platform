# Banking Data Platform

End-to-end data platform for a simulated retail bank: real-time fraud detection via CDC + streaming, and a nightly end-of-day batch with reconciliation, SCD Type 2 history and PII masking.

> 🚧 Work in progress · Design doc: [docs/01-design.md](docs/01-design.md)

## Architecture

```
                         ┌──────────────── REAL-TIME (seconds) ─────────────────────┐
Simulator ──► Core Banking ──► Debezium ──► Kafka ──► Spark Streaming ──► Telegram alert
             (Postgres OLTP)                  │        (fraud rules)
                                              ▼
                                      Lake (RustFS): BRONZE (raw Parquet)
                         ┌──────────────── BATCH (nightly, Airflow) ────────────────┐
Vietcombank FX API ──► BRONZE ──► Warehouse ──► dbt: SILVER ──► GOLD ──► Metabase
                                                   │
                                      Reconciliation + data tests
```

## Tech stack

| Layer | Tool |
|---|---|
| Source (OLTP) | PostgreSQL |
| CDC | Debezium |
| Message broker | Kafka (KRaft) |
| Stream processing | Spark Structured Streaming |
| Data lake | RustFS (S3-compatible) + Parquet |
| Warehouse | PostgreSQL |
| Transformation | dbt |
| Orchestration | Airflow |
| Dashboard | Metabase |
| CI | GitHub Actions |

## Project structure

```
banking-data-platform/
├── docs/                   # Design docs, architecture diagrams, screenshots
├── infra/                  # Configuration for each Docker service
│   ├── postgres-oltp/init/ # Core Banking schema (runs on first start)
│   ├── postgres-dwh/init/  # Warehouse schemas & roles
│   ├── kafka-connect/      # Debezium connector configs
│   ├── spark/              # Spark image / config
│   ├── airflow/            # Airflow image / requirements
│   └── metabase/
├── data/
│   ├── raw/                # Large source files (Berka) — not committed
│   └── sample/             # Small sample data for tests
├── src/                    # Python source code
│   ├── common/             # Shared: config, logging, S3 client, Telegram
│   ├── simulator/          # Generates live transactions into Core Banking
│   ├── ingestion/          # Berka loader, FX rate API ingestion
│   ├── streaming/          # Spark jobs: CDC → bronze, fraud detection
│   └── batch/              # Load bronze → warehouse, reconciliation
├── dbt/banking/            # dbt project
│   ├── models/silver/      # Cleaned, deduplicated, PII-masked
│   ├── models/gold/        # Star schema: facts & dimensions
│   ├── snapshots/          # SCD Type 2
│   ├── tests/              # Custom data tests
│   ├── macros/
│   └── seeds/              # Small static reference data
├── airflow/dags/           # DAG definitions (EOD pipeline)
├── tests/                  # pytest: unit & integration
├── scripts/                # Helper scripts (download data, setup)
└── .github/workflows/      # CI
```

## Getting started

```bash
# 1. Configure
cp .env.example .env

# 2. Start infrastructure (Core Banking DB, Warehouse, data lake)
docker compose up -d --wait

# 3. Python environment (Python 3.11)
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 4. Download the Berka dataset and load it into Core Banking
./scripts/download_berka.sh
python -m src.ingestion.load_berka
```

| Service | URL | Credentials |
|---|---|---|
| Core Banking (Postgres) | `localhost:5433` | see `.env` |
| Warehouse (Postgres) | `localhost:5434` | see `.env` |
| Data lake console | http://localhost:9001/rustfs/console/ | `LAKE_ACCESS_KEY` / `LAKE_SECRET_KEY` |

## Roadmap

- [x] Week 1 — Docker Compose, OLTP schema, load Berka
- [ ] Week 2 — Transaction simulator, FX rate ingestion
- [ ] Week 3 — Debezium + Kafka CDC → lake bronze
- [ ] Week 4 — dbt silver: cleaning, PII masking, SCD2
- [ ] Week 5 — dbt gold: star schema, tests
- [ ] Week 6 — Airflow EOD DAG + reconciliation
- [ ] Week 7 — Spark Streaming fraud detection
- [ ] Week 8 — Dashboard, CI, documentation

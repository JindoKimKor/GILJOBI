# GILJOBI : Skill Demand Analytics Platform

<p align="center">
  <img src="giljobi.png" width="300">
</p>

A data engineering capstone project that processes job postings from Canada Job Bank and LinkedIn to provide career insights — market trends, salary distribution, and skill demand analysis.

## Project Overview

This platform processes job postings from two data sources:

- **Canada Job Bank Open Data** (~3.3M postings) — market trends, salary distribution, hiring locations, classified by NOC (National Occupational Classification) 2021
- **LinkedIn/Kaggle Dataset** (~123K postings) — skill extraction, job title normalization using LLM

DAG-managed modular infrastructure where Airflow orchestrates the entire data lifecycle — starting/stopping databases and Spark clusters as needed.

## Architecture

### Two-Stream Pipeline

| Stream | Source | Volume | Processing | Status |
|--------|--------|--------|------------|--------|
| **Market Trend** | Canada Job Bank Open Data | ~3.3M rows | Python + pandas | Completed |
| **Matching Insights** | LinkedIn/Kaggle | ~123K rows | Spark + Claude LLM | Under Construction |

### Infrastructure (Modular Docker Compose)

```
infra/
├── docker-compose.airflow.yml     # Airflow + Redis + metadata DB (always on)
├── docker-compose.postgres.yml    # Pipeline data DB (DAG-managed)
├── docker-compose.spark.yml       # Spark + Livy (DAG-managed)
├── config/                        # .env.development, .env.production
├── up.sh                          # Start modules
└── down.sh                        # Stop modules
```

Only Airflow is started manually. DAGs manage all other infrastructure (DB, Spark) automatically.

### Repo Structure

```
Giljobi-DataPipeline/
├── dags/                          # Airflow DAGs (one per stream)
│   ├── market_trend_dag.py
│   └── matching_insights_preprocessing_dag.py
├── pipelines/
│   ├── market-trend/              # Job Bank ETL (5-stage pipeline)
│   └── matching-insights/         # LinkedIn pre-processing + LLM normalization
├── infra/                         # Modular shared infrastructure
└── data/                          # Local data (gitignored)
```

## Quick Start

```bash
# Start Airflow
./infra/up.sh airflow

# Open Airflow UI — trigger market_trend_pipeline DAG
# http://localhost:8090 (airflow / airflow)

# Stop
./infra/down.sh
```

Configuration (replicas, memory, cores, credentials) is in `infra/config/.env.development`.

## Market Trend Pipeline

### Via Airflow (recommended)

1. Start Airflow: `./infra/up.sh airflow`
2. Open Airflow UI: http://localhost:8090
3. Enable `market_trend_pipeline` DAG
4. Click "Trigger DAG" — DB starts/stops automatically

![Airflow DAG List](images/airflow-dag-list.png)

The DAG manages the full lifecycle:

`start_db` → `noc_setup` → `scrape` → `download` → `validate` → `transform` → `load` → `stop_db`

![Market Trend DAG Gantt](images/market-trend-dag-gantt.png)

### Via CLI (standalone)

```bash
cd pipelines/market-trend
py main.py                          # Full pipeline (local DB)
py main.py --db "postgresql://..."  # Custom DB (e.g., Neon)
```

See [pipelines/market-trend/RUNBOOK.md](pipelines/market-trend/RUNBOOK.md) for full documentation.

## Matching Insights Pipeline

> Under Construction — This stream is being restructured to integrate with the new modular infrastructure and DAG-managed Spark orchestration.

## Technology Stack

- **Data Processing**: Apache Spark (PySpark), Pandas
- **LLM**: Claude API (job title normalization)
- **Orchestration**: Apache Airflow (CeleryExecutor)
- **Storage**: PostgreSQL (Neon DB for production), Docker volumes
- **Infrastructure**: Docker Compose (modular), Docker-in-Docker (DAG-managed)
- **Languages**: Python 3

---

**Team**: 3 members | **Timeline**: 4 months | **Status**: Active Development

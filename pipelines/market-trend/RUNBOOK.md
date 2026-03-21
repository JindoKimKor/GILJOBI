# Market Trend Pipeline — Runbook

## Prerequisites

- Python 3.10+
- pip packages: `pandas psycopg2`

```bash
pip install pandas psycopg2
```

- PostgreSQL database (local Docker or Neon DB)

---

## 1. Database Setup

### Option A — Local Docker (default)

Start only the PostgreSQL container using the modular infrastructure scripts:

```bash
./infra/up.sh postgres
```

- Host: `localhost:5432`
- DB: `giljobi`
- User/Password: `postgres / postgres`
- Schema is auto-applied from `infra/init/01-market-trend.sql` on first run

To fully reset (delete containers + volume):

```bash
./infra/down.sh postgres -v
```

### Option B — Neon DB (remote)

Pass the connection string directly via `--db` flag (`.env` file is not auto-loaded):

```bash
py main.py --db "postgresql://username:password@host.neon.tech/dbname?sslmode=require"
```

Schema setup (first time only):

```bash
# If psql is installed
psql "postgresql://username:password@host.neon.tech/dbname?sslmode=require" -f infra/init/01-market-trend.sql

# If psql is not installed — use Docker postgres container as client
docker exec -i postgres psql "postgresql://username:password@host.neon.tech/dbname?sslmode=require" < infra/init/01-market-trend.sql
```

---

## 2. Running the Pipeline

Run from the `pipelines/market-trend/` directory.

### Full pipeline (recommended)

```bash
py main.py
```

Runs all 5 stages in order:
1. SCRAPE — fetch CSV URLs from Canada Job Bank Open Data Portal
2. DOWNLOAD — download monthly CSVs to `data/raw/market-trend/` (skips existing files)
3. VALIDATE — check file integrity and encoding
4. TRANSFORM — normalize salaries, filter outliers, map NOC codes
5. LOAD — bulk insert into `job_postings` table via PostgreSQL COPY

### Custom DB connection

```bash
py main.py --db postgresql://user:pass@host:5432/dbname
```

### Custom worker count (parallel downloads)

```bash
py main.py --workers 8
```

Default is 5 parallel threads.

### NOC setup only

Run this first if the `noc_titles` table is empty and you want to verify it separately:

```bash
py main.py --noc-setup
```

> Note: The full pipeline automatically runs NOC setup before the 5 stages, so this flag is optional.

---

## 3. Running Tests

### Unit tests only (no DB required)

```bash
py -m pytest -m "not integration"
```

### All tests (requires running PostgreSQL)

```bash
py -m pytest
```

---

## 4. Expected Output

```
[NOC SETUP] Downloading NOC 2021 master CSV...
[NOC SETUP] 516 NOC titles in DB (516 in source CSV).

[1/5 SCRAPE] Fetching dataset metadata from CKAN API...
[1/5 SCRAPE] Found 38 English CSV URLs.

[2/5 DOWNLOAD] 38 new files to download (0 already exist).
  Downloaded 2023-01
  ...

[3/5 VALIDATE] Checking 38 files...
[3/5 VALIDATE] 38 valid, 0 skipped.

[4/5 TRANSFORM] Processing 38 files...
  Transformed 2023-01.csv: 44123 rows
  ...

[5/5 LOAD] Inserting into job_postings...
  Loaded 1/38: 44123 rows (total: 44123)
  ...

[DONE] Pipeline complete. ~3,300,000 job postings loaded.

--- Timing Summary ---
  SCRAPE      :    2.1s
  DOWNLOAD    :   45.3s
  VALIDATE    :    8.7s
  TRANSFORM   :   62.4s
  LOAD        :   38.2s
  TOTAL       :  156.7s
```

---

## 5. Re-runs

The pipeline is **idempotent**:
- DOWNLOAD skips files already on disk
- NOC setup skips duplicate codes (`ON CONFLICT DO NOTHING`)
- LOAD appends — does **not** deduplicate `job_postings`

To do a full reload, truncate first then re-run:

```bash
# Local Docker
docker exec market-trend-db psql -U postgres -d giljobi -c "TRUNCATE job_postings, noc_titles RESTART IDENTITY CASCADE;"

# Neon DB
docker exec market-trend-db psql "postgresql://username:password@host.neon.tech/dbname?sslmode=require" -c "TRUNCATE job_postings, noc_titles RESTART IDENTITY CASCADE;"
```

Then re-run the pipeline as normal.

---

## 6. Running via Airflow

The pipeline can also be orchestrated by Airflow for scheduled runs and visual monitoring.
The DAG automatically manages infrastructure — it starts the pipeline database if not already running.
The DB is **not stopped** after the pipeline — data stays accessible for backend/frontend.

### Start Airflow

```bash
./infra/up.sh airflow
```

That's it. The DAG handles the rest:
- `ensure_db` — starts pipeline PostgreSQL if not running (idempotent — safe to re-trigger)
- `noc_setup` + `scrape` — run in parallel after DB is confirmed
- `download → validate → transform → load` — sequential ETL stages

For external DB (e.g., Neon), set `MANAGE_PIPELINE_DB=false` in `infra/config/.env.development`.
When set to `false`, `ensure_db` becomes a no-op (EmptyOperator).

### Configuration

All settings (replicas, memory, cores, credentials) are in `infra/config/`:

```bash
infra/config/.env.development   # Local development defaults
infra/config/.env.production    # Production settings
infra/config/.env.example       # Template
```

Active config is copied to `infra/.env` on first run. To switch environments:

```bash
cp infra/config/.env.production infra/.env
```

### Stop infrastructure

```bash
./infra/down.sh                  # Stop all
./infra/down.sh airflow          # Stop Airflow only
./infra/down.sh -v               # Stop all + remove volumes
```

### Access UI

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8090 | airflow / airflow |

### Trigger the pipeline

1. Open Airflow UI at http://localhost:8090
2. Enable `market_trend_pipeline` DAG (toggle unpause)
3. Click "Trigger DAG" to run manually, or wait for `@monthly` schedule
4. Monitor in Graph view — all tasks show success/failure in real-time
5. Check Gantt view for per-task timing breakdown

---

## 7. Data Location

| Type | Path |
|---|---|
| Raw CSVs | `data/raw/market-trend/YYYY-MM.csv` |
| Pipeline source | `pipelines/market-trend/` |
| DB schema | `infra/init/01-market-trend.sql` |
| Airflow DAG | `dags/market_trend_dag.py` |
| Pipeline spec | `pipelines/market-trend/SPEC.md` |

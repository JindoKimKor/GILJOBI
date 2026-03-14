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

Start only the PostgreSQL container — `docker-compose.yml` includes Airflow/Spark/Livy, but only Postgres is needed for the pipeline.

```bash
docker compose -f infra/docker-compose.yml up postgres -d
```

- Host: `localhost:5432`
- DB: `giljobi`
- User/Password: `postgres / postgres`
- Schema is auto-applied from `infra/init.sql` on first run

To fully reset (delete containers + volume):

```bash
docker compose -f infra/docker-compose.yml down -v
```

### Option B — Neon DB (remote)

Pass the connection string directly via `--db` flag (`.env` file is not auto-loaded):

```bash
py main.py --db "postgresql://username:password@host.neon.tech/dbname?sslmode=require"
```

Schema setup (first time only):

```bash
# If psql is installed
psql "postgresql://username:password@host.neon.tech/dbname?sslmode=require" -f infra/init.sql

# If psql is not installed — use Docker postgres container as client
docker exec -i postgres psql "postgresql://username:password@host.neon.tech/dbname?sslmode=require" < infra/init.sql
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
docker exec postgres psql -U postgres -d giljobi -c "TRUNCATE job_postings, noc_titles RESTART IDENTITY CASCADE;"

# Neon DB
docker exec postgres psql "postgresql://username:password@host.neon.tech/dbname?sslmode=require" -c "TRUNCATE job_postings, noc_titles RESTART IDENTITY CASCADE;"
```

Then re-run the pipeline as normal.

---

## 6. Data Location

| Type | Path |
|---|---|
| Raw CSVs | `data/raw/market-trend/YYYY-MM.csv` |
| Pipeline source | `pipelines/market-trend/` |
| DB schema | `infra/init.sql` |
| Pipeline spec | `pipelines/market-trend/SPEC.md` |

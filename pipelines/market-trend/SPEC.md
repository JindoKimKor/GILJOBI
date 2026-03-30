# Market Trend Pipeline Specification

**Jira:** JA-53
**Stream Purpose:** Power the market analytics dashboard — industry share, salary distribution, hiring locations, posting trends.

## Data Source

| Property | Value |
|---|---|
| Source | Canada Job Bank Open Data |
| Format | CSV (mixed: UTF-16/tab, UTF-8-sig/comma, Latin-1/comma) |
| Release Frequency | Monthly |
| Historical Range | Jan 2023 ~ Feb 2026 (~38 files) |
| Rows per Month | ~44,000 |
| Total Estimated Rows | ~3,300,000 |
| Processing Engine | PySpark (Spark cluster via Airflow) |
| Open Data Portal | https://open.canada.ca/data/en/dataset/ea639e28-c0fc-48bf-b5dd-b8899bd43072 |

## Data Acquisition

### Strategy

The ingestion script should **dynamically scrape** the Open Data Portal page to extract all available monthly CSV download URLs, rather than relying on hardcoded links. This ensures:
- New monthly releases are automatically discovered
- No manual SPEC updates needed when new data is published
- Single source of truth: the portal page itself

### Portal Page
```
https://open.canada.ca/data/en/dataset/ea639e28-c0fc-48bf-b5dd-b8899bd43072
```

### Extraction Rules
- Scrape the dataset page for all resource download links
- Filter for English CSVs only (filename contains `-en-` or `-en.`)
- Parse month/year from filename (e.g., `january2023`, `feb2025`)
- Download to `data/raw/market-trend/YYYY-MM.csv` (normalized naming)

### NOC21 Master Data

| Property | Value |
|---|---|
| Source | Statistics Canada — NOC 2021 Version 1.0 |
| URL | `https://www.statcan.gc.ca/en/subjects/standard/noc/2021/indexV1/noc-2021-v1.0-classification-structure.csv` |
| Rows | 823 (5-level hierarchy: Broad, Major, Sub-major, Minor, Unit) |
| Purpose | Reference/validation for `noc_titles` table |

### `noc_titles` Population Strategy

1. Download the official NOC 2021 classification CSV from Statistics Canada
2. Filter to Level 5 (Unit Group) rows — these are the 5-digit codes used in Job Bank data
3. Insert into `noc_titles` — one row per NOC21 unit group code

### Validation Rules
- Extracted URL count must be >= 37 (Jan 2023 ~ Feb 2026 baseline)
- No gaps in consecutive months from 2023-01 to latest available
- All filenames must match English pattern (`-en-` or `-en.`)
- Each downloaded file must be non-empty and readable as UTF-16 tab-separated CSV
- Skip files already present in `data/raw/market-trend/` (idempotent re-runs)

## Architecture

### DAG-Managed Pipeline

The pipeline runs as an Airflow DAG (`market_trend_dag.py`) that manages its own infrastructure.
DB is NOT stopped after pipeline — data stays accessible for backend.

```mermaid
flowchart LR
    subgraph infra ["Infrastructure (BashOperator)"]
        ENSURE["ensure_db<br/>━━━━━━━━━━━<br/>Start pipeline DB<br/>if not running"]
    end

    subgraph extract ["Extract"]
        NOC["noc_setup<br/>━━━━━━━━━━━<br/>516 NOC21 titles<br/>from Statistics Canada"]
        SCRAPE["scrape<br/>━━━━━━━━━━━<br/>~38 CSV URLs<br/>from CKAN API"]
    end

    subgraph ingest ["Ingest"]
        DL["download<br/>━━━━━━━━━━━<br/>Fetch monthly CSVs<br/>skip existing files"]
        VAL["validate<br/>━━━━━━━━━━━<br/>Detect encoding<br/>verify columns"]
    end

    subgraph process ["Transform & Load"]
        TX["transform<br/>━━━━━━━━━━━<br/>Salary normalize<br/>Outlier filter<br/>NOC mapping<br/>→ .parquet"]
        LOAD["load<br/>━━━━━━━━━━━<br/>COPY bulk insert<br/>→ PostgreSQL"]
    end

    ENSURE --> NOC
    ENSURE --> SCRAPE
    SCRAPE --> DL --> VAL
    NOC --> TX
    VAL --> TX --> LOAD

    style ENSURE fill:#607D8B,color:#fff
    style NOC fill:#FF9800,color:#fff
    style SCRAPE fill:#2196F3,color:#fff
    style DL fill:#2196F3,color:#fff
    style VAL fill:#2196F3,color:#fff
    style TX fill:#9C27B0,color:#fff
    style LOAD fill:#4CAF50,color:#fff
```

### Data Flow

```mermaid
flowchart LR
    subgraph sources ["Data Sources"]
        JB[("Canada Job Bank<br/>Open Data Portal<br/>━━━━━━━━━━━<br/>~44K rows/month<br/>CSV (mixed encoding)")]
        SC[("Statistics Canada<br/>NOC 2021 v1.0<br/>━━━━━━━━━━━<br/>516 unit groups<br/>CSV")]
    end

    subgraph storage ["Local Storage"]
        RAW["data/raw/market-trend/<br/>━━━━━━━━━━━<br/>YYYY-MM.csv<br/>(38+ files)"]
        PROC["data/processed/market-trend/<br/>━━━━━━━━━━━<br/>YYYY-MM.parquet<br/>(cleaned + transformed)"]
    end

    subgraph db ["Pipeline DB (market-trend-db:5432)"]
        NOC_T[("noc_titles<br/>━━━━━━━━━━━<br/>516 rows<br/>NOC21 codes")]
        JP[("job_postings<br/>━━━━━━━━━━━<br/>~3.3M rows<br/>normalized data")]
    end

    JB -->|"scrape + download"| RAW
    RAW -->|"validate + transform"| PROC
    PROC -->|"COPY bulk insert"| JP
    SC -->|"noc_setup"| NOC_T
    NOC_T -.->|"FK lookup"| JP

    style JB fill:#1565C0,color:#fff
    style SC fill:#1565C0,color:#fff
    style RAW fill:#F57F17,color:#fff
    style PROC fill:#F57F17,color:#fff
    style NOC_T fill:#2E7D32,color:#fff
    style JP fill:#2E7D32,color:#fff
```

### Execution Modes: CLI vs Airflow

Two execution modes produce the same output. CLI uses pandas for local testing without infrastructure. Airflow uses PySpark for distributed processing in production.

```mermaid
flowchart LR
    subgraph cli ["CLI Mode — py main.py (pandas, local test)"]
        direction LR
        C_SCRAPE["scrape"] --> C_DL["download<br/>→ CSV files"]
        C_DL --> C_VAL["validate"]
        C_VAL --> C_TX["transform<br/>━━━━━━━━━━━<br/>pandas DataFrame<br/>in memory"]
        C_TX -->|"DataFrame<br/>(in memory)"| C_LOAD["load<br/>━━━━━━━━━━━<br/>COPY from<br/>DataFrame directly"]
    end

    subgraph airflow ["Airflow Mode — DAG (PySpark, production)"]
        direction LR
        A_SCRAPE["scrape"] --> A_DL["download<br/>→ CSV files"]
        A_DL --> A_VAL["validate"]
        A_VAL --> A_TX["transform<br/>━━━━━━━━━━━<br/>PySpark + UDF<br/>Broadcast Join<br/>Schema Enforcement"]
        A_TX -->|"partitioned<br/>.parquet"| A_LOAD["load<br/>━━━━━━━━━━━<br/>Spark JDBC write<br/>to PostgreSQL"]
    end

    style cli fill:#1B5E20,stroke:#4CAF50,color:#fff
    style airflow fill:#0D47A1,stroke:#2196F3,color:#fff
    style C_TX fill:#9C27B0,color:#fff
    style C_LOAD fill:#4CAF50,color:#fff
    style A_TX fill:#9C27B0,color:#fff
    style A_LOAD fill:#4CAF50,color:#fff
```

| | CLI (`py main.py`) | Airflow (DAG) |
|---|---|---|
| **Purpose** | Local testing, no infra needed | Production, distributed processing |
| **Processing engine** | pandas (single process) | PySpark (Spark cluster) |
| **Data between stages** | In-memory DataFrame | Partitioned `.parquet` on disk |
| **NOC mapping** | dict lookup | Broadcast Join |
| **Salary normalization** | pandas apply | Spark UDF |
| **Metrics** | print statements | Spark Accumulator |
| **Infrastructure** | Manual: `./infra/up.sh postgres` | Automatic: `ensure_db` + `ensure_spark` |
| **Output** | Same `job_postings` table | Same `job_postings` table |

### Schedule & Config

| Setting | Value |
|---------|-------|
| Schedule | `@monthly` (manual trigger also supported) |
| DAG file | `dags/market_trend_dag.py` |
| Task API | TaskFlow (`@task` decorators) + BashOperator (infra) |
| XCom data | File paths only (not DataFrames) |
| `MANAGE_PIPELINE_DB=true` | `ensure_db` starts local Docker DB |
| `MANAGE_PIPELINE_DB=false` | `ensure_db` is EmptyOperator (external DB like Neon) |
| Standalone | `py main.py` still works independently without Airflow |

## Infrastructure

### Required Services

| Service | Compose File | How it starts |
|---------|-------------|---------------|
| Airflow (webserver, scheduler, worker, init) | `docker-compose.airflow.yml` | Manual: `./infra/up.sh airflow` |
| Airflow metadata DB (port 5433) | `docker-compose.airflow.yml` | Included with Airflow |
| Redis (Celery broker) | `docker-compose.airflow.yml` | Included with Airflow |
| Pipeline DB (port 5432) | `docker-compose.postgres.yml` | Automatic: DAG `ensure_db` task |

Spark is used for the TRANSFORM and LOAD stages via LivyOperator.

| Service | Compose File | How it starts |
|---------|-------------|---------------|
| Spark master + workers + Livy | `docker-compose.spark.yml` | Automatic: DAG `ensure_spark` task |

### Configuration

All settings are in `infra/config/.env.development`:

```env
# Pipeline DB
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=giljobi
PIPELINE_DB_CONN=postgresql://postgres:postgres@market-trend-db:5432/giljobi
MANAGE_PIPELINE_DB=true

# Airflow
AIRFLOW_WORKER_REPLICAS=1
AIRFLOW_WORKER_MEMORY=2g
```

### Schema Initialization

Pipeline DB schema is defined in `infra/init/01-market-trend.sql`. PostgreSQL Docker executes this automatically on first volume creation. For re-initialization:

```bash
./infra/down.sh postgres -v    # Delete volume
./infra/up.sh postgres         # Fresh start with schema
```

## Output Schema (Database)

### Table: `noc_titles`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `id` | SERIAL | PRIMARY KEY | Auto-generated |
| `noc21_code` | VARCHAR(10) | UNIQUE NOT NULL | `NOC21 Code` |
| `noc21_name` | VARCHAR(100) | | `NOC21 Code Name` |

### Table: `job_postings`

| Column | Type | Constraint | Source | Transformation |
|---|---|---|---|---|
| `id` | SERIAL | PRIMARY KEY | Auto-generated | |
| `noc_id` | INT | FK → noc_titles(id) | `NOC21 Code` | Lookup |
| `normalized_title` | VARCHAR(100) | NOT NULL | `Job Title` | Direct |
| `vacancy_count` | INT | | `Vacancy Count` | Direct |
| `province` | VARCHAR(50) | | `Province/Territory` | Direct |
| `city` | VARCHAR(100) | | `City` | Direct |
| `first_posting_date` | DATE | | `First Posting Date` | Direct |
| `salary_min_hourly` | NUMERIC(6,2) | | `Salary Minimum` | Normalize to hourly |
| `salary_max_hourly` | NUMERIC(6,2) | | `Salary Maximum` | Normalize to hourly |

## Spark Features

The TRANSFORM and LOAD stages run as a PySpark job submitted via LivyOperator from the Airflow DAG. The following Spark features are used:

### Schema Enforcement (StructType)

Define the expected CSV schema upfront. Rows that don't match the schema are rejected at read time — no silent data corruption.

```python
schema = StructType([
    StructField("Job Title", StringType(), nullable=False),
    StructField("NOC21 Code", StringType()),
    StructField("Salary Minimum", DoubleType()),
    StructField("Salary Maximum", DoubleType()),
    StructField("Salary Per", StringType()),
    ...
])
df = spark.read.csv(path, header=True, schema=schema)
```

### Broadcast Join (NOC Lookup)

The `noc_titles` table (516 rows) is small enough to broadcast to all workers. This avoids expensive shuffle joins — each worker has a local copy of the lookup table.

```python
noc_df = spark.read.jdbc(db_url, "noc_titles")
noc_broadcast = broadcast(noc_df)
df = df.join(noc_broadcast, df["noc21_code"] == noc_broadcast["noc21_code"], "left")
```

### UDF (Salary Normalization)

Custom salary-to-hourly conversion logic runs as a Spark UDF, distributed across all workers.

```python
@udf(returnType=DoubleType())
def normalize_salary(value, salary_per):
    divisors = {"Hour": 1, "Day": 8, "Week": 40, "Month": 173.33, "Year": 2080}
    if salary_per not in divisors or value is None:
        return None
    return value / divisors[salary_per]
```

### Partitioned Parquet Write

Output is written as parquet partitioned by year-month. Downstream queries benefit from partition pruning — only relevant partitions are read.

```python
df.write.partitionBy("year_month").parquet(output_path)
```

### Accumulator (Processing Metrics)

Accumulators track pipeline metrics across distributed workers without collecting full DataFrames to the driver.

```python
outlier_count = spark.sparkContext.accumulator(0)
null_salary_count = spark.sparkContext.accumulator(0)

@udf(returnType=DoubleType())
def filter_outlier(hourly_rate):
    if hourly_rate is not None and (hourly_rate < 10 or hourly_rate > 500):
        outlier_count.add(1)
        return None
    return hourly_rate
```

### Summary

| Feature | Purpose | Why Not pandas |
|---------|---------|----------------|
| Schema Enforcement | Reject malformed rows at read | pandas reads everything, fails later |
| Broadcast Join | NOC lookup without shuffle | pandas has no distributed join concept |
| UDF | Distribute custom logic across workers | pandas runs on single core |
| Partitioned Write | Query-time partition pruning | pandas writes single file |
| Accumulator | Distributed metric collection | pandas uses simple counters (single process) |

## Transformation Rules

### Salary Normalization

All salary values are normalized to **hourly rate**.

| Salary Per | Divisor | Assumption |
|---|---|---|
| Hour | 1 | As-is |
| Day | 8 | 8 hours/day |
| Week | 40 | 40 hours/week |
| Bi-weekly | 80 | 40 hours/week × 2 |
| Month | 173.33 | 2,080 hours/year ÷ 12 |
| Year | 2,080 | 40 hours/week × 52 weeks |

### Outlier Handling

- Hourly rate < $10 or > $500 → set to NULL (~0.3% of records)
- `Salary Per` is NULL → set salary fields to NULL (69 rows/month)

### NOC Lookup

- `noc_titles` is pre-populated from the official Statistics Canada NOC 2021 master CSV (not from pipeline data)
- `job_postings.noc_id` references `noc_titles.id` via NOC21 code lookup
- NOC21 Major Group (first 2 digits) can be derived at query time for industry-level aggregation (45 categories)

## Data Quality Notes

- **NOC21 coverage**: 99.3% — 0.7% rows have no classification
- **NAICS**: Only 40.3% filled (External=0 postings only) — use NOC21 Major Group as alternative industry classification
- **Experience Level / Education LOS**: Only 40.3% filled (External=0 only) — not included in Phase 1 output schema
- **Vacancy Count**: Reliable — high values (50-150) are legitimate bulk hiring (agriculture, retail)
- **Monthly snapshot**: Each file contains only that month's postings, not cumulative

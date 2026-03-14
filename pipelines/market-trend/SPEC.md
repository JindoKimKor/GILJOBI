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
| Processing Engine | pandas (no Spark required) |
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

## Pipeline Stages

### Pre-requisite
- `noc_titles` table must be populated from Statistics Canada master CSV before pipeline runs

### Pipeline Flow
```
1. SCRAPE    → Extract CSV download URLs from Open Data Portal
2. DOWNLOAD  → Fetch monthly CSVs to data/raw/market-trend/
3. VALIDATE  → Check file integrity (UTF-16, non-empty, expected columns)
4. TRANSFORM → Salary normalization, outlier filtering, NOC21 code lookup
5. LOAD      → Insert job_postings into PostgreSQL
```

## Input Schema

| Source Column | Type | Coverage | Description |
|---|---|---|---|
| `Job Title` | string | 100% | NOC-normalized job title (6,053 unique/month) |
| `NOC21 Code` | string | 99.3% | 5-digit NOC 2021 classification code |
| `NOC21 Code Name` | string | 99.3% | Official NOC21 occupation name |
| `NOC 2016 Code` | string | - | Legacy NOC 2016 code |
| `NOC 2016 Code Name` | string | - | Legacy NOC 2016 occupation name |
| `Vacancy Count` | int | 100% | Number of positions per posting |
| `First Posting Date` | date | 100% | Posting date (YYYY/MM/DD) |
| `Salary Minimum` | numeric | 99.97% | Minimum salary (mixed units) |
| `Salary Maximum` | numeric | 99.97% | Maximum salary (mixed units) |
| `Salary Per` | string | 99.8% | Salary unit: Hour, Day, Week, Bi-weekly, Month, Year |
| `Province/Territory` | string | 100% | 13 provinces/territories |
| `City` | string | 99.2% | City name |
| `Employment Type` | string | 96.8% | Full time, Part time, Part time leading to full time |
| `NAICS` | string | 40.3% | Industry code (External=0 only) |
| `Experience Level` | string | 40.3% | 7 levels (External=0 only) |
| `Education LOS` | string | 40.3% | 11 levels (External=0 only) |
| `External Indicator` | int | 100% | 0=Job Bank internal, 1=External employer |

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

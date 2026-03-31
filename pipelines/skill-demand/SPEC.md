# Skill Demand Pipeline Specification

**Stream Purpose:** Extract and aggregate skill demand from LinkedIn job postings — answer "which skills are most in demand per NOC category and seniority level?" to power the skill analytics feature.

## Data Source

| Property | Value |
|---|---|
| Source | [arshkon/linkedin-job-postings](https://www.kaggle.com/datasets/arshkon/linkedin-job-postings) (Kaggle) |
| Format | CSV (zip archive, 166.5MB compressed) |
| Total Postings | ~124,000 |
| Date Range | 2023–2024 |
| License | CC BY-SA 4.0 |
| Processing Engine | PySpark (Spark cluster) |
| Raw Data Location | `data/raw/skill-demand/` |

### Key Files from Dataset

| File | Columns Used | Purpose |
|------|-------------|---------|
| `postings.csv` | `job_id`, `title`, `description`, `company_name`, `formatted_experience_level` | Main pipeline input |

Note: `company_name` is already in `postings.csv` — no JOIN with `companies.csv` needed.

### Available Columns in `postings.csv`

| Column | Type | Pipeline Usage |
|--------|------|----------------|
| `job_id` | INT | Primary key |
| `title` | TEXT | ST NOC matching + seniority keyword extraction |
| `description` | TEXT | LLM skills extraction input |
| `company_name` | TEXT | LLM context for NOC matching |
| `formatted_experience_level` | TEXT | Seniority 1st priority (76.3% filled) |
| `skills_desc` | TEXT | Not used — LLM extracts structured skills from `description` |

### Download

Automated via DAG `download` task:

```bash
curl -L -o /tmp/linkedin-job-postings.zip \
  https://www.kaggle.com/api/v1/datasets/download/arshkon/linkedin-job-postings
unzip -o /tmp/linkedin-job-postings.zip -d data/raw/skill-demand/
```

Requires Kaggle API credentials (`~/.kaggle/kaggle.json`). Skip if `postings.csv` already exists (idempotent).

## Architecture

### Pipeline Flow

```
Step 1: Extract columns (job_id, company_name, title, description, formatted_experience_level)
  ↓ V1 (file integrity) → V2 (null/length)
Step 2: ST NOC Match (Sentence Transformers, threshold 0.65)
  + Seniority: formatted_experience_level → title keywords → remaining for LLM
  ↓ V3 (match rate + seniority stats)
  ├─ matched (25.9%): NOC + seniority → Step 3 (skills extraction)
  └─ unmatched (74.1%): → Step 3 (NOC + skills + seniority)
Step 3: LLM Enrich — ALL rows processed with 2 prompt types:
  ├─ unmatched: NOC list + JD → NOC + seniority(if missing) + skills (5/call)
  └─ matched: JD → seniority(if missing) + skills (10/call)
  Adaptive Batch Strategy (similarity-based grouping)
  Skills categorized: hard_skill, soft_skill, tool, certification
  ↓ V4 (NOC + seniority + skills validation)
Step 4: DB Load (jd_postings + jd_skills with category)
```

### DAG Task Flow (Airflow)

```
ensure_db → noc_setup → download → validate_file_integrity → step1_extract → validate_nulls_and_length
  → start_spark_cluster (Master + Livy)
  → start_spark_workers (Workers)
  → step2_noc_match_st → validate_noc_match_rate
  → step3_enrich → validate_enrich
  → stop_spark_workers
  → step4_load
  → stop_spark_cluster
```

### NOC Matching — Two-Stage Strategy

**Why two stages?**
- ST (Sentence Transformers) is fast, free, local — catches obvious matches (25.7%)
- LLM handles the rest with full NOC context — expensive but accurate (72.3% additional)
- Combined: **97.9% NOC coverage** (verified on 721-row sample)

```mermaid
flowchart TD
    TITLE["raw title + company"]
    ST["Step 2: Sentence Transformers<br/>all-MiniLM-L6-v2<br/>cosine similarity vs 510 NOC titles"]
    SCORE{score >= 0.65?}
    MATCH_ST["NOC confirmed<br/>noc_match_method = 'sentence_transformer'<br/>noc_match_score = cosine score"]
    LLM["Step 3: Claude Haiku CLI<br/>title + company + full NOC list (510)<br/>'Pick the best NOC or null'"]
    MATCH_LLM["NOC confirmed<br/>noc_match_method = 'llm_noc_match'<br/>noc_match_score = NULL"]
    NO_MATCH["noc_id = NULL<br/>(LLM said no match — 2.1%)"]

    TITLE --> ST --> SCORE
    SCORE -->|"yes (25.7%)"| MATCH_ST
    SCORE -->|"no (74.3%)"| LLM
    LLM -->|"matched (72.3%)"| MATCH_LLM
    LLM -->|"null (2.1%)"| NO_MATCH

    style MATCH_ST fill:#4CAF50,color:#fff
    style MATCH_LLM fill:#FF9800,color:#fff
    style NO_MATCH fill:#607D8B,color:#fff
    style ST fill:#2196F3,color:#fff
    style LLM fill:#9C27B0,color:#fff
```

### Design Evolution — What We Tried

Three approaches were tested before the final design:

1. **❌ LLM title normalize → ST re-match** (36.8%)
   - LLM cleaned up titles (e.g. "Unix Manager in Jersey City, NJ" → "IT Manager")
   - Cleaned title re-matched against NOC via ST
   - Problem: LLM doesn't know NOC categories — titles already clean (e.g. "Marketing Analyst") still scored 0.69 vs NOC's long name "Business development officers and market researchers and analysts"

2. **❌ ST with title + JD** (worse than title-only)
   - Hypothesis: JD contains role context → better matching
   - Test result: scores dropped (0.71 → 0.46) — JD noise (benefits, company desc) diluted cosine similarity
   - `all-MiniLM-L6-v2` max 256 tokens → most JD (avg 3768 chars) truncated

3. **✅ LLM with full NOC list** (97.9%)
   - LLM receives: title + company + all 510 NOC categories
   - Picks best match or returns null
   - Simple, accurate, LLM handles semantic understanding

### Schedule & Config

| Setting | Value |
|---------|-------|
| Schedule | `None` (manual trigger only) |
| DAG file | `dags/skill_demand_dag.py` |
| DAG params | `noc_threshold`, `batch_delay_sec`, `step1_input`, executor memory/instances |
| Spark submission | `@task + LivyHook` (real-time log + file-based progress monitoring) |

## Pipeline Steps — Detail

### Step 1 — Column Extraction (`src/step1_select_columns.py`)

| | Value |
|---|---|
| Input | `postings.csv` |
| Output | `processed/step1/step1_extracted.parquet` |
| Columns | `job_id`, `company_name`, `title`, `description`, `formatted_experience_level` |

- Drop rows with null `title` or `description`
- `run()` function saves parquet; `select_columns()` returns DataFrame (for testing)

### Step 2 — NOC Match via Sentence Transformers (`spark/step2_noc_spark.py`)

| | Value |
|---|---|
| Input | Step 1 parquet |
| Output | `processed/step2/step2_normalized.parquet` |
| Model | `sentence-transformers/all-MiniLM-L6-v2` (local, ~80MB, no API) |
| Threshold | 0.65 (configurable via DAG param `noc_threshold`) |

**Why Sentence Transformers over TF-IDF:**
- Semantic understanding: `"Full Stack Developer"` → `"Web designers and developers"` ✅
- TF-IDF misses this because keywords don't overlap

**Why ST is filter, not final matcher:**
- ST scores 0.65-0.69 include correct matches (e.g. "Marketing Analyst" → 0.69) that fail threshold
- NOC titles are long and differently worded — cosine sim has ceiling
- ST catches 25.7% confidently; remaining 74.3% needs LLM semantic judgment

**Seniority extraction in Step 2 (no LLM needed):**

| Priority | Source | Coverage |
|---|---|---|
| 1st | `formatted_experience_level` from CSV | 76.3% (94,440/123,849) |
| 2nd | Title keyword matching | +6.2% (6,192 additional) |
| 3rd | LLM (Step 3) | 18.7% remaining (23,217) |

Title keyword rules:
```
intern:      intern, internship, co-op, coop
entry_level: junior, jr, entry level, entry-level, graduate, trainee
mid_level:   mid level, mid-level, intermediate
senior:      senior, sr, lead, principal, staff
executive:   director, vp, vice president, head of, chief, cto, ceo, cfo
```

`formatted_experience_level` distribution (full dataset):
```
Mid-Senior level    41,489
Entry level         36,708
Associate            9,826
Director             3,746
Internship           1,449
Executive            1,222
Null                29,409
```

**Spark features:**
- `repartition(N)`: distribute across workers
- Broadcast: NOC 510 embeddings sent to all workers
- Chunk encoding: 1000 titles/chunk for progress reporting
- Progress files: worker writes JSON → Airflow reads directly (not via Livy stdout)
- `HF_HUB_DISABLE_PROGRESS_BARS=1`: prevent Livy LineBufferedStream encoding crash

### Step 3 — LLM Enrich: NOC + Seniority + Skills (`spark/step3_enrich_spark.py`)

| | Value |
|---|---|
| Input | Step 2 output (ALL rows — matched + unmatched) |
| Output | `processed/step3/step3_enriched.parquet` |
| Model | Claude Haiku CLI (subscription, `HOME=/tmp` for credentials) |

**2 prompt types** based on NOC match status from Step 2:

| Prompt Type | Target Rows | Includes NOC List | Batch Size | Extracts |
|---|---|---|---|---|
| **unmatched** | noc_id = null (74.1%) | ✅ 510 categories | 5/call | NOC + seniority + skills |
| **matched** | noc_id filled (25.9%) | ❌ | 10/call | seniority + skills |

**Seniority handling per row (within same batch):**
- Already filled (82.5%) → prompt says "Seniority: {tier} (already determined - skip)"
- Missing (17.5%) → prompt says "Seniority: missing - determine from description"

LLM receives per-row seniority status and acts accordingly.

**Skill categories (4 types):**
```
- hard_skill: domain-specific technical knowledge, programming languages (e.g. Python, data modeling, welding)
- soft_skill: interpersonal and behavioral (e.g. communication, leadership, teamwork)
- tool: software, platform, system (e.g. Excel, SAP, Docker, AWS, Kubernetes)
- certification: formal credential or license (e.g. CPA, PMP, AWS Certified)
```

**Seniority guide (provided to LLM):**
```
- intern: internship, co-op, student placement
- entry_level: junior, graduate, trainee
- mid_level: intermediate, mid-level
- senior: senior, lead, principal, staff
- executive: director, VP, C-level, head of
```

`noc_match_method = "llm_noc_match"` for unmatched rows. `noc_match_score = None` (LLM doesn't give numeric score).

#### Adaptive Batch Strategy — Similarity-Based Grouping

LLM accuracy improves when similar JDs are batched together. Grouping priority:

**Matched rows (10/call):**

| Priority | Grouping | Rationale |
|---|---|---|
| 1st | Same company + same noc_id | Same company, same position → nearly identical JDs |
| 2nd | Same noc_id (different companies) | Same occupation → similar skill requirements |
| 3rd | Remaining | Fill batch by size |

**Unmatched rows (5/call):**

| Priority | Grouping | Rationale |
|---|---|---|
| 1st | Same company | Same company → similar JD writing style |
| 2nd | Remaining | Fill batch by size |

Each group fills batches up to the batch size limit. Rows that don't fill a complete batch in higher-priority groups fall through to lower-priority groups.

**Spark features:**
- `repartition("company_name")`: same company → same worker
- `mapPartitions`: classify rows → group by priority → batch → LLM call
- Checkpoint: per-batch JSON files → resume on failure
- Progress: per-batch JSON → Airflow reads directly
- Session loop: `max_batches_per_session` → cooldown → resume (rate limit handling)

### Step 4 — DB Load (`src/step4_load.py`)

| | Value |
|---|---|
| Input | `processed/step3/step3_enriched.parquet` (all rows with NOC + seniority + skills) |
| Output | `jd_postings` + `jd_skills` tables in PostgreSQL |

- Bulk insert `jd_postings` (one row per job posting)
- Explode `skills[]` array → bulk insert `jd_skills` (one row per skill per posting, with category)
- `ON CONFLICT DO NOTHING` for idempotent reruns
- Schema created by `schema.sql` file

## Infrastructure

### Services

| Service | Compose File | How it starts | Port |
|---|---|---|---|
| Airflow | `docker-compose.airflow.yml` | Manual: `./up.sh airflow` | 8090 |
| Skill-demand DB | `docker-compose.postgres-sd.yml` | DAG: `ensure_db` | 5434 |
| Spark SD Master | `docker-compose.spark-sd.yml` | DAG: `start_spark_cluster` | 8081 |
| Spark SD Workers | `docker-compose.spark-sd.yml` | DAG: `start_spark_workers` | - |
| Livy SD | `docker-compose.spark-sd.yml` | DAG: `start_spark_cluster` | 8999 |

**Dedicated Spark cluster** (`giljobi-spark-sd`) with:
- `spark-worker-sd/Dockerfile`: Python 3.11 + sentence-transformers + PyTorch + Claude CLI + Node.js
- `livy-sd/Dockerfile`: Same packages for driver + `JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"`

**Spark lifecycle in DAG (resource management):**
```
start_spark_cluster (Master + Livy only)
  → start_spark_workers (Workers — alive only during Spark jobs)
    → [Step 2 + Step 3 Spark jobs]
  → stop_spark_workers (free resources)
  → [Step 4 DB load — no Spark needed]
→ stop_spark_cluster (cleanup)
```

### Monitoring

| Source | What it shows | How |
|---|---|---|
| Airflow task log | Driver stdout + progress | `@task + LivyHook` polling (Livy log + file read) |
| Worker progress files | Rows processed, matched count | Workers write JSON → Airflow reads directly |
| Spark Master UI (:8081) | Worker status, executors | Browser |
| Spark App UI (:4040) | Job/stage/task progress | Browser (during job only) |

**Why not Livy stdout for progress:**
- Accumulator: lazy — values update only after `collect()` completes
- Threading print: Livy `LineBufferedStream` doesn't capture from non-main threads during `collect()` block
- Solution: Workers write files to shared volume, Airflow reads directly in polling loop

## DB Schema

### Table: `jd_postings`

| Column | Type | Constraint | Description |
|---|---|---|---|
| `id` | SERIAL | PRIMARY KEY | Auto-generated |
| `company` | VARCHAR(300) | | Company name |
| `raw_title` | VARCHAR(300) | NOT NULL | Original job title |
| `noc_id` | INT | FK → noc_titles(id) | NULL if no match (2.1%) |
| `noc_match_score` | NUMERIC(4,3) | | ST cosine score; NULL for LLM |
| `noc_match_method` | VARCHAR(30) | | `sentence_transformer` or `llm_noc_match` |
| `seniority` | VARCHAR(50) | CHECK enum | intern/entry_level/mid_level/senior/executive |
| `description` | TEXT | | Full JD text |

### Table: `jd_skills`

| Column | Type | Constraint | Description |
|---|---|---|---|
| `id` | SERIAL | PRIMARY KEY | |
| `jd_id` | INT | FK → jd_postings(job_id) | |
| `skill` | VARCHAR(100) | NOT NULL | Lowercase (e.g. "python", "data modeling") |
| `category` | VARCHAR(20) | NOT NULL | `hard_skill`, `soft_skill`, `tool`, `certification` |

### View: `skill_demand_summary`

```sql
SELECT noc21_name, seniority, skill, category, demand_count
FROM skill_demand_summary
WHERE noc21_name ILIKE '%software%' AND category = 'hard_skill'
ORDER BY demand_count DESC
LIMIT 20;
```

## Key Technical Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Data source | `arshkon/linkedin-job-postings` (Kaggle) | 124K postings, single CSV with all needed columns |
| NOC primary match | Sentence Transformers (0.65) | Fast, free, local. Filters 25.7% confidently |
| NOC fallback | LLM with full NOC list (510) | 97.9% total. Tried: normalize+re-match (36.8%), ST+JD (worse) |
| Seniority | 3-tier: CSV (76.3%) → keyword (6.2%) → LLM (17.5%) | Minimize LLM usage, word boundary matching |
| Skills | LLM from JD, 4 categories | hard_skill, soft_skill, tool, certification |
| Step 3 design | 2 prompts (matched/unmatched), not 4 | Seniority handled per-row within prompt |
| Batch strategy | Similarity-based grouping | Company + noc_id grouping for accuracy |
| Spark submission | `@task + LivyHook` | Real-time log (vs LivyOperator state-only) |
| Progress | Airflow reads worker files directly | Livy stdout unreliable (encoding/threading/blocking) |
| Spark infra | Dedicated cluster (`spark-sd`) | Separate Dockerfiles with sentence-transformers + Claude CLI |
| LLM auth | `~/.claude:/tmp/.claude` mount, `HOME=/tmp` | Claude CLI subscription credentials |

## Roadmap

### Phase 1: Pipeline (current)

| Step | Status | Note |
|---|---|---|
| Download | ✅ | Kaggle API, idempotent |
| V1, V2 | ✅ | File integrity, null/length |
| Step 1 | ✅ | Column extraction + parquet |
| Step 2 | ✅ | ST NOC match (0.65) + seniority (CSV + keyword) |
| V3 | ✅ | Match rate stats |
| Step 3 | 🔲 | LLM Enrich: NOC + seniority + skills (2 prompts, similarity-based batch) |
| V4 | ✅ | Completion rate |
| V5 | ✅ | Seniority/skills validation (code ready) |
| Step 4 | ✅ | DB load (BIGINT PK, skill category, schema.sql) |
| DAG | ✅ | Full orchestration with resource lifecycle |
| Infra | ✅ | Separate Spark-SD cluster |
| Tests | 206 passing | step1(15) + step2_seniority(43) + step2_noc(11) + step3_enrich(36) + others |

### Phase 2: AWS (Terraform)

EC2 (Airflow Docker Compose) + EMR (Spark+Livy built-in) + S3 (data).
`terraform apply` → create all → work → `terraform destroy` → cost zero.
See: `resources/datapipeline/aws-deployment-options.md`

### Phase 3: market-trend Spark migration

Apply same Spark patterns (Broadcast Join, UDF, Partitioned Write, Accumulator) to market-trend pipeline.
See: `pipelines/market-trend/SPEC.md` Spark Features section.

## Reference Documents

- `resources/datapipeline/spark-cluster-architecture.md` — Spark architecture with mermaid diagrams
- `resources/datapipeline/spark-job-submission-comparison.md` — Livy vs SparkSubmit + LivyHook architecture change
- `resources/datapipeline/aws-deployment-options.md` — AWS Academy deployment + Terraform

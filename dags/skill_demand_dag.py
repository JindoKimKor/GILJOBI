"""
Skill Demand Pipeline — Airflow DAG.

Orchestrates the full pipeline lifecycle including infrastructure:
    VALIDATE_PARAMS
        ├── ENSURE_DB
        ├── NOC_SETUP          (parallel)
        └── START_SPARK
                ↓
            DOWNLOAD → V1 → STEP1 → V2
        → STEP2 (ST NOC + seniority)
        → STEP3 (LLM enrich: NOC + seniority + skills)
            ├── V4 → STEP4 (DB load)
            └── STOP_SPARK       (parallel)

Infrastructure management:
    MANAGE_PIPELINE_DB=true  → Starts PostgreSQL container (skill-demand DB)
    MANAGE_SPARK=true        → Starts/stops Spark+Livy cluster, scales workers per step

Step 3 processes ALL rows with 2 prompt types:
    Unmatched: NOC list + JD → NOC + seniority + skills (5/call)
    Matched: JD only → seniority + skills (10/call)
    Similarity-based adaptive batch, checkpoint resume, corrupted detection.

Schedule: Manual trigger only (run after new dataset is downloaded)
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param

# Pipeline code mounted at /opt/airflow/pipelines/skill-demand/
PIPELINE_DIR = "/opt/airflow/pipelines/skill-demand"
sys.path.insert(0, PIPELINE_DIR)

# Config
MANAGE_DB = os.environ.get("MANAGE_PIPELINE_DB", "true").lower() == "true"
MANAGE_SPARK = os.environ.get("MANAGE_SPARK", "true").lower() == "true"
INFRA_DIR = "/opt/airflow/infra"
RAW_DIR = "/opt/airflow/data/raw/skill-demand"
PROCESSED_DIR = "/opt/airflow/data/processed/skill-demand"

# Skill-demand uses its own DB (not shared with market-trend)
SD_DB_CONN = os.environ.get(
    "SKILL_DEMAND_DB_CONN",
    "postgresql://postgres:postgres@skill-demand-db:5432/giljobi_sd"
)

default_args = {
    "owner": "giljobi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="skill_demand_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["skill-demand", "etl", "llm", "spark"],
    default_args=default_args,
    params={
        # LLM session control (Step 3)
        "batch_delay_sec": Param(5, type="integer", description="Seconds between LLM batches"),
        "max_batches_per_session": Param(800, type="integer", description="Max LLM calls before cooldown. 0 = no limit. Measured: 1 full session ≈ 1,771 calls (Claude Max 20x)."),
        "session_cooldown_min": Param(90, type="integer", description="Minutes to wait for session reset after reaching max_batches_per_session"),
        "max_sessions": Param(0, type="integer", description="Max session cycles to run. 0 = run until all batches complete. Remaining batches resume on next DAG trigger."),
        # Step 2 — input file (sample for testing, step1_extracted.parquet for full)
        "step2_input_file": Param("", type="string", description="REQUIRED — Parquet filename in processed/step1/. Example: step1_extracted_sample.parquet (test) or step1_extracted.parquet (production)"),
        # Step 2 — NOC matching
        "step2_noc_similarity_threshold": Param(0.65, type="number", description="Minimum cosine similarity score to accept a NOC match (0.0-1.0)"),
        # Spark resources — Step 2 (Sentence Transformers, memory-heavy, fewer workers)
        "step2_workers": Param(4, type="integer", description="Spark worker count for Step 2"),
        "step2_executor_memory": Param("2g", type="string", description="Step 2 executor memory (model loading)"),
        "step2_executor_instances": Param(4, type="integer", description="Step 2 executor count"),
        "step2_partitions": Param(16, type="integer", description="Step 2 partition count (= checkpoint granularity, ~8K rows each at 124K)"),
        # Spark resources — Step 3 (LLM calls, IO-heavy, more workers)
        "step3_workers": Param(8, type="integer", description="Spark worker count for Step 3"),
        "step3_executor_memory": Param("512m", type="string", description="Step 3 executor memory"),
        "step3_executor_instances": Param(8, type="integer", description="Step 3 executor count"),
    },
    doc_md="""
    ## Skill Demand Pipeline

    Extracts skill demand from LinkedIn job postings.

    **Data Source:** Kaggle arshkon/linkedin-job-postings (~124K postings)

    **Steps:**
    1. Download dataset from Kaggle API
    2. Extract columns + ST NOC matching + seniority (CSV + keyword)
    3. LLM Enrich: NOC (unmatched) + seniority (missing) + skills (4 categories)
    4. Load into skill-demand PostgreSQL (star schema: dim + fact tables)

    **Spark Features:** Broadcast Join, mapPartitionsWithIndex, checkpoint resume
    **LLM:** Claude Haiku CLI (subscription, similarity-based adaptive batch)
    **Skills:** hard_skill, soft_skill, tool, certification

    **Parameters (adjustable at trigger time):**
    - `max_batches_per_session`: LLM calls before cooldown (default: 800, 1 session ≈ 1,771 calls)
    - `batch_delay_sec`: Seconds between LLM batches (default: 5)
    - `step2_noc_similarity_threshold`: Cosine similarity threshold (default: 0.65)

    **Trigger:** Manual only
    """,
) as dag:

    # =========================================================================
    # Validate Required Params — fail fast before any work
    # =========================================================================

    @task(task_id="sd_validate_params", task_display_name="Validate Params", doc_md="""
### Validate Params
Fail-fast guard: checks required params before any infrastructure starts.
- `step2_input_file` must not be empty
- Prints all trigger configuration params for audit
""")
    def validate_params(**context):
        """Fail immediately if required params are missing."""
        params = context["params"]
        input_file = params.get("step2_input_file", "").strip()
        if not input_file:
            raise ValueError(
                "step2_input_file is REQUIRED. "
                "Set to 'step1_extracted_sample.parquet' (test) or 'step1_extracted.parquet' (production)."
            )
        print(f"[SD:PARAMS] === Trigger Configuration ===")
        for key, value in sorted(params.items()):
            print(f"  {key}: {value}")

    # =========================================================================
    # Infrastructure — Ensure DB
    # =========================================================================

    if MANAGE_DB:
        ensure_db = BashOperator(
            task_id="sd_ensure_db",
            task_display_name="Ensure DB",
            doc_md="Start skill-demand PostgreSQL (port 5434) if not running. Idempotent — skips if already up.",
            bash_command=(
                f"docker ps --filter name=skill-demand-db --filter status=running -q | grep -q . "
                f"&& echo 'DB already running' "
                f"|| (cd {INFRA_DIR} && bash up.sh postgres-sd 2>&1)"
            ),
        )
    else:
        ensure_db = EmptyOperator(task_id="sd_ensure_db", task_display_name="Ensure DB")

    # =========================================================================
    # Infrastructure — Spark Cluster (Master + Livy only)
    # =========================================================================

    if MANAGE_SPARK:
        start_spark_cluster = BashOperator(
            task_id="sd_start_spark",
            task_display_name="Start Spark Cluster",
            doc_md="Start Spark Master + Livy (no workers yet). Workers are scaled by bridge tasks based on step requirements.",
            bash_command=f"cd {INFRA_DIR} && bash up.sh spark-sd-cluster 2>&1",
        )
    else:
        start_spark_cluster = EmptyOperator(task_id="sd_start_spark", task_display_name="Start Spark Cluster")

    # (Spark worker scaling is now handled inside bridge tasks:
    #  step1_review_step2_prep and step2_review_step3_prep)

    # =========================================================================
    # NOC Setup — Populate noc_titles in skill-demand DB
    # =========================================================================

    @task(task_id="sd_load_noc", task_display_name="Seed NOC Codes", doc_md="""
### Seed NOC Codes
Download and load Canadian NOC 2021 occupation codes into `noc_titles` table.
- Source: Statistics Canada NOC 2021 V1.0 classification CSV (direct download)
- Filters to Level 5 (Unit Group) only → 510 entries
- Creates `noc_titles` table (id, noc21_code, noc21_name)
- Idempotent: skips if table already populated
- Depends on: Ensure DB (needs PostgreSQL running)
""")
    def noc_setup():
        """Download NOC 2021 master CSV and load into skill-demand DB."""
        import pandas as pd
        import psycopg2

        NOC_URL = (
            "https://www.statcan.gc.ca/en/subjects/standard/noc/2021/"
            "indexV1/noc-2021-v1.0-classification-structure.csv"
        )

        conn = psycopg2.connect(SD_DB_CONN)
        conn.autocommit = True
        cur = conn.cursor()

        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS noc_titles (
                id SERIAL PRIMARY KEY,
                noc21_code VARCHAR(10) UNIQUE NOT NULL,
                noc21_name VARCHAR(200)
            )
        """)

        # Check if already populated
        cur.execute("SELECT COUNT(*) FROM noc_titles")
        count = cur.fetchone()[0]
        if count > 0:
            print(f"[SD:INFRA] Already populated: {count} titles. Skipping.")
            cur.close()
            conn.close()
            return

        # Download and filter to Level 5 (Unit Group)
        df = pd.read_csv(NOC_URL)
        unit_groups = df[df["Level"] == 5].reset_index(drop=True)
        noc = unit_groups[["Code - NOC 2021 V1.0", "Class title"]].copy()
        noc = noc.rename(columns={
            "Code - NOC 2021 V1.0": "noc21_code",
            "Class title": "noc21_name",
        })
        noc = noc.dropna(subset=["noc21_code"])

        # Insert
        for _, row in noc.iterrows():
            cur.execute(
                "INSERT INTO noc_titles (noc21_code, noc21_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (str(row["noc21_code"]), row["noc21_name"]),
            )

        cur.close()
        conn.close()
        print(f"[SD:INFRA] Loaded {len(noc)} unit group titles into skill-demand DB.")

    # =========================================================================
    # Download — Kaggle Dataset
    # =========================================================================

    @task(task_id="sd_download", task_display_name="Download Dataset", doc_md="""
### Download Dataset
Download LinkedIn job postings from Kaggle API.
- Dataset: `arshkon/linkedin-job-postings` (~124K postings, 166.5MB zip)
- Extracts `postings.csv` to `data/raw/skill-demand/`
- Idempotent: skips if file already exists
""")
    def download():
        """Download LinkedIn Job Postings from Kaggle API."""
        from pathlib import Path
        from src.download import download_dataset

        output_dir = download_dataset(output_dir=Path(RAW_DIR))
        return str(output_dir)

    # =========================================================================
    # V1 — Post-Download Validation
    # =========================================================================

    @task(task_id="sd_validate_source", task_display_name="Validate Source", doc_md="""
### Validate Source
Verify downloaded CSV integrity before processing.
- File exists and is readable
- Required columns present: job_id, title, description, company_name
- Row count check
- Reports basic stats (total rows, null counts)
""")
    def validate_v1(raw_dir: str):
        """Validate downloaded CSV: file integrity, required columns."""
        import os
        from src.validators.v1_download import validate_download

        csv_path = os.path.join(raw_dir, "postings.csv")
        result = validate_download(csv_path)

        if not result["valid"]:
            raise ValueError(f"V1 validation failed: {result['error']}")

        print(f"[SD:STEP1] Valid. {result['rows']} rows found.")
        return csv_path

    # =========================================================================
    # Step 1 — Column Extraction
    # =========================================================================

    @task(task_id="sd_step1", task_display_name="Step 1: Extract Columns", doc_md="""
### Step 1: Extract Columns
Extract relevant columns from raw CSV → parquet.
- Columns: job_id, company_name, title, description, formatted_experience_level
- Drop rows with null title or description
- **Output:** `step1_extracted.parquet`
""")
    def step1_extract(csv_path: str):
        """Extract job_id, company_name, title, description → parquet."""
        from pathlib import Path
        from src.step1_select_columns import run

        output_path = run(csv_path, output_dir=Path(PROCESSED_DIR) / "step1")
        print(f"[SD:STEP1] Output: {output_path}")
        return output_path

    # =========================================================================
    # Step 1 Review + Step 2 Prep
    # =========================================================================

    @task(task_id="sd_bridge_12", task_display_name="Step 1→2 Bridge", doc_md="""
### Step 1→2 Bridge
Review Step 1 output + prepare Spark environment for Step 2.

1. **Step 1 Review:** Row count, column verification, null stats
2. **Worker Scaling:** Scale Spark workers for Step 2 (memory-heavy, fewer workers — default 4 × 2g)
3. **Step 2 Preview:** NOC similarity threshold, partition count, executor config
""")
    def step1_review_step2_prep(parquet_path: str, **context):
        """Review Step 1 output + clean data + scale workers for Step 2."""
        import subprocess
        import pandas as pd
        from src.validators.v2_extract import validate_extract

        params = context["params"]

        # --- Step 1 Review ---
        df = pd.read_parquet(parquet_path)
        cleaned = validate_extract(df)
        cleaned.to_parquet(parquet_path, index=False)
        dropped = len(df) - len(cleaned)

        print(f"[SD:STEP1:REVIEW]")
        print(f"  Rows: {len(df):,} extracted, {dropped:,} dropped (null/short) → {len(cleaned):,} kept")
        print(f"  Input: {parquet_path}")

        # --- Step 2 Prep ---
        if MANAGE_SPARK:
            n = params["step2_workers"]
            cmd = f"cd {INFRA_DIR} && SPARK_SD_WORKER_REPLICAS={n} bash up.sh spark-sd-workers 2>&1"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            print(result.stdout)
            if result.returncode != 0:
                print(result.stderr)

        print(f"[SD:STEP2:PREP]")
        print(f"  Workers: {params['step2_workers']}, Memory: {params['step2_executor_memory']}")
        print(f"  Partitions: {params['step2_partitions']}, Threshold: {params['step2_noc_similarity_threshold']}")
        print(f"  Input: {params['step2_input_file']}")

        return parquet_path

    # =========================================================================
    # Step 2 — NOC Normalize (Sentence Transformers via Spark)
    # =========================================================================

    @task(task_id="sd_step2", task_display_name="Step 2: NOC Match (ST)", doc_md="""
### Step 2: NOC Match (Sentence Transformers)
Spark job via Livy: NOC occupation matching + seniority extraction.

**NOC Matching:**
- `all-MiniLM-L6-v2` model (384-dim embeddings)
- Cosine similarity ≥ threshold (default 0.65) → matched
- Below threshold → unmatched (sent to LLM in Step 3)
- Broadcast Join: 510 NOC embeddings (~780KB) broadcast to all executors

**Seniority Extraction:**
- Priority 1: `formatted_experience_level` CSV column mapping
- Priority 2: Title keyword matching (word boundary regex)
- Remaining: sent to LLM in Step 3

**Output:** `step2_normalized.parquet` (noc_id, noc_match_score, noc_match_method, seniority)
""")
    def step2_noc(**context):
        """Step 2: NOC Normalize via Sentence Transformers (Spark/Livy with live logs)."""
        import time
        from airflow.providers.apache.livy.hooks.livy import LivyHook

        params = context["params"]
        hook = LivyHook(livy_conn_id="livy_sd")

        batch_id = hook.post_batch(
            file="/opt/spark/pipelines/skill-demand/spark/step2_noc_spark.py",
            args=[
                "--threshold", str(params["step2_noc_similarity_threshold"]),
                "--input", f"/opt/spark/data/processed/skill-demand/step1/{params['step2_input_file']}",
                "--partitions", str(params["step2_partitions"]),
            ],
            conf={
                "spark.executor.memory": params["step2_executor_memory"],
                "spark.executor.instances": str(params["step2_executor_instances"]),
                "spark.driver.memory": "2g",
            },
        )
        print(f"[SD:STEP2] Submitted batch {batch_id}")

        log_offset = 0
        progress_dir = f"{PROCESSED_DIR}/step2/progress"
        last_progress = ""

        while True:
            state = hook.get_batch_state(batch_id)

            # Fetch and print new Livy log lines
            try:
                log_response = hook.run_method(
                    endpoint=f"/batches/{batch_id}/log?from={log_offset}",
                )
                if log_response.status_code == 200:
                    log_data = log_response.json()
                    lines = log_data.get("log", [])
                    if lines:
                        for line in lines:
                            print(line)
                        log_offset += len(lines)
            except Exception:
                pass

            # Read worker progress files directly (same volume mount)
            try:
                import json
                from pathlib import Path
                total_done = 0
                total_matched = 0
                total_unmatched = 0
                for pf in Path(progress_dir).glob("p*.json"):
                    with open(pf) as f:
                        data = json.load(f)
                        total_done += data.get("rows", 0)
                        total_matched += data.get("matched", 0)
                        total_unmatched += data.get("unmatched", 0)
                if total_done > 0:
                    msg = f"[SD:STEP2] Progress: {total_done} rows — matched: {total_matched}, unmatched: {total_unmatched}"
                    if msg != last_progress:
                        print(msg)
                        last_progress = msg
            except Exception:
                pass

            if state in hook.TERMINAL_STATES:
                break
            time.sleep(10)

        state_str = str(state).lower()
        if "success" not in state_str:
            raise Exception(f"Step 2 failed with state: {state}")

    # =========================================================================
    # Step 2 Review + Step 3 Prep
    # =========================================================================

    @task(task_id="sd_bridge_23", task_display_name="Step 2→3 Bridge", doc_md="""
### Step 2→3 Bridge
Review Step 2 output + prepare Spark for Step 3 LLM enrichment.

**1. Step 2 Review:**
- NOC match rate and avg similarity score
- Seniority breakdown (CSV, keyword, null counts)
- Matched vs unmatched row counts

**2. Worker Scaling:**
- Scale Spark workers for Step 3 (IO-heavy, more workers — default 8 × 512m)
- Step 2 was memory-heavy (model loading), Step 3 is IO-heavy (LLM calls)

**3. Grouping Preview — Adaptive Batch Strategy:**
- Matched (10/batch): priority-based overflow grouping
  1. Same company + same NOC (most similar) → full batches, overflow to next
  2. Same NOC only (similar skills) → full batches, overflow to next
  3. Remaining (mixed)
- Unmatched (5/batch):
  1. Same company (similar JD style) → full batches, overflow to next
  2. Remaining (mixed)
- Shows batch count + rows consumed + overflow at each priority
- Session estimation: e.g. "at 800 batches/session → ~27 sessions, ~64h"

**Input:** `step2_normalized.parquet`
""")
    def step2_review_step3_prep(**context):
        """Review Step 2 results + scale workers + grouping preview for Step 3."""
        import subprocess
        import pandas as pd
        from src.validators.v3_normalize import validate_normalize

        params = context["params"]

        # --- Step 2 Review ---
        df = pd.read_parquet(f"{PROCESSED_DIR}/step2/step2_normalized.parquet")
        stats = validate_normalize(df)
        matched = df[df["noc_id"].notna()]
        unmatched = df[df["noc_id"].isna()]

        print(f"[SD:STEP2:REVIEW]")
        print(f"  Total: {len(df):,} rows")
        print(f"  NOC matched: {stats['matched']:,} ({stats['match_rate']:.1%}), avg score: {stats['avg_score']:.3f}")
        print(f"  NOC unmatched: {stats['unmatched']:,} → LLM")
        if "seniority" in df.columns:
            sen_filled = int(df["seniority"].notna().sum())
            print(f"  Seniority: {sen_filled:,}/{len(df):,} ({sen_filled/len(df)*100:.1f}%)")
            for tier, count in df["seniority"].value_counts(dropna=False).items():
                label = tier if pd.notna(tier) else "null (needs LLM)"
                print(f"    {label}: {count:,}")

        # --- Step 3 Prep: Scale workers ---
        if MANAGE_SPARK:
            n = params["step3_workers"]
            cmd = f"cd {INFRA_DIR} && SPARK_SD_WORKER_REPLICAS={n} bash up.sh spark-sd-workers 2>&1"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            print(result.stdout)
            if result.returncode != 0:
                print(result.stderr)

        print(f"[SD:STEP3:PREP]")
        print(f"  Workers: {params['step3_workers']}, Memory: {params['step3_executor_memory']}")
        print(f"  Session: {params['max_batches_per_session']} batches/session, {params['session_cooldown_min']} min cooldown")
        if params['max_sessions'] > 0:
            print(f"  Max sessions: {params['max_sessions']}")

        # --- Step 3 Prep: Grouping preview ---
        # Same logic as step3_enrich_spark.py _create_batches_*()
        # Priority-based overflow: full batches only at each priority, partial → next
        from itertools import groupby as _groupby

        BATCH_MATCHED = 10
        BATCH_UNMATCHED = 5

        def _count_batches(rows_list, group_keys_list, batch_size):
            """Count batches using same priority-overflow logic as Spark job."""
            stats = []
            remaining = list(rows_list)

            for name, key_fn in group_keys_list:
                remaining.sort(key=key_fn)
                next_remaining = []
                full_count = 0
                full_rows = 0
                for _, group in _groupby(remaining, key=key_fn):
                    group_rows = list(group)
                    for i in range(0, len(group_rows), batch_size):
                        chunk = group_rows[i:i + batch_size]
                        if len(chunk) == batch_size:
                            full_count += 1
                            full_rows += batch_size
                        else:
                            next_remaining.extend(chunk)
                stats.append((name, full_count, full_rows, len(next_remaining)))
                remaining = next_remaining

            # Remaining: all partial batches including last partial
            rem_count = (len(remaining) + batch_size - 1) // batch_size if remaining else 0
            stats.append(("Remaining (mixed)", rem_count, len(remaining), 0))
            return stats

        m_calls, u_calls = 0, 0

        if len(matched) > 0:
            m_rows = matched.to_dict("records")
            m_stats = _count_batches(m_rows, [
                ("Group by company + NOC", lambda r: (r.get("company_name") or "", r.get("noc_id") or 0)),
                ("Group by NOC only", lambda r: r.get("noc_id") or 0),
            ], BATCH_MATCHED)
            print(f"\n[SD:STEP3:GROUPING] === Matched: {len(matched):,} rows → {BATCH_MATCHED}/batch ===")
            for i, (name, batches, rows, rem) in enumerate(m_stats, 1):
                m_calls += batches
                rem_str = f", {rem:,} remaining" if rem > 0 else ""
                print(f"  Step {i}) {name:<25} → {batches:>6,} batches ({rows:>6,} rows consumed{rem_str})")
            print(f"  Total: {m_calls:,} LLM calls")

        if len(unmatched) > 0:
            u_rows = unmatched.to_dict("records")
            u_stats = _count_batches(u_rows, [
                ("Group by company", lambda r: r.get("company_name") or ""),
            ], BATCH_UNMATCHED)
            print(f"\n[SD:STEP3:GROUPING] === Unmatched: {len(unmatched):,} rows → {BATCH_UNMATCHED}/batch ===")
            for i, (name, batches, rows, rem) in enumerate(u_stats, 1):
                u_calls += batches
                rem_str = f", {rem:,} remaining" if rem > 0 else ""
                print(f"  Step {i}) {name:<25} → {batches:>6,} batches ({rows:>6,} rows consumed{rem_str})")
            print(f"  Total: {u_calls:,} LLM calls")

        total_calls = m_calls + u_calls
        print(f"\n[SD:STEP3:GROUPING] Total: {total_calls:,} LLM calls")

        bps = params['max_batches_per_session']
        if bps > 0 and total_calls > 0:
            sessions_needed = (total_calls + bps - 1) // bps
            cooldown = params['session_cooldown_min']
            sec_per_call = params['batch_delay_sec'] + 6  # ~6s LLM response + batch_delay
            processing_hours = (total_calls * sec_per_call) / 3600
            cooldown_hours = (sessions_needed - 1) * (cooldown / 60)
            total_hours = processing_hours + cooldown_hours
            print(f"  (e.g. at {bps} batches/session, {cooldown} min cooldown → ~{sessions_needed} sessions, ~{total_hours:.1f} hours)")
            print(f"  (processing ~{processing_hours:.1f}h + cooldown ~{cooldown_hours:.1f}h)")

    # =========================================================================
    # Step 3 — LLM Enrich: NOC + Seniority + Skills
    # =========================================================================

    @task(task_id="sd_step3", task_display_name="Step 3: LLM Enrich", doc_md="""
### Step 3: LLM Enrich (Spark + Claude Haiku)
Spark job via Livy: LLM enrichment for NOC, seniority, and skills.

**Architecture — Driver Batch Factory:**
- Driver creates all batches upfront with content-based batch_id (MD5 hash of sorted job_ids)
- Batches distributed to Workers via `mapPartitionsWithIndex`
- Workers only execute LLM calls + checkpoint — no grouping logic

**Two Prompt Types:**
- Unmatched (5/batch): NOC 510 list + JD → NOC + seniority + skills
- Matched (10/batch): JD only → seniority + skills

**Adaptive Grouping (Driver-side) — priority-based overflow:**
- Matched (10/batch):
  1. Same company + same NOC → most similar JDs (same role at same company)
  2. Same NOC, different companies → similar skill requirements
  3. Remaining → mixed batch (partial groups from above)
- Unmatched (5/batch):
  1. Same company → similar JD writing style, company context helps NOC selection
  2. Remaining → mixed batch
- Each priority: full batches only, partial groups overflow to next priority

**Session Control (Driver-level):**
- `max_batches_per_session`: exact N batches per `collect()`
- Cooldown between sessions (configurable)
- `max_sessions`: stop after N sessions, resume on next trigger

**Checkpoint:** Content-based batch_id = same rows always = same filename.
Worker-count independent. 2-level resume: Driver `done_job_ids` + Worker `check_checkpoint`.

**Output:** `step3_enriched.parquet` + checkpoints for resume
""")
    def step3_enrich(**context):
        """Step 3: LLM enrich — NOC + seniority + skills (Spark/Livy)."""
        import time
        from airflow.providers.apache.livy.hooks.livy import LivyHook

        params = context["params"]
        hook = LivyHook(livy_conn_id="livy_sd")

        batch_id = hook.post_batch(
            file="/opt/spark/pipelines/skill-demand/spark/step3_enrich_spark.py",
            args=[
                "--batch-delay", str(params["batch_delay_sec"]),
                "--max-batches-per-session", str(params["max_batches_per_session"]),
                "--session-cooldown-min", str(params["session_cooldown_min"]),
                "--max-sessions", str(params["max_sessions"]),
            ],
            conf={
                "spark.executor.memory": params["step3_executor_memory"],
                "spark.executor.instances": str(params["step3_executor_instances"]),
                "spark.driver.memory": "2g",
            },
        )
        print(f"[SD:STEP3] Submitted batch {batch_id}")

        log_offset = 0
        progress_dir = f"{PROCESSED_DIR}/step3/progress"
        last_progress = ""

        # Clear previous progress before polling (race condition: DAG reads before Spark job resets)
        # Note: can't rmtree — dir owned by Spark (root), Airflow user can't delete dir itself
        from pathlib import Path as _Path
        for pf in _Path(progress_dir).glob("*.json"):
            try:
                pf.unlink()
            except OSError:
                pass

        while True:
            state = hook.get_batch_state(batch_id)

            # Fetch and print new Livy log lines
            try:
                log_response = hook.run_method(
                    endpoint=f"/batches/{batch_id}/log?from={log_offset}",
                )
                if log_response.status_code == 200:
                    log_data = log_response.json()
                    lines = log_data.get("log", [])
                    if lines:
                        for line in lines:
                            print(line)
                        log_offset += len(lines)
            except Exception:
                pass

            # Read progress files directly
            try:
                import json as _json
                from pathlib import Path
                unmatched_rows = 0
                unmatched_noc = 0
                matched_rows = 0
                matched_skills = 0
                for pf in Path(progress_dir).glob("*.json"):
                    with open(pf) as f:
                        data = _json.load(f)
                        if data.get("type") == "unmatched":
                            unmatched_rows += data.get("rows", 0)
                            unmatched_noc += data.get("matched", 0)
                        elif data.get("type") == "matched":
                            matched_rows += data.get("rows", 0)
                            matched_skills += data.get("with_skills", 0)
                batch_count = sum(1 for pf in Path(progress_dir).glob("*.json") if not pf.name.startswith("_"))
                total = unmatched_rows + matched_rows

                # Check cooldown status (Driver writes single _cooldown.json)
                cooldown_file = Path(progress_dir) / "_cooldown.json"
                cooldown_msg = ""
                if cooldown_file.exists():
                    try:
                        cd = _json.load(open(cooldown_file))
                        cooldown_msg = f" | COOLDOWN session {cd.get('session')}, resume ~{cd.get('resume_at')}"
                    except Exception:
                        cooldown_msg = f" | COOLDOWN"

                if total > 0:
                    lines = [f"[SD:STEP3] Progress: {batch_count} batches, {total} rows"]
                    lines.append(f"  unmatched: {unmatched_rows} rows ({unmatched_noc} NOC matched)")
                    lines.append(f"  matched:   {matched_rows} rows ({matched_skills} with skills)")
                    if cooldown_msg:
                        lines.append(f"  {cooldown_msg.strip(' |')}")
                    msg = "\n".join(lines)
                    if msg != last_progress:
                        print(msg)
                        last_progress = msg
            except Exception:
                pass

            if state in hook.TERMINAL_STATES:
                break
            time.sleep(10)

        state_str = str(state).lower()
        if "success" not in state_str:
            raise Exception(f"Step 3 failed with state: {state}")

    # =========================================================================
    # Step 3 Review + Step 4 Prep
    # =========================================================================

    @task(task_id="sd_bridge_34", task_display_name="Step 3→4 Bridge", doc_md="""
### Step 3→4 Bridge
Review Step 3 output + validate before DB load.

**1. Step 3 Review:**
- Total rows, NOC completion rate, NOC by method (ST vs LLM)
- Seniority: valid, null, invalid counts
- Skills: count with skills, avg per posting

**2. Job ID Uniqueness Check:**
- Verify all job_ids are unique in enriched parquet
- If duplicates found → **fail task, block Step 4** (prevent corrupt DB load)

**3. Quality Filter Stats:**
- Missing NOC / skills / seniority counts
- Excluded row count and percentage
- Preview of DB load row count
""")
    def step3_review_step4_prep():
        """Review Step 3 results + prepare for DB load."""
        import pandas as pd
        from src.validators.v4_enrich import validate_enrich

        df = pd.read_parquet(f"{PROCESSED_DIR}/step3/step3_enriched.parquet")
        stats = validate_enrich(df)

        print(f"[SD:STEP3:REVIEW]")
        print(f"  Total: {stats['total']:,} rows")
        print(f"  NOC: {stats['noc_mapped']:,}/{stats['total']:,} ({stats['noc_completion_rate']:.1%})")
        print(f"  NOC by method: {stats['noc_by_method']}")
        print(f"  Seniority: {stats['valid_seniority']:,} valid, {stats['null_seniority']:,} null, {stats['invalid_seniority']:,} invalid")
        print(f"  Skills: {stats['with_skills']:,}/{stats['total']:,}, avg {stats['avg_skills_per_posting']:.1f}/posting")

        # Unique job_id verification
        total = len(df)
        unique = df["job_id"].nunique()
        dupes = total - unique
        if dupes > 0:
            print(f"\n[SD:STEP3:REVIEW] WARNING: {dupes:,} duplicate job_ids detected ({dupes/total*100:.1f}%)")
            dupe_ids = df[df["job_id"].duplicated(keep=False)]["job_id"].unique()[:5]
            print(f"  Sample duplicates: {list(dupe_ids)}")
            raise Exception(f"Step 3 output has {dupes:,} duplicate job_ids. Check checkpoint integrity.")
        else:
            print(f"\n[SD:STEP3:REVIEW] job_id uniqueness: {unique:,}/{total:,} (100%)")

        # Quality filter — incomplete rows excluded from DB load
        has_noc = df["noc_id"].notna()
        has_skills = df["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0).gt(0)
        has_seniority = df["seniority"].notna() & ~df["seniority"].isin(["NaN", "nan", "None", ""])
        complete = has_noc & has_skills & has_seniority
        excluded = total - int(complete.sum())

        print(f"\n[SD:STEP3:REVIEW] Quality filter (excluded from DB load):")
        print(f"  Missing NOC:       {int((~has_noc).sum()):,}")
        print(f"  Missing skills:    {int((~has_skills).sum()):,}")
        print(f"  Missing seniority: {int((~has_seniority).sum()):,}")
        print(f"  Excluded: {excluded:,} ({excluded/total*100:.1f}%)")
        print(f"  DB load:  {int(complete.sum()):,} rows")

        print(f"\n[SD:STEP4:PREP]")
        print(f"  DB: {SD_DB_CONN}")
        print(f"  Schema: {PIPELINE_DIR}/schema.sql")
        print(f"  DB load: {int(complete.sum()):,} rows (incomplete excluded)")
        return stats

    # =========================================================================
    # Step 4 — DB Load
    # =========================================================================

    @task(task_id="sd_step4", task_display_name="Step 4: DB Load", doc_md="""
### Step 4: Transform + Load (Star Schema)
Skill normalization + quality filter + full rebuild DB load.

**1. Quality Filter:**
- Drop rows missing NOC, skills, or seniority
- Same criteria as bridge_34 review

**2. Skill Normalization:**
- `inflect` plural→singular (e.g. "configurations" → "configuration")
- `KEEP_PLURAL` for business terms (sales, operations, logistics, analytics...)
- Category majority vote: same skill with multiple categories → most frequent wins
- Before/after stats + examples logged

**3. Star Schema Load (full rebuild):**
- Drop all star schema tables (preserves noc_titles)
- Recreate from schema.sql
- COPY bulk insert (io.StringIO + csv.writer + copy_from)
- Order: dim_seniority → dim_companies → dim_skills → fact_job_postings → fact_job_skill_demand

**4. DB Verification:**
- Row counts for all dim + fact tables
- Category breakdown from dim_skills
""")
    def step4_load():
        """Load enriched data into skill-demand PostgreSQL (Star Schema). Full rebuild each run."""
        import pandas as pd
        import psycopg2
        from src.step4_load import (
            explode_skills, normalize_skills, drop_star_schema,
            load_dim_seniority, load_dim_companies, load_dim_skills,
            load_fact_postings, load_fact_skills,
        )

        # 1. Load input + quality filter
        input_path = f"{PROCESSED_DIR}/step3/step3_enriched.parquet"
        df_raw = pd.read_parquet(input_path)
        has_noc = df_raw["noc_id"].notna()
        has_skills = df_raw["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0).gt(0)
        has_seniority = df_raw["seniority"].notna() & ~df_raw["seniority"].isin(["NaN", "nan", "None", ""])
        df = df_raw[has_noc & has_skills & has_seniority].copy()
        print(f"[SD:STEP4] Input: {input_path} ({len(df_raw):,} total, {len(df):,} after quality filter)")

        # 2. Skill normalization (inflect singular + category majority vote)
        raw_skill_rows = explode_skills(df)
        unique_before = len(set(r["skill"] for r in raw_skill_rows))

        skill_rows, dim_skills_dict, norm_stats = normalize_skills(raw_skill_rows)
        plural_map = norm_stats["plural_map"]
        multi_cat = norm_stats["category_votes"]

        print(f"[SD:STEP4] === Skill Normalization ===")
        print(f"  Before: {len(raw_skill_rows):,} entries, {unique_before:,} unique names")
        print(f"  1) Plural→Singular: {len(plural_map):,} plural forms found, merged into singular")
        if plural_map:
            for plural, singular in sorted(plural_map.items())[:5]:
                print(f"     e.g. '{plural}' → '{singular}'")
        print(f"  2) Category majority: {len(multi_cat):,} skills had multiple categories, resolved by vote")
        if multi_cat:
            top_multi = sorted(multi_cat.items(), key=lambda x: -sum(x[1].values()))[:5]
            for name, votes in top_multi:
                winner = votes.most_common(1)[0][0]
                breakdown = ", ".join(f"{cat}({cnt})" for cat, cnt in votes.most_common())
                print(f"     e.g. '{name}': {breakdown} → {winner}")
        print(f"  After:  {len(skill_rows):,} entries, {len(dim_skills_dict):,} unique names")

        conn = psycopg2.connect(SD_DB_CONN)
        conn.autocommit = True

        try:
            # 3. Drop + recreate schema (full rebuild — data warehouse pattern)
            drop_star_schema(conn)
            cur = conn.cursor()
            print(f"[SD:STEP4] Schema: drop + recreate from {PIPELINE_DIR}/schema.sql...")
            cur.execute(open(f"{PIPELINE_DIR}/schema.sql").read())
            cur.close()
            print(f"[SD:STEP4] Schema ready.")

            # 4. Load dimensions
            seniority_map = load_dim_seniority(conn)
            print(f"[SD:STEP4] dim_seniority: {len(seniority_map)} levels")

            companies = df["company_name"].dropna().unique().tolist()
            company_map = load_dim_companies(companies, conn)
            print(f"[SD:STEP4] dim_companies: {len(company_map):,} companies")

            skill_map = load_dim_skills(dim_skills_dict, conn)
            print(f"[SD:STEP4] dim_skills: {len(skill_map):,} skills")

            # 5. Load facts
            count_postings = load_fact_postings(df, company_map, seniority_map, conn)
            print(f"[SD:STEP4] fact_job_postings: {count_postings:,} inserted")

            count_skills = load_fact_skills(skill_rows, skill_map, conn)
            print(f"[SD:STEP4] fact_job_skill_demand: {count_skills:,} inserted")

            # 6. Verify
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM fact_job_postings")
            db_postings = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM fact_job_skill_demand")
            db_skills = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dim_skills")
            db_dim_skills = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dim_companies")
            db_dim_companies = cur.fetchone()[0]
            cur.execute("SELECT category, COUNT(*) FROM dim_skills GROUP BY category ORDER BY COUNT(*) DESC")
            cat_counts = cur.fetchall()
            cur.close()

            print(f"[SD:STEP4] === DB Verification ===")
            print(f"  dim_companies:         {db_dim_companies:,}")
            print(f"  dim_skills:            {db_dim_skills:,}")
            print(f"  fact_job_postings:     {db_postings:,}")
            print(f"  fact_job_skill_demand: {db_skills:,} entries")
            for cat, cnt in cat_counts:
                print(f"    {cat}: {cnt}")
        finally:
            conn.close()

    # =========================================================================
    # Step 4 Review + Final Summary
    # =========================================================================

    @task(task_id="sd_final", task_display_name="Pipeline Complete: Final Summary", doc_md="""
### Pipeline Complete: Final Summary
End-to-end pipeline completion report.

**Coverage:**
- Total dataset vs processed vs quality-filtered vs DB-loaded
- Quality filter breakdown: missing NOC / skills / seniority
- NOC category coverage (out of 510)

**Star Schema Stats:**
- dim_companies, dim_skills, fact_job_postings, fact_job_skill_demand counts
- Skill category breakdown (hard_skill, soft_skill, tool, certification)
- Seniority distribution
- Avg skills per posting

**Analysis:**
- Top 10 most demanded skills (from fact × dim JOIN)
- Sample 10 random postings with company, title, seniority, top skills
""")
    def step4_review_final_summary():
        """Review DB load results + final pipeline summary (Star Schema)."""
        import psycopg2
        import pandas as pd

        # --- Read original parquet for total/filter stats ---
        input_path = f"{PROCESSED_DIR}/step3/step3_enriched.parquet"
        df_raw = pd.read_parquet(input_path)
        total_processed = len(df_raw)
        has_noc_raw = df_raw["noc_id"].notna()
        has_skills_raw = df_raw["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0).gt(0)
        has_seniority_raw = df_raw["seniority"].notna() & ~df_raw["seniority"].isin(["NaN", "nan", "None", ""])
        total_complete = int((has_noc_raw & has_skills_raw & has_seniority_raw).sum())
        total_dropped = total_processed - total_complete

        conn = psycopg2.connect(SD_DB_CONN)
        cur = conn.cursor()

        # --- DB stats ---
        cur.execute("SELECT COUNT(*) FROM fact_job_postings")
        db_postings = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fact_job_skill_demand")
        db_skill_facts = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM dim_skills")
        db_dim_skills = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM dim_companies")
        db_dim_companies = cur.fetchone()[0]

        print(f"[SD:STEP4:REVIEW]")
        print(f"  fact_job_postings:     {db_postings:,}")
        print(f"  fact_job_skill_demand: {db_skill_facts:,} entries")
        print(f"  dim_skills:            {db_dim_skills:,}")
        print(f"  dim_companies:         {db_dim_companies:,}")

        # --- Final Pipeline Summary ---
        cur.execute("SELECT COUNT(*) FROM fact_job_postings WHERE noc_id IS NOT NULL")
        with_noc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fact_job_postings WHERE seniority_id IS NOT NULL")
        with_sen = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT n.noc21_code) FROM fact_job_postings p JOIN noc_titles n ON p.noc_id = n.id")
        noc_covered = cur.fetchone()[0]
        cur.execute("SELECT ROUND(AVG(cnt)::numeric, 1) FROM (SELECT COUNT(*) as cnt FROM fact_job_skill_demand GROUP BY job_id) sub")
        avg_skills = cur.fetchone()[0]

        print(f"\n{'='*60}")
        print(f"  SD:FINAL — PIPELINE COMPLETE")
        print(f"{'='*60}")
        print(f"  Total dataset:   123,782")
        print(f"  Processed:       {total_processed:,} ({total_processed/123782*100:.1f}%)")
        print(f"  Quality filter:  {total_dropped:,} dropped ({total_dropped/total_processed*100:.1f}%)")
        print(f"    Missing NOC:       {int((~has_noc_raw).sum()):,}")
        print(f"    Missing skills:    {int((~has_skills_raw).sum()):,}")
        print(f"    Missing seniority: {int((~has_seniority_raw).sum()):,}")
        print(f"  DB loaded:       {db_postings:,} ({db_postings/123782*100:.1f}% of total)")
        print(f"  NOC classified:  {with_noc:,}/{db_postings:,} ({with_noc/db_postings*100:.1f}%)")
        print(f"  Seniority:       {with_sen:,}/{db_postings:,} ({with_sen/db_postings*100:.1f}%)")
        print(f"  NOC coverage:    {noc_covered} / 510 categories")
        print(f"  Skills:          {db_skill_facts:,} fact entries, {db_dim_skills:,} unique (normalized)")
        print(f"  Avg skills/post: {avg_skills}")

        # Category breakdown (from dim_skills)
        cur.execute("SELECT category, COUNT(*) FROM dim_skills GROUP BY category ORDER BY COUNT(*) DESC")
        print(f"\n  Skill Categories (dim_skills):")
        for cat, cnt in cur.fetchall():
            print(f"    {cat:<15} {cnt:>8,}")

        # Seniority breakdown
        cur.execute("""
            SELECT ds.level, COUNT(*)
            FROM fact_job_postings p
            JOIN dim_seniority ds ON p.seniority_id = ds.id
            GROUP BY ds.level ORDER BY COUNT(*) DESC
        """)
        print(f"\n  Seniority:")
        for sen, cnt in cur.fetchall():
            print(f"    {sen:<15} {cnt:>8,}")

        # Top 10 skills (from fact + dim join)
        cur.execute("""
            SELECT sk.name, sk.category, COUNT(*) as cnt
            FROM fact_job_skill_demand f
            JOIN dim_skills sk ON f.skill_id = sk.id
            GROUP BY sk.name, sk.category
            ORDER BY cnt DESC LIMIT 10
        """)
        print(f"\n  Top 10 Skills:")
        print(f"    {'Skill':<30} {'Category':<15} {'Count':>6}")
        print(f"    {'-'*55}")
        for skill, cat, cnt in cur.fetchall():
            print(f"    {skill:<30} {cat:<15} {cnt:>6,}")

        # Sample 10 postings
        cur.execute("""
            SELECT dc.name, p.raw_title, ds.level,
                   (SELECT COUNT(*) FROM fact_job_skill_demand f WHERE f.job_id = p.job_id) as skills,
                   (SELECT STRING_AGG(sk.name, ', ')
                    FROM (SELECT skill_id FROM fact_job_skill_demand WHERE job_id = p.job_id LIMIT 3) f2
                    JOIN dim_skills sk ON f2.skill_id = sk.id
                   ) as top_skills
            FROM fact_job_postings p
            LEFT JOIN dim_companies dc ON p.company_id = dc.id
            LEFT JOIN dim_seniority ds ON p.seniority_id = ds.id
            WHERE p.noc_id IS NOT NULL
            ORDER BY RANDOM() LIMIT 10
        """)
        print(f"\n  Sample 10 Postings:")
        print(f"    {'Company':<25} {'Title':<30} {'Seniority':<12} {'#':>3} {'Top Skills'}")
        print(f"    {'-'*100}")
        for company, title, sen, skills_cnt, top_skills in cur.fetchall():
            co = (company or "")[:24]
            ti = (title or "")[:29]
            sn = (sen or "?")[:11]
            ts = (top_skills or "")[:40]
            print(f"    {co:<25} {ti:<30} {sn:<12} {skills_cnt:>3} {ts}")

        print(f"\n{'='*60}")
        cur.close()
        conn.close()

    # =========================================================================
    # Infrastructure — Stop Workers (after Spark jobs, before DB load)
    # =========================================================================

    if MANAGE_SPARK:
        stop_spark_workers = BashOperator(
            task_id="sd_stop_workers",
            task_display_name="Stop Spark Workers",
            doc_md="Release Spark workers after all Spark jobs complete. Master + Livy stay up for Airflow polling. Trigger: all_done (runs even if upstream failed).",
            bash_command=f"cd {INFRA_DIR} && bash down.sh spark-sd-workers 2>&1",
            trigger_rule="all_done",
        )
    else:
        stop_spark_workers = EmptyOperator(task_id="sd_stop_workers", task_display_name="Stop Spark Workers", trigger_rule="all_done")

    # =========================================================================
    # Infrastructure — Stop Spark Cluster (Master + Livy, after everything)
    # =========================================================================

    if MANAGE_SPARK:
        stop_spark_cluster = BashOperator(
            task_id="sd_stop_spark",
            task_display_name="Stop Spark Cluster",
            doc_md="Shut down Spark Master + Livy. Final cleanup — all Spark resources released. Trigger: all_done.",
            bash_command=f"cd {INFRA_DIR} && bash down.sh spark-sd-cluster 2>&1",
            trigger_rule="all_done",
        )
    else:
        stop_spark_cluster = EmptyOperator(task_id="sd_stop_spark", task_display_name="Stop Spark Cluster", trigger_rule="all_done")

    # =========================================================================
    # DAG Dependency Chain
    # =========================================================================
    #
    #   ensure_db → noc_setup → download → v1 → step1 → v2  (no Spark needed)
    #   → start_spark_cluster (Master + Livy)
    #   validate_params
    #       ├── ensure_db
    #       ├── noc_setup                      (parallel)
    #       └── start_spark_cluster
    #               ↓ (all complete)
    #           download → v1 → step1
    #       → step1_review_step2_prep          (review + scale + config)
    #       → step2
    #       → step2_review_step3_prep          (review + scale + grouping preview)
    #       → step3_enrich
    #           ├── step3_review_step4_prep → step4_load → step4_review_final_summary
    #           └── stop_workers → stop_cluster             (Spark cleanup, parallel)
    #

    noc = noc_setup()
    dl = download()
    v1 = validate_v1(dl)
    s1 = step1_extract(v1)
    bridge_12 = step1_review_step2_prep(s1)
    s2 = step2_noc()
    bridge_23 = step2_review_step3_prep()
    s3 = step3_enrich()
    bridge_34 = step3_review_step4_prep()
    s4_load = step4_load()
    final = step4_review_final_summary()

    vp = validate_params()

    # validate_params → [ensure_db + start_spark] parallel, noc after db → download
    vp >> [ensure_db, start_spark_cluster]
    ensure_db >> noc
    [noc, start_spark_cluster] >> dl >> v1 >> s1
    s1 >> bridge_12 >> s2 >> bridge_23 >> s3

    # After step3: review + DB load + final summary, parallel with Spark shutdown
    s3 >> bridge_34 >> s4_load >> final
    s3 >> stop_spark_workers >> stop_spark_cluster

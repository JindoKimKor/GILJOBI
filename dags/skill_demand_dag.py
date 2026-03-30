"""
Skill Demand Pipeline — Airflow DAG.

Orchestrates the full pipeline lifecycle including infrastructure:
    [ENSURE_DB] → [ENSURE_SPARK] → DOWNLOAD → V1 → STEP1 → V2
    → STEP2 (Sentence Transformers via LivyOperator)
    → V3 → STEP3+4 (LLM batch pipeline via LivyOperator)
    → V4 → V5 → STEP5 → [STOP_SPARK]

Infrastructure management:
    MANAGE_PIPELINE_DB=true  → Starts PostgreSQL container (skill-demand DB)
    MANAGE_SPARK=true        → Starts/stops Spark+Livy cluster

LLM steps (3+4) are combined into a single batch processor:
    Each batch: NOC fallback → seniority + skills extraction → checkpoint
    Rate limited for Claude CLI subscription session limits.

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
        # LLM rate limiting
        "batch_size": Param(10, type="integer", description="JDs per LLM call"),
        "batch_delay_sec": Param(5, type="integer", description="Seconds between LLM batches"),
        "max_batches_per_session": Param(50, type="integer", description="Max batches per session window"),
        "session_cooldown_min": Param(60, type="integer", description="Minutes to wait for session reset after hitting limit"),
        # NOC matching
        "noc_threshold": Param(0.75, type="number", description="Sentence Transformers cosine similarity threshold"),
        # Spark resources — Step 2 (Sentence Transformers, memory-heavy)
        "step2_executor_memory": Param("2g", type="string", description="Step 2 executor memory (model loading)"),
        "step2_executor_instances": Param(2, type="integer", description="Step 2 executor count"),
        # Spark resources — Step 3+4 (LLM calls, IO-heavy)
        "step3_4_executor_memory": Param("1g", type="string", description="Step 3+4 executor memory"),
        "step3_4_executor_instances": Param(2, type="integer", description="Step 3+4 executor count"),
    },
    doc_md="""
    ## Skill Demand Pipeline

    Extracts skill demand from LinkedIn job postings.

    **Data Source:** Kaggle arshkon/linkedin-job-postings (~124K postings)

    **Steps:**
    1. Download dataset from Kaggle API
    2. Extract columns (job_id, company_name, title, description)
    3. NOC normalize via Sentence Transformers (cosine similarity)
    4. LLM fallback for sub-threshold NOC matches + seniority + skills extraction
    5. Load into skill-demand PostgreSQL (jd_postings + jd_skills)

    **Spark Features:** Broadcast Join, UDF, mapPartitions, Schema Enforcement
    **LLM:** Claude Haiku CLI (subscription, rate limited, checkpoint-based resume)

    **Parameters (adjustable at trigger time):**
    - `batch_size`: JDs per LLM call (default: 10)
    - `batch_delay_sec`: Seconds between batches (default: 5)
    - `max_batches_per_run`: Stop after N batches, resume next run (default: 50)
    - `noc_threshold`: Cosine similarity threshold for Step 2 (default: 0.75)

    **Trigger:** Manual only
    """,
) as dag:

    # =========================================================================
    # Infrastructure — Ensure DB
    # =========================================================================

    if MANAGE_DB:
        ensure_db = BashOperator(
            task_id="ensure_db",
            bash_command=(
                f"docker ps --filter name=skill-demand-db --filter status=running -q | grep -q . "
                f"&& echo 'DB already running' "
                f"|| (cd {INFRA_DIR} && bash up.sh postgres-sd 2>&1)"
            ),
        )
    else:
        ensure_db = EmptyOperator(task_id="ensure_db")

    # =========================================================================
    # Infrastructure — Spark Cluster (Master + Livy only)
    # =========================================================================

    if MANAGE_SPARK:
        start_spark_cluster = BashOperator(
            task_id="start_spark_cluster",
            bash_command=f"cd {INFRA_DIR} && bash up.sh spark-sd-cluster 2>&1",
        )
    else:
        start_spark_cluster = EmptyOperator(task_id="start_spark_cluster")

    # =========================================================================
    # Infrastructure — Spark Workers (on-demand, before Spark jobs)
    # =========================================================================

    if MANAGE_SPARK:
        start_spark_workers = BashOperator(
            task_id="start_spark_workers",
            bash_command=f"cd {INFRA_DIR} && bash up.sh spark-sd-workers 2>&1",
        )
    else:
        start_spark_workers = EmptyOperator(task_id="start_spark_workers")

    # =========================================================================
    # NOC Setup — Populate noc_titles in skill-demand DB
    # =========================================================================

    @task
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
            print(f"[NOC] Already populated: {count} titles. Skipping.")
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
        print(f"[NOC] Loaded {len(noc)} unit group titles into skill-demand DB.")

    # =========================================================================
    # Download — Kaggle Dataset
    # =========================================================================

    @task
    def download():
        """Download LinkedIn Job Postings from Kaggle API."""
        from pathlib import Path
        from src.download import download_dataset

        output_dir = download_dataset(output_dir=Path(RAW_DIR))
        return str(output_dir)

    # =========================================================================
    # V1 — Post-Download Validation
    # =========================================================================

    @task(task_id="validate_file_integrity")
    def validate_v1(raw_dir: str):
        """Validate downloaded CSV: file integrity, required columns."""
        import os
        from src.validators.v1_download import validate_download

        csv_path = os.path.join(raw_dir, "postings.csv")
        result = validate_download(csv_path)

        if not result["valid"]:
            raise ValueError(f"V1 validation failed: {result['error']}")

        print(f"[V1] Valid. {result['rows']} rows found.")
        return csv_path

    # =========================================================================
    # Step 1 — Column Extraction
    # =========================================================================

    @task
    def step1_extract(csv_path: str):
        """Extract job_id, company_name, title, description → parquet."""
        from pathlib import Path
        from src.step1_select_columns import run

        output_path = run(csv_path, output_dir=Path(PROCESSED_DIR) / "step1")
        print(f"[STEP 1] Output: {output_path}")
        return output_path

    # =========================================================================
    # V2 — Post-Extract Validation
    # =========================================================================

    @task(task_id="validate_nulls_and_length")
    def validate_v2(parquet_path: str):
        """Validate extracted data: nulls, description min length."""
        import pandas as pd
        from src.validators.v2_extract import validate_extract

        df = pd.read_parquet(parquet_path)
        cleaned = validate_extract(df)

        # Overwrite parquet with cleaned data
        cleaned.to_parquet(parquet_path, index=False)

        dropped = len(df) - len(cleaned)
        print(f"[V2] {len(cleaned)} rows kept, {dropped} dropped.")
        return parquet_path

    # =========================================================================
    # Step 2 — NOC Normalize (Sentence Transformers via Spark)
    # =========================================================================

    @task(task_id="step2_noc_normalize")
    def step2_noc(**context):
        """Step 2: NOC Normalize via Sentence Transformers (Spark/Livy with live logs)."""
        import time
        from airflow.providers.apache.livy.hooks.livy import LivyHook

        params = context["params"]
        hook = LivyHook(livy_conn_id="livy_sd")

        batch_id = hook.post_batch(
            file="/opt/spark/pipelines/skill-demand/spark/step2_noc_spark.py",
            args=["--threshold", str(params["noc_threshold"])],
            conf={
                "spark.executor.memory": params["step2_executor_memory"],
                "spark.executor.instances": str(params["step2_executor_instances"]),
                "spark.driver.memory": "2g",
            },
        )
        print(f"[STEP 2] Submitted batch {batch_id}")

        log_offset = 0
        while True:
            state = hook.get_batch_state(batch_id)

            # Fetch and print new log lines
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

            if state in hook.TERMINAL_STATES:
                break
            time.sleep(10)

        state_str = str(state).lower()
        if "success" not in state_str:
            raise Exception(f"Step 2 failed with state: {state}")

    # =========================================================================
    # V3 — Post-Normalize Validation
    # =========================================================================

    @task(task_id="validate_noc_match_rate")
    def validate_v3():
        """Validate NOC normalization: match rate, split stats."""
        import pandas as pd
        from src.validators.v3_normalize import validate_normalize

        df = pd.read_parquet(f"{PROCESSED_DIR}/step2/step2_normalized.parquet")
        stats = validate_normalize(df)

        print(f"[V3] Match rate: {stats['match_rate']:.1%}")
        print(f"[V3] Matched: {stats['matched']}, Unmatched: {stats['unmatched']}")
        print(f"[V3] Avg score: {stats['avg_score']:.3f}")
        return stats

    # =========================================================================
    # Step 3+4 — LLM Batch Pipeline (NOC fallback + seniority + skills)
    # =========================================================================

    @task(task_id="step3_4_llm_batch")
    def step3_4_llm(**context):
        """Step 3+4: LLM batch pipeline (Spark/Livy with live logs)."""
        import time
        from airflow.providers.apache.livy.hooks.livy import LivyHook

        params = context["params"]
        hook = LivyHook(livy_conn_id="livy_sd")

        batch_id = hook.post_batch(
            file="/opt/spark/pipelines/skill-demand/spark/step3_4_llm_spark.py",
            args=[
                "--batch-size", str(params["batch_size"]),
                "--batch-delay", str(params["batch_delay_sec"]),
                "--max-batches-per-session", str(params["max_batches_per_session"]),
                "--session-cooldown-min", str(params["session_cooldown_min"]),
            ],
            conf={
                "spark.executor.memory": params["step3_4_executor_memory"],
                "spark.executor.instances": str(params["step3_4_executor_instances"]),
                "spark.driver.memory": "2g",
            },
        )
        print(f"[STEP 3+4] Submitted batch {batch_id}")

        log_offset = 0
        while True:
            state = hook.get_batch_state(batch_id)

            # Fetch and print new log lines
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

            if state in hook.TERMINAL_STATES:
                break
            time.sleep(10)

        state_str = str(state).lower()
        if "success" not in state_str:
            raise Exception(f"Step 3+4 failed with state: {state}")

    # =========================================================================
    # V4 — Post-Fallback Validation
    # =========================================================================

    @task(task_id="validate_noc_completion")
    def validate_v4():
        """Validate NOC completion rate after LLM fallback."""
        import pandas as pd
        from src.validators.v4_fallback import validate_fallback

        df = pd.read_parquet(f"{PROCESSED_DIR}/step3_4/step3_4_enriched.parquet")
        stats = validate_fallback(df)

        print(f"[V4] Completion rate: {stats['completion_rate']:.1%}")
        print(f"[V4] By method: {stats['by_method']}")
        return stats

    # =========================================================================
    # V5 — Post-Enrich Validation
    # =========================================================================

    @task(task_id="validate_seniority_and_skills")
    def validate_v5():
        """Validate seniority + skills extraction."""
        import pandas as pd
        from src.validators.v5_enrich import validate_enrich

        df = pd.read_parquet(f"{PROCESSED_DIR}/step3_4/step3_4_enriched.parquet")
        stats = validate_enrich(df)

        print(f"[V5] Valid seniority: {stats['valid_seniority']}/{stats['total']}")
        print(f"[V5] With skills: {stats['with_skills']}/{stats['total']}")
        print(f"[V5] Avg skills/posting: {stats['avg_skills_per_posting']:.1f}")
        return stats

    # =========================================================================
    # Step 5 — DB Load
    # =========================================================================

    @task
    def step5_load():
        """Load enriched data into skill-demand PostgreSQL."""
        import pandas as pd
        import psycopg2
        from src.step5_load import prepare_postings_rows, prepare_skills_rows, load_postings, load_skills

        df = pd.read_parquet(f"{PROCESSED_DIR}/step3_4/step3_4_enriched.parquet")

        conn = psycopg2.connect(SD_DB_CONN)
        conn.autocommit = True

        try:
            # Create tables if not exist
            cur = conn.cursor()
            cur.execute(open(f"{PIPELINE_DIR}/schema.sql").read())
            cur.close()

            posting_rows = prepare_postings_rows(df)
            count_postings = load_postings(posting_rows, conn)
            print(f"[STEP 5] {count_postings} postings inserted.")

            skill_data = df[["job_id", "skills"]].to_dict("records")
            skill_rows = prepare_skills_rows(skill_data)
            count_skills = load_skills(skill_rows, conn)
            print(f"[STEP 5] {count_skills} skills inserted.")
        finally:
            conn.close()

    # =========================================================================
    # Infrastructure — Stop Workers (after Spark jobs, before DB load)
    # =========================================================================

    if MANAGE_SPARK:
        stop_spark_workers = BashOperator(
            task_id="stop_spark_workers",
            bash_command=f"cd {INFRA_DIR} && bash down.sh spark-sd-workers 2>&1",
            trigger_rule="all_done",
        )
    else:
        stop_spark_workers = EmptyOperator(task_id="stop_spark_workers", trigger_rule="all_done")

    # =========================================================================
    # Infrastructure — Stop Spark Cluster (Master + Livy, after everything)
    # =========================================================================

    if MANAGE_SPARK:
        stop_spark_cluster = BashOperator(
            task_id="stop_spark_cluster",
            bash_command=f"cd {INFRA_DIR} && bash down.sh spark-sd-cluster 2>&1",
            trigger_rule="all_done",
        )
    else:
        stop_spark_cluster = EmptyOperator(task_id="stop_spark_cluster", trigger_rule="all_done")

    # =========================================================================
    # DAG Dependency Chain
    # =========================================================================
    #
    #   ensure_db → noc_setup → download → v1 → step1 → v2  (no Spark needed)
    #   → start_spark_cluster (Master + Livy)
    #   → start_spark_workers
    #   → step2_noc → v3 → step3_4_llm → v4 → v5         (Spark jobs)
    #   → stop_spark_workers                                (free worker resources)
    #   → step5_load                                        (DB only, no Spark)
    #   → stop_spark_cluster                                (cleanup)
    #

    noc = noc_setup()
    dl = download()
    v1 = validate_v1(dl)
    s1 = step1_extract(v1)
    v2 = validate_v2(s1)
    s2 = step2_noc()
    v3 = validate_v3()
    s3_4 = step3_4_llm()
    v4 = validate_v4()
    v5 = validate_v5()
    s5 = step5_load()

    (
        ensure_db
        >> noc >> dl >> v1 >> s1 >> v2
        >> start_spark_cluster
        >> start_spark_workers
        >> s2 >> v3
        >> s3_4 >> v4 >> v5
        >> stop_spark_workers
        >> s5
        >> stop_spark_cluster
    )

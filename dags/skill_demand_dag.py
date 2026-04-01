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
    4. Load into skill-demand PostgreSQL (jd_postings + jd_skills with category)

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

    @task(task_id="sd_validate_params", task_display_name="Validate Params")
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
            bash_command=f"cd {INFRA_DIR} && bash up.sh spark-sd-cluster 2>&1",
        )
    else:
        start_spark_cluster = EmptyOperator(task_id="sd_start_spark", task_display_name="Start Spark Cluster")

    # (Spark worker scaling is now handled inside bridge tasks:
    #  step1_review_step2_prep and step2_review_step3_prep)

    # =========================================================================
    # NOC Setup — Populate noc_titles in skill-demand DB
    # =========================================================================

    @task(task_id="sd_load_noc", task_display_name="Seed NOC Codes")
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

    @task(task_id="sd_download", task_display_name="Download Dataset")
    def download():
        """Download LinkedIn Job Postings from Kaggle API."""
        from pathlib import Path
        from src.download import download_dataset

        output_dir = download_dataset(output_dir=Path(RAW_DIR))
        return str(output_dir)

    # =========================================================================
    # V1 — Post-Download Validation
    # =========================================================================

    @task(task_id="sd_validate_source", task_display_name="Validate Source")
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

    @task(task_id="sd_step1", task_display_name="Step 1: Extract Columns")
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

    @task(task_id="sd_bridge_12", task_display_name="Step 1→2 Bridge")
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

    @task(task_id="sd_step2", task_display_name="Step 2: NOC Match (ST)")
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
### Step 2 Review + Step 3 Prep

**What this task does:**
1. Reviews Step 2 output (NOC match rate, seniority distribution)
2. Scales Spark workers for Step 3 (IO-heavy, more workers)
3. Shows grouping preview (matched/unmatched LLM call estimate)

**Input:** `processed/step2/step2_normalized.parquet`

**Expected Logs:**
- `[SD:STEP2:REVIEW]` — NOC match %, seniority breakdown
- `[SD:STEP3:PREP]` — worker count, memory, session control config
- `[SD:STEP3:GROUPING]` — P1/P2/remaining batches, estimated LLM calls
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
        BATCH_MATCHED = 10
        BATCH_UNMATCHED = 5

        def _simulate(df_in, group_cols_list, batch_size):
            remaining = df_in.copy()
            stats = []
            for name, cols in group_cols_list:
                full_batches, full_rows = 0, 0
                next_rem = []
                for _, grp in remaining.groupby(cols):
                    fb = len(grp) // batch_size
                    full_batches += fb
                    full_rows += fb * batch_size
                    left = len(grp) % batch_size
                    if left > 0:
                        next_rem.append(grp.tail(left))
                rem_df = pd.concat(next_rem) if next_rem else pd.DataFrame()
                stats.append((name, full_batches, full_rows, len(rem_df)))
                remaining = rem_df
            rem_b = (len(remaining) + batch_size - 1) // batch_size if len(remaining) > 0 else 0
            stats.append(("remaining", rem_b, len(remaining), 0))
            return stats

        m_calls, u_calls = 0, 0

        if len(matched) > 0:
            m_stats = _simulate(matched, [
                ("Group by company + NOC", ["company_name", "noc_id"]),
                ("Group by NOC only", ["noc_id"]),
            ], BATCH_MATCHED)
            print(f"\n[SD:STEP3:GROUPING] === Matched: {len(matched):,} rows → {BATCH_MATCHED}/batch ===")
            for i, (name, batches, rows, rem) in enumerate(m_stats, 1):
                m_calls += batches
                remaining = f", {rem:,} remaining" if rem > 0 else ""
                print(f"  Step {i}) {name:<25} → {batches:>6,} batches ({rows:>6,} rows consumed{remaining})")
            print(f"  Total: {m_calls:,} LLM calls")

        if len(unmatched) > 0:
            u_stats = _simulate(unmatched, [
                ("Group by company", ["company_name"]),
            ], BATCH_UNMATCHED)
            print(f"\n[SD:STEP3:GROUPING] === Unmatched: {len(unmatched):,} rows → {BATCH_UNMATCHED}/batch ===")
            for i, (name, batches, rows, rem) in enumerate(u_stats, 1):
                u_calls += batches
                remaining = f", {rem:,} remaining" if rem > 0 else ""
                print(f"  Step {i}) {name:<25} → {batches:>6,} batches ({rows:>6,} rows consumed{remaining})")
            print(f"  Total: {u_calls:,} LLM calls")

        total_calls = m_calls + u_calls
        print(f"\n[SD:STEP3:GROUPING] Total: {total_calls:,} LLM calls")

        bps = params['max_batches_per_session']
        if bps > 0 and total_calls > 0:
            sessions_needed = (total_calls + bps - 1) // bps
            cooldown = params['session_cooldown_min']
            total_hours = sessions_needed * (cooldown / 60)
            print(f"  (e.g. at {bps} batches/session, {cooldown} min cooldown → ~{sessions_needed} sessions, ~{total_hours:.1f} hours)")

    # =========================================================================
    # Step 3 — LLM Enrich: NOC + Seniority + Skills
    # =========================================================================

    @task(task_id="sd_step3", task_display_name="Step 3: LLM Enrich")
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

    @task(task_id="sd_bridge_34", task_display_name="Step 3→4 Bridge")
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

    @task(task_id="sd_step4", task_display_name="Step 4: DB Load")
    def step4_load():
        """Load enriched data into skill-demand PostgreSQL."""
        import pandas as pd
        import psycopg2
        from src.step4_load import prepare_postings_rows, prepare_skills_rows, load_postings, load_skills

        # Load input + quality filter (same as bridge_34 review)
        input_path = f"{PROCESSED_DIR}/step3/step3_enriched.parquet"
        df_raw = pd.read_parquet(input_path)
        has_noc = df_raw["noc_id"].notna()
        has_skills = df_raw["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0).gt(0)
        has_seniority = df_raw["seniority"].notna() & ~df_raw["seniority"].isin(["NaN", "nan", "None", ""])
        df = df_raw[has_noc & has_skills & has_seniority].copy()
        print(f"[SD:STEP4] Input: {input_path} ({len(df_raw):,} total, {len(df):,} after quality filter)")

        conn = psycopg2.connect(SD_DB_CONN)
        conn.autocommit = True

        try:
            # Create tables if not exist
            cur = conn.cursor()
            print(f"[SD:STEP4] Creating schema from {PIPELINE_DIR}/schema.sql...")
            cur.execute(open(f"{PIPELINE_DIR}/schema.sql").read())
            cur.close()
            print(f"[SD:STEP4] Schema ready.")

            # Insert postings
            posting_rows = prepare_postings_rows(df)
            count_postings = load_postings(posting_rows, conn)
            print(f"[SD:STEP4] {count_postings} postings inserted.")

            # Insert skills
            skill_data = df[["job_id", "skills"]].to_dict("records")
            skill_rows = prepare_skills_rows(skill_data)
            count_skills = load_skills(skill_rows, conn)
            print(f"[SD:STEP4] {count_skills} skills inserted.")

            # Verify
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM jd_postings")
            db_postings = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM jd_skills")
            db_skills = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT skill) FROM jd_skills")
            unique_skills = cur.fetchone()[0]
            cur.execute("SELECT category, COUNT(*) FROM jd_skills GROUP BY category ORDER BY COUNT(*) DESC")
            cat_counts = cur.fetchall()
            cur.close()

            print(f"[SD:STEP4] === DB Verification ===")
            print(f"  jd_postings: {db_postings} rows")
            print(f"  jd_skills: {db_skills} entries ({unique_skills} unique skills)")
            for cat, cnt in cat_counts:
                print(f"    {cat}: {cnt}")
        finally:
            conn.close()

    # =========================================================================
    # Step 4 Review + Final Summary
    # =========================================================================

    @task(task_id="sd_final", task_display_name="Pipeline Complete: Final Summary")
    def step4_review_final_summary():
        """Review DB load results + final pipeline summary."""
        import psycopg2

        conn = psycopg2.connect(SD_DB_CONN)
        cur = conn.cursor()

        # --- Step 4 Review ---
        cur.execute("SELECT COUNT(*) FROM jd_postings")
        db_postings = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM jd_skills")
        db_skills = cur.fetchone()[0]

        print(f"[SD:STEP4:REVIEW]")
        print(f"  jd_postings: {db_postings:,} rows")
        print(f"  jd_skills: {db_skills:,} entries")

        # --- Final Pipeline Summary ---
        cur.execute("SELECT COUNT(*) FROM jd_postings WHERE noc_id IS NOT NULL")
        with_noc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM jd_postings WHERE seniority IS NOT NULL")
        with_sen = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT skill) FROM jd_skills")
        unique_skills = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT n.noc21_code) FROM jd_postings p JOIN noc_titles n ON p.noc_id = n.id")
        noc_covered = cur.fetchone()[0]
        cur.execute("SELECT ROUND(AVG(cnt)::numeric, 1) FROM (SELECT COUNT(*) as cnt FROM jd_skills GROUP BY jd_id) sub")
        avg_skills = cur.fetchone()[0]

        print(f"\n{'='*60}")
        print(f"  SD:FINAL — PIPELINE COMPLETE")
        print(f"{'='*60}")
        print(f"  Postings:        {db_postings:,}")
        print(f"  NOC classified:  {with_noc:,} ({with_noc/db_postings*100:.1f}%)")
        print(f"  Seniority:       {with_sen:,} ({with_sen/db_postings*100:.1f}%)")
        print(f"  NOC coverage:    {noc_covered} / 510 categories")
        print(f"  Skills:          {db_skills:,} total, {unique_skills:,} unique")
        print(f"  Avg skills/post: {avg_skills}")

        # Category breakdown
        cur.execute("SELECT category, COUNT(*) FROM jd_skills GROUP BY category ORDER BY COUNT(*) DESC")
        print(f"\n  Skill Categories:")
        for cat, cnt in cur.fetchall():
            print(f"    {cat:<15} {cnt:>8,}")

        # Seniority breakdown
        cur.execute("SELECT COALESCE(seniority, 'unknown') as sen, COUNT(*) FROM jd_postings GROUP BY seniority ORDER BY COUNT(*) DESC")
        print(f"\n  Seniority:")
        for sen, cnt in cur.fetchall():
            print(f"    {sen:<15} {cnt:>8,}")

        # Top 10 skills
        cur.execute("SELECT skill, category, COUNT(*) as cnt FROM jd_skills GROUP BY skill, category ORDER BY cnt DESC LIMIT 10")
        print(f"\n  Top 10 Skills:")
        print(f"    {'Skill':<30} {'Category':<15} {'Count':>6}")
        print(f"    {'-'*55}")
        for skill, cat, cnt in cur.fetchall():
            print(f"    {skill:<30} {cat:<15} {cnt:>6,}")

        # Sample 10 postings
        cur.execute("""
            SELECT p.company, p.raw_title, n.noc21_name, p.seniority,
                   (SELECT COUNT(*) FROM jd_skills s WHERE s.jd_id = p.job_id) as skills,
                   (SELECT STRING_AGG(s.skill, ', ')
                    FROM (SELECT skill FROM jd_skills WHERE jd_id = p.job_id ORDER BY skill LIMIT 3) s
                   ) as top_skills
            FROM jd_postings p
            LEFT JOIN noc_titles n ON p.noc_id = n.id
            WHERE p.noc_id IS NOT NULL
            ORDER BY RANDOM() LIMIT 10
        """)
        print(f"\n  Sample 10 Postings:")
        print(f"    {'Company':<25} {'Title':<30} {'Seniority':<12} {'#':>3} {'Top Skills'}")
        print(f"    {'-'*100}")
        for company, title, noc, sen, skills_cnt, top_skills in cur.fetchall():
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

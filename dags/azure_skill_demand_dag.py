"""
Azure Skill Demand Pipeline — Airflow DAG (Databricks).

Azure cloud version of skill_demand_dag.py.
Uses Azure Databricks instead of local Docker Spark + Livy.

Key differences from local version:
    - ensure_db → EmptyOperator (Neon DB, always available)
    - start_spark → Databricks workspace connectivity check
    - step2/step3 → DatabricksSubmitRunOperator (auto cluster create/destroy)
    - Worker scaling → Databricks job cluster num_workers param
    - stop_spark → EmptyOperator (job clusters auto-terminate)
    - Data paths → Azure Blob Storage (wasbs://)

Schedule: Manual trigger only
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param

# Pipeline code mounted at /opt/airflow/pipelines/skill-demand/
PIPELINE_DIR = "/opt/airflow/pipelines/skill-demand"
sys.path.insert(0, PIPELINE_DIR)

# Config — Azure
INFRA_DIR = "/opt/airflow/infra"
RAW_DIR = "/opt/airflow/data/raw/skill-demand"
PROCESSED_DIR = "/opt/airflow/data/processed/skill-demand"

# Databricks
DATABRICKS_CONN_ID = os.environ.get("DATABRICKS_CONN_ID", "databricks_default")
DBFS_PIPELINE_DIR = os.environ.get("DBFS_PIPELINE_DIR", "dbfs:/giljobi/pipelines/skill-demand")
BLOB_BASE_PATH = os.environ.get(
    "BLOB_BASE_PATH",
    "wasbs://pipeline-data@giljobistorage.blob.core.windows.net"
)

# Neon DB — always external, no local Docker DB
SD_DB_CONN = os.environ.get(
    "SKILL_DEMAND_DB_CONN",
    ""  # must be set via env var (Neon URL)
)
SD_DB_CONN_NEON = os.environ.get("SKILL_DEMAND_DB_CONN_NEON", "")


def _sd_targets():
    """Active DB load targets. In Azure mode, primary is Neon."""
    targets = []
    if SD_DB_CONN:
        targets.append(("neon-primary", SD_DB_CONN))
    if SD_DB_CONN_NEON:
        targets.append(("neon-secondary", SD_DB_CONN_NEON))
    return targets


default_args = {
    "owner": "giljobi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="azure_skill_demand_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["skill-demand", "etl", "llm", "databricks", "azure"],
    default_args=default_args,
    params={
        # LLM session control (Step 3)
        "batch_delay_sec": Param(5, type="integer", description="Seconds between LLM batches"),
        "max_batches_per_session": Param(800, type="integer", description="Max LLM calls before cooldown"),
        "session_cooldown_min": Param(90, type="integer", description="Minutes to wait for session reset"),
        "max_sessions": Param(0, type="integer", description="Max session cycles. 0 = all"),
        # Step 2
        "step2_input_file": Param("", type="string", description="REQUIRED — e.g. step1_extracted.parquet (full) or step1_extracted_sample.parquet (test)"),
        "step2_noc_similarity_threshold": Param(0.65, type="number", description="Cosine similarity threshold"),
        # Databricks cluster sizing — Step 2 (memory-heavy: model loading)
        "step2_num_workers": Param(4, type="integer", description="Databricks workers for Step 2"),
        "step2_executor_memory": Param("2g", type="string", description="Step 2 executor memory"),
        "step2_partitions": Param(16, type="integer", description="Step 2 partition count"),
        # Databricks cluster sizing — Step 3 (IO-heavy: LLM calls)
        "step3_num_workers": Param(8, type="integer", description="Databricks workers for Step 3"),
        "step3_executor_memory": Param("512m", type="string", description="Step 3 executor memory"),
    },
    doc_md="""
    ## Azure Skill Demand Pipeline (Databricks)

    Cloud version — uses Azure Databricks for Spark processing.
    Neon DB for storage, Azure Blob for intermediate data.

    **Infrastructure:**
    - Spark: Azure Databricks (job clusters — auto create/destroy per step)
    - DB: Neon PostgreSQL (external, always available)
    - Storage: Azure Blob (wasbs://)

    **Same pipeline logic as local version, different execution engine.**
    """,
) as dag:

    # =========================================================================
    # Validate Required Params
    # =========================================================================

    @task(task_id="az_validate_params", task_display_name="Validate Params")
    def validate_params(**context):
        """Fail immediately if required params are missing."""
        params = context["params"]
        input_file = params.get("step2_input_file", "").strip()
        if not input_file:
            raise ValueError(
                "step2_input_file is REQUIRED. "
                "Set to 'step1_extracted_sample.parquet' (test) or 'step1_extracted.parquet' (production)."
            )
        print(f"[AZ:SD:PARAMS] === Trigger Configuration ===")
        print(f"  Environment: AZURE (Databricks)")
        for key, value in sorted(params.items()):
            print(f"  {key}: {value}")

        targets = _sd_targets()
        print(f"\n[AZ:SD:PARAMS] === DB Load Targets ({len(targets)}) ===")
        if not targets:
            raise ValueError("No DB targets configured. Set SKILL_DEMAND_DB_CONN env var.")
        for label, conn_str in targets:
            safe = conn_str
            if "://" in conn_str and "@" in conn_str:
                scheme, rest = conn_str.split("://", 1)
                if ":" in rest.split("@")[0]:
                    user = rest.split(":")[0]
                    host = rest.split("@", 1)[1]
                    safe = f"{scheme}://{user}:***@{host}"
            print(f"  [{label}] {safe}")

    # =========================================================================
    # Infrastructure — Check Blob Storage
    # =========================================================================

    @task(task_id="az_check_storage", task_display_name="Check Blob Storage")
    def check_blob_storage():
        """Verify Azure Blob Storage is accessible and pipeline-data container exists."""
        from azure.storage.blob import BlobServiceClient

        conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
        if not conn_str:
            print("[AZ:SD:INFRA] AZURE_STORAGE_CONNECTION_STRING not set — using local storage fallback")
            return

        client = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client("pipeline-data")
        props = container.get_container_properties()
        print(f"[AZ:SD:INFRA] Blob Storage connected.")
        print(f"  Container: pipeline-data")
        print(f"  Last modified: {props.last_modified}")

    # =========================================================================
    # Ping DB Targets
    # =========================================================================

    @task(task_id="az_ping_targets", task_display_name="Ping DB Targets")
    def ping_targets():
        """Smoke test all DB targets."""
        import psycopg2
        targets = _sd_targets()
        print(f"[AZ:SD:INFRA] Testing {len(targets)} DB target(s)...")
        for label, conn_str in targets:
            safe = conn_str
            if "://" in conn_str and "@" in conn_str:
                scheme, rest = conn_str.split("://", 1)
                if ":" in rest.split("@")[0]:
                    user = rest.split(":")[0]
                    host = rest.split("@", 1)[1]
                    safe = f"{scheme}://{user}:***@{host}"
            try:
                conn = psycopg2.connect(conn_str, connect_timeout=10)
                cur = conn.cursor()
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                cur.close()
                conn.close()
                print(f"  [{label}] OK — {safe}")
                print(f"    version: {version[:80]}")
            except Exception as e:
                print(f"  [{label}] FAIL — {safe}")
                print(f"    error: {e}")
                raise

    # =========================================================================
    # Infrastructure — Databricks Connectivity Check
    # =========================================================================

    @task(task_id="az_check_databricks", task_display_name="Check Databricks")
    def check_databricks():
        """Verify Databricks workspace is reachable via REST API."""
        from airflow.providers.databricks.hooks.databricks import DatabricksHook
        hook = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)
        # GET /api/2.0/clusters/list — verify workspace connectivity
        response = hook._do_api_call(("GET", "2.0/clusters/list"), {})
        clusters = response.get("clusters", [])
        print(f"[AZ:SD:INFRA] Databricks workspace connected.")
        print(f"  Active clusters: {len(clusters)}")

    # =========================================================================
    # NOC Setup
    # =========================================================================

    @task(task_id="az_load_noc", task_display_name="Seed NOC Codes")
    def noc_setup():
        """Download NOC 2021 master CSV and load into Neon DB."""
        import pandas as pd
        import psycopg2

        NOC_URL = (
            "https://www.statcan.gc.ca/en/subjects/standard/noc/2021/"
            "indexV1/noc-2021-v1.0-classification-structure.csv"
        )

        df = pd.read_csv(NOC_URL)
        unit_groups = df[df["Level"] == 5].reset_index(drop=True)
        noc = unit_groups[["Code - NOC 2021 V1.0", "Class title"]].copy()
        noc = noc.rename(columns={
            "Code - NOC 2021 V1.0": "noc21_code",
            "Class title": "noc21_name",
        })
        noc = noc.dropna(subset=["noc21_code"])

        for label, conn_str in _sd_targets():
            conn = psycopg2.connect(conn_str)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS noc_titles (
                    id SERIAL PRIMARY KEY,
                    noc21_code VARCHAR(10) UNIQUE NOT NULL,
                    noc21_name VARCHAR(200)
                )
            """)
            cur.execute("SELECT COUNT(*) FROM noc_titles")
            count = cur.fetchone()[0]
            if count > 0:
                print(f"[AZ:SD:INFRA] {label}: already populated ({count} titles). Skipping.")
                cur.close()
                conn.close()
                continue

            for _, row in noc.iterrows():
                cur.execute(
                    "INSERT INTO noc_titles (noc21_code, noc21_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (str(row["noc21_code"]), row["noc21_name"]),
                )
            cur.close()
            conn.close()
            print(f"[AZ:SD:INFRA] {label}: loaded {len(noc)} unit group titles.")

    # =========================================================================
    # Download
    # =========================================================================

    @task(task_id="az_download", task_display_name="Download Dataset")
    def download():
        """Download LinkedIn Job Postings from Kaggle API."""
        from pathlib import Path
        from src.download import download_dataset
        output_dir = download_dataset(output_dir=Path(RAW_DIR))
        return str(output_dir)

    # =========================================================================
    # V1 — Post-Download Validation
    # =========================================================================

    @task(task_id="az_validate_source", task_display_name="Validate Source")
    def validate_v1(raw_dir: str):
        """Validate downloaded CSV."""
        import os
        from src.validators.v1_download import validate_download
        csv_path = os.path.join(raw_dir, "postings.csv")
        result = validate_download(csv_path)
        if not result["valid"]:
            raise ValueError(f"V1 validation failed: {result['error']}")
        print(f"[AZ:SD:V1] Valid. {result['rows']} rows found.")
        return csv_path

    # =========================================================================
    # Step 1 — Column Extraction (runs on Airflow worker, no Spark)
    # =========================================================================

    @task(task_id="az_step1", task_display_name="Step 1: Extract Columns")
    def step1_extract(csv_path: str):
        """Extract columns → parquet."""
        from pathlib import Path
        from src.step1_select_columns import run
        output_path = run(csv_path, output_dir=Path(PROCESSED_DIR) / "step1")
        print(f"[AZ:SD:STEP1] Output: {output_path}")
        return output_path

    # =========================================================================
    # Step 1 Review + Step 2 Prep (V2 validation, no worker scaling)
    # =========================================================================

    @task(task_id="az_bridge_12", task_display_name="Step 1→2 Bridge")
    def step1_review_step2_prep(parquet_path: str, **context):
        """Review Step 1 output + validate. No worker scaling — Databricks handles it."""
        import pandas as pd
        from src.validators.v2_extract import validate_extract

        params = context["params"]
        df = pd.read_parquet(parquet_path)
        cleaned = validate_extract(df)
        cleaned.to_parquet(parquet_path, index=False)
        dropped = len(df) - len(cleaned)

        print(f"[AZ:SD:STEP1:REVIEW]")
        print(f"  Rows: {len(df):,} extracted, {dropped:,} dropped → {len(cleaned):,} kept")
        print(f"[AZ:SD:STEP2:PREP]")
        print(f"  Databricks workers: {params['step2_num_workers']}, Memory: {params['step2_executor_memory']}")
        print(f"  Partitions: {params['step2_partitions']}, Threshold: {params['step2_noc_similarity_threshold']}")
        print(f"  Input: {params['step2_input_file']}")
        return parquet_path

    # =========================================================================
    # Step 2 — NOC Normalize (Databricks)
    # =========================================================================

    # Driver: Standard_DS2_v2 (2 vCPU, 7GB) + Workers: Standard_DS1_v2 (1 vCPU, 3.5GB)
    # Canada Central quota: 6 vCPU → driver(2) + workers(4×1) = 6 max
    # Note: executor_memory is NOT configurable on Databricks — node_type determines it
    step2_noc = DatabricksSubmitRunOperator(
        task_id="az_step2",
        task_display_name="Step 2: NOC Match (ST) — Databricks",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster={
            "spark_version": "14.3.x-scala2.12",
            "driver_node_type_id": "Standard_DS2_v2",
            "node_type_id": "Standard_DS1_v2",
            "num_workers": "{{ params.step2_num_workers }}",
        },
        spark_python_task={
            "python_file": f"{DBFS_PIPELINE_DIR}/spark/step2_noc_spark.py",
            "parameters": [
                "--threshold", "{{ params.step2_noc_similarity_threshold }}",
                "--input", f"{BLOB_BASE_PATH}/processed/skill-demand/step1/{{{{ params.step2_input_file }}}}",
                "--partitions", "{{ params.step2_partitions }}",
            ],
        },
        doc_md="""
### Step 2: NOC Match (Sentence Transformers) — Databricks
Databricks job cluster auto-created with DAG param worker count.
Cluster auto-terminates after job completion.
Same Spark code as local — paths use wasbs:// (Blob Storage).
""",
    )

    # =========================================================================
    # Step 2 Review + Step 3 Prep
    # =========================================================================

    @task(task_id="az_bridge_23", task_display_name="Step 2→3 Bridge")
    def step2_review_step3_prep(**context):
        """Review Step 2 + grouping preview. No worker scaling — Databricks handles it."""
        import pandas as pd
        from src.validators.v3_normalize import validate_normalize

        params = context["params"]
        df = pd.read_parquet(f"{PROCESSED_DIR}/step2/step2_normalized.parquet")
        stats = validate_normalize(df)

        print(f"[AZ:SD:STEP2:REVIEW]")
        print(f"  Total: {len(df):,} rows")
        print(f"  NOC matched: {stats['matched']:,} ({stats['match_rate']:.1%}), avg score: {stats['avg_score']:.3f}")
        print(f"  NOC unmatched: {stats['unmatched']:,} → LLM")
        print(f"[AZ:SD:STEP3:PREP]")
        print(f"  Databricks workers: {params['step3_num_workers']}, Memory: {params['step3_executor_memory']}")

    # =========================================================================
    # Step 3 — LLM Enrich (Databricks)
    # =========================================================================

    # Same cluster config as Step 2 — quota limit 6 vCPU
    step3_enrich = DatabricksSubmitRunOperator(
        task_id="az_step3",
        task_display_name="Step 3: LLM Enrich — Databricks",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster={
            "spark_version": "14.3.x-scala2.12",
            "driver_node_type_id": "Standard_DS2_v2",
            "node_type_id": "Standard_DS1_v2",
            "num_workers": "{{ params.step3_num_workers }}",
        },
        spark_python_task={
            "python_file": f"{DBFS_PIPELINE_DIR}/spark/step3_enrich_spark.py",
            "parameters": [
                "--batch-delay", "{{ params.batch_delay_sec }}",
                "--max-batches-per-session", "{{ params.max_batches_per_session }}",
                "--session-cooldown-min", "{{ params.session_cooldown_min }}",
                "--max-sessions", "{{ params.max_sessions }}",
            ],
        },
        doc_md="""
### Step 3: LLM Enrich — Databricks
Databricks job cluster with more workers (IO-heavy LLM calls).
Same adaptive batch + checkpoint logic as local.
Claude CLI must be available on Databricks worker nodes.
""",
    )

    # =========================================================================
    # Step 3 Review + Step 4 Prep
    # =========================================================================

    @task(task_id="az_bridge_34", task_display_name="Step 3→4 Bridge")
    def step3_review_step4_prep():
        """Review Step 3 results + validate before DB load."""
        import pandas as pd
        from src.validators.v4_enrich import validate_enrich

        df = pd.read_parquet(f"{PROCESSED_DIR}/step3/step3_enriched.parquet")
        stats = validate_enrich(df)

        print(f"[AZ:SD:STEP3:REVIEW]")
        print(f"  Total: {stats['total']:,} rows")
        print(f"  NOC: {stats['noc_mapped']:,}/{stats['total']:,} ({stats['noc_completion_rate']:.1%})")
        print(f"  Seniority: {stats['valid_seniority']:,} valid, {stats['null_seniority']:,} null")
        print(f"  Skills: {stats['with_skills']:,}/{stats['total']:,}, avg {stats['avg_skills_per_posting']:.1f}/posting")

        # Unique job_id check
        total = len(df)
        unique = df["job_id"].nunique()
        dupes = total - unique
        if dupes > 0:
            raise Exception(f"Step 3 output has {dupes:,} duplicate job_ids.")
        print(f"  job_id uniqueness: {unique:,}/{total:,} (100%)")

        # Quality filter preview
        has_noc = df["noc_id"].notna()
        has_skills = df["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0).gt(0)
        has_seniority = df["seniority"].notna() & ~df["seniority"].isin(["NaN", "nan", "None", ""])
        complete = has_noc & has_skills & has_seniority
        excluded = total - int(complete.sum())
        print(f"  Quality filter: {excluded:,} excluded, {int(complete.sum()):,} for DB load")
        return stats

    # =========================================================================
    # Step 4 — DB Load (same as local — runs on Airflow worker, not Spark)
    # =========================================================================

    @task(task_id="az_step4", task_display_name="Step 4: DB Load")
    def step4_load():
        """Load enriched data into Neon DB (Star Schema)."""
        import pandas as pd
        import psycopg2
        from src.step4_load import (
            explode_skills, normalize_skills, drop_star_schema,
            load_dim_seniority, load_dim_companies, load_dim_skills,
            load_fact_postings, load_fact_skills,
        )

        input_path = f"{PROCESSED_DIR}/step3/step3_enriched.parquet"
        df_raw = pd.read_parquet(input_path)
        has_noc = df_raw["noc_id"].notna()
        has_skills = df_raw["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0).gt(0)
        has_seniority = df_raw["seniority"].notna() & ~df_raw["seniority"].isin(["NaN", "nan", "None", ""])
        df = df_raw[has_noc & has_skills & has_seniority].copy()
        print(f"[AZ:SD:STEP4] Input: {len(df_raw):,} total, {len(df):,} after quality filter")

        raw_skill_rows = explode_skills(df)
        skill_rows, dim_skills_dict, norm_stats = normalize_skills(raw_skill_rows)
        companies = df["company_name"].dropna().unique().tolist()
        schema_sql = open(f"{PIPELINE_DIR}/schema.sql").read()

        for label, conn_str in _sd_targets():
            print(f"\n[AZ:SD:STEP4] Loading into {label.upper()}")
            conn = psycopg2.connect(conn_str)
            conn.autocommit = True
            try:
                drop_star_schema(conn)
                cur = conn.cursor()
                cur.execute(schema_sql)
                cur.close()

                seniority_map = load_dim_seniority(conn)
                company_map = load_dim_companies(companies, conn)
                skill_map = load_dim_skills(dim_skills_dict, conn)
                count_postings = load_fact_postings(df, company_map, seniority_map, conn)
                count_skills = load_fact_skills(skill_rows, skill_map, conn)

                print(f"  fact_job_postings: {count_postings:,}")
                print(f"  fact_job_skill_demand: {count_skills:,}")
                print(f"  dim_skills: {len(skill_map):,}")
                print(f"  dim_companies: {len(company_map):,}")
            finally:
                conn.close()

    # =========================================================================
    # Final Summary
    # =========================================================================

    @task(task_id="az_final", task_display_name="Pipeline Complete: Final Summary")
    def final_summary():
        """Final pipeline summary from Neon DB."""
        import psycopg2

        for label, conn_str in _sd_targets():
            conn = psycopg2.connect(conn_str)
            cur = conn.cursor()

            cur.execute("SELECT COUNT(*) FROM fact_job_postings")
            db_postings = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM fact_job_skill_demand")
            db_skills = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dim_skills")
            db_dim_skills = cur.fetchone()[0]

            print(f"\n{'='*60}")
            print(f"  AZ:SD:FINAL — PIPELINE COMPLETE ({label.upper()})")
            print(f"{'='*60}")
            print(f"  fact_job_postings:     {db_postings:,}")
            print(f"  fact_job_skill_demand: {db_skills:,}")
            print(f"  dim_skills:            {db_dim_skills:,}")

            cur.execute("""
                SELECT sk.name, sk.category, COUNT(*) as cnt
                FROM fact_job_skill_demand f
                JOIN dim_skills sk ON f.skill_id = sk.id
                GROUP BY sk.name, sk.category
                ORDER BY cnt DESC LIMIT 10
            """)
            print(f"\n  Top 10 Skills:")
            for skill, cat, cnt in cur.fetchall():
                print(f"    {skill:<30} {cat:<15} {cnt:>6,}")

            print(f"\n{'='*60}")
            cur.close()
            conn.close()

    # =========================================================================
    # Infrastructure — Stop Spark (no-op, Databricks auto-terminates)
    # =========================================================================

    stop_spark = EmptyOperator(
        task_id="az_stop_spark",
        task_display_name="Stop Spark (auto — Databricks)",
        doc_md="No-op. Databricks job clusters auto-terminate after job completion.",
        trigger_rule="all_done",
    )

    # =========================================================================
    # DAG Dependency Chain
    # =========================================================================
    #
    #   validate_params → [ensure_db, check_databricks]
    #   ensure_db → ping_targets → noc_setup
    #   [noc_setup, check_databricks] → download → v1 → step1
    #   → bridge_12 → step2 (Databricks) → bridge_23 → step3 (Databricks)
    #   → bridge_34 → step4 (DB load) → final_summary
    #   step3 → stop_spark (no-op)
    #

    vp = validate_params()
    ping = ping_targets()
    check_db = check_databricks()
    check_blob = check_blob_storage()
    noc = noc_setup()
    dl = download()
    v1 = validate_v1(dl)
    s1 = step1_extract(v1)
    bridge_12 = step1_review_step2_prep(s1)
    bridge_23 = step2_review_step3_prep()
    bridge_34 = step3_review_step4_prep()
    s4 = step4_load()
    final = final_summary()

    #   validate_params → [check_blob, check_databricks, ping_targets]
    #   ping_targets → noc_setup
    #   [noc, check_db, check_blob] → download → v1 → step1
    #   → bridge_12 → step2 (Databricks) → bridge_23 → step3 (Databricks)
    #   → bridge_34 → step4 (DB load) → final_summary
    #   step3 → stop_spark (no-op)

    vp >> [check_blob, check_db, ping]
    ping >> noc
    [noc, check_db, check_blob] >> dl >> v1 >> s1
    s1 >> bridge_12 >> step2_noc >> bridge_23 >> step3_enrich
    step3_enrich >> bridge_34 >> s4 >> final
    step3_enrich >> stop_spark

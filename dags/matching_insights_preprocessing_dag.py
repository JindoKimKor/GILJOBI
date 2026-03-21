"""
Matching Insights Pre-processing Pipeline — Airflow DAG.

Orchestrates the full pipeline lifecycle including infrastructure:
    [START_DB] → [START_SPARK] → Step 1 → Step 2 → Step 3 → [STOP_SPARK] → [STOP_DB]

Infrastructure management is conditional based on environment config:
    MANAGE_PIPELINE_DB=true  → DAG starts/stops PostgreSQL container
    MANAGE_SPARK=true        → DAG starts/stops Spark/Livy containers

Steps 4-5 (LLM normalization, accuracy evaluation) run via their own
standalone docker-compose files in pipelines/matching-insights/.

Schedule: Manual trigger only (one-time dataset processing)

Migrated from: pipelines/matching-insights/dags/data_pre_processing.py
"""

import os
import time

from airflow import DAG
from airflow.providers.apache.livy.hooks.livy import LivyHook
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime

# Config from environment (.env via docker-compose)
MANAGE_DB = os.environ.get("MANAGE_PIPELINE_DB", "true").lower() == "true"
MANAGE_SPARK = os.environ.get("MANAGE_SPARK", "true").lower() == "true"
PIPELINE_DIR = "/opt/spark/notebooks"
INFRA_DIR = "/opt/airflow/infra"

with DAG(
    dag_id="matching_insights_preprocessing",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["matching-insights", "pre-processing"],
    params={
        "enable_partial_match": Param(True, type="boolean"),
    },
    doc_md="""
    ## Matching Insights Pre-processing

    Full lifecycle pipeline with conditional infrastructure management.

    **Infrastructure:** PostgreSQL and Spark/Livy are started/stopped based on config.

    **Stages:** [START_DB] → [START_SPARK] → Step 1-3 → [STOP_SPARK] → [STOP_DB]

    **Steps:**
    1. Column selection + null filtering
    2. Seniority extraction (regex UDF)
    3. Rule-based O*NET primary tag mapping

    **Trigger:** Manual only (one-time dataset processing)
    """,
) as dag:

    # =========================================================================
    # Infrastructure — Ensure DB Running
    # =========================================================================
    # Checks if pipeline DB is running. If not, starts it via up.sh.
    # Does NOT stop DB after pipeline — data stays accessible.

    if MANAGE_DB:
        ensure_db = BashOperator(
            task_id="ensure_db",
            bash_command=(
                f"docker ps --filter name=market-trend-db --filter status=running -q | grep -q . "
                f"&& echo 'DB already running' "
                f"|| (cd {INFRA_DIR} && bash up.sh postgres 2>&1)"
            ),
        )
    else:
        ensure_db = EmptyOperator(task_id="ensure_db")

    # =========================================================================
    # Infrastructure — Start Spark (on-demand)
    # =========================================================================

    if MANAGE_SPARK:
        start_spark = BashOperator(
            task_id="start_spark",
            bash_command=f"cd {INFRA_DIR} && bash up.sh spark 2>&1",
        )
    else:
        start_spark = EmptyOperator(task_id="start_spark")

    # =========================================================================
    # Pipeline — Pre-processing Steps
    # =========================================================================

    step1 = LivyOperator(
        task_id="step1_select_columns",
        file=f"{PIPELINE_DIR}/step1_select_colums.py",
        livy_conn_id="livy_default",
        polling_interval=15,
    )

    step2 = LivyOperator(
        task_id="step2_extract_seniority",
        file=f"{PIPELINE_DIR}/step2_extract_seniority.py",
        livy_conn_id="livy_default",
        polling_interval=15,
    )

    @task
    def step3_rule_based_tagging(**context):
        """Step 3: Rule-based primary tag with optional partial matching."""
        enable = context["params"].get("enable_partial_match", True)
        args = ["--enable-partial-match"] if enable else []

        hook = LivyHook(livy_conn_id="livy_default")
        batch_id = hook.post_batch(
            file=f"{PIPELINE_DIR}/step3_rule_based_primary_tag.py",
            args=args,
        )

        while True:
            state = hook.get_batch_state(batch_id)
            if state in hook.TERMINAL_STATES:
                break
            time.sleep(15)

        state_str = str(state).lower()
        if "success" not in state_str:
            raise Exception(f"Step 3 failed with state: {state}")

    # =========================================================================
    # Infrastructure — Stop Spark (on-demand cleanup)
    # =========================================================================

    if MANAGE_SPARK:
        stop_spark = BashOperator(
            task_id="stop_spark",
            bash_command=f"cd {INFRA_DIR} && bash down.sh spark 2>&1",
            trigger_rule="all_done",
        )
    else:
        stop_spark = EmptyOperator(task_id="stop_spark", trigger_rule="all_done")

    # =========================================================================
    # DAG Dependency Chain
    # =========================================================================
    #   ensure_db → start_spark → step1 → step2 → step3 → stop_spark

    step3 = step3_rule_based_tagging()

    ensure_db >> start_spark >> step1 >> step2 >> step3 >> stop_spark

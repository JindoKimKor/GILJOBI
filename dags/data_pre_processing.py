import time

from airflow import DAG
from airflow.providers.apache.livy.hooks.livy import LivyHook
from airflow.providers.apache.livy.operators.livy import LivyOperator
from datetime import datetime
from airflow.decorators import task
from airflow.models.param import Param

PIPELINE_DIR = "/opt/spark/notebooks"

with DAG(
    dag_id="pre_processing_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    tags=["pre-processing"],
    params={
        "enable_partial_match": Param(True, type="boolean")
    },
) as dag:

    t1 = LivyOperator(
        task_id="step1_extract",
        file=f"{PIPELINE_DIR}/step1_select_colums.py",
        livy_conn_id="livy_default",
        polling_interval=15,
    )
    t2 = LivyOperator(
        task_id="step2_clean",
        file=f"{PIPELINE_DIR}/step2_extract_seniority.py",
        livy_conn_id="livy_default",
        polling_interval=15,
    )

    @task
    def step3_normalize(**context):
        enable = context["params"].get("enable_partial_match", True)
        args = ["--enable-partial-match"] if enable else []

        hook = LivyHook(livy_conn_id="livy_default")
        batch_id = hook.post_batch(
            file=f"{PIPELINE_DIR}/step3_rule_based_primary_tag.py",
            args=args
        )

        while True:
            state = hook.get_batch_state(batch_id)
            print(f"Current state: {state}, type: {type(state)}")
            if state in hook.TERMINAL_STATES:
                break
            time.sleep(15)

        state_str = str(state).lower()
        if "success" not in state_str:
            raise Exception(f"Step3 failed with state: {state}")
        
    t1 >> t2 >> step3_normalize()
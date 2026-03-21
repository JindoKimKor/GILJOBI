"""
Market Trend Pipeline — Airflow DAG.

Orchestrates the full pipeline lifecycle including infrastructure:
    [START_DB] → NOC_SETUP + (SCRAPE → DOWNLOAD → VALIDATE) → TRANSFORM → LOAD → [STOP_DB]

Infrastructure management is conditional based on environment config:
    MANAGE_PIPELINE_DB=true  → DAG starts/stops PostgreSQL container
    MANAGE_PIPELINE_DB=false → External DB (Neon), no container management

Schedule: @monthly (Job Bank releases data monthly)
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Pipeline code is mounted at /opt/airflow/pipelines/market-trend/
PIPELINE_DIR = "/opt/airflow/pipelines/market-trend"
sys.path.insert(0, PIPELINE_DIR)

# Config from environment (.env via docker-compose)
DB_CONN = os.environ.get("PIPELINE_DB_CONN", "postgresql://postgres:postgres@market-trend-db:5432/giljobi")
MANAGE_DB = os.environ.get("MANAGE_PIPELINE_DB", "true").lower() == "true"
RAW_DIR = "/opt/airflow/data/raw/market-trend"
INFRA_DIR = "/opt/airflow/infra"

default_args = {
    "owner": "giljobi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_trend_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["market-trend", "etl"],
    default_args=default_args,
    doc_md="""
    ## Market Trend Pipeline

    Full lifecycle ETL from Canada Job Bank Open Data Portal to PostgreSQL.

    **Infrastructure:** PostgreSQL is started/stopped automatically when
    `MANAGE_PIPELINE_DB=true` (local Docker). Skipped for external DB (Neon).

    **Stages:** [START_DB] → SCRAPE → DOWNLOAD → VALIDATE → TRANSFORM → LOAD → [STOP_DB]

    **Source:** [Job Bank Open Data](https://open.canada.ca/data/en/dataset/ea639e28-c0fc-48bf-b5dd-b8899bd43072)

    **Data:** ~3.3M job postings, ~44K rows/month, Jan 2023 ~ present
    """,
) as dag:

    # =========================================================================
    # Infrastructure — Ensure DB Running
    # =========================================================================
    # Checks if pipeline DB is running. If not, starts it via up.sh.
    # Does NOT stop DB after pipeline — data stays accessible for backend.

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
    # Pipeline — ETL Stages
    # =========================================================================

    @task
    def noc_setup():
        """Pre-requisite: Load NOC 2021 titles into database."""
        import pandas as pd
        from scripts.noc_setup import NOC_MASTER_URL, prepare_noc_titles
        from src.load import get_connection, load_noc_titles

        conn = get_connection(DB_CONN)
        try:
            noc_df = pd.read_csv(NOC_MASTER_URL)
            noc_titles = prepare_noc_titles(noc_df)
            count = load_noc_titles(noc_titles, conn)
            return {"noc_count": count}
        finally:
            conn.close()

    @task
    def scrape():
        """Stage 1: Extract CSV download URLs from CKAN API."""
        import urllib.request
        from src.scrape import API_URL, extract_csv_urls, parse_year_month

        with urllib.request.urlopen(API_URL) as resp:
            api_response = resp.read().decode("utf-8")

        urls = extract_csv_urls(api_response)
        url_map = {}
        for url in urls:
            year_month = parse_year_month(url)
            url_map[year_month] = url

        return url_map

    @task
    def download(url_map: dict):
        """Stage 2: Download new CSVs (skips existing files)."""
        import urllib.request
        from src.download import build_download_plan, save_csv

        os.makedirs(RAW_DIR, exist_ok=True)
        plan = build_download_plan(url_map, RAW_DIR)

        for ym, url in plan.items():
            with urllib.request.urlopen(url) as resp:
                content = resp.read()
            save_csv(content, ym, RAW_DIR)

        return {"downloaded": len(plan), "skipped": len(url_map) - len(plan)}

    @task
    def validate():
        """Stage 3: Validate all CSVs — detect encoding, check columns."""
        from src.validate import validate_csv

        csv_files = sorted(f for f in os.listdir(RAW_DIR) if f.endswith(".csv"))
        valid_files = []

        for filename in csv_files:
            filepath = os.path.join(RAW_DIR, filename)
            result = validate_csv(filepath)
            if result["valid"]:
                valid_files.append({
                    "filepath": filepath,
                    "encoding": result["encoding"],
                    "sep": result["sep"],
                })

        return valid_files

    @task
    def transform(valid_files: list):
        """Stage 4: Normalize salaries, filter outliers, map NOC codes."""
        import pandas as pd
        from src.load import get_connection
        from src.transform import (
            normalize_salary_to_hourly,
            apply_outlier_filter,
            map_noc_ids,
            prepare_job_postings,
        )

        conn = get_connection(DB_CONN)
        try:
            cur = conn.cursor()
            cur.execute("SELECT noc21_code, id FROM noc_titles")
            noc_lookup = {code: noc_id for code, noc_id in cur.fetchall()}
            cur.close()
        finally:
            conn.close()

        output_dir = "/opt/airflow/data/processed/market-trend"
        os.makedirs(output_dir, exist_ok=True)

        paths = []
        for file_info in valid_files:
            df = pd.read_csv(
                file_info["filepath"],
                encoding=file_info["encoding"],
                sep=file_info["sep"],
            )
            df = normalize_salary_to_hourly(df)
            df = apply_outlier_filter(df)
            df = map_noc_ids(df, noc_lookup)
            df = prepare_job_postings(df)

            out_path = os.path.join(
                output_dir,
                os.path.basename(file_info["filepath"]).replace(".csv", ".parquet"),
            )
            df.to_parquet(out_path, index=False)
            paths.append(out_path)

        return paths

    @task
    def load(parquet_paths: list):
        """Stage 5: Bulk insert into PostgreSQL via COPY."""
        import pandas as pd
        from src.load import get_connection, load_job_postings

        conn = get_connection(DB_CONN)
        try:
            total = 0
            for path in parquet_paths:
                df = pd.read_parquet(path)
                count = load_job_postings(df, conn)
                total += count
            return {"total_loaded": total}
        finally:
            conn.close()

    # =========================================================================
    # DAG Dependency Chain
    # =========================================================================
    #   ensure_db → noc_setup ──────────────────┐
    #             → scrape → download → validate → transform → load

    noc = noc_setup()
    urls = scrape()
    dl = download(urls)
    valid = validate()
    transformed = transform(valid)
    loaded = load(transformed)

    ensure_db >> [noc, urls]
    noc >> transformed
    dl >> valid

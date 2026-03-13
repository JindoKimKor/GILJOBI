"""
Market Trend Pipeline — End-to-end orchestrator.

Executes the full pipeline in stage order:
    1. SCRAPE    → Extract CSV download URLs from Open Data Portal
    2. DOWNLOAD  → Fetch monthly CSVs in parallel (skips existing files)
    3. VALIDATE  → Check file integrity, separate valid/invalid files
    4. TRANSFORM → Normalize salaries, filter outliers, map NOC codes
    5. LOAD      → Insert job_postings into PostgreSQL

Pre-requisite: NOC titles must be loaded before the pipeline runs.
Use --noc-setup to run only the NOC setup step.

Usage:
    py main.py                          # Run full pipeline (NOC setup + all stages)
    py main.py --noc-setup              # Run NOC setup only
    py main.py --db postgresql://...    # Custom DB connection string
    py main.py --workers 8              # Parallel download threads (default: 5)
"""

import argparse
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

# Add pipeline root to path so imports work when running as script
sys.path.insert(0, os.path.dirname(__file__))

from scripts.noc_setup import NOC_MASTER_URL, prepare_noc_titles
from src.scrape import API_URL, extract_csv_urls, parse_year_month
from src.download import build_download_plan, save_csv
from src.validate import validate_csv
from src.transform import (
    normalize_salary_to_hourly,
    apply_outlier_filter,
    map_noc_ids,
    prepare_job_postings,
)
from src.load import get_connection, load_noc_titles, load_job_postings

# Default PostgreSQL connection — override with DATABASE_URL env var or --db flag.
# Local default matches Docker infra/docker-compose.yml.
DEFAULT_DB = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/giljobi")

# Raw CSV storage directory (relative to repo root)
RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "market-trend")


# ============================================================
# Pre-requisite: NOC Setup
# ============================================================

def run_noc_setup(conn):
    """Download NOC 2021 master CSV and load into noc_titles table.

    Downloads the official Statistics Canada classification CSV,
    filters to Level 5 (Unit Group) rows, and inserts into the
    noc_titles table. Safe for repeated runs — duplicates are skipped.

    Args:
        conn: Active psycopg2 connection.

    Returns:
        DataFrame of NOC titles that were prepared (for logging/debugging).
    """
    print("[NOC SETUP] Downloading NOC 2021 master CSV...")
    noc_df = pd.read_csv(NOC_MASTER_URL)
    noc_titles = prepare_noc_titles(noc_df)
    count = load_noc_titles(noc_titles, conn)
    print(f"[NOC SETUP] {count} NOC titles in DB ({len(noc_titles)} in source CSV).")
    return noc_titles


def build_noc_lookup(conn) -> dict:
    """Query noc_titles table to build a code-to-id lookup dictionary.

    Used by the TRANSFORM stage to map NOC21 codes in job postings
    to the corresponding noc_titles.id foreign key.

    Args:
        conn: Active psycopg2 connection.

    Returns:
        Dictionary mapping {noc21_code: noc_titles.id}.
    """
    cur = conn.cursor()
    cur.execute("SELECT noc21_code, id FROM noc_titles")
    lookup = {code: noc_id for code, noc_id in cur.fetchall()}
    cur.close()
    return lookup


# ============================================================
# Stage 1: SCRAPE
# ============================================================

def stage_scrape() -> dict[str, str]:
    """Scrape the Open Data Portal for CSV download URLs.

    Returns:
        Dictionary mapping {YYYY-MM: download_url} for all available months.
    """
    print("\n[1/5 SCRAPE] Fetching dataset metadata from CKAN API...")
    with urllib.request.urlopen(API_URL) as resp:
        api_response = resp.read().decode("utf-8")

    urls = extract_csv_urls(api_response)
    print(f"[1/5 SCRAPE] Found {len(urls)} English CSV URLs.")

    # Build {YYYY-MM: url} mapping
    url_map = {}
    for url in urls:
        year_month = parse_year_month(url)
        url_map[year_month] = url

    return url_map


# ============================================================
# Stage 2: DOWNLOAD (parallel)
# ============================================================

def _download_one(year_month: str, url: str, output_dir: str) -> str:
    """Download a single CSV file. Used as a thread worker.

    Args:
        year_month: Date identifier (e.g., '2023-01').
        url: Download URL for the CSV file.
        output_dir: Directory to save the file.

    Returns:
        year_month string on success (for progress tracking).
    """
    with urllib.request.urlopen(url) as resp:
        content = resp.read()
    save_csv(content, year_month, output_dir)
    return year_month


def stage_download(url_map: dict[str, str], max_workers: int = 5) -> str:
    """Download all new CSVs in parallel, skipping existing files.

    Args:
        url_map: Mapping of {YYYY-MM: download_url}.
        max_workers: Number of parallel download threads.

    Returns:
        Absolute path to the raw data directory.
    """
    raw_dir = os.path.abspath(RAW_DIR)
    plan = build_download_plan(url_map, raw_dir)

    already_exist = len(url_map) - len(plan)
    print(f"\n[2/5 DOWNLOAD] {len(plan)} new files to download ({already_exist} already exist).")

    if not plan:
        return raw_dir

    # Download in parallel using thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_download_one, ym, url, raw_dir): ym
            for ym, url in plan.items()
        }
        for future in as_completed(futures):
            ym = futures[future]
            try:
                future.result()
                print(f"  Downloaded {ym}")
            except Exception as e:
                print(f"  FAILED {ym}: {e}")

    return raw_dir


# ============================================================
# Stage 3: VALIDATE
# ============================================================

def stage_validate(raw_dir: str) -> list[dict]:
    """Validate all CSV files in the raw directory.

    Args:
        raw_dir: Path to directory containing downloaded CSVs.

    Returns:
        List of dicts with filepath, encoding, and sep for each valid file.
    """
    csv_files = sorted(f for f in os.listdir(raw_dir) if f.endswith(".csv"))
    print(f"\n[3/5 VALIDATE] Checking {len(csv_files)} files...")

    valid_files = []
    for filename in csv_files:
        filepath = os.path.join(raw_dir, filename)
        result = validate_csv(filepath)
        if result["valid"]:
            valid_files.append({
                "filepath": filepath,
                "encoding": result["encoding"],
                "sep": result["sep"],
            })
        else:
            print(f"  SKIP {filename}: {result['error']}")

    print(f"[3/5 VALIDATE] {len(valid_files)} valid, {len(csv_files) - len(valid_files)} skipped.")
    return valid_files


# ============================================================
# Stage 4: TRANSFORM
# ============================================================

def stage_transform(valid_files: list[dict], noc_lookup: dict) -> list[pd.DataFrame]:
    """Transform all validated CSV files into DB-ready DataFrames.

    Applies the full transform chain to each file:
    salary normalization → outlier filter → NOC mapping → column selection.

    Args:
        valid_files: List of dicts with filepath, encoding, and sep
            from the VALIDATE stage.
        noc_lookup: Dictionary mapping {noc21_code: noc_titles.id}.

    Returns:
        List of transformed DataFrames ready for DB insertion.
    """
    print(f"\n[4/5 TRANSFORM] Processing {len(valid_files)} files...")

    transformed = []
    for file_info in valid_files:
        filepath = file_info["filepath"]
        filename = os.path.basename(filepath)
        df = pd.read_csv(filepath, encoding=file_info["encoding"], sep=file_info["sep"])
        df = normalize_salary_to_hourly(df)
        df = apply_outlier_filter(df)
        df = map_noc_ids(df, noc_lookup)
        df = prepare_job_postings(df)
        transformed.append(df)
        print(f"  Transformed {filename}: {len(df)} rows")

    total = sum(len(df) for df in transformed)
    print(f"[4/5 TRANSFORM] {total} total rows ready for loading.")
    return transformed


# ============================================================
# Stage 5: LOAD
# ============================================================

def stage_load(transformed: list[pd.DataFrame], conn) -> int:
    """Insert all transformed DataFrames into the job_postings table.

    Args:
        transformed: List of DataFrames from the TRANSFORM stage.
        conn: Active psycopg2 connection.

    Returns:
        Total number of rows inserted.
    """
    print(f"\n[5/5 LOAD] Inserting into job_postings...")

    total = 0
    for i, df in enumerate(transformed, 1):
        count = load_job_postings(df, conn)
        total += count
        print(f"  Loaded {i}/{len(transformed)}: {count} rows (total: {total})")

    print(f"[5/5 LOAD] {total} rows inserted.")
    return total


# ============================================================
# Main
# ============================================================

def main():
    """Parse arguments and execute the pipeline."""
    parser = argparse.ArgumentParser(description="Market Trend Pipeline")
    parser.add_argument("--db", default=DEFAULT_DB, help="PostgreSQL connection string")
    parser.add_argument("--noc-setup", action="store_true", help="Run NOC setup only")
    parser.add_argument("--workers", type=int, default=5, help="Parallel download threads")
    args = parser.parse_args()

    conn = get_connection(args.db)
    try:
        # Pre-requisite: ensure NOC titles are in DB
        run_noc_setup(conn)

        if args.noc_setup:
            print("\n[DONE] NOC setup complete.")
            return

        # Build NOC lookup for TRANSFORM stage
        noc_lookup = build_noc_lookup(conn)

        # Execute pipeline stages with timing
        pipeline_start = time.time()
        timings = {}

        t = time.time()
        url_map = stage_scrape()
        timings["SCRAPE"] = time.time() - t

        t = time.time()
        raw_dir = stage_download(url_map, max_workers=args.workers)
        timings["DOWNLOAD"] = time.time() - t

        t = time.time()
        valid_files = stage_validate(raw_dir)
        timings["VALIDATE"] = time.time() - t

        t = time.time()
        transformed = stage_transform(valid_files, noc_lookup)
        timings["TRANSFORM"] = time.time() - t

        t = time.time()
        total = stage_load(transformed, conn)
        timings["LOAD"] = time.time() - t

        pipeline_total = time.time() - pipeline_start

        print(f"\n[DONE] Pipeline complete. {total} job postings loaded.")
        print(f"\n--- Timing Summary ---")
        for stage, elapsed in timings.items():
            print(f"  {stage:12s}: {elapsed:6.1f}s")
        print(f"  {'TOTAL':12s}: {pipeline_total:6.1f}s")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

"""
Skill Demand Pipeline — End-to-end orchestrator.

Executes the full pipeline in stage order:
    0. DOWNLOAD  → Fetch LinkedIn Job Postings dataset from Kaggle
    1. EXTRACT   → Select columns (title, description, company) + company JOIN
    2. NORMALIZE → NOC matching via Sentence Transformers (cosine similarity)
    3. FALLBACK  → LLM fallback for sub-threshold NOC matches
    4. ENRICH    → Extract seniority + tech skills via LLM
    5. LOAD      → Insert jd_postings + jd_skills into PostgreSQL

Pre-requisite: noc_titles must already exist in the DB (populated by market-trend pipeline).

Usage:
    py main.py                              # Run full pipeline
    py main.py --db postgresql://...        # Custom DB connection string
    py main.py --download-only              # Download dataset only
    py main.py --force-download             # Re-download even if exists
    py main.py --step 2                     # Run from step 2 onward (skip download + extract)
"""

import argparse
import os
import sys
import time

import psycopg2

# =============================================================================
# Path Setup
# =============================================================================
sys.path.insert(0, os.path.dirname(__file__))

from src.download import download_dataset

# =============================================================================
# Config
# =============================================================================
DEFAULT_DB = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/giljobi"
)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "skill-demand")
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed", "skill-demand")

# =============================================================================
# DB Schema
# =============================================================================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jd_postings (
    id SERIAL PRIMARY KEY,
    company VARCHAR(300),
    raw_title VARCHAR(300) NOT NULL,
    noc_id INT REFERENCES noc_titles(id),
    noc_match_score NUMERIC(4,3),
    noc_match_method VARCHAR(30),
    seniority VARCHAR(50) CHECK (seniority IN (
        'intern', 'entry_level', 'mid_level', 'senior', 'executive'
    )),
    description TEXT
);

CREATE TABLE IF NOT EXISTS jd_skills (
    id SERIAL PRIMARY KEY,
    jd_id INT REFERENCES jd_postings(id),
    skill VARCHAR(100) NOT NULL
);

CREATE OR REPLACE VIEW skill_demand_summary AS
SELECT
    n.noc21_code,
    n.noc21_name,
    p.seniority,
    s.skill,
    COUNT(*) AS demand_count
FROM jd_skills s
JOIN jd_postings p ON s.jd_id = p.id
LEFT JOIN noc_titles n ON p.noc_id = n.id
GROUP BY n.noc21_code, n.noc21_name, p.seniority, s.skill
ORDER BY demand_count DESC;
"""

# =============================================================================
# DB Connection
# =============================================================================
def get_connection(connection_string: str):
    conn = psycopg2.connect(connection_string)
    conn.autocommit = True
    return conn


def ensure_schema(conn):
    """Create tables and views if they don't exist."""
    print("[SCHEMA] Ensuring jd_postings, jd_skills, skill_demand_summary ...")
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    cur.close()
    print("[SCHEMA] Ready.")


def verify_noc_titles(conn):
    """Check that noc_titles table exists and has data."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM noc_titles WHERE LENGTH(noc21_code) = 5")
    count = cur.fetchone()[0]
    cur.close()
    if count == 0:
        raise RuntimeError(
            "noc_titles table is empty. Run market-trend pipeline first: "
            "py pipelines/market-trend/main.py --noc-setup"
        )
    print(f"[NOC] {count} unit group titles available.")
    return count


# =============================================================================
# Stage 0: DOWNLOAD
# =============================================================================
def stage_download(force: bool = False) -> str:
    print("\n[0/5 DOWNLOAD] Kaggle dataset ...")
    from pathlib import Path
    output_dir = download_dataset(output_dir=Path(RAW_DIR).resolve(), force=force)
    return str(output_dir)


# =============================================================================
# Stage 1: EXTRACT
# =============================================================================
def stage_extract(raw_dir: str):
    print("\n[1/5 EXTRACT] Selecting columns + company JOIN ...")
    # TODO: implement src/step1_select_columns.py
    raise NotImplementedError("Step 1 not yet implemented")


# =============================================================================
# Stage 2: NOC NORMALIZE (Sentence Transformers)
# =============================================================================
def stage_noc_normalize():
    print("\n[2/5 NORMALIZE] Sentence Transformers NOC matching ...")
    # TODO: implement src/step2_noc_normalize.py
    raise NotImplementedError("Step 2 not yet implemented")


# =============================================================================
# Stage 3: NOC LLM FALLBACK
# =============================================================================
def stage_noc_fallback():
    print("\n[3/5 FALLBACK] LLM fallback for sub-threshold matches ...")
    # TODO: implement src/step3_noc_llm_fallback.py
    raise NotImplementedError("Step 3 not yet implemented")


# =============================================================================
# Stage 4: ENRICH (LLM seniority + skills)
# =============================================================================
def stage_enrich():
    print("\n[4/5 ENRICH] LLM seniority + skill extraction ...")
    # TODO: implement src/step4_extract.py
    raise NotImplementedError("Step 4 not yet implemented")


# =============================================================================
# Stage 5: LOAD
# =============================================================================
def stage_load(conn):
    print("\n[5/5 LOAD] Inserting into jd_postings + jd_skills ...")
    # TODO: implement src/step5_load.py
    raise NotImplementedError("Step 5 not yet implemented")


# =============================================================================
# Main
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Skill Demand Pipeline")
    parser.add_argument("--db", default=DEFAULT_DB, help="PostgreSQL connection string")
    parser.add_argument("--download-only", action="store_true", help="Download dataset only")
    parser.add_argument("--force-download", action="store_true", help="Re-download even if exists")
    parser.add_argument("--step", type=int, default=0, help="Start from step N (skip earlier steps)")
    args = parser.parse_args()

    # DB setup
    conn = get_connection(args.db)
    try:
        ensure_schema(conn)
        verify_noc_titles(conn)

        pipeline_start = time.time()
        timings = {}

        # Stage 0: Download
        if args.step <= 0:
            t = time.time()
            raw_dir = stage_download(force=args.force_download)
            timings["DOWNLOAD"] = time.time() - t

            if args.download_only:
                print("\n[DONE] Download complete.")
                return
        else:
            raw_dir = os.path.abspath(RAW_DIR)

        # Stage 1: Extract
        if args.step <= 1:
            t = time.time()
            stage_extract(raw_dir)
            timings["EXTRACT"] = time.time() - t

        # Stage 2: NOC Normalize
        if args.step <= 2:
            t = time.time()
            stage_noc_normalize()
            timings["NORMALIZE"] = time.time() - t

        # Stage 3: NOC LLM Fallback
        if args.step <= 3:
            t = time.time()
            stage_noc_fallback()
            timings["FALLBACK"] = time.time() - t

        # Stage 4: Enrich
        if args.step <= 4:
            t = time.time()
            stage_enrich()
            timings["ENRICH"] = time.time() - t

        # Stage 5: Load
        if args.step <= 5:
            t = time.time()
            stage_load(conn)
            timings["LOAD"] = time.time() - t

        pipeline_total = time.time() - pipeline_start

        print(f"\n[DONE] Pipeline complete.")
        print(f"\n--- Timing Summary ---")
        for stage, elapsed in timings.items():
            print(f"  {stage:12s}: {elapsed:6.1f}s")
        print(f"  {'TOTAL':12s}: {pipeline_total:6.1f}s")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

"""
step3_4_llm_spark.py — Combined LLM batch pipeline on Spark.

Submitted by LivyOperator from skill_demand_dag.py.
For each batch:
    1. Step 3: NOC LLM fallback (sub-threshold rows only)
    2. Step 4: Seniority + skills extraction (all rows)
    3. Checkpoint

Rate limited for Claude CLI subscription session limits.
Checkpoint-based resume: completed batches are skipped on restart.

Auth: Host ~/.claude mounted to /tmp/.claude in Spark container.

Usage (from Spark master container):
    /opt/spark/bin/spark-submit step3_4_llm_spark.py [options]
"""

# =============================================================================
# Args
# =============================================================================
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--batch-size", type=int, default=10,
                    help="JDs per LLM call")
parser.add_argument("--batch-delay", type=int, default=5,
                    help="Seconds between batches")
parser.add_argument("--max-batches-per-session", type=int, default=50,
                    help="Max batches per session window")
parser.add_argument("--session-cooldown-min", type=int, default=60,
                    help="Minutes to wait for session reset after hitting limit")
parser.add_argument("--input", type=str,
                    default="/opt/airflow/data/processed/skill-demand/step2/step2_normalized.parquet")
parser.add_argument("--output", type=str,
                    default="/opt/airflow/data/processed/skill-demand/step3_4")
parser.add_argument("--db-conn", type=str,
                    default="postgresql://postgres:postgres@skill-demand-db:5432/giljobi_sd")
args = parser.parse_args()

# =============================================================================
# SparkSession
# =============================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("skill-demand-step3-4-llm-batch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# Load Data
# =============================================================================
import os
import json
import time
import datetime
import pandas as pd
import numpy as np
from pathlib import Path

print(f"[STEP 3+4] Loading input: {args.input}")
df_pd = pd.read_parquet(args.input)
total_rows = len(df_pd)
print(f"[STEP 3+4] {total_rows} rows loaded.")

# Load NOC reference for LLM fallback
noc_pd = pd.read_sql(
    "SELECT id, noc21_code, noc21_name FROM noc_titles WHERE LENGTH(noc21_code) = 5",
    args.db_conn,
)
noc_names = noc_pd["noc21_name"].tolist()
name_to_id = {row["noc21_name"]: row["id"] for _, row in noc_pd.iterrows()}

# =============================================================================
# Checkpoint Setup
# =============================================================================
output_dir = Path(args.output)
checkpoint_dir = output_dir / "checkpoints"
output_dir.mkdir(parents=True, exist_ok=True)
checkpoint_dir.mkdir(parents=True, exist_ok=True)

# Find completed batches
completed_ids = set()
for cp in checkpoint_dir.glob("batch_*.json"):
    completed_ids.add(int(cp.stem.replace("batch_", "")))

# =============================================================================
# Assign batch IDs
# =============================================================================
df_pd["batch_id"] = df_pd.index // args.batch_size
total_batches = int(df_pd["batch_id"].max()) + 1 if len(df_pd) > 0 else 0

# =============================================================================
# Broadcast NOC data
# =============================================================================
noc_names_broadcast = spark.sparkContext.broadcast(noc_names)
name_to_id_broadcast = spark.sparkContext.broadcast(name_to_id)

# =============================================================================
# mapPartitions — combined Step 3 + Step 4 per batch
# =============================================================================
def process_partition(rows):
    """Process one partition: NOC fallback + seniority/skills extraction."""
    import subprocess
    import json
    import re
    import os
    import time
    from itertools import groupby
    from pathlib import Path

    all_rows = sorted(list(rows), key=lambda r: r["batch_id"])
    if not all_rows:
        return

    noc_candidates = noc_names_broadcast.value
    noc_lookup = name_to_id_broadcast.value

    SENIORITY_TIERS = ["intern", "entry_level", "mid_level", "senior", "executive"]

    def call_claude(prompt):
        """Call Claude CLI and return stdout."""
        result = subprocess.run(
            ["claude", "--print", "--model", "haiku", "-"],
            input=prompt,
            capture_output=True,
            text=True,
            env={**os.environ, "HOME": "/tmp"},
        )
        return result.stdout if result.returncode == 0 else ""

    def parse_json(response):
        """Extract JSON from response (handles markdown blocks)."""
        if not response or not response.strip():
            return {}
        cleaned = response.strip()
        md = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', cleaned, re.DOTALL)
        if md:
            cleaned = md.group(1).strip()
        jm = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if not jm:
            return {}
        try:
            return json.loads(jm.group())
        except json.JSONDecodeError:
            return {}

    for batch_id, group in groupby(all_rows, key=lambda r: r["batch_id"]):
        batch = list(group)

        # Check checkpoint
        checkpoint = Path(
            f"/opt/airflow/data/processed/skill-demand/step3_4/checkpoints/batch_{batch_id}.json"
        )
        if checkpoint.exists():
            with open(checkpoint) as f:
                for row in json.load(f):
                    yield row
            continue

        results = []
        for row in batch:
            noc_id = row["noc_id"]
            noc_method = row["noc_match_method"]

            # --- Step 3: NOC fallback (only if unmatched) ---
            if noc_id is None:
                candidates_str = "\n".join(f"- {c}" for c in noc_candidates)
                noc_prompt = f"""Given this job posting, determine the best NOC 2021 category.
If none fit, return null.

Job Title: {row['title']}
Job Description: {str(row.get('description', ''))[:2000]}

Available NOC categories:
{candidates_str}

Return JSON only: {{"noc_title": "matching category or null"}}"""

                noc_response = call_claude(noc_prompt)
                parsed_noc = parse_json(noc_response)
                noc_title = parsed_noc.get("noc_title")
                if noc_title and noc_title in noc_lookup:
                    noc_id = noc_lookup[noc_title]
                    noc_method = "llm"

            # --- Step 4: Seniority + Skills ---
            tiers_str = ", ".join(SENIORITY_TIERS)
            extract_prompt = f"""Analyze this job description and extract:
1. seniority: one of [{tiers_str}]
2. skills: list of technical skills (lowercase)

Job Description: {str(row.get('description', ''))[:3000]}

Return JSON only: {{"seniority": "tier", "skills": ["skill1", "skill2"]}}"""

            extract_response = call_claude(extract_prompt)
            parsed_extract = parse_json(extract_response)

            seniority = parsed_extract.get("seniority")
            if seniority not in SENIORITY_TIERS:
                seniority = None

            skills = parsed_extract.get("skills", [])
            if not isinstance(skills, list):
                skills = []
            skills = [s.lower().strip() for s in skills if isinstance(s, str)]

            results.append({
                "job_id": row["job_id"],
                "company_name": row["company_name"],
                "title": row["title"],
                "description": row.get("description"),
                "noc_id": noc_id,
                "noc_match_score": row.get("noc_match_score"),
                "noc_match_method": noc_method,
                "seniority": seniority,
                "skills": skills,
                "batch_id": batch_id,
            })

        # Save checkpoint
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        with open(checkpoint, "w") as f:
            json.dump(results, f)

        for r in results:
            yield r

        # Rate limit delay
        time.sleep(args.batch_delay)

# =============================================================================
# Execute — Session Loop
# =============================================================================
# Processes max_batches_per_session batches, then waits for cooldown,
# then resumes from checkpoint. Repeats until all batches are done.
# One DAG trigger → fully automatic completion.
# =============================================================================

session_number = 0
pipeline_start = datetime.datetime.now()

while True:
    # Refresh completed checkpoints
    completed_ids = set()
    for cp in checkpoint_dir.glob("batch_*.json"):
        completed_ids.add(int(cp.stem.replace("batch_", "")))

    df_pending = df_pd[~df_pd["batch_id"].isin(completed_ids)].copy()
    remaining_batches = df_pending["batch_id"].nunique()

    print(f"\n[STEP 3+4] === Session {session_number + 1} ===")
    print(f"  Total batches:  {total_batches}")
    print(f"  Completed:      {len(completed_ids)}")
    print(f"  Remaining:      {remaining_batches}")

    if remaining_batches == 0:
        print("[STEP 3+4] All batches completed!")
        break

    # Limit to max_batches_per_session
    max_per_session = args.max_batches_per_session
    if max_per_session > 0 and remaining_batches > max_per_session:
        keep_ids = sorted(df_pending["batch_id"].unique())[:max_per_session]
        df_pending = df_pending[df_pending["batch_id"].isin(keep_ids)].copy()
        print(f"  This session:   {len(keep_ids)} batches (limit: {max_per_session})")

    # Process session
    pending_count = df_pending["batch_id"].nunique()
    df_spark = spark.createDataFrame(df_pending)
    df_spark = df_spark.repartition(pending_count, "batch_id")

    session_start = datetime.datetime.now()
    print(f"  Start:          {session_start.strftime('%H:%M:%S')}")

    result_rdd = df_spark.rdd.mapPartitions(process_partition)
    result_rdd.collect()

    session_elapsed = datetime.datetime.now() - session_start
    session_number += 1
    print(f"  Elapsed:        {session_elapsed}")

    # Check if more batches remain
    completed_after = set()
    for cp in checkpoint_dir.glob("batch_*.json"):
        completed_after.add(int(cp.stem.replace("batch_", "")))
    still_remaining = total_batches - len(completed_after)

    if still_remaining == 0:
        print("[STEP 3+4] All batches completed!")
        break

    # Session cooldown
    cooldown_sec = args.session_cooldown_min * 60
    print(f"\n[STEP 3+4] Session limit reached. {still_remaining} batches remaining.")
    print(f"[STEP 3+4] Waiting {args.session_cooldown_min} min for session reset...")
    time.sleep(cooldown_sec)

pipeline_elapsed = datetime.datetime.now() - pipeline_start
print(f"\n[STEP 3+4] Pipeline complete in {session_number} sessions, {pipeline_elapsed} total.")

# =============================================================================
# Collect checkpoints → save output parquet
# =============================================================================
all_rows = []
for cp in sorted(checkpoint_dir.glob("batch_*.json")):
    with open(cp) as f:
        all_rows.extend(json.load(f))

if all_rows:
    df_result = pd.DataFrame(all_rows)
    output_path = output_dir / "step3_4_enriched.parquet"
    df_result.to_parquet(output_path, index=False)
    print(f"\n[STEP 3+4] === Summary ===")
    print(f"  Total processed: {len(df_result)}")
    print(f"  With NOC: {df_result['noc_id'].notna().sum()}")
    print(f"  With seniority: {df_result['seniority'].notna().sum()}")
    print(f"  Output: {output_path}")
else:
    print("[STEP 3+4] No results to save.")

spark.stop()

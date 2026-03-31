"""
step3_enrich_spark.py — LLM Enrich: NOC + Seniority + Skills on Spark.

Processes ALL rows from Step 2 with 2 prompt types:
  - unmatched (noc_id null): NOC list + JD → NOC + seniority + skills (5/call)
  - matched (noc_id filled): JD only → seniority + skills (10/call)

Adaptive Batch Strategy — similarity-based grouping:
  Matched:   1) same company + same noc_id  2) same noc_id  3) remaining
  Unmatched: 1) same company  2) remaining

Seniority handled per-row (skip if filled, determine if missing).
Skills categorized: hard_skill, soft_skill, tool, certification.

Auth: Host ~/.claude mounted to /tmp/.claude in Spark container.

Usage:
    /opt/spark/bin/spark-submit step3_enrich_spark.py [options]
"""

# =============================================================================
# Args
# =============================================================================
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str,
                    default="/opt/spark/data/processed/skill-demand/step2/step2_normalized.parquet")
parser.add_argument("--output", type=str,
                    default="/opt/spark/data/processed/skill-demand/step3")
parser.add_argument("--db-conn", type=str,
                    default="postgresql://postgres:postgres@skill-demand-db:5432/giljobi_sd")
parser.add_argument("--batch-delay", type=int, default=5)
parser.add_argument("--max-batches-per-session", type=int, default=50)
parser.add_argument("--session-cooldown-min", type=int, default=60)
args = parser.parse_args()

# =============================================================================
# Suppress HF warnings
# =============================================================================
import os
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"

# =============================================================================
# SparkSession
# =============================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("skill-demand-step3-enrich") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# Load Data
# =============================================================================
import pandas as pd
import datetime
import json
import shutil
from pathlib import Path

print(f"[STEP 3] Loading input: {args.input}")
df_all = pd.read_parquet(args.input)
total_rows = len(df_all)

matched = df_all[df_all["noc_id"].notna()].copy()
unmatched = df_all[df_all["noc_id"].isna()].copy()
print(f"[STEP 3] Total: {total_rows}, Matched: {len(matched)}, Unmatched: {len(unmatched)}")

# =============================================================================
# Grouping Preview (driver-side stats before Spark processing)
# =============================================================================
BATCH_UNMATCHED = 5
BATCH_MATCHED = 10

def _simulate_grouping(df, group_cols_list, batch_size):
    """Simulate multi-priority grouping, return stats per priority."""
    remaining = df.copy()
    stats = []
    for priority_name, group_cols in group_cols_list:
        full_rows = 0
        full_batches = 0
        next_remaining = []
        for _, grp in remaining.groupby(group_cols):
            n = len(grp)
            fb = n // batch_size
            full_batches += fb
            full_rows += fb * batch_size
            leftover = n % batch_size
            if leftover > 0:
                next_remaining.append(grp.tail(leftover))
        rem_df = pd.concat(next_remaining) if next_remaining else pd.DataFrame()
        stats.append((priority_name, full_batches, full_rows, len(rem_df)))
        remaining = rem_df
    # Final remainder
    rem_batches = (len(remaining) + batch_size - 1) // batch_size if len(remaining) > 0 else 0
    stats.append(("remaining", rem_batches, len(remaining), 0))
    return stats

m_total_calls = 0
u_total_calls = 0

if len(matched) > 0:
    m_stats = _simulate_grouping(matched, [
        ("P1 company+noc_id", ["company_name", "noc_id"]),
        ("P2 same noc_id", ["noc_id"]),
    ], BATCH_MATCHED)
    print(f"[STEP 3] Matched grouping preview ({len(matched)} rows, batch={BATCH_MATCHED}):")
    for name, batches, rows, rem in m_stats:
        m_total_calls += batches
        print(f"  {name}: {batches} batches ({rows} rows), {rem} → next priority")

if len(unmatched) > 0:
    u_stats = _simulate_grouping(unmatched, [
        ("P1 same company", ["company_name"]),
    ], BATCH_UNMATCHED)
    print(f"[STEP 3] Unmatched grouping preview ({len(unmatched)} rows, batch={BATCH_UNMATCHED}):")
    for name, batches, rows, rem in u_stats:
        u_total_calls += batches
        print(f"  {name}: {batches} batches ({rows} rows), {rem} → next priority")

print(f"[STEP 3] Total estimated LLM calls: ~{m_total_calls + u_total_calls}")

# =============================================================================
# Load NOC list for LLM prompt
# =============================================================================
noc_pd = pd.read_sql(
    "SELECT id, noc21_code, noc21_name FROM noc_titles WHERE LENGTH(noc21_code) = 5",
    args.db_conn,
)
noc_names = noc_pd["noc21_name"].tolist()
name_to_id = {row["noc21_name"]: row["id"] for _, row in noc_pd.iterrows()}
noc_list_str = "\n".join(f"- {name}" for name in noc_names)
print(f"[STEP 3] {len(noc_names)} NOC categories loaded.")

# =============================================================================
# Setup
# =============================================================================
output_dir = Path(args.output)
checkpoint_dir = output_dir / "checkpoints"
progress_dir = output_dir / "progress"

# Keep checkpoints for resume, only reset progress
checkpoint_dir.mkdir(parents=True, exist_ok=True)
if progress_dir.exists():
    shutil.rmtree(progress_dir)
progress_dir.mkdir(parents=True, exist_ok=True)
output_dir.mkdir(parents=True, exist_ok=True)

# Validate existing checkpoints
valid_cps = []
corrupted_cps = []
for cp in sorted(checkpoint_dir.glob("*.json")):
    try:
        with open(cp) as f:
            json.load(f)
        valid_cps.append(cp.name)
    except (json.JSONDecodeError, ValueError):
        corrupted_cps.append(cp.name)
        cp.unlink()

if valid_cps or corrupted_cps:
    print(f"[STEP 3] Resuming — {len(valid_cps)} valid, {len(corrupted_cps)} corrupted (deleted)")
    if corrupted_cps:
        print(f"[STEP 3]   Corrupted: {corrupted_cps}")

# Broadcast
noc_list_broadcast = spark.sparkContext.broadcast(noc_list_str)
name_to_id_broadcast = spark.sparkContext.broadcast(name_to_id)

batch_delay = args.batch_delay

CHECKPOINT_BASE = "/opt/spark/data/processed/skill-demand/step3/checkpoints"
PROGRESS_BASE = "/opt/spark/data/processed/skill-demand/step3/progress"

# =============================================================================
# mapPartitions — Unmatched (NOC + seniority + skills, 5/call)
# =============================================================================
def process_unmatched(partition_idx, rows):
    """Process unmatched rows: NOC + seniority + skills via LLM."""
    import subprocess, json, re, os, time
    import sys
    from itertools import groupby
    from pathlib import Path

    if "/opt/spark/pipelines/skill-demand" not in sys.path:
        sys.path.insert(0, "/opt/spark/pipelines/skill-demand")
    from src.step3_enrich import (
        build_unmatched_prompt, parse_enrich_response,
        BATCH_SIZE_UNMATCHED, MAX_DESC_CHARS,
    )

    all_rows = list(rows)
    if not all_rows:
        return

    pid = f"p{partition_idx}"  # unique per partition
    noc_list = noc_list_broadcast.value
    noc_lookup = name_to_id_broadcast.value
    BATCH_SIZE = BATCH_SIZE_UNMATCHED

    def call_claude(prompt):
        result = subprocess.run(
            ["claude", "--print", "--model", "haiku", "-"],
            input=prompt, capture_output=True, text=True,
            env={**os.environ, "HOME": "/tmp"},
        )
        return result.stdout if result.returncode == 0 else ""

    def safe_name(s):
        return re.sub(r'[^\w\-]', '_', s or "unknown")[:50]

    def save_checkpoint(name, results):
        cp = Path(f"{CHECKPOINT_BASE}/unmatched_{name}.json")
        for _attempt in range(3):
            try:
                cp.parent.mkdir(parents=True, exist_ok=True)
                with open(cp, "w") as f:
                    json.dump(results, f, default=str)
                break
            except OSError:
                if _attempt < 2:
                    time.sleep(1)
                else:
                    raise

    def check_checkpoint(name):
        cp = Path(f"{CHECKPOINT_BASE}/unmatched_{name}.json")
        if cp.exists():
            try:
                with open(cp) as f:
                    return json.load(f)
            except (json.JSONDecodeError, ValueError):
                cp.unlink()  # corrupted → delete and reprocess
        return None

    def save_progress(name, rows_count, matched_count):
        pf = Path(f"{PROGRESS_BASE}/unmatched_{name}.json")
        try:
            pf.parent.mkdir(parents=True, exist_ok=True)
            with open(pf, "w") as f:
                json.dump({"name": name, "rows": rows_count, "matched": matched_count, "type": "unmatched"}, f)
        except OSError:
            pass  # progress is non-critical

    def process_batch(batch_rows, batch_name):
        """Process a batch of rows with the unmatched prompt."""
        cached = check_checkpoint(batch_name)
        if cached:
            for r in cached:
                yield r
            return

        row_dicts = [r.asDict() if hasattr(r, "asDict") else r for r in batch_rows]
        prompt = build_unmatched_prompt(row_dicts, noc_list)
        response = call_claude(prompt)
        parsed = parse_enrich_response(response, noc_lookup)
        time.sleep(batch_delay)

        # Build job_id → parsed result map
        parsed_map = {p["job_id"]: p for p in parsed}

        results = []
        for row in row_dicts:
            p = parsed_map.get(row["job_id"], {})
            results.append({
                "job_id": row["job_id"],
                "company_name": row["company_name"],
                "title": row["title"],
                "description": row["description"],
                "formatted_experience_level": row["formatted_experience_level"],
                "noc_id": int(p["noc_id"]) if p.get("noc_id") else row.get("noc_id"),
                "noc_match_score": row.get("noc_match_score"),
                "noc_match_method": "llm_noc_match" if p.get("noc_id") else row.get("noc_match_method"),
                "seniority": p.get("seniority") or row.get("seniority"),
                "skills": p.get("skills", []),
            })

        save_checkpoint(batch_name, results)
        noc_matched = sum(1 for r in results if r["noc_id"] is not None)
        save_progress(batch_name, len(results), noc_matched)

        for r in results:
            yield r

    # --- Group by company ---
    sorted_rows = sorted(all_rows, key=lambda r: r["company_name"] or "")

    # Priority 1: Same company batches
    remaining = []
    batch_idx = 0
    for company, group in groupby(sorted_rows, key=lambda r: r["company_name"] or ""):
        company_rows = list(group)
        for i in range(0, len(company_rows), BATCH_SIZE):
            chunk = company_rows[i:i + BATCH_SIZE]
            if len(chunk) == BATCH_SIZE:
                batch_name = f"{pid}_co_{safe_name(company)}_{batch_idx}"
                yield from process_batch(chunk, batch_name)
                batch_idx += 1
            else:
                remaining.extend(chunk)

    # Priority 2: Remaining rows batched together
    for i in range(0, len(remaining), BATCH_SIZE):
        chunk = remaining[i:i + BATCH_SIZE]
        batch_name = f"{pid}_rem_{batch_idx}"
        yield from process_batch(chunk, batch_name)
        batch_idx += 1


# =============================================================================
# mapPartitions — Matched (seniority + skills, 10/call)
# =============================================================================
def process_matched(partition_idx, rows):
    """Process matched rows: seniority + skills via LLM. No NOC list."""
    import subprocess, json, re, os, time
    import sys
    from itertools import groupby
    from pathlib import Path

    if "/opt/spark/pipelines/skill-demand" not in sys.path:
        sys.path.insert(0, "/opt/spark/pipelines/skill-demand")
    from src.step3_enrich import (
        build_matched_prompt, parse_enrich_response,
        BATCH_SIZE_MATCHED, MAX_DESC_CHARS,
    )

    all_rows = list(rows)
    if not all_rows:
        return

    pid = f"p{partition_idx}"
    noc_lookup = name_to_id_broadcast.value
    BATCH_SIZE = BATCH_SIZE_MATCHED

    def call_claude(prompt):
        result = subprocess.run(
            ["claude", "--print", "--model", "haiku", "-"],
            input=prompt, capture_output=True, text=True,
            env={**os.environ, "HOME": "/tmp"},
        )
        return result.stdout if result.returncode == 0 else ""

    def safe_name(s):
        return re.sub(r'[^\w\-]', '_', s or "unknown")[:50]

    def save_checkpoint(name, results):
        cp = Path(f"{CHECKPOINT_BASE}/matched_{name}.json")
        for _attempt in range(3):
            try:
                cp.parent.mkdir(parents=True, exist_ok=True)
                with open(cp, "w") as f:
                    json.dump(results, f, default=str)
                break
            except OSError:
                if _attempt < 2:
                    time.sleep(1)
                else:
                    raise

    def check_checkpoint(name):
        cp = Path(f"{CHECKPOINT_BASE}/matched_{name}.json")
        if cp.exists():
            try:
                with open(cp) as f:
                    return json.load(f)
            except (json.JSONDecodeError, ValueError):
                cp.unlink()
        return None

    def save_progress(name, rows_count, skills_count):
        pf = Path(f"{PROGRESS_BASE}/matched_{name}.json")
        try:
            pf.parent.mkdir(parents=True, exist_ok=True)
            with open(pf, "w") as f:
                json.dump({"name": name, "rows": rows_count, "with_skills": skills_count, "type": "matched"}, f)
        except OSError:
            pass  # progress is non-critical

    def process_batch(batch_rows, batch_name):
        """Process a batch of rows with the matched prompt."""
        cached = check_checkpoint(batch_name)
        if cached:
            for r in cached:
                yield r
            return

        row_dicts = [r.asDict() if hasattr(r, "asDict") else r for r in batch_rows]
        prompt = build_matched_prompt(row_dicts)
        response = call_claude(prompt)
        parsed = parse_enrich_response(response, noc_lookup)
        time.sleep(batch_delay)

        parsed_map = {p["job_id"]: p for p in parsed}

        results = []
        for row in row_dicts:
            p = parsed_map.get(row["job_id"], {})
            results.append({
                "job_id": row["job_id"],
                "company_name": row["company_name"],
                "title": row["title"],
                "description": row["description"],
                "formatted_experience_level": row["formatted_experience_level"],
                "noc_id": int(row["noc_id"]) if row.get("noc_id") else None,
                "noc_match_score": row.get("noc_match_score"),
                "noc_match_method": row.get("noc_match_method"),
                "seniority": p.get("seniority") or row.get("seniority"),
                "skills": p.get("skills", []),
            })

        save_checkpoint(batch_name, results)
        with_skills = sum(1 for r in results if r["skills"])
        save_progress(batch_name, len(results), with_skills)

        for r in results:
            yield r

    # --- Similarity-based grouping ---
    # Priority 1: Same company + same noc_id
    sorted_rows = sorted(all_rows, key=lambda r: (r["company_name"] or "", r["noc_id"] or 0))

    remaining_after_p1 = []
    batch_idx = 0
    for key, group in groupby(sorted_rows, key=lambda r: (r["company_name"] or "", r["noc_id"] or 0)):
        group_rows = list(group)
        for i in range(0, len(group_rows), BATCH_SIZE):
            chunk = group_rows[i:i + BATCH_SIZE]
            if len(chunk) == BATCH_SIZE:
                batch_name = f"{pid}_co_noc_{safe_name(key[0])}_{batch_idx}"
                yield from process_batch(chunk, batch_name)
                batch_idx += 1
            else:
                remaining_after_p1.extend(chunk)

    # Priority 2: Same noc_id (different companies)
    remaining_after_p1.sort(key=lambda r: r["noc_id"] or 0)
    remaining_after_p2 = []
    for noc_id, group in groupby(remaining_after_p1, key=lambda r: r["noc_id"] or 0):
        group_rows = list(group)
        for i in range(0, len(group_rows), BATCH_SIZE):
            chunk = group_rows[i:i + BATCH_SIZE]
            if len(chunk) == BATCH_SIZE:
                batch_name = f"{pid}_noc_{noc_id}_{batch_idx}"
                yield from process_batch(chunk, batch_name)
                batch_idx += 1
            else:
                remaining_after_p2.extend(chunk)

    # Priority 3: Remaining
    for i in range(0, len(remaining_after_p2), BATCH_SIZE):
        chunk = remaining_after_p2[i:i + BATCH_SIZE]
        batch_name = f"{pid}_rem_{batch_idx}"
        yield from process_batch(chunk, batch_name)
        batch_idx += 1


# =============================================================================
# Execute
# =============================================================================
start = datetime.datetime.now()
print(f"[STEP 3] Start: {start.strftime('%H:%M:%S')}")

# --- Process unmatched ---
if len(unmatched) > 0:
    print(f"\n[STEP 3] Processing {len(unmatched)} unmatched rows (NOC + seniority + skills)...")
    num_partitions = int(spark.conf.get("spark.executor.instances", "4"))
    df_unmatched = spark.createDataFrame(unmatched)
    df_unmatched = df_unmatched.repartition(num_partitions, "company_name")

    result_rdd = df_unmatched.rdd.mapPartitionsWithIndex(process_unmatched)
    result_rdd.collect()

    elapsed = datetime.datetime.now() - start
    print(f"[STEP 3] Unmatched done: {elapsed}")
else:
    print("[STEP 3] No unmatched rows.")

# --- Process matched ---
mid = datetime.datetime.now()
if len(matched) > 0:
    print(f"\n[STEP 3] Processing {len(matched)} matched rows (seniority + skills)...")
    num_partitions = int(spark.conf.get("spark.executor.instances", "4"))
    df_matched = spark.createDataFrame(matched)
    df_matched = df_matched.repartition(num_partitions, "company_name")

    result_rdd = df_matched.rdd.mapPartitionsWithIndex(process_matched)
    result_rdd.collect()

    elapsed = datetime.datetime.now() - mid
    print(f"[STEP 3] Matched done: {elapsed}")
else:
    print("[STEP 3] No matched rows.")

total_elapsed = datetime.datetime.now() - start
print(f"[STEP 3] Total elapsed: {total_elapsed}")

# =============================================================================
# Merge checkpoints → output parquet
# =============================================================================
all_results = []
for cp in sorted(checkpoint_dir.glob("*.json")):
    with open(cp) as f:
        all_results.extend(json.load(f))

if all_results:
    df_result = pd.DataFrame(all_results)
    output_path = output_dir / "step3_enriched.parquet"
    df_result.to_parquet(output_path, index=False)

    # Summary
    total = len(df_result)
    with_noc = int(df_result["noc_id"].notna().sum())
    with_seniority = int(df_result["seniority"].notna().sum())
    with_skills = int(df_result["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0).gt(0).sum())

    print(f"\n[STEP 3] === Summary ===")
    print(f"  Total rows:       {total}")
    print(f"  With NOC:         {with_noc}/{total} ({with_noc/total*100:.1f}%)")
    print(f"  With seniority:   {with_seniority}/{total} ({with_seniority/total*100:.1f}%)")
    print(f"  With skills:      {with_skills}/{total} ({with_skills/total*100:.1f}%)")
    print(f"  Elapsed:          {total_elapsed}")
    print(f"  Output:           {output_path}")
else:
    print("[STEP 3] No results to save.")

spark.stop()

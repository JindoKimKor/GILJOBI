"""
step3_title_normalize_llm_spark.py — LLM NOC Matching with full NOC list.

Takes unmatched rows from step2_noc_match_st, sends title + company + full
NOC list (510 categories) to Claude Haiku, asks LLM to pick the best NOC
or return null if none fit.

Uses Adaptive Batch Strategy based on company size:
  >=30 titles: 1 company per call
  10-29:       2-3 companies per call
  1-9:         mixed batch, 40 titles per call

Auth: Host ~/.claude mounted to /tmp/.claude in Spark container.

Usage:
    /opt/spark/bin/spark-submit step3_title_normalize_llm_spark.py [options]
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
args = parser.parse_args()

# =============================================================================
# Suppress HF progress bars
# =============================================================================
import os
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"

# =============================================================================
# SparkSession
# =============================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("skill-demand-step3-title-normalize-llm") \
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
print(f"[STEP 3] Total: {total_rows}, Already matched: {len(matched)}, Unmatched: {len(unmatched)}")

# =============================================================================
# Load NOC list for LLM prompt
# =============================================================================
noc_pd = pd.read_sql(
    "SELECT id, noc21_code, noc21_name FROM noc_titles WHERE LENGTH(noc21_code) = 5",
    args.db_conn,
)
noc_names = noc_pd["noc21_name"].tolist()
name_to_id = {row["noc21_name"]: row["id"] for _, row in noc_pd.iterrows()}

# Pre-build NOC list string (reused in every prompt)
noc_list_str = "\n".join(f"- {name}" for name in noc_names)
print(f"[STEP 3] {len(noc_names)} NOC categories loaded for LLM matching.")

# =============================================================================
# Setup
# =============================================================================
output_dir = Path(args.output)
checkpoint_dir = output_dir / "checkpoints"
progress_dir = output_dir / "progress"

for d in [checkpoint_dir, progress_dir]:
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True, exist_ok=True)
output_dir.mkdir(parents=True, exist_ok=True)

# Broadcast
noc_list_broadcast = spark.sparkContext.broadcast(noc_list_str)
name_to_id_broadcast = spark.sparkContext.broadcast(name_to_id)

# =============================================================================
# Adaptive Batch Strategy — mapPartitions
# =============================================================================
batch_delay = args.batch_delay

CHECKPOINT_BASE = "/opt/spark/data/processed/skill-demand/step3/checkpoints"
PROGRESS_BASE = "/opt/spark/data/processed/skill-demand/step3/progress"

def process_partition(rows):
    """Adaptive batch: company-grouped LLM NOC matching with full NOC list."""
    import subprocess
    import json
    import re
    import os
    import time
    from itertools import groupby
    from pathlib import Path

    all_rows = sorted(list(rows), key=lambda r: r["company_name"] or "")
    if not all_rows:
        return

    noc_list = noc_list_broadcast.value
    noc_lookup = name_to_id_broadcast.value

    def call_claude(prompt):
        result = subprocess.run(
            ["claude", "--print", "--model", "haiku", "-"],
            input=prompt,
            capture_output=True,
            text=True,
            env={**os.environ, "HOME": "/tmp"},
        )
        return result.stdout if result.returncode == 0 else ""

    def parse_response(response):
        """Parse LLM response into {raw_title: noc_title} mapping."""
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
            parsed = json.loads(jm.group())
            result = {}
            for m in parsed.get("mappings", []):
                raw = m.get("raw", "")
                noc = m.get("noc_title")
                if raw and noc and noc != "null" and noc in noc_lookup:
                    result[raw] = noc
            return result
        except (json.JSONDecodeError, KeyError):
            return {}

    def build_company_prompt(company, titles):
        titles_str = "\n".join(f"- {t}" for t in titles)
        return f"""For each job title, pick the most relevant NOC 2021 category from the list below.
If none fit, use null. Use the company name for context.

Company: {company}

Job titles:
{titles_str}

Available NOC 2021 categories:
{noc_list}

Return JSON only:
{{"mappings": [{{"raw": "job title", "noc_title": "NOC category or null"}}]}}"""

    def build_mixed_prompt(title_company_pairs):
        lines = "\n".join(f'- "{t}" ({c})' for t, c in title_company_pairs)
        return f"""For each job title, pick the most relevant NOC 2021 category from the list below.
If none fit, use null. Company name is provided for context.

Job titles:
{lines}

Available NOC 2021 categories:
{noc_list}

Return JSON only:
{{"mappings": [{{"raw": "job title", "noc_title": "NOC category or null"}}]}}"""

    def apply_mappings(rows_list, mappings):
        results = []
        for row in rows_list:
            noc_title = mappings.get(row["title"])
            noc_id = noc_lookup.get(noc_title) if noc_title else None
            results.append({
                "job_id": row["job_id"],
                "company_name": row["company_name"],
                "title": row["title"],
                "description": row["description"],
                "noc_id": int(noc_id) if noc_id else None,
                "noc_match_score": None,
                "noc_match_method": "llm_noc_match" if noc_id else None,
            })
        return results

    def safe_name(s):
        return re.sub(r'[^\w\-]', '_', s or "unknown")[:50]

    def save_checkpoint(name, results):
        cp = Path(f"{CHECKPOINT_BASE}/{name}.json")
        try:
            cp.parent.mkdir(parents=True, exist_ok=True)
        except FileExistsError:
            pass
        with open(cp, "w") as f:
            json.dump(results, f, default=str)

    def save_progress(name, rows_count, matched_count):
        pf = Path(f"{PROGRESS_BASE}/{name}.json")
        try:
            pf.parent.mkdir(parents=True, exist_ok=True)
        except FileExistsError:
            pass
        with open(pf, "w") as f:
            json.dump({"name": name, "rows": rows_count, "matched": matched_count}, f)

    def check_checkpoint(name):
        cp = Path(f"{CHECKPOINT_BASE}/{name}.json")
        if cp.exists():
            with open(cp) as f:
                return json.load(f)
        return None

    # --- Group by company ---
    company_groups = {}
    for company, group in groupby(all_rows, key=lambda r: r["company_name"] or ""):
        company_groups[company] = list(group)

    # --- Categorize by company size ---
    large = {}     # >=30: 1 company per call
    medium = {}    # 10-29: multi-company per call
    small = {}     # 1-9: mixed batch

    for company, rows_list in company_groups.items():
        n = len(set(r["title"] for r in rows_list))
        if n >= 30:
            large[company] = rows_list
        elif n >= 10:
            medium[company] = rows_list
        else:
            small[company] = rows_list

    # === LARGE (>=30): 1 company per call, 50 titles/batch ===
    for company, rows_list in large.items():
        cp_name = safe_name(company)
        cached = check_checkpoint(cp_name)
        if cached:
            for r in cached:
                yield r
            continue

        unique_titles = list(set(r["title"] for r in rows_list))
        all_mappings = {}
        for i in range(0, len(unique_titles), 50):
            chunk = unique_titles[i:i + 50]
            response = call_claude(build_company_prompt(company, chunk))
            all_mappings.update(parse_response(response))
            time.sleep(batch_delay)

        results = apply_mappings(rows_list, all_mappings)
        save_checkpoint(cp_name, results)
        matched = sum(1 for r in results if r["noc_id"] is not None)
        save_progress(cp_name, len(results), matched)
        for r in results:
            yield r

    # === MEDIUM (10-29): 2-3 companies per call ===
    medium_list = list(medium.items())
    i = 0
    while i < len(medium_list):
        company, rows_list = medium_list[i]
        n = len(set(r["title"] for r in rows_list))
        group_size = 2 if n >= 20 else 3

        call_companies = []
        call_rows = []
        for j in range(i, min(i + group_size, len(medium_list))):
            c, rl = medium_list[j]
            call_companies.append(c)
            call_rows.extend(rl)

        batch_name = safe_name("_".join(call_companies))
        cached = check_checkpoint(batch_name)
        if cached:
            for r in cached:
                yield r
            i += len(call_companies)
            continue

        pairs = list(set((r["title"], r["company_name"] or "") for r in call_rows))
        response = call_claude(build_mixed_prompt(pairs))
        mappings = parse_response(response)
        time.sleep(batch_delay)

        results = apply_mappings(call_rows, mappings)
        save_checkpoint(batch_name, results)
        matched = sum(1 for r in results if r["noc_id"] is not None)
        save_progress(batch_name, len(results), matched)
        for r in results:
            yield r
        i += len(call_companies)

    # === SMALL (1-9): mixed batch, 40 titles per call ===
    small_all_rows = []
    for rows_list in small.values():
        small_all_rows.extend(rows_list)

    if small_all_rows:
        for chunk_start in range(0, len(small_all_rows), 40):
            chunk_rows = small_all_rows[chunk_start:chunk_start + 40]
            batch_name = f"small_batch_{chunk_start}"

            cached = check_checkpoint(batch_name)
            if cached:
                for r in cached:
                    yield r
                continue

            pairs = list(set((r["title"], r["company_name"] or "Unknown") for r in chunk_rows))
            response = call_claude(build_mixed_prompt(pairs))
            mappings = parse_response(response)
            time.sleep(batch_delay)

            results = apply_mappings(chunk_rows, mappings)
            save_checkpoint(batch_name, results)
            matched = sum(1 for r in results if r["noc_id"] is not None)
            save_progress(batch_name, len(results), matched)
            for r in results:
                yield r

# =============================================================================
# Execute
# =============================================================================
if len(unmatched) == 0:
    print("[STEP 3] No unmatched rows. Skipping.")
else:
    num_companies = unmatched["company_name"].nunique()
    print(f"[STEP 3] Processing {len(unmatched)} unmatched rows from {num_companies} companies...")

    df_spark = spark.createDataFrame(unmatched)
    df_spark = df_spark.repartition("company_name")

    start = datetime.datetime.now()
    print(f"[STEP 3] Start: {start.strftime('%H:%M:%S')}")

    result_rdd = df_spark.rdd.mapPartitions(process_partition)
    result_rdd.collect()

    elapsed = datetime.datetime.now() - start
    print(f"[STEP 3] Elapsed: {elapsed}")

# =============================================================================
# Merge matched (step2) + LLM matched (step3)
# =============================================================================
step3_rows = []
for cp in sorted(checkpoint_dir.glob("*.json")):
    with open(cp) as f:
        step3_rows.extend(json.load(f))

df_step3 = pd.DataFrame(step3_rows) if step3_rows else pd.DataFrame()

if not df_step3.empty:
    df_final = pd.concat([matched, df_step3], ignore_index=True)
else:
    df_final = matched

output_path = output_dir / "step3_normalized.parquet"
df_final.to_parquet(output_path, index=False)

# =============================================================================
# Summary
# =============================================================================
total_final = len(df_final)
step2_count = len(matched)
step3_matched = len(df_step3[df_step3["noc_id"].notna()]) if not df_step3.empty else 0
step3_null = len(df_step3[df_step3["noc_id"].isna()]) if not df_step3.empty else 0
total_noc = step2_count + step3_matched

print(f"\n[STEP 3] === Summary ===")
print(f"  Total rows:       {total_final}")
print(f"  Step 2 matched:   {step2_count} (ST, threshold 0.65)")
print(f"  Step 3 matched:   {step3_matched} (LLM NOC match)")
print(f"  Step 3 null:      {step3_null} (LLM said no match)")
print(f"  Total NOC:        {total_noc} ({total_noc/total_final*100:.1f}%)")
print(f"  Output:           {output_path}")

spark.stop()

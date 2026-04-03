"""
step3_enrich_spark.py — LLM Enrich: NOC + Seniority + Skills on Spark.

Processes ALL rows from Step 2 with 2 prompt types:
  - unmatched (noc_id null): NOC list + JD → NOC + seniority + skills (5/call)
  - matched (noc_id filled): JD only → seniority + skills (10/call)

Adaptive Batch Strategy — similarity-based grouping:
  Matched:   1) same company + same noc_id  2) same noc_id  3) remaining
  Unmatched: 1) same company  2) remaining

Batches are created on the Driver (content-based batch_id via job_id hash),
then distributed to Workers. Workers only execute LLM calls + checkpoint.
This makes session control and checkpoints independent of worker count.

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
parser.add_argument("--max-batches-per-session", type=int, default=800,
                    help="Max LLM calls before cooldown. 0 = no limit. "
                         "Measured: 1 full session ≈ 1,771 calls (Claude Max 20x, 2026-03-31).")
parser.add_argument("--session-cooldown-min", type=int, default=90,
                    help="Minutes to wait for session reset")
parser.add_argument("--max-sessions", type=int, default=0,
                    help="Max session cycles. 0 = unlimited.")
args = parser.parse_args()

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
import hashlib
import re
import shutil
from pathlib import Path
from itertools import groupby

print(f"[SD:STEP3] Loading input: {args.input}")
df_all = pd.read_parquet(args.input)
total_rows = len(df_all)

matched_df = df_all[df_all["noc_id"].notna()].copy()
unmatched_df = df_all[df_all["noc_id"].isna()].copy()
print(f"[SD:STEP3] Total: {total_rows}, Matched: {len(matched_df)}, Unmatched: {len(unmatched_df)}")

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
print(f"[SD:STEP3] {len(noc_names)} NOC categories loaded.")

# =============================================================================
# Setup
# =============================================================================
output_dir = Path(args.output)
checkpoint_dir = output_dir / "checkpoints"
progress_dir = output_dir / "progress"

checkpoint_dir.mkdir(parents=True, exist_ok=True)
if progress_dir.exists():
    shutil.rmtree(progress_dir)
progress_dir.mkdir(parents=True, exist_ok=True)
output_dir.mkdir(parents=True, exist_ok=True)

# Validate existing checkpoints + collect completed job_ids
valid_cps = []
corrupted_cps = []
done_job_ids = set()
for cp in sorted(checkpoint_dir.glob("*.json")):
    try:
        with open(cp) as f:
            data = json.load(f)
        valid_cps.append(cp.name)
        for r in data:
            done_job_ids.add(r.get("job_id"))
    except (json.JSONDecodeError, ValueError):
        corrupted_cps.append(cp.name)
        cp.unlink()

if valid_cps or corrupted_cps:
    print(f"[SD:STEP3] Resuming — {len(valid_cps)} checkpoints, {len(done_job_ids):,} job_ids done, {len(corrupted_cps)} corrupted (deleted)")
    if corrupted_cps:
        print(f"[SD:STEP3]   Corrupted: {corrupted_cps}")

# Broadcast
noc_list_broadcast = spark.sparkContext.broadcast(noc_list_str)
name_to_id_broadcast = spark.sparkContext.broadcast(name_to_id)

# =============================================================================
# Driver — Create all batches upfront
# =============================================================================
BATCH_UNMATCHED = 5
BATCH_MATCHED = 10


def _hash(job_ids):
    """Content-based hash from sorted job_ids. Worker-count independent."""
    key = ",".join(str(jid) for jid in sorted(job_ids))
    return hashlib.md5(key.encode()).hexdigest()[:12]


def _safe_name(s):
    return re.sub(r'[^\w\-]', '_', s or "unknown")[:50]


def _batch_id(prefix, job_ids):
    """Grouping prefix + content hash. E.g. 'co_Amazon_a3f8b2c1d4e5'."""
    return f"{prefix}_{_hash(job_ids)}"


def _safe_val(v, default):
    """NaN-safe value extraction. float('nan') or None → default."""
    if v is None:
        return default
    if isinstance(v, float) and (v != v):  # NaN check: NaN != NaN
        return default
    return v


def _create_batches_matched(rows):
    """Matched grouping: P1 company+noc_id → P2 same noc_id → remaining.
    Returns list of (batch_id, chunk) tuples."""
    batches = []

    # P1: same company + same noc_id
    rows_sorted = sorted(rows, key=lambda r: (_safe_val(r["company_name"], ""), _safe_val(r["noc_id"], 0)))
    remaining_p1 = []
    for key, group in groupby(rows_sorted, key=lambda r: (_safe_val(r["company_name"], ""), _safe_val(r["noc_id"], 0))):
        group_rows = list(group)
        for i in range(0, len(group_rows), BATCH_MATCHED):
            chunk = group_rows[i:i + BATCH_MATCHED]
            if len(chunk) == BATCH_MATCHED:
                prefix = f"co_noc_{_safe_name(key[0])}"
                batches.append((_batch_id(prefix, [r["job_id"] for r in chunk]), chunk))
            else:
                remaining_p1.extend(chunk)

    # P2: same noc_id
    remaining_p1.sort(key=lambda r: _safe_val(r["noc_id"], 0))
    remaining_p2 = []
    for noc_id, group in groupby(remaining_p1, key=lambda r: _safe_val(r["noc_id"], 0)):
        group_rows = list(group)
        for i in range(0, len(group_rows), BATCH_MATCHED):
            chunk = group_rows[i:i + BATCH_MATCHED]
            if len(chunk) == BATCH_MATCHED:
                prefix = f"noc_{noc_id}"
                batches.append((_batch_id(prefix, [r["job_id"] for r in chunk]), chunk))
            else:
                remaining_p2.extend(chunk)

    # Remaining
    for i in range(0, len(remaining_p2), BATCH_MATCHED):
        chunk = remaining_p2[i:i + BATCH_MATCHED]
        batches.append((_batch_id("rem", [r["job_id"] for r in chunk]), chunk))

    return batches


def _create_batches_unmatched(rows):
    """Unmatched grouping: P1 same company → remaining.
    Returns list of (batch_id, chunk) tuples."""
    batches = []

    rows_sorted = sorted(rows, key=lambda r: _safe_val(r["company_name"], ""))
    remaining = []
    for company, group in groupby(rows_sorted, key=lambda r: _safe_val(r["company_name"], "")):
        group_rows = list(group)
        for i in range(0, len(group_rows), BATCH_UNMATCHED):
            chunk = group_rows[i:i + BATCH_UNMATCHED]
            if len(chunk) == BATCH_UNMATCHED:
                prefix = f"co_{_safe_name(company)}"
                batches.append((_batch_id(prefix, [r["job_id"] for r in chunk]), chunk))
            else:
                remaining.extend(chunk)

    for i in range(0, len(remaining), BATCH_UNMATCHED):
        chunk = remaining[i:i + BATCH_UNMATCHED]
        batches.append((_batch_id("rem", [r["job_id"] for r in chunk]), chunk))

    return batches


# Convert to dicts
matched_rows = matched_df.to_dict("records")
unmatched_rows = unmatched_df.to_dict("records")

# Create batches
matched_batches = _create_batches_matched(matched_rows)
unmatched_batches = _create_batches_unmatched(unmatched_rows)

# Build batch descriptors (serializable to Spark)
all_batch_descriptors = []
skipped = 0

for bid, batch_rows in unmatched_batches:
    job_ids = [r["job_id"] for r in batch_rows]
    if all(jid in done_job_ids for jid in job_ids):
        skipped += 1
        continue
    all_batch_descriptors.append({
        "batch_id": bid,
        "batch_type": "unmatched",
        "rows_json": json.dumps(batch_rows, default=str),
    })

for bid, batch_rows in matched_batches:
    job_ids = [r["job_id"] for r in batch_rows]
    if all(jid in done_job_ids for jid in job_ids):
        skipped += 1
        continue
    all_batch_descriptors.append({
        "batch_id": bid,
        "batch_type": "matched",
        "rows_json": json.dumps(batch_rows, default=str),
    })

total_batches = len(all_batch_descriptors)
queued_unmatched = sum(1 for b in all_batch_descriptors if b["batch_type"] == "unmatched")
queued_matched = total_batches - queued_unmatched

print(f"[SD:STEP3] Batches: {total_batches} to process, {skipped} already done")
print(f"  Unmatched: {queued_unmatched}/{len(unmatched_batches)} batches ({len(unmatched_rows)} rows, {BATCH_UNMATCHED}/call)")
print(f"  Matched:   {queued_matched}/{len(matched_batches)} batches ({len(matched_rows)} rows, {BATCH_MATCHED}/call)")

# Session control
max_bps = args.max_batches_per_session
cooldown_seconds = args.session_cooldown_min * 60
max_sessions = args.max_sessions

if max_bps > 0:
    total_sessions = (total_batches + max_bps - 1) // max_bps if total_batches > 0 else 0
    if max_sessions > 0:
        total_sessions = min(total_sessions, max_sessions)
    print(f"[SD:STEP3] Session control: {max_bps} batches/session, {args.session_cooldown_min} min cooldown")
    print(f"  Sessions needed: {total_sessions} (max_sessions: {'unlimited' if max_sessions == 0 else max_sessions})")
else:
    print(f"[SD:STEP3] Session control: disabled (no limit)")

batch_delay = args.batch_delay
num_workers = int(spark.conf.get("spark.executor.instances", "6"))

CHECKPOINT_BASE = "/opt/spark/data/processed/skill-demand/step3/checkpoints"
PROGRESS_BASE = "/opt/spark/data/processed/skill-demand/step3/progress"

# =============================================================================
# mapPartitions — Process pre-built batches
# =============================================================================
def process_batches(partition_idx, rows):
    """Worker: receive pre-built batches, execute LLM calls, save checkpoints."""
    import subprocess, json, os, time
    import sys
    from pathlib import Path

    if "/opt/spark/pipelines/skill-demand" not in sys.path:
        sys.path.insert(0, "/opt/spark/pipelines/skill-demand")
    from src.step3_enrich import (
        build_unmatched_prompt, build_matched_prompt, parse_enrich_response,
    )

    all_batches = list(rows)
    if not all_batches:
        return

    pid = f"p{partition_idx}"
    noc_list = noc_list_broadcast.value
    noc_lookup = name_to_id_broadcast.value

    def call_claude(prompt):
        result = subprocess.run(
            ["claude", "--print", "--model", "haiku", "-"],
            input=prompt, capture_output=True, text=True,
            env={**os.environ, "HOME": "/tmp"},
        )
        if result.returncode != 0:
            stderr_snippet = (result.stderr or "")[:200]
            print(f"[SD:STEP3:LLM:ERROR] returncode={result.returncode}, stderr={stderr_snippet}")
            return ""
        if not result.stdout or not result.stdout.strip():
            print(f"[SD:STEP3:LLM:EMPTY] returncode=0 but empty stdout")
            return ""
        return result.stdout

    def save_checkpoint(bid, btype, results):
        cp = Path(f"{CHECKPOINT_BASE}/{btype}_{bid}.json")
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

    def check_checkpoint(bid, btype):
        cp = Path(f"{CHECKPOINT_BASE}/{btype}_{bid}.json")
        if cp.exists():
            try:
                with open(cp) as f:
                    return json.load(f)
            except (json.JSONDecodeError, ValueError):
                cp.unlink()
        return None

    def save_progress(bid, btype, rows_count, **metrics):
        pf = Path(f"{PROGRESS_BASE}/{btype}_{bid}.json")
        try:
            pf.parent.mkdir(parents=True, exist_ok=True)
            with open(pf, "w") as f:
                json.dump({"name": bid, "rows": rows_count, "type": btype, **metrics}, f)
        except OSError:
            pass

    # --- Process each batch ---
    processed = 0
    for batch_row in all_batches:
        bid = batch_row["batch_id"]
        btype = batch_row["batch_type"]
        row_dicts = json.loads(batch_row["rows_json"])

        # Check checkpoint
        cached = check_checkpoint(bid, btype)
        if cached:
            for r in cached:
                yield r
            continue

        # Build prompt
        if btype == "unmatched":
            prompt = build_unmatched_prompt(row_dicts, noc_list)
        else:
            prompt = build_matched_prompt(row_dicts)

        response = call_claude(prompt)
        parsed = parse_enrich_response(response, noc_lookup)
        time.sleep(batch_delay)

        if not parsed:
            continue  # LLM failed — skip, will retry on next run

        parsed_map = {p["job_id"]: p for p in parsed}

        results = []
        for row in row_dicts:
            p = parsed_map.get(row["job_id"], {})
            if btype == "unmatched":
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
            else:
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

        save_checkpoint(bid, btype, results)

        if btype == "unmatched":
            noc_matched = sum(1 for r in results if r["noc_id"] is not None)
            save_progress(bid, btype, len(results), matched=noc_matched)
        else:
            with_skills = sum(1 for r in results if r["skills"])
            save_progress(bid, btype, len(results), with_skills=with_skills)

        for r in results:
            yield r

        processed += 1

    print(f"[SD:STEP3:WORKER:{pid}] Done. {processed} batches processed.")


# =============================================================================
# Execute — Driver session loop
# =============================================================================
import time as _time

start = datetime.datetime.now()
print(f"[SD:STEP3] Start: {start.strftime('%H:%M:%S')}")

if total_batches > 0:
    remaining = list(all_batch_descriptors)
    session_num = 0

    while remaining:
        session_num += 1

        # Slice this session's batch
        if max_bps > 0:
            session_batch = remaining[:max_bps]
            remaining = remaining[max_bps:]
        else:
            session_batch = remaining
            remaining = []

        s_unmatched = sum(1 for b in session_batch if b["batch_type"] == "unmatched")
        s_matched = len(session_batch) - s_unmatched
        print(f"\n[SD:STEP3] Session {session_num}: {len(session_batch)} batches ({s_unmatched} unmatched + {s_matched} matched), {len(remaining)} remaining")

        # Submit to Spark
        df_batches = spark.createDataFrame(session_batch)
        df_batches = df_batches.repartition(num_workers)
        result_rdd = df_batches.rdd.mapPartitionsWithIndex(process_batches)
        result_rdd.collect()

        elapsed = datetime.datetime.now() - start
        print(f"[SD:STEP3] Session {session_num} done. Elapsed: {elapsed}")

        # Cooldown or stop
        if not remaining:
            print(f"[SD:STEP3] All batches processed.")
            break

        if max_sessions > 0 and session_num >= max_sessions:
            print(f"[SD:STEP3] Max sessions reached ({session_num}). {len(remaining)} batches remaining for next DAG trigger.")
            break

        if cooldown_seconds > 0:
            resume_time = (datetime.datetime.now() + datetime.timedelta(seconds=cooldown_seconds)).strftime("%H:%M:%S")
            print(f"[SD:STEP3] Cooldown {args.session_cooldown_min} min (resume ~{resume_time})...")

            # Write cooldown indicator for DAG polling
            cooldown_file = progress_dir / "_cooldown.json"
            try:
                cooldown_file.write_text(json.dumps({
                    "session": session_num,
                    "cooldown_min": args.session_cooldown_min,
                    "resume_at": resume_time,
                }))
            except OSError:
                pass

            _time.sleep(cooldown_seconds)

            # Remove cooldown indicator
            try:
                cooldown_file.unlink(missing_ok=True)
            except OSError:
                pass
else:
    print("[SD:STEP3] No batches to process.")

total_elapsed = datetime.datetime.now() - start
print(f"[SD:STEP3] Total elapsed: {total_elapsed}")

# =============================================================================
# Merge checkpoints → output parquet (with dedup)
# =============================================================================
all_results = []
seen_ids = set()
for cp in sorted(checkpoint_dir.glob("*.json")):
    with open(cp) as f:
        data = json.load(f)
    for r in data:
        jid = r.get("job_id")
        if jid not in seen_ids:
            all_results.append(r)
            seen_ids.add(jid)

if all_results:
    df_result = pd.DataFrame(all_results)
    output_path = output_dir / "step3_enriched.parquet"
    df_result.to_parquet(output_path, index=False)

    # Summary
    total = len(df_result)
    with_noc = int(df_result["noc_id"].notna().sum())
    with_seniority = int(df_result["seniority"].notna().sum())
    with_skills = int(df_result["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0).gt(0).sum())

    # This run stats
    new_rows = total - len(done_job_ids)
    new_ids = seen_ids - done_job_ids
    df_new = df_result[df_result["job_id"].isin(new_ids)] if new_ids else pd.DataFrame()

    print(f"\n[SD:STEP3] === This Run ===")
    if len(df_new) > 0:
        new_noc = int(df_new["noc_id"].notna().sum())
        new_sen = int(df_new["seniority"].notna().sum())
        new_skills = int(df_new["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0).gt(0).sum())
        print(f"  Rows:       {len(df_new)}")
        print(f"  With NOC:   {new_noc}/{len(df_new)} ({new_noc/len(df_new)*100:.1f}%)")
        print(f"  Seniority:  {new_sen}/{len(df_new)} ({new_sen/len(df_new)*100:.1f}%)")
        print(f"  Skills:     {new_skills}/{len(df_new)} ({new_skills/len(df_new)*100:.1f}%)")
    else:
        print(f"  No new rows processed.")
    print(f"  Elapsed:    {total_elapsed}")

    print(f"\n[SD:STEP3] === Accumulated Total ===")
    print(f"  Total rows:       {total}")
    print(f"  With NOC:         {with_noc}/{total} ({with_noc/total*100:.1f}%)")
    print(f"  With seniority:   {with_seniority}/{total} ({with_seniority/total*100:.1f}%)")
    print(f"  With skills:      {with_skills}/{total} ({with_skills/total*100:.1f}%)")
    print(f"  Progress:         {total}/{total_rows} ({total/total_rows*100:.1f}%)")
    print(f"  Output:           {output_path}")
else:
    print("[SD:STEP3] No results to save.")

spark.stop()

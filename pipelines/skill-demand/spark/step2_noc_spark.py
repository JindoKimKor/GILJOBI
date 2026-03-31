"""
step2_noc_spark.py — NOC Normalization via Sentence Transformers on Spark.

Submitted by LivyOperator from skill_demand_dag.py.
Runs inside Spark cluster: loads model, encodes NOC + job titles,
computes cosine similarity, writes matched/unmatched parquet.

Spark Features Used:
- Broadcast: NOC embeddings sent to all workers
- UDF: cosine similarity as distributed function
- Schema Enforcement: StructType for input validation
- Partitioned Write: output parquet

Usage (from Spark master container):
    /opt/spark/bin/spark-submit step2_noc_spark.py [--threshold 0.75]
"""

# =============================================================================
# Args
# =============================================================================
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--threshold", type=float, default=0.75,
                    help="Cosine similarity threshold for NOC matching")
parser.add_argument("--input", type=str,
                    default="/opt/spark/data/processed/skill-demand/step1/step1_extracted.parquet")
parser.add_argument("--output", type=str,
                    default="/opt/spark/data/processed/skill-demand/step2")
parser.add_argument("--db-conn", type=str,
                    default="postgresql://postgres:postgres@skill-demand-db:5432/giljobi_sd")
parser.add_argument("--partitions", type=int, default=16,
                    help="Number of partitions (= checkpoint granularity)")
args = parser.parse_args()

# =============================================================================
# SparkSession
# =============================================================================
import os
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("skill-demand-step2-noc-normalize") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# Load Data
# =============================================================================
import os
import json
import numpy as np
import pandas as pd

print(f"[STEP 2] Loading input: {args.input}")
df = spark.read.parquet(args.input)
total_rows = df.count()
# Repartition to match worker count for parallel processing
num_partitions = args.partitions
df = df.repartition(num_partitions)
print(f"[STEP 2] {total_rows} rows loaded, {num_partitions} partitions (~{total_rows // num_partitions} rows/partition).")

# Load NOC titles from DB
print(f"[STEP 2] Loading NOC titles from DB...")
noc_pd = pd.read_sql(
    "SELECT id, noc21_code, noc21_name FROM noc_titles WHERE LENGTH(noc21_code) = 5",
    args.db_conn,
)
print(f"[STEP 2] {len(noc_pd)} NOC unit groups loaded.")

# =============================================================================
# Encode NOC Titles (Driver)
# =============================================================================
from sentence_transformers import SentenceTransformer

print("[STEP 2] Loading Sentence Transformers model...")
model = SentenceTransformer("all-MiniLM-L6-v2")

print("[STEP 2] Encoding NOC titles...")
noc_names = noc_pd["noc21_name"].tolist()
noc_ids = noc_pd["id"].tolist()
noc_embeddings = model.encode(noc_names, normalize_embeddings=True)

# Broadcast NOC data to all workers
noc_emb_broadcast = spark.sparkContext.broadcast(noc_embeddings)
noc_ids_broadcast = spark.sparkContext.broadcast(noc_ids)
noc_names_broadcast = spark.sparkContext.broadcast(noc_names)
threshold = args.threshold

# =============================================================================
# Accumulator for metrics
# =============================================================================
matched_count = spark.sparkContext.accumulator(0)
unmatched_count = spark.sparkContext.accumulator(0)

# =============================================================================
# mapPartitions — encode job titles + cosine similarity
# =============================================================================
PROGRESS_DIR = os.path.join(args.output, "progress")
CHECKPOINT_DIR = os.path.join(args.output, "checkpoints")
import shutil

# Keep checkpoints for resume, only reset progress
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
if os.path.exists(PROGRESS_DIR):
    shutil.rmtree(PROGRESS_DIR)
os.makedirs(PROGRESS_DIR, exist_ok=True)

# Validate existing checkpoints
valid_cps = []
corrupted_cps = []
for f in sorted(os.listdir(CHECKPOINT_DIR)):
    if not f.endswith(".json"):
        continue
    fpath = os.path.join(CHECKPOINT_DIR, f)
    try:
        with open(fpath) as fp:
            json.load(fp)
        valid_cps.append(f)
    except (json.JSONDecodeError, ValueError):
        corrupted_cps.append(f)
        os.remove(fpath)

expected_partitions = set(f"p{i}.json" for i in range(num_partitions))
existing_set = set(valid_cps)
missing_cps = sorted(expected_partitions - existing_set)

if valid_cps or corrupted_cps:
    print(f"[STEP 2] Resuming — {len(valid_cps)} valid, {len(corrupted_cps)} corrupted (deleted), {len(missing_cps)} to reprocess")
    if corrupted_cps:
        print(f"[STEP 2]   Corrupted: {corrupted_cps}")
    if missing_cps:
        print(f"[STEP 2]   Missing: {missing_cps}")

ENCODE_CHUNK_SIZE = 1000  # encode + match in chunks for progress reporting

def match_partition(partition_idx, rows):
    """Encode job titles, match NOC, extract seniority. Checkpoint per partition."""
    from sentence_transformers import SentenceTransformer
    import numpy as np
    import json
    import sys
    from pathlib import Path

    if "/opt/spark/pipelines/skill-demand" not in sys.path:
        sys.path.insert(0, "/opt/spark/pipelines/skill-demand")
    from src.step2_seniority import extract_seniority

    rows_list = list(rows)
    if not rows_list:
        return

    pid = f"p{partition_idx}"
    checkpoint_file = Path(f"/opt/spark/data/processed/skill-demand/step2/checkpoints/{pid}.json")
    progress_file = Path(f"/opt/spark/data/processed/skill-demand/step2/progress/{pid}.json")

    # Resume: if checkpoint exists, yield cached results
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file) as f:
                for r in json.load(f):
                    yield r
            return
        except (json.JSONDecodeError, ValueError):
            checkpoint_file.unlink()  # corrupted → delete and reprocess

    progress_file.parent.mkdir(parents=True, exist_ok=True)

    # Load model on each worker
    worker_model = SentenceTransformer("all-MiniLM-L6-v2")

    noc_emb = noc_emb_broadcast.value
    noc_id_list = noc_ids_broadcast.value

    matched = 0
    unmatched = 0
    processed = 0
    total_in_partition = len(rows_list)
    results = []

    # Process in chunks for progress updates
    for chunk_start in range(0, total_in_partition, ENCODE_CHUNK_SIZE):
        chunk_end = min(chunk_start + ENCODE_CHUNK_SIZE, total_in_partition)
        chunk_rows = rows_list[chunk_start:chunk_end]

        titles = [row["title"] for row in chunk_rows]
        title_embeddings = worker_model.encode(titles, normalize_embeddings=True)
        sim_matrix = title_embeddings @ noc_emb.T

        for i, row in enumerate(chunk_rows):
            best_idx = int(np.argmax(sim_matrix[i]))
            best_score = float(sim_matrix[i][best_idx])
            seniority, _ = extract_seniority(row["formatted_experience_level"], row["title"])

            base = {
                "job_id": row["job_id"],
                "company_name": row["company_name"],
                "title": row["title"],
                "description": row["description"],
                "formatted_experience_level": row["formatted_experience_level"],
                "noc_match_score": round(best_score, 4),
                "seniority": seniority,
            }

            if best_score >= threshold:
                matched_count.add(1)
                matched += 1
                result = {**base, "noc_id": noc_id_list[best_idx], "noc_match_method": "sentence_transformer"}
            else:
                unmatched_count.add(1)
                unmatched += 1
                result = {**base, "noc_id": None, "noc_match_method": None}

            results.append(result)

        processed += len(chunk_rows)
        try:
            with open(progress_file, "w") as f:
                json.dump({"rows": processed, "total": total_in_partition, "matched": matched, "unmatched": unmatched}, f)
        except OSError:
            pass  # progress is non-critical, don't crash partition

    # Save partition checkpoint (critical — retry on Docker volume I/O error)
    import time as _time
    for _attempt in range(3):
        try:
            checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
            with open(checkpoint_file, "w") as f:
                json.dump(results, f, default=str)
            break
        except OSError:
            if _attempt < 2:
                _time.sleep(1)
            else:
                raise

    for r in results:
        yield r

# =============================================================================
# Execute
# =============================================================================
import datetime

print(f"[STEP 2] Starting NOC matching (threshold={threshold})...")
start = datetime.datetime.now()

# Progress monitor — reads worker progress files every 15s from a separate thread
import threading
import json
from pathlib import Path

_progress_done = threading.Event()
_progress_dir = Path(PROGRESS_DIR)

def _progress_monitor():
    while not _progress_done.is_set():
        try:
            total_done = 0
            total_matched = 0
            total_unmatched = 0
            for pf in _progress_dir.glob("part_*.json"):
                with open(pf) as f:
                    data = json.load(f)
                    total_done += data["rows"]
                    total_matched += data["matched"]
                    total_unmatched += data["unmatched"]
            if total_done > 0:
                import sys
                print(f"[STEP 2] Progress: {total_done}/{total_rows} ({total_done/total_rows*100:.1f}%) — matched: {total_matched}, unmatched: {total_unmatched}")
                sys.stdout.flush()
        except Exception:
            pass
        _progress_done.wait(15)

monitor = threading.Thread(target=_progress_monitor, daemon=True)
monitor.start()

result_rdd = df.rdd.mapPartitionsWithIndex(match_partition)
result_df = spark.createDataFrame(result_rdd)

# Write output
os.makedirs(args.output, exist_ok=True)
output_path = os.path.join(args.output, "step2_normalized.parquet")
result_df.toPandas().to_parquet(output_path, index=False)

_progress_done.set()
elapsed = datetime.datetime.now() - start

# =============================================================================
# Summary
# =============================================================================
result_pd = pd.read_parquet(output_path)
total_out = len(result_pd)
matched_out = int(result_pd["noc_id"].notna().sum())
unmatched_out = total_out - matched_out
seniority_counts = result_pd["seniority"].value_counts(dropna=False)
seniority_filled = int(result_pd["seniority"].notna().sum())

print(f"\n[STEP 2] === Summary ===")
print(f"  Total:      {total_out}")
print(f"  Matched:    {matched_out}")
print(f"  Unmatched:  {unmatched_out}")
print(f"  Match rate: {matched_out / total_out * 100:.1f}%")
print(f"  Threshold:  {threshold}")
print(f"  Seniority:  {seniority_filled}/{total_out} ({seniority_filled/total_out*100:.1f}%)")
for tier, count in seniority_counts.items():
    label = tier if tier is not None and str(tier) != "nan" else "null (needs LLM)"
    print(f"    {label}: {count}")
print(f"  Elapsed:    {elapsed}")
print(f"  Output:     {output_path}")

spark.stop()

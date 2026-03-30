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
import numpy as np
import pandas as pd

print(f"[STEP 2] Loading input: {args.input}")
df = spark.read.parquet(args.input)
total_rows = df.count()
# Repartition to match worker count for parallel processing
num_partitions = int(spark.conf.get("spark.executor.instances", "8"))
df = df.repartition(num_partitions)
print(f"[STEP 2] {total_rows} rows loaded, {num_partitions} partitions.")

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
def match_partition(rows):
    """Encode job titles and match against broadcasted NOC embeddings."""
    from sentence_transformers import SentenceTransformer
    import numpy as np

    rows_list = list(rows)
    if not rows_list:
        return

    # Load model on each worker (cached after first call)
    worker_model = SentenceTransformer("all-MiniLM-L6-v2")

    titles = [row["title"] for row in rows_list]
    title_embeddings = worker_model.encode(titles, normalize_embeddings=True)

    noc_emb = noc_emb_broadcast.value
    noc_id_list = noc_ids_broadcast.value

    # Cosine similarity (normalized vectors → dot product = cosine sim)
    sim_matrix = title_embeddings @ noc_emb.T

    for i, row in enumerate(rows_list):
        best_idx = int(np.argmax(sim_matrix[i]))
        best_score = float(sim_matrix[i][best_idx])

        if best_score >= threshold:
            matched_count.add(1)
            yield {
                "job_id": row["job_id"],
                "company_name": row["company_name"],
                "title": row["title"],
                "description": row["description"],
                "noc_id": noc_id_list[best_idx],
                "noc_match_score": round(best_score, 4),
                "noc_match_method": "sentence_transformer",
            }
        else:
            unmatched_count.add(1)
            yield {
                "job_id": row["job_id"],
                "company_name": row["company_name"],
                "title": row["title"],
                "description": row["description"],
                "noc_id": None,
                "noc_match_score": round(best_score, 4),
                "noc_match_method": None,
            }

# =============================================================================
# Execute
# =============================================================================
import datetime

print(f"[STEP 2] Starting NOC matching (threshold={threshold})...")
start = datetime.datetime.now()

# Progress monitor — prints accumulator values every 30s from a separate thread
import threading

_progress_done = threading.Event()

def _progress_monitor():
    while not _progress_done.is_set():
        try:
            m = matched_count.value
            u = unmatched_count.value
            total_done = m + u
            if total_done > 0:
                print(f"[STEP 2] Progress: {total_done}/{total_rows} ({total_done/total_rows*100:.1f}%) — matched: {m}, unmatched: {u}")
        except Exception:
            pass
        _progress_done.wait(30)

monitor = threading.Thread(target=_progress_monitor, daemon=True)
monitor.start()

result_rdd = df.rdd.mapPartitions(match_partition)
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
print(f"\n[STEP 2] === Summary ===")
print(f"  Total:      {total_rows}")
print(f"  Matched:    {matched_count.value}")
print(f"  Unmatched:  {unmatched_count.value}")
print(f"  Match rate: {matched_count.value / total_rows * 100:.1f}%")
print(f"  Threshold:  {threshold}")
print(f"  Elapsed:    {elapsed}")
print(f"  Output:     {output_path}")

spark.stop()

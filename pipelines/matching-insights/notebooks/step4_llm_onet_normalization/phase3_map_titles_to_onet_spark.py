"""
phase3_map_titles_to_onet_spark.py
-----------------------------------
LLM-based O*NET mapping for ~58,980 unique titles using Spark + Claude CLI (Opus).

Run from inside the master container:
    /opt/spark/bin/spark-submit \
        /opt/spark/notebooks/step4_llm_onet_normalization/phase3_map_titles_to_onet_spark.py

Options:
    --max-batches N   Process only N batches (for session usage testing)

Why script, not notebook:
    - All paths use container-side paths (/opt/spark/data/...)
    - Driver runs inside the container → same Docker network as workers
    - No SPARK_DRIVER_HOST or path translation needed
"""

# =============================================================================
# Args
# =============================================================================
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--max-batches', type=int, default=0,
                    help='Max batches to process (0 = all remaining)')
args = parser.parse_args()

# =============================================================================
# Config
# =============================================================================
from pathlib import Path

SPARK_MASTER   = 'spark://spark-master-mapping:7077'
APP_NAME       = 'onet-title-mapping'

INPUT_PARQUET  = Path('/opt/spark/data/processed/step4_llm_onet_normalization/phase2_extract_unique_titles/no_match_unique_titles.parquet')
TAXONOMY_CSV   = Path('/opt/spark/data/raw/onet_job_occupation_taxonomy.csv')
OUTPUT_DIR     = Path('/opt/spark/data/processed/step4_llm_onet_normalization/phase3_llm_mapping/output')
CHECKPOINT_DIR = OUTPUT_DIR / 'checkpoints'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 100
MODEL      = 'opus'

# =============================================================================
# SparkSession
# =============================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName(APP_NAME) \
    .config('spark.executor.memory', '512m') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
print(f'Spark version : {spark.version}')
print(f'Master        : {spark.sparkContext.master}')

# =============================================================================
# Load data + O*NET taxonomy
# =============================================================================
import pandas as pd
import json

df_titles = pd.read_parquet(INPUT_PARQUET)
df_taxonomy = pd.read_csv(TAXONOMY_CSV)

onet_titles = df_taxonomy['Title'].tolist()
onet_titles_json = json.dumps(onet_titles)

print(f'\nUnique titles   : {len(df_titles):,}')
print(f'O*NET categories: {len(onet_titles):,}')

# Assign batch_id
df_titles['batch_id'] = df_titles.index // BATCH_SIZE
total_batches = int(df_titles['batch_id'].max()) + 1
print(f'Total batches   : {total_batches}')

# =============================================================================
# Resume — skip already-completed batches
# =============================================================================
completed_ids = set()
for cp in CHECKPOINT_DIR.glob('batch_*.json'):
    completed_ids.add(int(cp.stem.replace('batch_', '')))

df_pending = df_titles[~df_titles['batch_id'].isin(completed_ids)].copy()
remaining_batches = df_pending['batch_id'].nunique()

print(f'\nCompleted checkpoints : {len(completed_ids)}')
print(f'Remaining batches     : {remaining_batches}')
print(f'Titles to process     : {len(df_pending):,}')

# Apply --max-batches limit
if args.max_batches > 0 and remaining_batches > args.max_batches:
    keep_ids = sorted(df_pending['batch_id'].unique())[:args.max_batches]
    df_pending = df_pending[df_pending['batch_id'].isin(keep_ids)].copy()
    print(f'\n--max-batches={args.max_batches} → processing {len(keep_ids)} batches ({len(df_pending):,} titles)')

# =============================================================================
# Broadcast O*NET taxonomy to all workers
# =============================================================================
onet_broadcast = spark.sparkContext.broadcast(onet_titles_json)

# =============================================================================
# map_partition — runs on each Spark worker
# =============================================================================
def map_partition(rows):
    """
    Receives one Spark partition (may contain multiple batch_ids due to
    repartition hash collisions). Processes each batch_id separately:
    separate Claude CLI call + separate checkpoint per batch_id.
    """
    import subprocess
    import json
    import re
    import os
    from pathlib import Path
    from itertools import groupby

    all_rows = sorted(list(rows), key=lambda r: r['batch_id'])
    if not all_rows:
        return

    for batch_id, group in groupby(all_rows, key=lambda r: r['batch_id']):
        batch = list(group)

        checkpoint = Path(
            '/opt/spark/data/processed/step4_llm_onet_normalization'
            '/phase3_llm_mapping/output/checkpoints'
            f'/batch_{batch_id}.json'
        )

        # Skip if already done (double-check inside worker)
        if checkpoint.exists():
            with open(checkpoint) as f:
                for row in json.load(f):
                    yield row
            continue

        # Build prompt
        titles_list = [row['seniority_removed_title'] for row in batch]
        onet_categories = onet_broadcast.value

        prompt = f"""Map each job title to the most relevant O*NET category.
If the input is not a recognizable job title (e.g. empty, salary info, gibberish), use null.

Job titles to map:
{json.dumps(titles_list)}

Available O*NET categories:
{onet_categories}

Return JSON only, no explanation:
{{"mappings": [{{"raw": "original title", "onet": "O*NET category or null"}}]}}
"""

        # Call Claude CLI via stdin pipe (parallel-safe, no temp file)
        # Override HOME=/tmp so Claude CLI finds credentials at /tmp/.claude
        result = subprocess.run(
            ['claude', '--print', '--model', 'opus', '-'],
            input=prompt,
            capture_output=True,
            text=True,
            env={**os.environ, 'HOME': '/tmp'}
        )

        # Parse JSON response
        mapping_dict = {}
        json_match = re.search(r'\{.*\}', result.stdout, re.DOTALL)
        if json_match:
            try:
                parsed = json.loads(json_match.group())
                for m in parsed.get('mappings', []):
                    mapping_dict[m['raw']] = m['onet']
            except (json.JSONDecodeError, KeyError):
                pass

        # Build result rows
        result_rows = []
        for row in batch:
            title = row['seniority_removed_title']
            result_rows.append({
                'batch_id': batch_id,
                'seniority_removed_title': title,
                'onet_tag': mapping_dict.get(title, 'error')
            })

        # Save checkpoint (including all-error batches to prevent infinite retry)
        # NOTE: has_valid guard was removed because persistent all-error batches
        #       (batch_253, batch_498) caused infinite retry loops.
        #       Original guard (enable if re-running from scratch):
        # has_valid = any(r['onet_tag'] != 'error' for r in result_rows)
        # if has_valid:
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        with open(checkpoint, 'w') as f:
            json.dump(result_rows, f)

        for row in result_rows:
            yield row

# =============================================================================
# Spark job
# =============================================================================
import datetime

if len(df_pending) == 0:
    print('\nAll batches already completed. Loading from checkpoints...')
else:
    pending_batches = df_pending['batch_id'].nunique()
    df_spark = spark.createDataFrame(df_pending)
    df_spark = df_spark.repartition(pending_batches, 'batch_id')

    start_time = datetime.datetime.now()
    print(f'\nStart : {start_time.strftime("%H:%M:%S")}')
    print(f'Submitting {pending_batches} batches to Spark cluster...')

    results_rdd = df_spark.rdd.mapPartitions(map_partition)
    results_rdd.collect()  # triggers execution

    elapsed = datetime.datetime.now() - start_time
    print(f'Done  : {datetime.datetime.now().strftime("%H:%M:%S")}')
    print(f'Elapsed: {elapsed}')

# =============================================================================
# Collect checkpoints → save output (parquet)
# =============================================================================
all_rows = []
for cp in sorted(CHECKPOINT_DIR.glob('batch_*.json')):
    with open(cp) as f:
        all_rows.extend(json.load(f))

df_result = pd.DataFrame(all_rows)

output_parquet = OUTPUT_DIR / 'no_match_onet_mapped.parquet'
df_result[['seniority_removed_title', 'onet_tag']] \
    .to_parquet(output_parquet, index=False)

print(f'\nSaved: {output_parquet}')

# =============================================================================
# Summary
# =============================================================================
total   = len(df_result)
mapped  = df_result['onet_tag'].notna() & (df_result['onet_tag'] != 'error')
nulls   = df_result['onet_tag'].isna()
errors  = (df_result['onet_tag'] == 'error')
unique_tags = df_result.loc[mapped, 'onet_tag'].nunique()

print(f'\n=== Mapping Summary ===')
print(f'Total titles     : {total:,}')
print(f'Mapped           : {mapped.sum():,} ({mapped.sum()/total*100:.1f}%)')
print(f'Null (not a title): {nulls.sum():,} ({nulls.sum()/total*100:.1f}%)')
print(f'Errors           : {errors.sum():,} ({errors.sum()/total*100:.1f}%)')
print(f'Unique O*NET tags: {unique_tags}')

print(f'\nTop 20 O*NET tags:')
print(df_result['onet_tag'].value_counts().head(20).to_string())

spark.stop()

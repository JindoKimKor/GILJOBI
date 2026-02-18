"""
evaluate_accuracy_all_companies_spark.py
-----------------------------------------
Full O*NET tag accuracy evaluation across all 11 companies using Spark.

Run from inside the master container:
    docker exec spark-master-eval python \
        /opt/spark/notebooks/step-5-spark-eval/evaluate_accuracy_all_companies_spark.py

Why script, not notebook:
    - All paths use container-side paths (/opt/spark/data/...)
    - Driver runs inside the container → same Docker network as workers
    - No SPARK_DRIVER_HOST or path translation needed
"""

# =============================================================================
# Config
# =============================================================================
from pathlib import Path

SPARK_MASTER   = 'spark://spark-master-eval:7077'
APP_NAME       = 'onet-accuracy-eval'

INPUT_DIR      = Path('/opt/spark/data/processed/llm_title_normalization_test/step-3-add-descriptions/top10_onet_tagged_with_desc')
OUTPUT_DIR     = Path('/opt/spark/data/processed/llm_title_normalization_test/step-5-spark-eval/all_companies_eval')
CHECKPOINT_DIR = OUTPUT_DIR / 'checkpoints'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 20
MODEL      = 'sonnet'  # claude-sonnet-4-5-20250929

# =============================================================================
# SparkSession
# =============================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName(APP_NAME) \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
print(f'Spark version : {spark.version}')
print(f'Master        : {spark.sparkContext.master}')

# =============================================================================
# Load data + assign batch_id
# =============================================================================
import pandas as pd

csv_files = sorted(INPUT_DIR.glob('*_verified.csv'))
print(f'\nFound {len(csv_files)} company files:')
for f in csv_files:
    print(f'  - {f.name}')

df_all = pd.concat(
    [pd.read_csv(f, dtype=str) for f in csv_files],
    ignore_index=True
)
df_all['description'] = df_all['description'].fillna('')
df_all['batch_id'] = df_all.index // BATCH_SIZE
total_batches = int(df_all['batch_id'].max()) + 1

print(f'\nTotal titles  : {len(df_all):,}')
print(f'Total batches : {total_batches}')
print(f'Companies     : {df_all["company_name"].nunique()}')

# =============================================================================
# Resume — skip already-completed batches
# =============================================================================
import json

completed_ids = set()
for cp in CHECKPOINT_DIR.glob('batch_*.json'):
    completed_ids.add(int(cp.stem.replace('batch_', '')))

df_pending = df_all[~df_all['batch_id'].isin(completed_ids)].copy()
print(f'\nCompleted checkpoints : {len(completed_ids)}')
print(f'Remaining batches     : {df_pending["batch_id"].nunique()}')
print(f'Titles to process     : {len(df_pending):,}')

# =============================================================================
# evaluate_partition — runs on each Spark worker
# =============================================================================
def evaluate_partition(rows):
    """
    Receives one partition (= one batch of 20 titles).
    Calls Claude CLI via stdin pipe — no temp files, parallel-safe.
    Yields result rows as dicts.
    """
    import subprocess
    import json
    import os
    from pathlib import Path

    batch = list(rows)
    if not batch:
        return

    batch_id   = batch[0]['batch_id']
    checkpoint = Path(
        '/opt/spark/data/processed/llm_title_normalization_test'
        '/step-5-spark-eval/all_companies_eval/checkpoints'
        f'/batch_{batch_id}.json'
    )

    # Skip if already done (double-check inside worker)
    if checkpoint.exists():
        with open(checkpoint) as f:
            for row in json.load(f):
                yield row
        return

    # Build numbered prompt
    numbered = [
        f"{i+1}. raw_title: {row['raw_title']}\n"
        f"   onet_tag: {row['onet_tag']}\n"
        f"   description: {row['description']}"
        for i, row in enumerate(batch)
    ]
    items_text = '\n\n'.join(numbered)

    prompt = f"""You are evaluating whether an O*NET job category is a reasonable match for a LinkedIn job posting.

For each item, evaluate if the onet_tag fits the raw_title and job description.
Verdict options:
- exact: onet_tag directly and specifically matches this role
- broader: onet_tag is a valid broader/umbrella category that includes this role
- mismatch: onet_tag does not fit this job at all

Jobs to evaluate:
{items_text}

Respond with one line per item in this exact format:
<number>|<verdict>|<reason>

Example:
1|exact|Direct match for software engineering role
2|broader|Data scientists is a broader umbrella for analytics work
3|mismatch|Operations management does not fit this technical role

Rules:
- One line per item, no extra lines
- Use | as delimiter
- reason: one plain sentence, no special characters"""

    # Call Claude CLI via stdin pipe (parallel-safe, no temp file)
    # Override HOME=/tmp so Claude CLI finds credentials at /tmp/.claude
    # (spark user home is /nonexistent; credentials are volume-mounted to /tmp/.claude)
    result = subprocess.run(
        ['claude', '--print', '--output-format', 'json', '--model', 'sonnet', '-'],
        input=prompt,
        capture_output=True,
        text=True,
        env={**os.environ, 'HOME': '/tmp'}
    )

    # Parse CLI JSON envelope
    try:
        cli_json      = json.loads(result.stdout)
        response_text = cli_json.get('result', '').strip()
        u             = cli_json.get('usage', {})
        cost_usd      = cli_json.get('total_cost_usd', 0.0)
        input_tokens  = (u.get('input_tokens', 0)
                       + u.get('cache_creation_input_tokens', 0)
                       + u.get('cache_read_input_tokens', 0))
        output_tokens = u.get('output_tokens', 0)
    except json.JSONDecodeError:
        response_text = ''
        input_tokens = output_tokens = 0
        cost_usd = 0.0

    # Parse line-based response: number|verdict|reason
    verdict_map = {}
    for line in response_text.split('\n'):
        line = line.strip()
        if not line:
            continue
        parts = line.split('|', maxsplit=2)
        if len(parts) == 3:
            try:
                idx = int(parts[0]) - 1
                verdict_map[idx] = {
                    'verdict': parts[1].strip(),
                    'reason' : parts[2].strip()
                }
            except (ValueError, IndexError):
                pass

    # Build result rows
    result_rows = []
    for i, row in enumerate(batch):
        v = verdict_map.get(i, {})
        result_rows.append({
            'batch_id'     : batch_id,
            'onet_tag'     : row['onet_tag'],
            'company_name' : row['company_name'],
            'raw_title'    : row['raw_title'],
            'verdict'      : v.get('verdict', 'error'),
            'reason'       : v.get('reason', ''),
            'input_tokens' : input_tokens,
            'output_tokens': output_tokens,
            'cost_usd'     : cost_usd
        })

    # Save checkpoint
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

    results_rdd = df_spark.rdd.mapPartitions(evaluate_partition)
    results_rdd.collect()  # triggers execution

    elapsed = datetime.datetime.now() - start_time
    print(f'Done  : {datetime.datetime.now().strftime("%H:%M:%S")}')
    print(f'Elapsed: {elapsed}')

# =============================================================================
# Collect checkpoints → save output
# =============================================================================
all_rows = []
for cp in sorted(CHECKPOINT_DIR.glob('batch_*.json')):
    with open(cp) as f:
        all_rows.extend(json.load(f))

df_eval = pd.DataFrame(all_rows)

all_output = OUTPUT_DIR / 'all_companies_eval.csv'
df_eval.drop(columns=['batch_id', 'input_tokens', 'output_tokens', 'cost_usd']) \
       .to_csv(all_output, index=False, encoding='utf-8-sig')

for company, df_company in df_eval.groupby('company_name'):
    fname = OUTPUT_DIR / f'{company.replace("/", "_")}_eval.csv'
    df_company[['onet_tag', 'company_name', 'raw_title', 'verdict', 'reason']] \
        .to_csv(fname, index=False, encoding='utf-8-sig')

print(f'\nSaved: {all_output}')

# =============================================================================
# Summary
# =============================================================================
total  = len(df_eval)
counts = df_eval['verdict'].value_counts()

print(f'\n=== Overall Accuracy ===')
print(f'Total titles  : {total:,}')
print(f'Total tokens  : {df_eval["input_tokens"].sum():,.0f} input / {df_eval["output_tokens"].sum():,.0f} output')
print(f'Total cost    : ${df_eval["cost_usd"].sum():.4f} USD')
print()
for verdict in ['exact', 'broader', 'mismatch', 'error']:
    n = counts.get(verdict, 0)
    print(f'  {verdict:<10}: {n:>4} ({n/total*100:.1f}%)')

valid = counts.get('exact', 0) + counts.get('broader', 0)
print(f'\n  Valid (exact + broader): {valid} ({valid/total*100:.1f}%)')

print('\n=== Per-Company Breakdown ===')
summary_rows = []
for company, grp in df_eval.groupby('company_name'):
    n  = len(grp)
    vc = grp['verdict'].value_counts()
    exact    = vc.get('exact', 0)
    broader  = vc.get('broader', 0)
    mismatch = vc.get('mismatch', 0)
    valid_n  = exact + broader
    summary_rows.append({
        'company'  : company,
        'total'    : n,
        'exact'    : exact,
        'broader'  : broader,
        'mismatch' : mismatch,
        'valid_pct': round(valid_n / n * 100, 1)
    })

df_summary = pd.DataFrame(summary_rows).sort_values('valid_pct', ascending=False)
df_summary.to_csv(OUTPUT_DIR / 'summary.csv', index=False, encoding='utf-8-sig')
print(df_summary.to_string(index=False))

spark.stop()

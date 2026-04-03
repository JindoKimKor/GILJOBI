"""
One-off script: Migrate old-format checkpoints to new format.

Old: unmatched_p0_co_Amazon_0.json / matched_p1_co_noc_Life_Care_32.json
New: unmatched_co_Amazon_{hash}.json / matched_co_noc_Life_Care_{hash}.json

Steps:
  1. Read all old checkpoints → dedup by job_id
  2. Re-batch using the same grouping rules as the redesigned step3_enrich_spark.py
  3. Save new format checkpoints
  4. Move old files to _old/ subfolder (not deleted)

Usage:
  cd Giljobi-DataPipeline
  py pipelines/skill-demand/scripts/migrate_checkpoints.py
"""

import json
import hashlib
import re
import shutil
from pathlib import Path
from itertools import groupby

CHECKPOINT_DIR = Path("data/processed/skill-demand/step3/checkpoints")
OLD_BACKUP_DIR = CHECKPOINT_DIR / "_old"

BATCH_UNMATCHED = 5
BATCH_MATCHED = 10


def _hash(job_ids):
    key = ",".join(str(jid) for jid in sorted(job_ids))
    return hashlib.md5(key.encode()).hexdigest()[:12]


def _safe_name(s):
    return re.sub(r'[^\w\-]', '_', s or "unknown")[:50]


def _batch_id(prefix, job_ids):
    return f"{prefix}_{_hash(job_ids)}"


# =========================================================================
# 1. Read all old checkpoints → dedup by job_id
# =========================================================================
print(f"Reading checkpoints from {CHECKPOINT_DIR}...")
all_rows = {}  # job_id → row (dedup)
old_files = []

for cp in sorted(CHECKPOINT_DIR.glob("*.json")):
    try:
        with open(cp) as f:
            data = json.load(f)
        for r in data:
            jid = r.get("job_id")
            if jid is not None:
                all_rows[jid] = r
        old_files.append(cp)
    except (json.JSONDecodeError, ValueError):
        print(f"  Corrupted (skipping): {cp.name}")
        old_files.append(cp)

print(f"  Old files: {len(old_files)}")
print(f"  Unique job_ids: {len(all_rows)}")

# =========================================================================
# 2. Split by type and re-batch
# =========================================================================
# Determine type from checkpoint filename prefix
unmatched_rows = []
matched_rows = []

for cp in sorted(CHECKPOINT_DIR.glob("*.json")):
    try:
        with open(cp) as f:
            data = json.load(f)
    except:
        continue

    is_unmatched = cp.name.startswith("unmatched_")
    for r in data:
        jid = r.get("job_id")
        if jid in all_rows and all_rows[jid] is r:  # only keep dedup winner
            pass  # we'll split by content instead

# Actually, simpler: use noc_match_method to determine type
for r in all_rows.values():
    method = r.get("noc_match_method")
    if method == "sentence_transformer":
        matched_rows.append(r)
    else:
        unmatched_rows.append(r)

print(f"  Unmatched: {len(unmatched_rows)}")
print(f"  Matched: {len(matched_rows)}")


def create_batches_matched(rows):
    batches = []
    rows_sorted = sorted(rows, key=lambda r: (r.get("company_name") or "", r.get("noc_id") or 0))
    remaining_p1 = []
    for key, group in groupby(rows_sorted, key=lambda r: (r.get("company_name") or "", r.get("noc_id") or 0)):
        group_rows = list(group)
        for i in range(0, len(group_rows), BATCH_MATCHED):
            chunk = group_rows[i:i + BATCH_MATCHED]
            if len(chunk) == BATCH_MATCHED:
                prefix = f"co_noc_{_safe_name(key[0])}"
                batches.append((_batch_id(prefix, [r["job_id"] for r in chunk]), chunk))
            else:
                remaining_p1.extend(chunk)

    remaining_p1.sort(key=lambda r: r.get("noc_id") or 0)
    remaining_p2 = []
    for noc_id, group in groupby(remaining_p1, key=lambda r: r.get("noc_id") or 0):
        group_rows = list(group)
        for i in range(0, len(group_rows), BATCH_MATCHED):
            chunk = group_rows[i:i + BATCH_MATCHED]
            if len(chunk) == BATCH_MATCHED:
                prefix = f"noc_{noc_id}"
                batches.append((_batch_id(prefix, [r["job_id"] for r in chunk]), chunk))
            else:
                remaining_p2.extend(chunk)

    for i in range(0, len(remaining_p2), BATCH_MATCHED):
        chunk = remaining_p2[i:i + BATCH_MATCHED]
        batches.append((_batch_id("rem", [r["job_id"] for r in chunk]), chunk))

    return batches


def create_batches_unmatched(rows):
    batches = []
    rows_sorted = sorted(rows, key=lambda r: r.get("company_name") or "")
    remaining = []
    for company, group in groupby(rows_sorted, key=lambda r: r.get("company_name") or ""):
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


new_matched = create_batches_matched(matched_rows)
new_unmatched = create_batches_unmatched(unmatched_rows)

print(f"\nNew batches:")
print(f"  Matched: {len(new_matched)} batches")
print(f"  Unmatched: {len(new_unmatched)} batches")

# =========================================================================
# 3. Move old files to _old/
# =========================================================================
OLD_BACKUP_DIR.mkdir(exist_ok=True)
for cp in old_files:
    shutil.move(str(cp), str(OLD_BACKUP_DIR / cp.name))
print(f"\nMoved {len(old_files)} old files to {OLD_BACKUP_DIR}")

# =========================================================================
# 4. Save new format checkpoints
# =========================================================================
saved = 0
for bid, chunk in new_unmatched:
    cp = CHECKPOINT_DIR / f"unmatched_{bid}.json"
    with open(cp, "w") as f:
        json.dump(chunk, f, default=str)
    saved += 1

for bid, chunk in new_matched:
    cp = CHECKPOINT_DIR / f"matched_{bid}.json"
    with open(cp, "w") as f:
        json.dump(chunk, f, default=str)
    saved += 1

print(f"Saved {saved} new format checkpoints")

# =========================================================================
# 5. Verify
# =========================================================================
new_job_ids = set()
for cp in CHECKPOINT_DIR.glob("*.json"):
    if cp.name == "_old":
        continue
    try:
        with open(cp) as f:
            for r in json.load(f):
                new_job_ids.add(r.get("job_id"))
    except:
        pass

print(f"\nVerification:")
print(f"  Original unique job_ids: {len(all_rows)}")
print(f"  New checkpoint job_ids:  {len(new_job_ids)}")
print(f"  Match: {'✅' if len(all_rows) == len(new_job_ids) else '❌'}")

# Sample new filenames
samples = sorted(CHECKPOINT_DIR.glob("*.json"))[:5]
print(f"\nSample new filenames:")
for s in samples:
    print(f"  {s.name}")

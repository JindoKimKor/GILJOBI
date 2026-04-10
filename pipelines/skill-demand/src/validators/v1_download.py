"""
v1_download.py — Post-download validation gate.

Validates that the downloaded CSV file:
- Exists and is not empty
- Contains data rows (not header-only)
- Has all required columns for the pipeline
"""

import os

import pandas as pd

# =============================================================================
# Config
# =============================================================================
REQUIRED_COLUMNS = ["job_id", "company_name", "title", "description"]

# =============================================================================
# Validate
# =============================================================================
def validate_download(csv_path: str) -> dict:
    """
    Validate the downloaded postings.csv file.

    Returns:
        {"valid": True, "rows": N} on success.
        {"valid": False, "error": "reason"} on failure.
    """
    if not os.path.exists(csv_path):
        return {"valid": False, "error": f"File not found: {csv_path}"}

    if os.path.getsize(csv_path) == 0:
        return {"valid": False, "error": "File is empty"}

    try:
        df = pd.read_csv(csv_path, nrows=5)
    except Exception as e:
        return {"valid": False, "error": f"Cannot read CSV: {e}"}

    if len(df) == 0:
        return {"valid": False, "error": "No data rows (header only)"}

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        return {"valid": False, "error": f"Missing required columns: {missing}"}

    # Count total rows without loading full file into memory
    # (postings.csv is 516MB — pd.read_csv causes OOM on 2GB worker)
    total = 0
    for chunk in pd.read_csv(csv_path, usecols=[0], chunksize=10000):
        total += len(chunk)

    return {"valid": True, "rows": total}

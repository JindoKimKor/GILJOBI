"""
VALIDATE stage — Check file integrity before transformation.

Each downloaded CSV must pass three checks:
1. File exists and is non-empty
2. Readable as a CSV (tries UTF-16/tab first, then UTF-8-sig/comma fallback)
3. Contains all required columns for the pipeline

Job Bank changed CSV format mid-2024: earlier files use UTF-16 + tab,
later files use UTF-8 (with BOM) + comma. The validator detects which
format each file uses and returns encoding/separator metadata so the
TRANSFORM stage can read it correctly.
"""

import os
import pandas as pd

# Columns required by the TRANSFORM and LOAD stages.
# See SPEC.md Input Schema for full column descriptions.
REQUIRED_COLUMNS = [
    "Job Title", "NOC21 Code", "NOC21 Code Name",
    "Vacancy Count", "First Posting Date",
    "Salary Minimum", "Salary Maximum", "Salary Per",
    "Province/Territory", "City",
]

# Encoding/separator combinations to try, in order.
# Job Bank CSV encoding history:
#   2023-01 ~ 2024-05: UTF-16 + tab
#   2024-06, 2024-09:  UTF-8 (BOM) + comma
#   2024-07:           Latin-1 + comma (contains 0xA0 non-breaking spaces)
CSV_FORMATS = [
    {"encoding": "utf-16", "sep": "\t"},
    {"encoding": "utf-8-sig", "sep": ","},
    {"encoding": "latin-1", "sep": ","},
]


def validate_csv(filepath: str) -> dict:
    """Validate a downloaded Job Bank CSV file.

    Tries each known CSV format (UTF-16/tab, UTF-8-sig/comma, latin-1/comma)
    until one succeeds. Returns the detected format so the TRANSFORM
    stage can reuse it without re-detecting.

    Args:
        filepath: Path to the CSV file to validate.

    Returns:
        {"valid": True, "encoding": str, "sep": str} if a format works
        and all required columns are present, or
        {"valid": False, "error": "reason"} on failure.
    """
    # Check 1: File exists and is non-empty
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return {"valid": False, "error": "File is empty"}

    # Check 2: Try each known format
    df = None
    matched_fmt = None
    for fmt in CSV_FORMATS:
        try:
            df = pd.read_csv(filepath, encoding=fmt["encoding"], sep=fmt["sep"], nrows=1)
            matched_fmt = fmt
            break
        except Exception:
            continue

    if df is None:
        return {"valid": False, "error": "Not readable as any known CSV format"}

    # Check 3: All required columns present
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        return {"valid": False, "error": f"Missing columns: {missing}"}

    return {"valid": True, "encoding": matched_fmt["encoding"], "sep": matched_fmt["sep"]}

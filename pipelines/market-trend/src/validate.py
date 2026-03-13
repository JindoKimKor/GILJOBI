"""
VALIDATE stage — Check file integrity (UTF-16, non-empty, expected columns).
"""

import os
import pandas as pd

REQUIRED_COLUMNS = [
    "Job Title", "NOC21 Code", "NOC21 Code Name",
    "Vacancy Count", "First Posting Date",
    "Salary Minimum", "Salary Maximum", "Salary Per",
    "Province/Territory", "City",
]


def validate_csv(filepath: str) -> dict:
    """
    Validate a downloaded Job Bank CSV file.
    Returns {"valid": True} or {"valid": False, "error": "reason"}.
    """
    # Check non-empty
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return {"valid": False, "error": "File is empty"}

    # Check UTF-16 tab-separated readable
    try:
        df = pd.read_csv(filepath, encoding="utf-16", sep="\t", nrows=1)
    except Exception:
        return {"valid": False, "error": "Not readable as UTF-16 tab-separated CSV"}

    # Check required columns
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        return {"valid": False, "error": f"Missing columns: {missing}"}

    return {"valid": True}

"""
v3_normalize.py — Post-normalize validation gate.

Validates NOC normalization results:
- Splits DataFrame into matched vs sub-threshold rows
- Reports match rate and average score statistics
"""

import numpy as np
import pandas as pd


# =============================================================================
# Split
# =============================================================================
def split_by_match(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split DataFrame into matched and sub-threshold rows.

    Args:
        df: DataFrame with noc_id column from step2.

    Returns:
        (matched, sub_threshold) — two DataFrames.
        matched: rows where noc_id is not null.
        sub_threshold: rows where noc_id is null (for LLM fallback).
    """
    matched = df[df["noc_id"].notna()].reset_index(drop=True)
    sub_threshold = df[df["noc_id"].isna()].reset_index(drop=True)
    return matched, sub_threshold


# =============================================================================
# Stats
# =============================================================================
def validate_normalize(df: pd.DataFrame) -> dict:
    """
    Compute match rate statistics after NOC normalization.

    Args:
        df: DataFrame with noc_id and noc_match_score columns.

    Returns:
        Dict with total, matched, unmatched, match_rate, avg_score.
    """
    total = len(df)
    matched = int(df["noc_id"].notna().sum())
    unmatched = total - matched

    match_rate = matched / total if total > 0 else 0.0

    scores = df["noc_match_score"].dropna()
    avg_score = float(scores.mean()) if len(scores) > 0 else 0.0

    return {
        "total": total,
        "matched": matched,
        "unmatched": unmatched,
        "match_rate": match_rate,
        "avg_score": avg_score,
    }

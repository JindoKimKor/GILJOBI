"""
v4_fallback.py — Post-fallback validation gate.

Validates NOC mapping after LLM fallback:
- Completion rate (mapped vs unmapped)
- Method breakdown (sentence_transformer vs llm counts)
"""

import pandas as pd


# =============================================================================
# Validate
# =============================================================================
def validate_fallback(df: pd.DataFrame) -> dict:
    """
    Compute NOC mapping completion stats after LLM fallback.

    Args:
        df: DataFrame with noc_id and noc_match_method columns.

    Returns:
        Dict with total, mapped, unmapped, completion_rate, by_method.
    """
    total = len(df)
    mapped = int(df["noc_id"].notna().sum())
    unmapped = total - mapped
    completion_rate = mapped / total if total > 0 else 0.0

    # Count by method
    by_method = {}
    if mapped > 0:
        method_counts = df.loc[df["noc_match_method"].notna(), "noc_match_method"].value_counts()
        by_method = method_counts.to_dict()

    return {
        "total": total,
        "mapped": mapped,
        "unmapped": unmapped,
        "completion_rate": completion_rate,
        "by_method": by_method,
    }

"""
v5_enrich.py — Post-enrich validation gate.

Validates seniority + skills extraction results:
- Seniority values in allowed set
- Skills arrays not empty
- Average skills per posting
"""

import pandas as pd

from src.step4_extract import SENIORITY_TIERS


# =============================================================================
# Validate
# =============================================================================
def validate_enrich(df: pd.DataFrame) -> dict:
    """
    Compute enrichment stats after LLM seniority + skill extraction.

    Args:
        df: DataFrame with seniority and skills columns.

    Returns:
        Dict with total, valid/null/invalid seniority counts,
        with_skills, empty_skills, avg_skills_per_posting.
    """
    total = len(df)

    if total == 0:
        return {
            "total": 0,
            "valid_seniority": 0,
            "null_seniority": 0,
            "invalid_seniority": 0,
            "with_skills": 0,
            "empty_skills": 0,
            "avg_skills_per_posting": 0.0,
        }

    # Seniority stats
    null_seniority = int(df["seniority"].isna().sum())
    non_null = df["seniority"].dropna()
    valid_seniority = int(non_null.isin(SENIORITY_TIERS).sum())
    invalid_seniority = len(non_null) - valid_seniority

    # Skills stats
    skill_counts = df["skills"].apply(lambda x: len(x) if isinstance(x, list) else 0)
    empty_skills = int((skill_counts == 0).sum())
    with_skills = total - empty_skills
    avg_skills = float(skill_counts.mean())

    return {
        "total": total,
        "valid_seniority": valid_seniority,
        "null_seniority": null_seniority,
        "invalid_seniority": invalid_seniority,
        "with_skills": with_skills,
        "empty_skills": empty_skills,
        "avg_skills_per_posting": avg_skills,
    }

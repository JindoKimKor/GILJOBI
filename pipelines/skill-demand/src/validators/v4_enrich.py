"""
v4_enrich.py — Post-enrich validation gate.

Validates Step 3 output: NOC completion + seniority + skills.
Combined from v4_fallback.py (NOC) + v5_enrich.py (seniority + skills).
"""

import pandas as pd

from src.step2_seniority import SENIORITY_TIERS


# =============================================================================
# Validate
# =============================================================================
def validate_enrich(df: pd.DataFrame) -> dict:
    """
    Compute enrichment stats after Step 3 LLM processing.

    Args:
        df: DataFrame with noc_id, noc_match_method, seniority, skills columns.

    Returns:
        Dict with NOC, seniority, and skills statistics.
    """
    total = len(df)

    if total == 0:
        return {
            "total": 0,
            "noc_mapped": 0, "noc_unmapped": 0, "noc_completion_rate": 0.0,
            "noc_by_method": {},
            "valid_seniority": 0, "null_seniority": 0, "invalid_seniority": 0,
            "with_skills": 0, "empty_skills": 0, "avg_skills_per_posting": 0.0,
        }

    # --- NOC ---
    noc_mapped = int(df["noc_id"].notna().sum())
    noc_unmapped = total - noc_mapped
    noc_completion_rate = noc_mapped / total

    noc_by_method = {}
    if noc_mapped > 0:
        method_counts = df.loc[df["noc_match_method"].notna(), "noc_match_method"].value_counts()
        noc_by_method = method_counts.to_dict()

    # --- Seniority ---
    null_seniority = int(df["seniority"].isna().sum())
    non_null = df["seniority"].dropna()
    valid_seniority = int(non_null.isin(SENIORITY_TIERS).sum())
    invalid_seniority = len(non_null) - valid_seniority

    # --- Skills ---
    skill_counts = df["skills"].apply(lambda x: len(x) if hasattr(x, '__len__') else 0)
    empty_skills = int((skill_counts == 0).sum())
    with_skills = total - empty_skills
    avg_skills = float(skill_counts.mean())

    return {
        "total": total,
        "noc_mapped": noc_mapped,
        "noc_unmapped": noc_unmapped,
        "noc_completion_rate": noc_completion_rate,
        "noc_by_method": noc_by_method,
        "valid_seniority": valid_seniority,
        "null_seniority": null_seniority,
        "invalid_seniority": invalid_seniority,
        "with_skills": with_skills,
        "empty_skills": empty_skills,
        "avg_skills_per_posting": avg_skills,
    }

"""
TRANSFORM stage — Salary normalization, outlier filtering, NOC21 code lookup.
"""

import pandas as pd

SALARY_DIVISORS = {
    "Hour": 1,
    "Day": 8,
    "Week": 40,
    "Bi-weekly": 80,
    "Month": 173.33,
    "Year": 2080,
}


def normalize_salary_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Salary Minimum/Maximum to hourly rate based on Salary Per unit.
    Null Salary Per → null salary fields.
    """
    result = df.copy()
    result["salary_min_hourly"] = None
    result["salary_max_hourly"] = None

    for unit, divisor in SALARY_DIVISORS.items():
        mask = result["Salary Per"] == unit
        result.loc[mask, "salary_min_hourly"] = result.loc[mask, "Salary Minimum"] / divisor
        result.loc[mask, "salary_max_hourly"] = result.loc[mask, "Salary Maximum"] / divisor

    return result


def apply_outlier_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hourly rate < $10 or > $500 → set to NULL.
    """
    result = df.copy()

    for col in ["salary_min_hourly", "salary_max_hourly"]:
        numeric = pd.to_numeric(result[col], errors="coerce")
        outlier = (numeric < 10) | (numeric > 500)
        result.loc[outlier, col] = None

    return result


def map_noc_ids(df: pd.DataFrame, noc_lookup: dict) -> pd.DataFrame:
    """
    Map NOC21 Code to noc_titles.id via lookup dictionary.
    Missing/unknown codes → null.
    """
    result = df.copy()
    result["noc_id"] = result["NOC21 Code"].map(noc_lookup)
    return result


def prepare_job_postings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and rename columns to match job_postings table schema.
    Input: raw DataFrame with source columns + noc_id, salary_*_hourly from prior stages.
    Output: DataFrame with only the DB-ready columns.
    """
    result = df.rename(columns={
        "Job Title": "normalized_title",
        "Vacancy Count": "vacancy_count",
        "Province/Territory": "province",
        "City": "city",
        "First Posting Date": "first_posting_date",
    })

    result["first_posting_date"] = pd.to_datetime(result["first_posting_date"])

    return result[[
        "noc_id", "normalized_title", "vacancy_count",
        "province", "city", "first_posting_date",
        "salary_min_hourly", "salary_max_hourly",
    ]]

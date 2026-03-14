"""
TRANSFORM stage — Salary normalization, outlier filtering, NOC21 code lookup.

Transforms raw Job Bank CSV data into the job_postings DB schema:
1. Normalize mixed-unit salaries (Hour/Day/Week/etc.) to a single hourly rate
2. Filter outlier hourly rates (< $10 or > $500) to NULL
3. Map NOC21 codes to noc_titles.id foreign keys
4. Select and rename columns to match the DB schema
"""

import pandas as pd


# ============================================================
# Constants
# ============================================================

# Divisors to convert each salary unit to hourly rate.
# Based on standard full-time assumptions: 8h/day, 40h/week, 52 weeks/year.
SALARY_DIVISORS = {
    "Hour": 1,          # Already hourly
    "Day": 8,           # 8 hours/day
    "Week": 40,         # 40 hours/week
    "Bi-weekly": 80,    # 40 hours/week x 2 weeks
    "Month": 173.33,    # 2,080 hours/year / 12 months
    "Year": 2080,       # 40 hours/week x 52 weeks
}


# ============================================================
# Salary Normalization
# ============================================================

def normalize_salary_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Salary Minimum/Maximum to hourly rate based on Salary Per unit.

    For each known salary unit, divides the salary values by the
    corresponding divisor. Rows with null Salary Per get null
    hourly values (approximately 69 rows/month in the dataset).

    Args:
        df: DataFrame with 'Salary Minimum', 'Salary Maximum',
            and 'Salary Per' columns.

    Returns:
        Copy of df with added 'salary_min_hourly' and 'salary_max_hourly' columns.
    """
    result = df.copy()
    result["salary_min_hourly"] = None
    result["salary_max_hourly"] = None

    for unit, divisor in SALARY_DIVISORS.items():
        mask = result["Salary Per"] == unit
        result.loc[mask, "salary_min_hourly"] = result.loc[mask, "Salary Minimum"] / divisor
        result.loc[mask, "salary_max_hourly"] = result.loc[mask, "Salary Maximum"] / divisor

    return result


# ============================================================
# Outlier Filtering
# ============================================================

def apply_outlier_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Set outlier hourly rates to NULL.

    Hourly rates below $10 or above $500 are considered data errors
    and set to NULL. Affects approximately 0.3% of records.
    Boundary values ($10 and $500) are kept as valid.

    Args:
        df: DataFrame with 'salary_min_hourly' and 'salary_max_hourly' columns.

    Returns:
        Copy of df with outlier salary values replaced by None.
    """
    result = df.copy()

    for col in ["salary_min_hourly", "salary_max_hourly"]:
        numeric = pd.to_numeric(result[col], errors="coerce")
        outlier = (numeric < 10) | (numeric > 500)
        result.loc[outlier, col] = None

    return result


# ============================================================
# NOC Mapping
# ============================================================

def map_noc_ids(df: pd.DataFrame, noc_lookup: dict) -> pd.DataFrame:
    """Map NOC21 codes to noc_titles.id foreign keys.

    Uses a pre-built lookup dictionary from the noc_titles table.
    Rows with missing or unknown NOC21 codes get null noc_id
    (approximately 0.7% of records have no classification).

    Args:
        df: DataFrame with 'NOC21 Code' column.
        noc_lookup: Mapping of {noc21_code: noc_titles.id}.

    Returns:
        Copy of df with added 'noc_id' column.
    """
    result = df.copy()
    # CSV reads NOC21 Code as float64 (because some rows have NaN),
    # so values like 21232 become 21232.0. Convert to int-string to match
    # the VARCHAR noc21_code in DB (e.g., 21232.0 → "21232").
    # NaN rows produce "nan" which won't match any key → null noc_id.
    codes = result["NOC21 Code"]
    result["noc_id"] = codes.dropna().astype(int).astype(str).map(noc_lookup)
    return result


# ============================================================
# Column Selection
# ============================================================

def prepare_job_postings(df: pd.DataFrame) -> pd.DataFrame:
    """Select and rename columns to match the job_postings DB schema.

    Takes a DataFrame that has been through all prior transform stages
    (normalize, outlier filter, NOC mapping) and produces the final
    DataFrame ready for DB insertion. Drops rows with null job titles
    (~14 out of 3M records) to satisfy the DB NOT NULL constraint.

    Args:
        df: DataFrame with source columns plus noc_id and salary_*_hourly
            from prior transform stages.

    Returns:
        DataFrame with exactly the 8 columns matching the job_postings
        table schema: noc_id, normalized_title, vacancy_count, province,
        city, first_posting_date, salary_min_hourly, salary_max_hourly.
    """
    result = df.rename(columns={
        "Job Title": "normalized_title",
        "Vacancy Count": "vacancy_count",
        "Province/Territory": "province",
        "City": "city",
        "First Posting Date": "first_posting_date",
    })

    result["first_posting_date"] = pd.to_datetime(result["first_posting_date"])

    # Drop rows with incomplete NOC classification (~0.8% of records).
    # Rows missing any of the 4 NOC columns are unclassified by Job Bank,
    # often with unnormalized titles (e.g., raw employer descriptions).
    noc_source_cols = ["NOC21 Code", "NOC21 Code Name", "NOC 2016 Code", "NOC 2016 Code Name"]
    result = result.dropna(subset=noc_source_cols)

    # Drop rows with no job title (DB NOT NULL constraint; ~14 out of 3M)
    result = result.dropna(subset=["normalized_title"])

    # Drop rows missing province or city (~1.4% of records)
    result = result.dropna(subset=["province", "city"])

    return result[[
        "noc_id", "normalized_title", "vacancy_count",
        "province", "city", "first_posting_date",
        "salary_min_hourly", "salary_max_hourly",
    ]]

"""
Pre-requisite — Download NOC 2021 master CSV and prepare noc_titles data.

Statistics Canada publishes an official NOC 2021 classification CSV with 823 rows
across 5 hierarchy levels. This module filters to Level 5 (Unit Group) rows — the
5-digit codes used in Job Bank data — and reshapes them for the noc_titles table.

Source: https://www.statcan.gc.ca/en/subjects/standard/noc/2021/indexV1
"""

import pandas as pd

# Official Statistics Canada NOC 2021 classification structure CSV
NOC_MASTER_URL = (
    "https://www.statcan.gc.ca/en/subjects/standard/noc/2021/"
    "indexV1/noc-2021-v1.0-classification-structure.csv"
)


def filter_unit_groups(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to Level 5 (Unit Group) rows only.

    The NOC hierarchy has 5 levels:
        1=Broad Category, 2=Major Group, 3=Sub-major Group,
        4=Minor Group, 5=Unit Group.
    Only Level 5 rows have the 5-digit codes used in Job Bank data.

    Args:
        df: Raw NOC master CSV DataFrame with a 'Level' column.

    Returns:
        DataFrame containing only Level 5 rows, index reset.
    """
    return df[df["Level"] == 5].reset_index(drop=True)


def prepare_noc_titles(df: pd.DataFrame) -> pd.DataFrame:
    """Reshape NOC master data to match the noc_titles DB schema.

    Filters to Unit Groups, renames columns to match the DB schema
    (noc21_code, noc21_name), and drops rows with null codes.

    Args:
        df: Raw NOC master CSV DataFrame (all hierarchy levels).

    Returns:
        DataFrame with columns ['noc21_code', 'noc21_name'],
        ready for DB insertion.
    """
    unit_groups = filter_unit_groups(df)

    result = unit_groups[["Code - NOC 2021 V1.0", "Class title"]].copy()
    result = result.rename(columns={
        "Code - NOC 2021 V1.0": "noc21_code",
        "Class title": "noc21_name",
    })

    # Drop rows where noc21_code is null (data quality safeguard)
    result = result.dropna(subset=["noc21_code"]).reset_index(drop=True)

    return result

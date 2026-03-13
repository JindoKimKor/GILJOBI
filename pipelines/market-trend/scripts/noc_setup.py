"""
Pre-requisite — Download NOC 2021 master CSV and prepare noc_titles data.
"""

import pandas as pd

NOC_MASTER_URL = (
    "https://www.statcan.gc.ca/en/subjects/standard/noc/2021/"
    "indexV1/noc-2021-v1.0-classification-structure.csv"
)


def filter_unit_groups(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only Level 5 (Unit Group) rows."""
    return df[df["Level"] == 5].reset_index(drop=True)


def prepare_noc_titles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter to Unit Groups and reshape to match noc_titles schema.
    Returns DataFrame with columns: noc21_code, noc21_name
    """
    unit_groups = filter_unit_groups(df)

    result = unit_groups[["Code - NOC 2021 V1.0", "Class title"]].copy()
    result = result.rename(columns={
        "Code - NOC 2021 V1.0": "noc21_code",
        "Class title": "noc21_name",
    })

    result = result.dropna(subset=["noc21_code"]).reset_index(drop=True)

    return result

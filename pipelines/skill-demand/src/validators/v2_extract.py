"""
v2_extract.py — Post-extract validation gate.

Validates the extracted DataFrame:
- Drops rows with null title or description
- Filters out descriptions shorter than MIN_DESCRIPTION_LENGTH
"""

import pandas as pd

# =============================================================================
# Config
# =============================================================================
MIN_DESCRIPTION_LENGTH = 50

# =============================================================================
# Validate
# =============================================================================
def validate_extract(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the extracted DataFrame.

    Args:
        df: DataFrame with job_id, company_name, title, description columns.

    Returns:
        Cleaned DataFrame with nulls dropped and short descriptions
        filtered out. Index is reset.
    """
    # Drop null title or description
    df = df.dropna(subset=["title", "description"])

    # Filter short descriptions
    if len(df) > 0:
        df = df[df["description"].str.len() >= MIN_DESCRIPTION_LENGTH]

    # Reset index
    df = df.reset_index(drop=True)

    return df

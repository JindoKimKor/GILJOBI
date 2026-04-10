"""
step1_select_columns.py — Extract required columns from raw LinkedIn CSV.

Selects job_id, company_name, title, description, formatted_experience_level
from postings.csv. Drops rows with null title or description.
Saves output as parquet to processed/step1/.
"""

import os
from pathlib import Path

import pandas as pd

# =============================================================================
# Config
# =============================================================================
REQUIRED_COLUMNS = ["job_id", "company_name", "title", "description", "formatted_experience_level"]
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "processed" / "skill-demand" / "step1"

# =============================================================================
# Select Columns
# =============================================================================
def select_columns(csv_path: str) -> pd.DataFrame:
    """
    Read raw CSV and return only the required columns.

    Args:
        csv_path: Path to the raw postings.csv file.

    Returns:
        DataFrame with REQUIRED_COLUMNS only, null title/description rows dropped.

    Raises:
        ValueError: If any required column is missing from the CSV.
    """
    # Read header first to validate columns (no data loaded)
    header = pd.read_csv(csv_path, nrows=0)
    missing = [col for col in REQUIRED_COLUMNS if col not in header.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Read in chunks to avoid OOM on large files (516MB postings.csv)
    chunks = []
    for chunk in pd.read_csv(csv_path, usecols=REQUIRED_COLUMNS, chunksize=50000):
        chunk = chunk.dropna(subset=["title", "description"])
        chunks.append(chunk)

    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=REQUIRED_COLUMNS)
    return df


def run(csv_path: str, output_dir: Path = DEFAULT_OUTPUT_DIR) -> str:
    """
    Extract columns and save as parquet.

    Args:
        csv_path: Path to the raw postings.csv file.
        output_dir: Directory to write the output parquet.

    Returns:
        Path to the saved parquet file.
    """
    df = select_columns(csv_path)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "step1_extracted.parquet"
    df.to_parquet(output_path, index=False)

    print(f"[STEP 1] {len(df)} rows → {output_path}")
    return str(output_path)

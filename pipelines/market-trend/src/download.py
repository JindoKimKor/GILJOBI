"""
DOWNLOAD stage — Fetch monthly CSVs to data/raw/market-trend/YYYY-MM.csv.
"""

import os


def build_download_plan(urls: dict[str, str], output_dir: str) -> dict[str, str]:
    """
    Given {YYYY-MM: url} mapping and output directory,
    return only the entries that don't already exist on disk.
    """
    plan = {}
    for year_month, url in urls.items():
        filepath = os.path.join(output_dir, f"{year_month}.csv")
        if not os.path.exists(filepath):
            plan[year_month] = url
    return plan


def save_csv(content: bytes, year_month: str, output_dir: str) -> str:
    """
    Save raw CSV bytes to output_dir/YYYY-MM.csv.
    Creates output_dir if it doesn't exist.
    Returns the filepath.
    """
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, f"{year_month}.csv")
    with open(filepath, "wb") as f:
        f.write(content)
    return filepath

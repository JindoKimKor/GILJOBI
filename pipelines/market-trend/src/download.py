"""
DOWNLOAD stage — Fetch monthly CSVs to data/raw/market-trend/.

Downloads Job Bank CSV files and saves them with normalized YYYY-MM.csv naming.
Supports idempotent re-runs by skipping files that already exist on disk.
"""

import os


# ============================================================
# Download Planning
# ============================================================

def build_download_plan(urls: dict[str, str], output_dir: str) -> dict[str, str]:
    """Determine which files still need to be downloaded.

    Compares the URL mapping against files already on disk to
    support idempotent re-runs — only new months get downloaded.

    Args:
        urls: Mapping of {YYYY-MM: download_url} for all available months.
        output_dir: Directory where CSVs are saved (e.g., data/raw/market-trend/).

    Returns:
        Filtered mapping containing only months not yet on disk.
    """
    plan = {}
    for year_month, url in urls.items():
        filepath = os.path.join(output_dir, f"{year_month}.csv")
        if not os.path.exists(filepath):
            plan[year_month] = url
    return plan


# ============================================================
# File I/O
# ============================================================

def save_csv(content: bytes, year_month: str, output_dir: str) -> str:
    """Save raw CSV bytes to disk with normalized naming.

    Creates the output directory if it doesn't exist.
    File is saved as YYYY-MM.csv (e.g., 2023-01.csv).

    Args:
        content: Raw file bytes from the download.
        year_month: Date identifier in 'YYYY-MM' format.
        output_dir: Directory to save the file in.

    Returns:
        Absolute filepath of the saved file.
    """
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, f"{year_month}.csv")
    with open(filepath, "wb") as f:
        f.write(content)
    return filepath

"""
download.py — Download LinkedIn Job Postings dataset from Kaggle API.

Fetches the arshkon/linkedin-job-postings dataset as a zip archive,
extracts it to data/raw/skill-demand/. Skips if already downloaded.

Usage (standalone):
    py download.py
    py download.py --output data/raw/skill-demand
    py download.py --force   # re-download even if exists

Usage (Airflow):
    Imported by skill_demand_dag.py as a BashOperator or @task.
"""

# =============================================================================
# Imports
# =============================================================================
import os
import subprocess
import zipfile
from pathlib import Path

# =============================================================================
# Config
# =============================================================================
KAGGLE_DATASET = "arshkon/linkedin-job-postings"
KAGGLE_URL = f"https://www.kaggle.com/api/v1/datasets/download/{KAGGLE_DATASET}"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "skill-demand"
EXPECTED_FILE = "postings.csv"

# =============================================================================
# Download
# =============================================================================
def download_dataset(output_dir: Path = DEFAULT_OUTPUT_DIR, force: bool = False) -> Path:
    """
    Download and extract the Kaggle dataset.

    Returns the path to the output directory containing extracted CSVs.
    Skips download if EXPECTED_FILE already exists (unless force=True).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    marker = output_dir / EXPECTED_FILE

    if marker.exists() and not force:
        print(f"Already downloaded: {marker}")
        return output_dir

    zip_path = output_dir / "linkedin-job-postings.zip"

    # Download via curl (Kaggle API requires ~/.kaggle/kaggle.json)
    print(f"Downloading {KAGGLE_DATASET} ...")
    result = subprocess.run(
        ["curl", "-L", "-o", str(zip_path), KAGGLE_URL],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Download failed: {result.stderr}")

    if not zip_path.exists() or zip_path.stat().st_size < 1_000:
        raise RuntimeError(f"Download file too small or missing: {zip_path}")

    # Extract
    print(f"Extracting to {output_dir} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(output_dir)

    # Verify
    if not marker.exists():
        raise RuntimeError(f"Expected file not found after extraction: {marker}")

    # Clean up zip
    zip_path.unlink()
    print(f"Done. Files at: {output_dir}")

    return output_dir


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download LinkedIn Job Postings from Kaggle")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory")
    parser.add_argument("--force", action="store_true", help="Re-download even if exists")
    args = parser.parse_args()

    download_dataset(output_dir=args.output, force=args.force)

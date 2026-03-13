"""
Tests for market-trend pipeline DOWNLOAD stage.
Based on SPEC.md — Download to data/raw/market-trend/YYYY-MM.csv, idempotent re-runs.
"""

import os
import tempfile

import pytest

from src.download import build_download_plan, save_csv


# ============================================================
# Download Plan (which files to download)
# ============================================================

class TestBuildDownloadPlan:
    """SPEC: Skip files already present in data/raw/market-trend/ (idempotent re-runs)."""

    def test_all_files_needed_when_dir_empty(self):
        urls = {
            "2023-01": "https://example.com/jan2023.csv",
            "2023-02": "https://example.com/feb2023.csv",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            plan = build_download_plan(urls, tmpdir)
            assert len(plan) == 2

    def test_skips_existing_files(self):
        urls = {
            "2023-01": "https://example.com/jan2023.csv",
            "2023-02": "https://example.com/feb2023.csv",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            # Simulate existing file
            with open(os.path.join(tmpdir, "2023-01.csv"), "w") as f:
                f.write("data")
            plan = build_download_plan(urls, tmpdir)
            assert len(plan) == 1
            assert "2023-02" in plan

    def test_empty_urls_returns_empty_plan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            plan = build_download_plan({}, tmpdir)
            assert len(plan) == 0


# ============================================================
# Save CSV
# ============================================================

class TestSaveCsv:
    """SPEC: Download to data/raw/market-trend/YYYY-MM.csv (normalized naming)."""

    def test_saves_with_correct_filename(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            content = b"col1\tcol2\nval1\tval2"
            save_csv(content, "2023-01", tmpdir)
            filepath = os.path.join(tmpdir, "2023-01.csv")
            assert os.path.exists(filepath)

    def test_saved_content_matches(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            content = b"col1\tcol2\nval1\tval2"
            save_csv(content, "2023-01", tmpdir)
            filepath = os.path.join(tmpdir, "2023-01.csv")
            with open(filepath, "rb") as f:
                assert f.read() == content

    def test_creates_output_dir_if_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = os.path.join(tmpdir, "sub", "dir")
            save_csv(b"data", "2023-01", nested)
            assert os.path.exists(os.path.join(nested, "2023-01.csv"))

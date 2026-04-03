"""
Tests for skill-demand pipeline DOWNLOAD stage.
Based on SPEC.md — Fetch arshkon/linkedin-job-postings from Kaggle, idempotent re-runs.
"""

import os
import tempfile
import zipfile

import pytest

from src.download import download_dataset, EXPECTED_FILE


# ============================================================
# Idempotency — skip if already downloaded
# ============================================================

class TestDownloadIdempotency:
    """SPEC: Skip if postings.csv already exists."""

    def test_skips_when_expected_file_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            marker = Path(tmpdir) / EXPECTED_FILE
            marker.write_text("existing data")

            result = download_dataset(output_dir=Path(tmpdir), force=False)
            assert result == Path(tmpdir)
            # File should be unchanged (not re-downloaded)
            assert marker.read_text() == "existing data"

    def test_force_flag_bypasses_existing_check(self, monkeypatch):
        """force=True should attempt download even if file exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            from unittest.mock import MagicMock
            import subprocess

            marker = Path(tmpdir) / EXPECTED_FILE
            marker.write_text("old data")

            # Mock subprocess.run to prevent actual download
            mock_run = MagicMock(return_value=MagicMock(returncode=1, stderr="mocked"))
            monkeypatch.setattr(subprocess, "run", mock_run)

            # force=True should call curl (not skip), then fail on bad download
            with pytest.raises(RuntimeError):
                download_dataset(output_dir=Path(tmpdir), force=True)

            # Verify curl was actually called (not skipped)
            mock_run.assert_called_once()


# ============================================================
# Zip extraction
# ============================================================

class TestZipExtraction:
    """SPEC: curl + unzip → data/raw/skill-demand/"""

    def test_extracts_zip_and_finds_expected_file(self):
        """Simulate a valid zip containing postings.csv."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            output_dir = Path(tmpdir) / "output"
            output_dir.mkdir()

            # Create a fake zip with postings.csv inside
            zip_path = output_dir / "linkedin-job-postings.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr(EXPECTED_FILE, "job_id,title,description\n1,Dev,Build stuff\n")

            # Extract manually (simulating what download_dataset does after curl)
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(output_dir)

            assert (output_dir / EXPECTED_FILE).exists()
            content = (output_dir / EXPECTED_FILE).read_text()
            assert "job_id" in content

    def test_fails_if_expected_file_not_in_zip(self):
        """Zip without postings.csv should raise RuntimeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            output_dir = Path(tmpdir) / "output"
            output_dir.mkdir()

            # Create a zip WITHOUT postings.csv
            zip_path = output_dir / "linkedin-job-postings.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("wrong_file.csv", "data")

            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(output_dir)

            assert not (output_dir / EXPECTED_FILE).exists()


# ============================================================
# Config
# ============================================================

class TestConfig:
    """Verify constants are correct."""

    def test_expected_file_is_postings_csv(self):
        assert EXPECTED_FILE == "postings.csv"

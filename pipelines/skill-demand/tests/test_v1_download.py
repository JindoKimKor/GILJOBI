"""
Tests for skill-demand pipeline validators.
Based on SPEC.md — V1 (post-download) validation gate.

RED phase: These tests should FAIL until src/validators/v1_download.py is implemented.
"""

import os
import tempfile

import pandas as pd
import pytest

from src.validators.v1_download import validate_download, REQUIRED_COLUMNS


# ============================================================
# V1: Post-Download Validation
# ============================================================

class TestValidateDownload:
    """SPEC V1: File integrity check, required columns exist."""

    def test_valid_file_passes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [1],
                "company_name": ["A"],
                "title": ["SWE"],
                "description": ["Build stuff"],
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            result = validate_download(path)
            assert result["valid"] is True

    def test_fails_if_file_not_found(self):
        result = validate_download("/nonexistent/path/postings.csv")
        assert result["valid"] is False
        assert "not found" in result["error"].lower()

    def test_fails_if_file_is_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "postings.csv")
            with open(path, "w") as f:
                f.write("")
            result = validate_download(path)
            assert result["valid"] is False
            assert "empty" in result["error"].lower()

    def test_fails_if_header_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "postings.csv")
            with open(path, "w") as f:
                f.write("job_id,company_name,title,description\n")
            result = validate_download(path)
            assert result["valid"] is False
            assert "no data" in result["error"].lower()

    def test_fails_if_required_column_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [1],
                "company_name": ["A"],
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            result = validate_download(path)
            assert result["valid"] is False
            assert "title" in result["error"].lower()

    def test_passes_with_extra_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [1],
                "company_name": ["A"],
                "title": ["SWE"],
                "description": ["Build stuff"],
                "max_salary": [100000],
                "location": ["NYC"],
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            result = validate_download(path)
            assert result["valid"] is True

    def test_returns_row_count_on_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [1, 2, 3],
                "company_name": ["A", "B", "C"],
                "title": ["SWE", "DS", "PM"],
                "description": ["d1", "d2", "d3"],
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            result = validate_download(path)
            assert result["valid"] is True
            assert result["rows"] == 3

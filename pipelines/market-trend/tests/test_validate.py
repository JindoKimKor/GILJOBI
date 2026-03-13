"""
Tests for market-trend pipeline VALIDATE stage.
Based on SPEC.md — File integrity checks.
"""

import os
import tempfile

import pytest

from src.validate import validate_csv

REQUIRED_COLUMNS = [
    "Job Title", "NOC21 Code", "NOC21 Code Name",
    "Vacancy Count", "First Posting Date",
    "Salary Minimum", "Salary Maximum", "Salary Per",
    "Province/Territory", "City",
]


# ============================================================
# File Validation
# ============================================================

class TestValidateCsv:
    """SPEC: Each downloaded file must be non-empty and readable as UTF-16 tab-separated CSV."""

    def _write_utf16_tsv(self, tmpdir, filename, header, rows=None):
        """Helper to create a UTF-16 tab-separated file."""
        lines = [header]
        if rows:
            lines.extend(rows)
        content = "\n".join(lines)
        filepath = os.path.join(tmpdir, filename)
        with open(filepath, "w", encoding="utf-16") as f:
            f.write(content)
        return filepath

    def test_valid_file_passes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            header = "\t".join(REQUIRED_COLUMNS)
            row = "\t".join(["Engineer", "21232", "Software developers", "1",
                             "2023/01/15", "25.00", "35.00", "Hour",
                             "Ontario", "Toronto"])
            filepath = self._write_utf16_tsv(tmpdir, "2023-01.csv", header, [row])
            result = validate_csv(filepath)
            assert result["valid"] is True

    def test_empty_file_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "empty.csv")
            with open(filepath, "w") as f:
                pass  # empty file
            result = validate_csv(filepath)
            assert result["valid"] is False
            assert "empty" in result["error"].lower()

    def test_missing_columns_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            header = "Col1\tCol2\tCol3"
            filepath = self._write_utf16_tsv(tmpdir, "bad.csv", header)
            result = validate_csv(filepath)
            assert result["valid"] is False
            assert "missing" in result["error"].lower()

    def test_not_utf16_still_handled(self):
        """Files that aren't UTF-16 should fail validation gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "ascii.csv")
            with open(filepath, "w", encoding="ascii") as f:
                f.write("col1,col2\nval1,val2")
            result = validate_csv(filepath)
            assert result["valid"] is False

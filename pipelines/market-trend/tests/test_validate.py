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
    """SPEC: Each downloaded file must be non-empty and readable as a known CSV format."""

    def _write_csv(self, tmpdir, filename, header, rows=None, encoding="utf-16", sep="\t"):
        """Helper to create a CSV file with given encoding and separator."""
        lines = [header]
        if rows:
            lines.extend(rows)
        content = "\n".join(lines)
        filepath = os.path.join(tmpdir, filename)
        with open(filepath, "w", encoding=encoding) as f:
            f.write(content)
        return filepath

    def test_valid_utf16_tab_file_passes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            header = "\t".join(REQUIRED_COLUMNS)
            row = "\t".join(["Engineer", "21232", "Software developers", "1",
                             "2023/01/15", "25.00", "35.00", "Hour",
                             "Ontario", "Toronto"])
            filepath = self._write_csv(tmpdir, "2023-01.csv", header, [row])
            result = validate_csv(filepath)
            assert result["valid"] is True
            assert result["encoding"] == "utf-16"
            assert result["sep"] == "\t"

    def test_valid_utf8_comma_file_passes(self):
        """Job Bank switched to UTF-8 + comma mid-2024."""
        with tempfile.TemporaryDirectory() as tmpdir:
            header = ",".join(REQUIRED_COLUMNS)
            row = ",".join(["Engineer", "21232", "Software developers", "1",
                            "2023/01/15", "25.00", "35.00", "Hour",
                            "Ontario", "Toronto"])
            filepath = self._write_csv(tmpdir, "2024-06.csv", header, [row],
                                       encoding="utf-8-sig", sep=",")
            result = validate_csv(filepath)
            assert result["valid"] is True
            assert result["encoding"] == "utf-8-sig"
            assert result["sep"] == ","

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
            filepath = self._write_csv(tmpdir, "bad.csv", header)
            result = validate_csv(filepath)
            assert result["valid"] is False
            assert "missing" in result["error"].lower()

    def test_unknown_format_fails(self):
        """Files with unrecognized encoding/columns should fail gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "weird.csv")
            with open(filepath, "w", encoding="ascii") as f:
                f.write("col1|col2\nval1|val2")
            result = validate_csv(filepath)
            assert result["valid"] is False

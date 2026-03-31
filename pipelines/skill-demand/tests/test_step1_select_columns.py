"""
Tests for skill-demand pipeline STEP 1 — Column Extraction.
Based on SPEC.md — Select job_id, company_name, title, description from postings.csv.

RED phase: These tests should FAIL until src/step1_select_columns.py is implemented.
"""

import os
import tempfile

import pandas as pd
import pytest

from src.step1_select_columns import select_columns, run, REQUIRED_COLUMNS


# ============================================================
# Column Selection
# ============================================================

class TestSelectColumns:
    """SPEC: Select job_id, company_name, title, description from raw CSV."""

    def _make_csv(self, tmpdir, filename="postings.csv", extra_cols=True):
        """Helper: create a minimal test CSV."""
        data = {
            "job_id": [1, 2, 3],
            "company_name": ["Google", "Meta", "Amazon"],
            "title": ["SWE", "Data Scientist", "DevOps Engineer"],
            "description": ["Build stuff", "Analyze data", "Deploy infra"],
        }
        if extra_cols:
            data["max_salary"] = [100000, 120000, 110000]
            data["location"] = ["NYC", "SF", "Seattle"]
            data["formatted_experience_level"] = ["Entry", "Mid", "Senior"]
            data["skills_desc"] = ["Python", "SQL, ML", "AWS, Docker"]
        df = pd.DataFrame(data)
        path = os.path.join(tmpdir, filename)
        df.to_csv(path, index=False)
        return path

    def test_returns_only_required_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._make_csv(tmpdir)
            result = select_columns(csv_path)
            assert list(result.columns) == REQUIRED_COLUMNS

    def test_preserves_all_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._make_csv(tmpdir)
            result = select_columns(csv_path)
            assert len(result) == 3

    def test_values_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._make_csv(tmpdir)
            result = select_columns(csv_path)
            assert result.iloc[0]["company_name"] == "Google"
            assert result.iloc[1]["title"] == "Data Scientist"
            assert result.iloc[2]["description"] == "Deploy infra"

    def test_extra_columns_are_dropped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._make_csv(tmpdir)
            result = select_columns(csv_path)
            assert "max_salary" not in result.columns
            assert "location" not in result.columns
            assert "skills_desc" not in result.columns

    def test_formatted_experience_level_is_kept(self):
        """formatted_experience_level is required for seniority extraction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = self._make_csv(tmpdir)
            result = select_columns(csv_path)
            assert "formatted_experience_level" in result.columns


# ============================================================
# Null Handling
# ============================================================

class TestNullHandling:
    """SPEC: Drop rows with null title or description."""

    def test_drops_rows_with_null_title(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [1, 2, 3],
                "company_name": ["A", "B", "C"],
                "title": ["SWE", None, "PM"],
                "description": ["desc1", "desc2", "desc3"],
                "formatted_experience_level": ["Entry level", "Mid-Senior level", None],
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            result = select_columns(path)
            assert len(result) == 2

    def test_drops_rows_with_null_description(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [1, 2, 3],
                "company_name": ["A", "B", "C"],
                "title": ["SWE", "DS", "PM"],
                "description": ["desc1", None, "desc3"],
                "formatted_experience_level": ["Entry level", "Mid-Senior level", None],
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            result = select_columns(path)
            assert len(result) == 2

    def test_keeps_rows_with_null_company(self):
        """company_name can be null — don't drop those rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [1, 2],
                "company_name": [None, "B"],
                "title": ["SWE", "DS"],
                "description": ["desc1", "desc2"],
                "formatted_experience_level": ["Entry level", None],
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            result = select_columns(path)
            assert len(result) == 2

    def test_keeps_rows_with_null_experience_level(self):
        """formatted_experience_level can be null (23.7%) — don't drop those rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [1, 2, 3],
                "company_name": ["A", "B", "C"],
                "title": ["SWE", "DS", "PM"],
                "description": ["desc1", "desc2", "desc3"],
                "formatted_experience_level": ["Entry level", None, "Senior"],
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            result = select_columns(path)
            assert len(result) == 3


# ============================================================
# Edge Cases
# ============================================================

class TestEdgeCases:
    """Edge cases for robustness."""

    def test_empty_csv_returns_empty_dataframe(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [],
                "company_name": [],
                "title": [],
                "description": [],
                "formatted_experience_level": [],
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            result = select_columns(path)
            assert len(result) == 0
            assert list(result.columns) == REQUIRED_COLUMNS

    def test_raises_on_missing_required_column(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({
                "job_id": [1],
                "company_name": ["A"],
                # missing title and description
            })
            path = os.path.join(tmpdir, "postings.csv")
            df.to_csv(path, index=False)
            with pytest.raises(ValueError):
                select_columns(path)


# ============================================================
# Parquet Output
# ============================================================

class TestRun:
    """SPEC: Output to processed/step1/*.parquet"""

    def _make_csv(self, tmpdir):
        df = pd.DataFrame({
            "job_id": [1, 2, 3],
            "company_name": ["Google", "Meta", "Amazon"],
            "title": ["SWE", "Data Scientist", "DevOps"],
            "description": ["Build stuff", "Analyze data", "Deploy infra"],
            "formatted_experience_level": ["Entry level", None, "Senior"],
            "max_salary": [100000, 120000, 110000],
        })
        path = os.path.join(tmpdir, "postings.csv")
        df.to_csv(path, index=False)
        return path

    def test_saves_parquet_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            csv_path = self._make_csv(tmpdir)
            output_dir = Path(tmpdir) / "output"
            result_path = run(csv_path, output_dir=output_dir)
            assert os.path.exists(result_path)
            assert result_path.endswith(".parquet")

    def test_parquet_has_correct_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            csv_path = self._make_csv(tmpdir)
            output_dir = Path(tmpdir) / "output"
            result_path = run(csv_path, output_dir=output_dir)
            df = pd.read_parquet(result_path)
            assert list(df.columns) == REQUIRED_COLUMNS

    def test_parquet_has_correct_row_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            csv_path = self._make_csv(tmpdir)
            output_dir = Path(tmpdir) / "output"
            result_path = run(csv_path, output_dir=output_dir)
            df = pd.read_parquet(result_path)
            assert len(df) == 3

    def test_returns_path_string(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from pathlib import Path
            csv_path = self._make_csv(tmpdir)
            output_dir = Path(tmpdir) / "output"
            result_path = run(csv_path, output_dir=output_dir)
            assert isinstance(result_path, str)

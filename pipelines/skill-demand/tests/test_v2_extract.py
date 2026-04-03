"""
Tests for skill-demand pipeline V2 — Post-Extract validation gate.
Based on SPEC.md — Null check, description min length.

RED phase: These tests should FAIL until src/validators/v2_extract.py is implemented.
"""

import pandas as pd
import pytest

from src.validators.v2_extract import validate_extract, MIN_DESCRIPTION_LENGTH


# ============================================================
# Null Check
# ============================================================

class TestNullCheck:
    """SPEC V2: Drop rows with null title or description."""

    def test_drops_null_title(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "company_name": ["A", "B"],
            "title": ["SWE", None],
            "description": ["Build things " * 10, "Analyze data " * 10],
        })
        result = validate_extract(df)
        assert len(result) == 1

    def test_drops_null_description(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "company_name": ["A", "B"],
            "title": ["SWE", "DS"],
            "description": ["Build things " * 10, None],
        })
        result = validate_extract(df)
        assert len(result) == 1

    def test_keeps_null_company(self):
        df = pd.DataFrame({
            "job_id": [1],
            "company_name": [None],
            "title": ["SWE"],
            "description": ["Build things " * 10],
        })
        result = validate_extract(df)
        assert len(result) == 1


# ============================================================
# Description Min Length
# ============================================================

class TestDescriptionMinLength:
    """SPEC V2: Filter out descriptions shorter than MIN_DESCRIPTION_LENGTH."""

    def test_drops_short_description(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "company_name": ["A", "B"],
            "title": ["SWE", "DS"],
            "description": ["ok", "This is a sufficiently long job description for proper skill extraction analysis"],
        })
        result = validate_extract(df)
        assert len(result) == 1
        assert result.iloc[0]["job_id"] == 2

    def test_drops_empty_string_description(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "company_name": ["A", "B"],
            "title": ["SWE", "DS"],
            "description": ["", "Valid long description for testing purposes and analysis " * 2],
        })
        result = validate_extract(df)
        assert len(result) == 1

    def test_keeps_description_at_threshold(self):
        df = pd.DataFrame({
            "job_id": [1],
            "company_name": ["A"],
            "title": ["SWE"],
            "description": ["x" * MIN_DESCRIPTION_LENGTH],
        })
        result = validate_extract(df)
        assert len(result) == 1

    def test_min_description_length_is_positive(self):
        assert MIN_DESCRIPTION_LENGTH > 0


# ============================================================
# Combined
# ============================================================

class TestCombined:
    """All V2 validations applied together."""

    def test_nulls_and_short_descriptions_filtered(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3],
            "company_name": ["A", "B", "C"],
            "title": ["SWE", None, "DS"],
            "description": [
                "Valid description for software engineering role " * 3,
                "Should be dropped because null title " * 3,
                "ok",
            ],
        })
        result = validate_extract(df)
        assert len(result) == 1
        assert result.iloc[0]["job_id"] == 1

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame({
            "job_id": [],
            "company_name": [],
            "title": [],
            "description": [],
        })
        result = validate_extract(df)
        assert len(result) == 0

    def test_returns_clean_index(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3],
            "company_name": ["A", "B", "C"],
            "title": ["SWE", None, "PM"],
            "description": ["Valid long description " * 5, "dropped", "Another valid one " * 5],
        })
        result = validate_extract(df)
        assert list(result.index) == list(range(len(result)))

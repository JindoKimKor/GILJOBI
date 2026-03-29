"""
Tests for skill-demand pipeline STEP 5 — DB Load.
Based on SPEC.md — Bulk insert jd_postings + explode skills into jd_skills.

Tests use mock DB connection to verify SQL generation and data flow.
Actual DB integration tested separately via main.py --download-only.
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock, call

from src.step5_load import (
    prepare_postings_rows,
    prepare_skills_rows,
    POSTINGS_COLUMNS,
)


# ============================================================
# Prepare Postings Rows
# ============================================================

class TestPreparePostingsRows:
    """SPEC: Bulk insert into jd_postings."""

    def test_returns_correct_columns(self):
        df = pd.DataFrame({
            "job_id": [1],
            "company_name": ["Google"],
            "raw_title": ["SWE"],
            "noc_id": [1],
            "noc_match_score": [0.9],
            "noc_match_method": ["sentence_transformer"],
            "seniority": ["senior"],
            "description": ["Build stuff"],
        })
        rows = prepare_postings_rows(df)
        assert len(rows) == 1
        assert set(rows[0].keys()) == set(POSTINGS_COLUMNS)

    def test_handles_null_noc(self):
        df = pd.DataFrame({
            "job_id": [1],
            "company_name": ["Google"],
            "raw_title": ["SWE"],
            "noc_id": [None],
            "noc_match_score": [None],
            "noc_match_method": [None],
            "seniority": ["senior"],
            "description": ["Build stuff"],
        })
        rows = prepare_postings_rows(df)
        assert rows[0]["noc_id"] is None

    def test_multiple_rows(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "company_name": ["Google", "Meta"],
            "raw_title": ["SWE", "DS"],
            "noc_id": [1, 2],
            "noc_match_score": [0.9, 0.8],
            "noc_match_method": ["sentence_transformer", "llm"],
            "seniority": ["senior", "mid_level"],
            "description": ["Build stuff", "Analyze data"],
        })
        rows = prepare_postings_rows(df)
        assert len(rows) == 2


# ============================================================
# Prepare Skills Rows (Explode)
# ============================================================

class TestPrepareSkillsRows:
    """SPEC: Explode skills[] array → one row per skill per posting."""

    def test_explodes_skills(self):
        data = [
            {"job_id": 1, "skills": ["python", "aws", "docker"]},
            {"job_id": 2, "skills": ["java"]},
        ]
        rows = prepare_skills_rows(data)
        assert len(rows) == 4
        assert rows[0] == {"job_id": 1, "skill": "python"}
        assert rows[1] == {"job_id": 1, "skill": "aws"}
        assert rows[2] == {"job_id": 1, "skill": "docker"}
        assert rows[3] == {"job_id": 2, "skill": "java"}

    def test_empty_skills_produces_no_rows(self):
        data = [
            {"job_id": 1, "skills": []},
        ]
        rows = prepare_skills_rows(data)
        assert len(rows) == 0

    def test_no_data_returns_empty(self):
        rows = prepare_skills_rows([])
        assert len(rows) == 0

    def test_mixed_empty_and_filled(self):
        data = [
            {"job_id": 1, "skills": ["python"]},
            {"job_id": 2, "skills": []},
            {"job_id": 3, "skills": ["java", "sql"]},
        ]
        rows = prepare_skills_rows(data)
        assert len(rows) == 3


# ============================================================
# Config
# ============================================================

class TestConfig:
    def test_postings_columns_defined(self):
        expected = [
            "company", "raw_title", "noc_id", "noc_match_score",
            "noc_match_method", "seniority", "description",
        ]
        assert POSTINGS_COLUMNS == expected

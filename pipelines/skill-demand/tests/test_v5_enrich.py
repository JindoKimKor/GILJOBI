"""
Tests for skill-demand pipeline V5 — Post-Enrich validation gate.
Based on SPEC.md — Seniority value in allowed set, skills array not empty.
"""

import pandas as pd
import pytest

from src.validators.v5_enrich import validate_enrich
from src.step4_extract import SENIORITY_TIERS


# ============================================================
# Seniority Validation
# ============================================================

class TestSeniorityValidation:
    """SPEC V5: Seniority must be in allowed set or None."""

    def test_valid_seniority_passes(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3],
            "seniority": ["intern", "mid_level", "senior"],
            "skills": [["python"], ["java"], ["aws"]],
        })
        stats = validate_enrich(df)
        assert stats["valid_seniority"] == 3
        assert stats["invalid_seniority"] == 0

    def test_null_seniority_counted(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "seniority": ["senior", None],
            "skills": [["python"], ["java"]],
        })
        stats = validate_enrich(df)
        assert stats["null_seniority"] == 1

    def test_all_tiers_accepted(self):
        df = pd.DataFrame({
            "job_id": list(range(len(SENIORITY_TIERS))),
            "seniority": SENIORITY_TIERS,
            "skills": [["skill"]] * len(SENIORITY_TIERS),
        })
        stats = validate_enrich(df)
        assert stats["valid_seniority"] == len(SENIORITY_TIERS)


# ============================================================
# Skills Validation
# ============================================================

class TestSkillsValidation:
    """SPEC V5: Skills array should not be empty."""

    def test_counts_empty_skills(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3],
            "seniority": ["senior", "mid_level", "entry_level"],
            "skills": [["python", "aws"], [], ["java"]],
        })
        stats = validate_enrich(df)
        assert stats["empty_skills"] == 1
        assert stats["with_skills"] == 2

    def test_all_have_skills(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "seniority": ["senior", "mid_level"],
            "skills": [["python"], ["java", "sql"]],
        })
        stats = validate_enrich(df)
        assert stats["empty_skills"] == 0

    def test_avg_skills_per_posting(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "seniority": ["senior", "mid_level"],
            "skills": [["python", "aws", "docker"], ["java"]],
        })
        stats = validate_enrich(df)
        assert stats["avg_skills_per_posting"] == 2.0


# ============================================================
# Combined
# ============================================================

class TestCombined:
    def test_full_stats(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3],
            "seniority": ["senior", None, "entry_level"],
            "skills": [["python", "aws"], [], ["java"]],
        })
        stats = validate_enrich(df)
        assert stats["total"] == 3
        assert stats["valid_seniority"] == 2
        assert stats["null_seniority"] == 1
        assert stats["with_skills"] == 2
        assert stats["empty_skills"] == 1

    def test_empty_dataframe(self):
        df = pd.DataFrame({
            "job_id": [],
            "seniority": [],
            "skills": [],
        })
        stats = validate_enrich(df)
        assert stats["total"] == 0
        assert stats["avg_skills_per_posting"] == 0.0

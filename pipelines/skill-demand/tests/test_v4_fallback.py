"""
Tests for skill-demand pipeline V4 — Post-Fallback validation gate.
Based on SPEC.md — NOC mapping completion rate, unmapped row count.
"""

import pandas as pd
import numpy as np
import pytest

from src.validators.v4_fallback import validate_fallback


# ============================================================
# Completion Rate
# ============================================================

class TestCompletionRate:
    """SPEC V4: NOC mapping completion rate after LLM fallback."""

    def test_all_mapped(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3],
            "noc_id": [1, 2, 3],
            "noc_match_method": ["sentence_transformer", "sentence_transformer", "llm"],
        })
        stats = validate_fallback(df)
        assert stats["total"] == 3
        assert stats["mapped"] == 3
        assert stats["unmapped"] == 0
        assert stats["completion_rate"] == 1.0

    def test_partial_mapped(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3, 4],
            "noc_id": [1, None, 3, None],
            "noc_match_method": ["sentence_transformer", None, "llm", None],
        })
        stats = validate_fallback(df)
        assert stats["mapped"] == 2
        assert stats["unmapped"] == 2
        assert stats["completion_rate"] == 0.5

    def test_none_mapped(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "noc_id": [None, None],
            "noc_match_method": [None, None],
        })
        stats = validate_fallback(df)
        assert stats["completion_rate"] == 0.0

    def test_empty_dataframe(self):
        df = pd.DataFrame({
            "job_id": [],
            "noc_id": [],
            "noc_match_method": [],
        })
        stats = validate_fallback(df)
        assert stats["total"] == 0
        assert stats["completion_rate"] == 0.0


# ============================================================
# Method Breakdown
# ============================================================

class TestMethodBreakdown:
    """SPEC V4: Count by noc_match_method (sentence_transformer vs llm)."""

    def test_counts_by_method(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3, 4, 5],
            "noc_id": [1, 2, 3, 4, None],
            "noc_match_method": [
                "sentence_transformer",
                "sentence_transformer",
                "sentence_transformer",
                "llm",
                None,
            ],
        })
        stats = validate_fallback(df)
        assert stats["by_method"]["sentence_transformer"] == 3
        assert stats["by_method"]["llm"] == 1

    def test_no_llm_matches(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "noc_id": [1, 2],
            "noc_match_method": ["sentence_transformer", "sentence_transformer"],
        })
        stats = validate_fallback(df)
        assert stats["by_method"].get("llm", 0) == 0

    def test_only_llm_matches(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "noc_id": [1, 2],
            "noc_match_method": ["llm", "llm"],
        })
        stats = validate_fallback(df)
        assert stats["by_method"]["llm"] == 2
        assert stats["by_method"].get("sentence_transformer", 0) == 0

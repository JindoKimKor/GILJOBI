"""
Tests for skill-demand pipeline V3 — Post-Normalize validation gate.
Based on SPEC.md — Threshold gate, match rate check, split matched vs sub-threshold.
"""

import pandas as pd
import numpy as np
import pytest

from src.validators.v3_normalize import validate_normalize, split_by_match


# ============================================================
# Split: Matched vs Sub-threshold
# ============================================================

class TestSplitByMatch:
    """SPEC V3: Split into matched rows and sub-threshold rows for LLM fallback."""

    def test_splits_matched_and_unmatched(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3],
            "title": ["SWE", "DS", "Chef"],
            "noc_id": [1, 2, None],
            "noc_match_score": [0.9, 0.8, np.nan],
            "noc_match_method": ["sentence_transformer", "sentence_transformer", None],
        })
        matched, sub_threshold = split_by_match(df)
        assert len(matched) == 2
        assert len(sub_threshold) == 1
        assert sub_threshold.iloc[0]["job_id"] == 3

    def test_all_matched(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "noc_id": [1, 2],
            "noc_match_score": [0.9, 0.85],
            "noc_match_method": ["sentence_transformer", "sentence_transformer"],
        })
        matched, sub_threshold = split_by_match(df)
        assert len(matched) == 2
        assert len(sub_threshold) == 0

    def test_all_unmatched(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "noc_id": [None, None],
            "noc_match_score": [np.nan, np.nan],
            "noc_match_method": [None, None],
        })
        matched, sub_threshold = split_by_match(df)
        assert len(matched) == 0
        assert len(sub_threshold) == 2

    def test_empty_dataframe(self):
        df = pd.DataFrame({
            "job_id": [],
            "noc_id": [],
            "noc_match_score": [],
            "noc_match_method": [],
        })
        matched, sub_threshold = split_by_match(df)
        assert len(matched) == 0
        assert len(sub_threshold) == 0


# ============================================================
# Match Rate Check
# ============================================================

class TestValidateNormalize:
    """SPEC V3: Validate match rate and return stats."""

    def test_returns_stats(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3, 4],
            "noc_id": [1, 2, None, None],
            "noc_match_score": [0.9, 0.8, np.nan, np.nan],
            "noc_match_method": ["sentence_transformer", "sentence_transformer", None, None],
        })
        stats = validate_normalize(df)
        assert stats["total"] == 4
        assert stats["matched"] == 2
        assert stats["unmatched"] == 2
        assert stats["match_rate"] == 0.5

    def test_100_percent_match_rate(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "noc_id": [1, 2],
            "noc_match_score": [0.9, 0.85],
            "noc_match_method": ["sentence_transformer", "sentence_transformer"],
        })
        stats = validate_normalize(df)
        assert stats["match_rate"] == 1.0

    def test_0_percent_match_rate(self):
        df = pd.DataFrame({
            "job_id": [1, 2],
            "noc_id": [None, None],
            "noc_match_score": [np.nan, np.nan],
            "noc_match_method": [None, None],
        })
        stats = validate_normalize(df)
        assert stats["match_rate"] == 0.0

    def test_empty_dataframe(self):
        df = pd.DataFrame({
            "job_id": [],
            "noc_id": [],
            "noc_match_score": [],
            "noc_match_method": [],
        })
        stats = validate_normalize(df)
        assert stats["total"] == 0
        assert stats["matched"] == 0
        assert stats["match_rate"] == 0.0

    def test_returns_avg_score(self):
        df = pd.DataFrame({
            "job_id": [1, 2, 3],
            "noc_id": [1, 2, None],
            "noc_match_score": [0.9, 0.8, np.nan],
            "noc_match_method": ["sentence_transformer", "sentence_transformer", None],
        })
        stats = validate_normalize(df)
        assert abs(stats["avg_score"] - 0.85) < 1e-6

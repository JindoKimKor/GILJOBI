"""
Tests for NOC21 master table setup (pre-requisite).
Based on SPEC.md — noc_titles Population Strategy.
"""

import pandas as pd
import pytest

from scripts.noc_setup import filter_unit_groups, prepare_noc_titles


# ============================================================
# Filter Unit Groups (Level 5)
# ============================================================

class TestFilterUnitGroups:
    """SPEC: Filter to Level 5 (Unit Group) rows — 5-digit codes."""

    def test_keeps_only_level_5(self):
        df = pd.DataFrame({
            "Level": [1, 2, 3, 4, 5, 5],
            "Hierarchical structure": [
                "Broad Category", "Major Group", "Sub-major Group",
                "Minor Group", "Unit Group", "Unit Group",
            ],
            "Code - NOC 2021 V1.0": ["0", "00", "000", "0001", "00010", "10010"],
            "Class title": [
                "Legislative", "Legislative mgrs", "Legislative mgrs",
                "Legislative mgrs", "Legislators", "Financial managers",
            ],
            "Class definition": ["d1", "d2", "d3", "d4", "d5", "d6"],
        })
        result = filter_unit_groups(df)
        assert len(result) == 2
        assert list(result["Code - NOC 2021 V1.0"]) == ["00010", "10010"]

    def test_excludes_non_level_5(self):
        df = pd.DataFrame({
            "Level": [1, 2, 3, 4],
            "Hierarchical structure": [
                "Broad Category", "Major Group", "Sub-major Group", "Minor Group",
            ],
            "Code - NOC 2021 V1.0": ["0", "00", "000", "0001"],
            "Class title": ["a", "b", "c", "d"],
            "Class definition": ["d1", "d2", "d3", "d4"],
        })
        result = filter_unit_groups(df)
        assert len(result) == 0


# ============================================================
# Prepare noc_titles DataFrame
# ============================================================

class TestPrepareNocTitles:
    """SPEC: noc_titles has noc21_code (UNIQUE NOT NULL) and noc21_name."""

    def test_renames_columns_correctly(self):
        df = pd.DataFrame({
            "Level": [5, 5],
            "Hierarchical structure": ["Unit Group", "Unit Group"],
            "Code - NOC 2021 V1.0": ["00010", "10010"],
            "Class title": ["Legislators", "Financial managers"],
            "Class definition": ["def1", "def2"],
        })
        result = prepare_noc_titles(df)
        assert list(result.columns) == ["noc21_code", "noc21_name"]
        assert result["noc21_code"].iloc[0] == "00010"
        assert result["noc21_name"].iloc[0] == "Legislators"

    def test_drops_extra_columns(self):
        df = pd.DataFrame({
            "Level": [5],
            "Hierarchical structure": ["Unit Group"],
            "Code - NOC 2021 V1.0": ["21230"],
            "Class title": ["Computer systems developers and programmers"],
            "Class definition": ["Write, modify, integrate..."],
        })
        result = prepare_noc_titles(df)
        assert "Level" not in result.columns
        assert "Class definition" not in result.columns

    def test_no_null_codes(self):
        df = pd.DataFrame({
            "Level": [5, 5],
            "Hierarchical structure": ["Unit Group", "Unit Group"],
            "Code - NOC 2021 V1.0": ["00010", None],
            "Class title": ["Legislators", "Unknown"],
            "Class definition": ["d1", "d2"],
        })
        result = prepare_noc_titles(df)
        assert result["noc21_code"].notna().all()
        assert len(result) == 1

"""
Tests for skill-demand pipeline — Seniority Extraction.
Based on SPEC.md — 3-tier: CSV mapping → title keywords → remaining for LLM.

RED phase: These tests should FAIL until src/step2_seniority.py is implemented.
"""

import pytest

from src.step2_seniority import (
    CSV_SENIORITY_MAP,
    TITLE_KEYWORDS,
    SENIORITY_TIERS,
    map_experience_level,
    extract_seniority_from_title,
    extract_seniority,
)


# ============================================================
# CSV Mapping
# ============================================================

class TestMapExperienceLevel:
    """SPEC: Map formatted_experience_level to seniority tier."""

    def test_internship_maps_to_intern(self):
        assert map_experience_level("Internship") == "intern"

    def test_entry_level_maps_to_entry_level(self):
        assert map_experience_level("Entry level") == "entry_level"

    def test_associate_maps_to_entry_level(self):
        assert map_experience_level("Associate") == "entry_level"

    def test_mid_senior_maps_to_mid_level(self):
        assert map_experience_level("Mid-Senior level") == "mid_level"

    def test_director_maps_to_executive(self):
        assert map_experience_level("Director") == "executive"

    def test_executive_maps_to_executive(self):
        assert map_experience_level("Executive") == "executive"

    def test_none_returns_none(self):
        assert map_experience_level(None) is None

    def test_unknown_value_returns_none(self):
        assert map_experience_level("Something else") is None

    def test_empty_string_returns_none(self):
        assert map_experience_level("") is None


# ============================================================
# Title Keyword Matching
# ============================================================

class TestExtractSeniorityFromTitle:
    """SPEC: Extract seniority from title keywords."""

    def test_senior_keyword(self):
        assert extract_seniority_from_title("Senior Software Engineer") == "senior"

    def test_sr_keyword(self):
        assert extract_seniority_from_title("Sr. Data Scientist") == "senior"

    def test_lead_keyword(self):
        assert extract_seniority_from_title("Lead DevOps Engineer") == "senior"

    def test_principal_keyword(self):
        assert extract_seniority_from_title("Principal Architect") == "senior"

    def test_staff_keyword(self):
        assert extract_seniority_from_title("Staff Engineer") == "senior"

    def test_junior_keyword(self):
        assert extract_seniority_from_title("Junior Developer") == "entry_level"

    def test_jr_keyword(self):
        assert extract_seniority_from_title("Jr Software Engineer") == "entry_level"

    def test_intern_keyword(self):
        assert extract_seniority_from_title("Software Engineering Intern") == "intern"

    def test_internship_keyword(self):
        assert extract_seniority_from_title("Data Science Internship") == "intern"

    def test_director_keyword(self):
        assert extract_seniority_from_title("Director of Engineering") == "executive"

    def test_vp_keyword(self):
        assert extract_seniority_from_title("VP of Product") == "executive"

    def test_cto_keyword(self):
        assert extract_seniority_from_title("CTO") == "executive"

    def test_head_of_keyword(self):
        assert extract_seniority_from_title("Head of Data Science") == "executive"

    def test_mid_level_keyword(self):
        assert extract_seniority_from_title("Mid-Level Engineer") == "mid_level"

    def test_graduate_keyword(self):
        assert extract_seniority_from_title("Graduate Software Engineer") == "entry_level"

    def test_no_keyword_returns_none(self):
        assert extract_seniority_from_title("Software Engineer") is None

    def test_case_insensitive(self):
        assert extract_seniority_from_title("SENIOR ENGINEER") == "senior"

    def test_empty_title_returns_none(self):
        assert extract_seniority_from_title("") is None

    def test_sr_in_israel_no_false_positive(self):
        """'sr' in 'Israel' should NOT match senior."""
        assert extract_seniority_from_title("Israel Market Analyst") is None

    def test_lead_in_leading_no_false_positive(self):
        """'lead' in 'leading' should NOT match senior."""
        assert extract_seniority_from_title("Leading Edge Technology Specialist") is None

    def test_vp_in_word_no_false_positive(self):
        """'vp' should only match as standalone word."""
        assert extract_seniority_from_title("Development Process Engineer") is None

    def test_jr_standalone_matches(self):
        """'Jr' as standalone word should match."""
        assert extract_seniority_from_title("Jr. Software Engineer") == "entry_level"

    def test_sr_dot_matches(self):
        """'Sr.' as standalone should match (dot is word boundary)."""
        assert extract_seniority_from_title("Sr. Software Engineer") == "senior"


# ============================================================
# Combined Extraction
# ============================================================

class TestExtractSeniority:
    """SPEC: Priority — CSV first, then title keyword, then None for LLM."""

    def test_csv_value_takes_priority(self):
        seniority, method = extract_seniority("Entry level", "Senior Engineer")
        assert seniority == "entry_level"
        assert method == "csv"

    def test_mid_senior_with_senior_title_upgrades(self):
        """Mid-Senior level + senior keyword in title → upgrade to senior."""
        seniority, method = extract_seniority("Mid-Senior level", "Senior Software Engineer")
        assert seniority == "senior"
        assert method == "csv_refined"

    def test_mid_senior_without_senior_keyword_stays_mid(self):
        """Mid-Senior level without senior keyword → mid_level."""
        seniority, method = extract_seniority("Mid-Senior level", "Software Engineer")
        assert seniority == "mid_level"
        assert method == "csv"

    def test_null_csv_falls_back_to_keyword(self):
        seniority, method = extract_seniority(None, "Senior Developer")
        assert seniority == "senior"
        assert method == "title_keyword"

    def test_null_both_returns_none(self):
        seniority, method = extract_seniority(None, "Software Engineer")
        assert seniority is None
        assert method is None

    def test_csv_internship_ignores_title(self):
        seniority, method = extract_seniority("Internship", "Senior Intern Manager")
        assert seniority == "intern"
        assert method == "csv"

    def test_result_always_in_seniority_tiers_or_none(self):
        """Any returned seniority value must be in SENIORITY_TIERS."""
        test_cases = [
            ("Internship", "SWE"),
            ("Entry level", "Dev"),
            ("Associate", "Analyst"),
            ("Mid-Senior level", "Engineer"),
            ("Mid-Senior level", "Senior Engineer"),
            ("Director", "Director of Eng"),
            ("Executive", "CTO"),
            (None, "Senior Dev"),
            (None, "Junior Dev"),
            (None, "Software Engineer"),
        ]
        for level, title in test_cases:
            seniority, _ = extract_seniority(level, title)
            assert seniority is None or seniority in SENIORITY_TIERS, \
                f"Invalid seniority '{seniority}' for ({level}, {title})"


# ============================================================
# Config
# ============================================================

class TestConfig:
    """Verify constants are well-formed."""

    def test_csv_map_has_all_linkedin_values(self):
        expected = {"Internship", "Entry level", "Associate", "Mid-Senior level", "Director", "Executive"}
        assert set(CSV_SENIORITY_MAP.keys()) == expected

    def test_all_csv_values_are_valid_tiers(self):
        for tier in CSV_SENIORITY_MAP.values():
            assert tier in SENIORITY_TIERS

    def test_all_keyword_keys_are_valid_tiers(self):
        for tier in TITLE_KEYWORDS:
            assert tier in SENIORITY_TIERS

    def test_seniority_tiers_has_5_values(self):
        assert len(SENIORITY_TIERS) == 5

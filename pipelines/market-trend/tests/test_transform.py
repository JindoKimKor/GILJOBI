"""
Tests for market-trend pipeline transformation rules.
Based on SPEC.md — Salary Normalization, Outlier Handling, NOC Lookup.
"""

import pandas as pd
import pytest

from src.transform import normalize_salary_to_hourly, apply_outlier_filter, map_noc_ids, prepare_job_postings


# ============================================================
# Salary Normalization
# ============================================================

class TestSalaryNormalization:
    """SPEC: All salary values are normalized to hourly rate."""

    def test_hourly_unchanged(self):
        """Hour → divisor 1 (as-is)"""
        df = pd.DataFrame({
            "Salary Minimum": [25.0],
            "Salary Maximum": [35.0],
            "Salary Per": ["Hour"],
        })
        result = normalize_salary_to_hourly(df)
        assert result["salary_min_hourly"].iloc[0] == 25.0
        assert result["salary_max_hourly"].iloc[0] == 35.0

    def test_daily_divided_by_8(self):
        """Day → divisor 8 (8 hours/day)"""
        df = pd.DataFrame({
            "Salary Minimum": [200.0],
            "Salary Maximum": [400.0],
            "Salary Per": ["Day"],
        })
        result = normalize_salary_to_hourly(df)
        assert result["salary_min_hourly"].iloc[0] == 25.0
        assert result["salary_max_hourly"].iloc[0] == 50.0

    def test_weekly_divided_by_40(self):
        """Week → divisor 40 (40 hours/week)"""
        df = pd.DataFrame({
            "Salary Minimum": [1000.0],
            "Salary Maximum": [2000.0],
            "Salary Per": ["Week"],
        })
        result = normalize_salary_to_hourly(df)
        assert result["salary_min_hourly"].iloc[0] == 25.0
        assert result["salary_max_hourly"].iloc[0] == 50.0

    def test_biweekly_divided_by_80(self):
        """Bi-weekly → divisor 80 (40 hours/week × 2)"""
        df = pd.DataFrame({
            "Salary Minimum": [2000.0],
            "Salary Maximum": [4000.0],
            "Salary Per": ["Bi-weekly"],
        })
        result = normalize_salary_to_hourly(df)
        assert result["salary_min_hourly"].iloc[0] == 25.0
        assert result["salary_max_hourly"].iloc[0] == 50.0

    def test_monthly_divided_by_173_33(self):
        """Month → divisor 173.33 (2,080 hours/year ÷ 12)"""
        df = pd.DataFrame({
            "Salary Minimum": [4333.25],
            "Salary Maximum": [8666.50],
            "Salary Per": ["Month"],
        })
        result = normalize_salary_to_hourly(df)
        assert result["salary_min_hourly"].iloc[0] == pytest.approx(25.0, abs=0.01)
        assert result["salary_max_hourly"].iloc[0] == pytest.approx(50.0, abs=0.01)

    def test_yearly_divided_by_2080(self):
        """Year → divisor 2,080 (40 hours/week × 52 weeks)"""
        df = pd.DataFrame({
            "Salary Minimum": [52000.0],
            "Salary Maximum": [104000.0],
            "Salary Per": ["Year"],
        })
        result = normalize_salary_to_hourly(df)
        assert result["salary_min_hourly"].iloc[0] == 25.0
        assert result["salary_max_hourly"].iloc[0] == 50.0

    def test_salary_per_null_sets_null(self):
        """SPEC: Salary Per is NULL → set salary fields to NULL"""
        df = pd.DataFrame({
            "Salary Minimum": [25.0],
            "Salary Maximum": [35.0],
            "Salary Per": [None],
        })
        result = normalize_salary_to_hourly(df)
        assert pd.isna(result["salary_min_hourly"].iloc[0])
        assert pd.isna(result["salary_max_hourly"].iloc[0])

    def test_mixed_salary_units(self):
        """Multiple rows with different units in one batch."""
        df = pd.DataFrame({
            "Salary Minimum": [25.0, 200.0, 52000.0],
            "Salary Maximum": [35.0, 400.0, 104000.0],
            "Salary Per": ["Hour", "Day", "Year"],
        })
        result = normalize_salary_to_hourly(df)
        assert result["salary_min_hourly"].iloc[0] == 25.0
        assert result["salary_min_hourly"].iloc[1] == 25.0
        assert result["salary_min_hourly"].iloc[2] == 25.0


# ============================================================
# Outlier Handling
# ============================================================

class TestOutlierFilter:
    """SPEC: Hourly rate < $10 or > $500 → set to NULL."""

    def test_normal_range_unchanged(self):
        df = pd.DataFrame({
            "salary_min_hourly": [25.0],
            "salary_max_hourly": [50.0],
        })
        result = apply_outlier_filter(df)
        assert result["salary_min_hourly"].iloc[0] == 25.0
        assert result["salary_max_hourly"].iloc[0] == 50.0

    def test_below_10_set_to_null(self):
        df = pd.DataFrame({
            "salary_min_hourly": [5.0],
            "salary_max_hourly": [9.99],
        })
        result = apply_outlier_filter(df)
        assert pd.isna(result["salary_min_hourly"].iloc[0])
        assert pd.isna(result["salary_max_hourly"].iloc[0])

    def test_above_500_set_to_null(self):
        df = pd.DataFrame({
            "salary_min_hourly": [501.0],
            "salary_max_hourly": [1000.0],
        })
        result = apply_outlier_filter(df)
        assert pd.isna(result["salary_min_hourly"].iloc[0])
        assert pd.isna(result["salary_max_hourly"].iloc[0])

    def test_boundary_10_is_valid(self):
        df = pd.DataFrame({
            "salary_min_hourly": [10.0],
            "salary_max_hourly": [10.0],
        })
        result = apply_outlier_filter(df)
        assert result["salary_min_hourly"].iloc[0] == 10.0

    def test_boundary_500_is_valid(self):
        df = pd.DataFrame({
            "salary_min_hourly": [500.0],
            "salary_max_hourly": [500.0],
        })
        result = apply_outlier_filter(df)
        assert result["salary_max_hourly"].iloc[0] == 500.0

    def test_null_salary_stays_null(self):
        df = pd.DataFrame({
            "salary_min_hourly": [None],
            "salary_max_hourly": [None],
        })
        result = apply_outlier_filter(df)
        assert pd.isna(result["salary_min_hourly"].iloc[0])
        assert pd.isna(result["salary_max_hourly"].iloc[0])


# ============================================================
# NOC Lookup
# ============================================================

class TestNocLookup:
    """SPEC: job_postings.noc_id references noc_titles.id via NOC21 code lookup."""

    def test_valid_noc_code_maps_to_id(self):
        noc_lookup = {"21232": 1, "41200": 2}
        df = pd.DataFrame({"NOC21 Code": ["21232", "41200"]})
        result = map_noc_ids(df, noc_lookup)
        assert result["noc_id"].iloc[0] == 1
        assert result["noc_id"].iloc[1] == 2

    def test_missing_noc_code_maps_to_null(self):
        """SPEC: 0.7% rows have no NOC21 classification."""
        noc_lookup = {"21232": 1}
        df = pd.DataFrame({"NOC21 Code": [None, "99999"]})
        result = map_noc_ids(df, noc_lookup)
        assert pd.isna(result["noc_id"].iloc[0])
        assert pd.isna(result["noc_id"].iloc[1])


# ============================================================
# Prepare job_postings (column selection + renaming)
# ============================================================

class TestPrepareJobPostings:
    """SPEC: job_postings table has exactly these columns (excluding auto-generated id)."""

    EXPECTED_COLUMNS = [
        "noc_id", "normalized_title", "vacancy_count",
        "province", "city", "first_posting_date",
        "salary_min_hourly", "salary_max_hourly",
    ]

    def _make_input_df(self):
        return pd.DataFrame({
            "Job Title": ["Software Engineer"],
            "NOC21 Code": ["21232"],
            "NOC21 Code Name": ["Software developers"],
            "Vacancy Count": [3],
            "Province/Territory": ["Ontario"],
            "City": ["Toronto"],
            "First Posting Date": ["2023/01/15"],
            "Salary Minimum": [25.0],
            "Salary Maximum": [35.0],
            "Salary Per": ["Hour"],
            "Employment Type": ["Full time"],
            "NAICS": ["541510"],
            "noc_id": [1],
            "salary_min_hourly": [25.0],
            "salary_max_hourly": [35.0],
        })

    def test_output_has_correct_columns(self):
        df = self._make_input_df()
        result = prepare_job_postings(df)
        assert list(result.columns) == self.EXPECTED_COLUMNS

    def test_drops_source_columns(self):
        df = self._make_input_df()
        result = prepare_job_postings(df)
        assert "Job Title" not in result.columns
        assert "NOC21 Code" not in result.columns
        assert "Employment Type" not in result.columns
        assert "NAICS" not in result.columns

    def test_renames_columns_correctly(self):
        df = self._make_input_df()
        result = prepare_job_postings(df)
        assert result["normalized_title"].iloc[0] == "Software Engineer"
        assert result["vacancy_count"].iloc[0] == 3
        assert result["province"].iloc[0] == "Ontario"
        assert result["city"].iloc[0] == "Toronto"

    def test_date_format_converted(self):
        df = self._make_input_df()
        result = prepare_job_postings(df)
        date_val = pd.to_datetime(result["first_posting_date"].iloc[0])
        assert date_val.year == 2023
        assert date_val.month == 1
        assert date_val.day == 15

    def test_preserves_salary_and_noc_id(self):
        df = self._make_input_df()
        result = prepare_job_postings(df)
        assert result["noc_id"].iloc[0] == 1
        assert result["salary_min_hourly"].iloc[0] == 25.0
        assert result["salary_max_hourly"].iloc[0] == 35.0

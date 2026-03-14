"""
Tests for market-trend pipeline LOAD stage.
DB integration tests — requires PostgreSQL connection.
"""

import pandas as pd
import pytest

from src.load import get_connection, load_noc_titles, load_job_postings


# All tests in this file require a running PostgreSQL instance.
pytestmark = pytest.mark.integration


# ============================================================
# Connection
# ============================================================

class TestGetConnection:
    """SPEC: Connect to PostgreSQL."""

    def test_returns_connection(self):
        conn = get_connection("postgresql://postgres:postgres@localhost:5432/giljobi")
        assert conn is not None
        conn.close()


# ============================================================
# Load noc_titles
# ============================================================

class TestLoadNocTitles:
    """SPEC: Insert noc_titles rows."""

    def test_inserts_rows(self):
        conn = get_connection("postgresql://postgres:postgres@localhost:5432/giljobi")
        try:
            df = pd.DataFrame({
                "noc21_code": ["00010", "10010"],
                "noc21_name": ["Legislators", "Financial managers"],
            })
            count = load_noc_titles(df, conn)
            assert count == 2
        finally:
            conn.close()

    def test_skips_duplicate_codes(self):
        """SPEC: noc21_code is UNIQUE — duplicates should be skipped."""
        conn = get_connection("postgresql://postgres:postgres@localhost:5432/giljobi")
        try:
            df = pd.DataFrame({
                "noc21_code": ["00010", "00010"],
                "noc21_name": ["Legislators", "Legislators"],
            })
            count = load_noc_titles(df, conn)
            assert count == 1
        finally:
            conn.close()


# ============================================================
# Load job_postings
# ============================================================

class TestLoadJobPostings:
    """SPEC: Insert job_postings rows with FK to noc_titles."""

    def test_inserts_rows(self):
        conn = get_connection("postgresql://postgres:postgres@localhost:5432/giljobi")
        try:
            # Pre-load noc_titles for FK
            noc_df = pd.DataFrame({
                "noc21_code": ["21232"],
                "noc21_name": ["Software developers"],
            })
            load_noc_titles(noc_df, conn)

            df = pd.DataFrame({
                "noc_id": [1],
                "normalized_title": ["Software Engineer"],
                "vacancy_count": [3],
                "province": ["Ontario"],
                "city": ["Toronto"],
                "first_posting_date": pd.to_datetime(["2023-01-15"]),
                "salary_min_hourly": [25.0],
                "salary_max_hourly": [35.0],
            })
            count = load_job_postings(df, conn)
            assert count == 1
        finally:
            conn.close()

    def test_null_noc_id_allowed(self):
        """SPEC: 0.7% rows have no NOC21 classification — noc_id can be NULL."""
        conn = get_connection("postgresql://postgres:postgres@localhost:5432/giljobi")
        try:
            df = pd.DataFrame({
                "noc_id": [None],
                "normalized_title": ["Unknown Job"],
                "vacancy_count": [1],
                "province": ["Alberta"],
                "city": ["Calgary"],
                "first_posting_date": pd.to_datetime(["2023-02-01"]),
                "salary_min_hourly": [None],
                "salary_max_hourly": [None],
            })
            count = load_job_postings(df, conn)
            assert count == 1
        finally:
            conn.close()

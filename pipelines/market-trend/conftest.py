"""
Shared fixtures for market-trend pipeline tests.
"""

import pytest


@pytest.fixture(autouse=True)
def clean_db():
    """Truncate tables before each integration test."""
    try:
        from src.load import get_connection
        conn = get_connection("postgresql://postgres:postgres@localhost:5432/giljobi")
        cur = conn.cursor()
        cur.execute("TRUNCATE job_postings, noc_titles RESTART IDENTITY CASCADE")
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass  # DB not running — skip cleanup (unit tests)
    yield

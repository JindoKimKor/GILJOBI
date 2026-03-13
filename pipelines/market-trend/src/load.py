"""
LOAD stage — Insert noc_titles and job_postings into PostgreSQL.
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def get_connection(connection_string: str):
    """Return a psycopg2 connection."""
    return psycopg2.connect(connection_string)


def load_noc_titles(df: pd.DataFrame, conn) -> int:
    """
    Insert noc_titles rows into PostgreSQL.
    Skips duplicates (ON CONFLICT DO NOTHING).
    Returns number of rows inserted.
    """
    cur = conn.cursor()
    rows = list(df[["noc21_code", "noc21_name"]].itertuples(index=False, name=None))
    execute_values(
        cur,
        "INSERT INTO noc_titles (noc21_code, noc21_name) VALUES %s ON CONFLICT (noc21_code) DO NOTHING",
        rows,
    )
    count = cur.rowcount
    conn.commit()
    cur.close()
    return count


def load_job_postings(df: pd.DataFrame, conn) -> int:
    """
    Insert job_postings rows into PostgreSQL.
    Returns number of rows inserted.
    """
    cur = conn.cursor()
    columns = [
        "noc_id", "normalized_title", "vacancy_count",
        "province", "city", "first_posting_date",
        "salary_min_hourly", "salary_max_hourly",
    ]
    # Convert NaN/NaT to None for psycopg2
    clean = df[columns].where(df[columns].notna(), None)
    rows = list(clean.itertuples(index=False, name=None))
    execute_values(
        cur,
        """INSERT INTO job_postings
           (noc_id, normalized_title, vacancy_count, province, city,
            first_posting_date, salary_min_hourly, salary_max_hourly)
           VALUES %s""",
        rows,
    )
    count = cur.rowcount
    conn.commit()
    cur.close()
    return count

"""
LOAD stage — Insert noc_titles and job_postings into PostgreSQL.

Handles database operations for the pipeline's two tables:
- noc_titles: Reference table of NOC 2021 occupation codes (one-time setup)
- job_postings: Monthly job posting data (appended each pipeline run)

Uses execute_values for noc_titles (needs ON CONFLICT) and
COPY for job_postings (bulk append, ~10x faster than INSERT).
"""

import io

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def get_connection(connection_string: str):
    """Create a PostgreSQL database connection.

    Args:
        connection_string: PostgreSQL connection URI.
            Example: 'postgresql://postgres:postgres@localhost:5432/giljobi'

    Returns:
        psycopg2 connection object.
    """
    return psycopg2.connect(connection_string)


def load_noc_titles(df: pd.DataFrame, conn) -> int:
    """Insert NOC title rows into the noc_titles table.

    Uses ON CONFLICT DO NOTHING to skip duplicate noc21_codes,
    making this safe for repeated runs without clearing the table.

    Args:
        df: DataFrame with 'noc21_code' and 'noc21_name' columns.
        conn: Active psycopg2 connection.

    Returns:
        Number of rows actually inserted (excludes skipped duplicates).
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
    """Insert job posting rows using PostgreSQL COPY for bulk loading.

    COPY streams data directly into the table without SQL parsing overhead,
    making it ~10x faster than execute_values for large datasets.
    Uses tab-separated in-memory buffer with \\N for NULL values.

    Args:
        df: DataFrame with columns matching the job_postings schema:
            noc_id, normalized_title, vacancy_count, province, city,
            first_posting_date, salary_min_hourly, salary_max_hourly.
        conn: Active psycopg2 connection.

    Returns:
        Number of rows inserted.
    """
    columns = [
        "noc_id", "normalized_title", "vacancy_count",
        "province", "city", "first_posting_date",
        "salary_min_hourly", "salary_max_hourly",
    ]
    # Build tab-separated text buffer for COPY.
    # PostgreSQL COPY uses \N to represent NULL values.
    int_cols = {"noc_id", "vacancy_count"}
    lines = []
    for row in df[columns].itertuples(index=False, name=None):
        parts = []
        for col_name, v in zip(columns, row):
            if pd.isna(v):
                parts.append("\\N")
            elif col_name in int_cols:
                parts.append(str(int(v)))
            else:
                parts.append(str(v))
        lines.append("\t".join(parts))

    buf = io.StringIO("\n".join(lines) + "\n")
    cur = conn.cursor()
    cur.copy_from(buf, "job_postings", columns=columns, null="\\N")
    conn.commit()
    cur.close()
    return len(lines)

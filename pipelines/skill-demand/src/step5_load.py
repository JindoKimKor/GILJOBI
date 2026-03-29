"""
step5_load.py — Load enriched data into PostgreSQL.

Inserts into:
- jd_postings: one row per job posting
- jd_skills: one row per skill per posting (exploded from skills[])

Tables are created by main.py ensure_schema() before this step runs.
"""

import pandas as pd

# =============================================================================
# Config
# =============================================================================
POSTINGS_COLUMNS = [
    "company", "raw_title", "noc_id", "noc_match_score",
    "noc_match_method", "seniority", "description",
]


# =============================================================================
# Prepare Rows
# =============================================================================
def prepare_postings_rows(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of dicts for jd_postings insert.

    Args:
        df: Enriched DataFrame with all pipeline columns.

    Returns:
        List of dicts with POSTINGS_COLUMNS keys.
    """
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "company": row.get("company_name"),
            "raw_title": row.get("raw_title"),
            "noc_id": None if pd.isna(row.get("noc_id")) else int(row["noc_id"]),
            "noc_match_score": None if pd.isna(row.get("noc_match_score")) else float(row["noc_match_score"]),
            "noc_match_method": row.get("noc_match_method"),
            "seniority": row.get("seniority"),
            "description": row.get("description"),
        })
    return rows


def prepare_skills_rows(data: list[dict]) -> list[dict]:
    """Explode skills arrays into individual rows for jd_skills insert.

    Args:
        data: List of dicts with job_id and skills (list of strings).

    Returns:
        List of dicts with job_id and skill (single string each).
    """
    rows = []
    for item in data:
        for skill in item.get("skills", []):
            rows.append({
                "job_id": item["job_id"],
                "skill": skill,
            })
    return rows


# =============================================================================
# DB Insert
# =============================================================================
def load_postings(rows: list[dict], conn) -> int:
    """Insert posting rows into jd_postings table.

    Args:
        rows: List of dicts from prepare_postings_rows().
        conn: psycopg2 connection.

    Returns:
        Number of rows inserted.
    """
    if not rows:
        return 0

    cur = conn.cursor()
    count = 0
    for row in rows:
        cur.execute(
            """INSERT INTO jd_postings
               (company, raw_title, noc_id, noc_match_score,
                noc_match_method, seniority, description)
               VALUES (%(company)s, %(raw_title)s, %(noc_id)s,
                       %(noc_match_score)s, %(noc_match_method)s,
                       %(seniority)s, %(description)s)
               ON CONFLICT DO NOTHING""",
            row,
        )
        count += 1
    conn.commit()
    cur.close()
    return count


def load_skills(skills_rows: list[dict], conn) -> int:
    """Insert skill rows into jd_skills table.

    Args:
        skills_rows: List of dicts from prepare_skills_rows().
        conn: psycopg2 connection.

    Returns:
        Number of rows inserted.
    """
    if not skills_rows:
        return 0

    cur = conn.cursor()
    count = 0
    for row in skills_rows:
        cur.execute(
            """INSERT INTO jd_skills (jd_id, skill)
               VALUES (%(job_id)s, %(skill)s)
               ON CONFLICT DO NOTHING""",
            row,
        )
        count += 1
    conn.commit()
    cur.close()
    return count

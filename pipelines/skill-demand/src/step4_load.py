"""
step4_load.py — Load enriched data into PostgreSQL.

Inserts into:
- jd_postings: one row per job posting (job_id as PK)
- jd_skills: one row per skill per posting (with category)

Tables created by schema.sql before this step runs.
"""

import pandas as pd


def _is_null(value) -> bool:
    """Check if value is null — handles pd.NaN, None, and string 'NaN'."""
    if value is None:
        return True
    if isinstance(value, str) and value in ("NaN", "nan", "None", ""):
        return True
    try:
        return pd.isna(value)
    except (TypeError, ValueError):
        return False


# =============================================================================
# Config
# =============================================================================
POSTINGS_COLUMNS = [
    "job_id", "company", "raw_title", "noc_id", "noc_match_score",
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
            "job_id": int(row["job_id"]),
            "company": row.get("company_name"),
            "raw_title": row.get("title"),
            "noc_id": None if _is_null(row.get("noc_id")) else int(row["noc_id"]),
            "noc_match_score": None if _is_null(row.get("noc_match_score")) else float(row["noc_match_score"]),
            "noc_match_method": None if _is_null(row.get("noc_match_method")) else row["noc_match_method"],
            "seniority": None if _is_null(row.get("seniority")) else row["seniority"],
            "description": row.get("description"),
        })
    return rows


def prepare_skills_rows(data: list[dict]) -> list[dict]:
    """Explode skills arrays into individual rows for jd_skills insert.

    Args:
        data: List of dicts with job_id and skills (list of {name, category} dicts).

    Returns:
        List of dicts with job_id, skill, category.
    """
    rows = []
    for item in data:
        skills = item.get("skills", [])
        if hasattr(skills, '__iter__'):
            for skill in skills:
                if isinstance(skill, dict):
                    rows.append({
                        "job_id": item["job_id"],
                        "skill": skill.get("name", ""),
                        "category": skill.get("category", ""),
                    })
                elif isinstance(skill, str):
                    rows.append({
                        "job_id": item["job_id"],
                        "skill": skill,
                        "category": "",
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
               (job_id, company, raw_title, noc_id, noc_match_score,
                noc_match_method, seniority, description)
               VALUES (%(job_id)s, %(company)s, %(raw_title)s, %(noc_id)s,
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
            """INSERT INTO jd_skills (jd_id, skill, category)
               VALUES (%(job_id)s, %(skill)s, %(category)s)
               ON CONFLICT DO NOTHING""",
            row,
        )
        count += 1
    conn.commit()
    cur.close()
    return count

"""
step4_load.py — Load enriched data into PostgreSQL (Star Schema).

Strategy: Drop all tables → recreate → COPY bulk insert.
Data warehouse pattern: full rebuild each run from parquet (source of truth).

Dimensions:
- dim_companies: unique company names
- dim_skills: normalized skills (inflect singular + category majority vote)
- dim_seniority: 5 fixed levels
- noc_titles: already exists (dim_noc)

Facts:
- fact_job_postings: one row per posting, FK to dimensions
- fact_job_skill_demand: one row per posting × skill, PK (job_id, skill_id)

Tables created by schema.sql.
"""

import io
import csv
import pandas as pd
from collections import Counter


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
# Skill Normalization
# =============================================================================
def normalize_skills(skill_rows: list[dict]) -> tuple[list[dict], dict, dict]:
    """Normalize skills: singular form + category majority vote.

    Args:
        skill_rows: [{"job_id": 123, "skill": "communications", "category": "soft_skill"}, ...]

    Returns:
        (normalized_rows, dim_skills_dict, stats)
        normalized_rows: rows with singular skill names + majority category
        dim_skills_dict: {"skill_name": "category"} for dim_skills insert
        stats: {"plural_map": {...}, "category_votes": {...}}
    """
    import inflect
    p = inflect.engine()

    # Words that should stay plural (business/domain terms)
    KEEP_PLURAL = {
        "sales", "operations", "logistics", "analytics", "economics",
        "mathematics", "statistics", "physics", "electronics", "robotics",
        "dynamics", "graphics", "ceramics", "genetics", "orthotics",
        "prosthetics", "diagnostics", "informatics", "bioinformatics",
    }

    # Step 1: plural → singular
    plural_map = {}
    for row in skill_rows:
        name = row["skill"]
        if name in KEEP_PLURAL:
            continue
        singular = p.singular_noun(name)
        if singular:
            plural_map[name] = singular
            row["skill"] = singular

    # Step 2: category majority vote
    category_votes = {}
    for row in skill_rows:
        name = row["skill"]
        if name not in category_votes:
            category_votes[name] = Counter()
        category_votes[name][row["category"]] += 1

    dim_skills = {}
    for skill_name, votes in category_votes.items():
        dim_skills[skill_name] = votes.most_common(1)[0][0]

    # Apply majority category to all rows
    for row in skill_rows:
        row["category"] = dim_skills[row["skill"]]

    stats = {
        "plural_map": plural_map,
        "category_votes": {k: v for k, v in category_votes.items() if len(v) > 1},
    }

    return skill_rows, dim_skills, stats


# =============================================================================
# Explode Skills from DataFrame
# =============================================================================
def explode_skills(df: pd.DataFrame) -> list[dict]:
    """Explode skills arrays into individual rows.

    Args:
        df: DataFrame with job_id and skills columns.

    Returns:
        [{"job_id": 123, "skill": "communication", "category": "soft_skill"}, ...]
    """
    rows = []
    seen = set()
    for _, row in df.iterrows():
        job_id = int(row["job_id"])
        skills = row.get("skills", [])
        if not hasattr(skills, '__iter__'):
            continue
        for skill in skills:
            if isinstance(skill, dict):
                name = skill.get("name", "").strip()
                category = skill.get("category", "").strip()
            elif isinstance(skill, str):
                name = skill.strip()
                category = ""
            else:
                continue
            if name and category:
                key = (job_id, name, category)
                if key not in seen:
                    seen.add(key)
                    rows.append({"job_id": job_id, "skill": name, "category": category})
    return rows


# =============================================================================
# Drop + Recreate
# =============================================================================
def drop_star_schema(conn):
    """Drop all star schema tables (preserves noc_titles)."""
    cur = conn.cursor()
    for t in ["fact_job_skill_demand", "fact_job_postings", "dim_skills", "dim_companies", "dim_seniority"]:
        cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    cur.execute("DROP VIEW IF EXISTS skill_demand_summary CASCADE")
    conn.commit()
    cur.close()


# =============================================================================
# Bulk Loaders (COPY)
# =============================================================================
def _copy_from_dicts(cur, table: str, columns: list[str], rows: list[dict]):
    """Bulk insert using COPY FROM with in-memory CSV buffer."""
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter='\t')
    for row in rows:
        writer.writerow([row.get(col) for col in columns])
    buf.seek(0)
    cur.copy_from(buf, table, columns=columns, null='None')


def load_dim_seniority(conn) -> dict:
    """Insert 5 seniority levels. Returns {level: id} mapping."""
    levels = ["intern", "entry_level", "mid_level", "senior", "executive"]
    cur = conn.cursor()
    for level in levels:
        cur.execute("INSERT INTO dim_seniority (level) VALUES (%s)", (level,))
    conn.commit()
    cur.execute("SELECT level, id FROM dim_seniority")
    mapping = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return mapping


def load_dim_companies(companies: list[str], conn) -> dict:
    """Bulk insert unique companies. Returns {name: id} mapping."""
    cur = conn.cursor()
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter='\t')
    for name in companies:
        if name and not _is_null(name):
            writer.writerow([name])
    buf.seek(0)
    cur.copy_from(buf, "dim_companies", columns=["name"], null='None')
    conn.commit()
    cur.execute("SELECT name, id FROM dim_companies")
    mapping = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return mapping


def load_dim_skills(dim_skills: dict, conn) -> dict:
    """Bulk insert normalized skills. Returns {name: id} mapping."""
    cur = conn.cursor()
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter='\t')
    for name, category in dim_skills.items():
        if name:
            writer.writerow([name, category])
    buf.seek(0)
    cur.copy_from(buf, "dim_skills", columns=["name", "category"], null='None')
    conn.commit()
    cur.execute("SELECT name, id FROM dim_skills")
    mapping = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return mapping


def load_fact_postings(df: pd.DataFrame, company_map: dict, seniority_map: dict, conn) -> int:
    """Bulk insert postings with FK lookups."""
    cur = conn.cursor()
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter='\t')
    count = 0
    for _, row in df.iterrows():
        company_name = row.get("company_name")
        company_id = company_map.get(company_name) if not _is_null(company_name) else None
        seniority = row.get("seniority")
        seniority_id = seniority_map.get(seniority) if not _is_null(seniority) else None

        writer.writerow([
            int(row["job_id"]),
            company_id,
            None if _is_null(row.get("noc_id")) else int(row["noc_id"]),
            seniority_id,
            row.get("title"),
            None if _is_null(row.get("noc_match_score")) else float(row["noc_match_score"]),
            None if _is_null(row.get("noc_match_method")) else row["noc_match_method"],
            row.get("description"),
        ])
        count += 1
    buf.seek(0)
    cur.copy_from(buf, "fact_job_postings",
                  columns=["job_id", "company_id", "noc_id", "seniority_id",
                           "raw_title", "noc_match_score", "noc_match_method", "description"],
                  null='None')
    conn.commit()
    cur.close()
    return count


def load_fact_skills(skill_rows: list[dict], skill_map: dict, conn) -> int:
    """Bulk insert job × skill facts with FK lookups."""
    cur = conn.cursor()
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter='\t')
    count = 0
    seen = set()
    for row in skill_rows:
        skill_id = skill_map.get(row["skill"])
        if skill_id is None:
            continue
        key = (row["job_id"], skill_id)
        if key in seen:
            continue
        seen.add(key)
        writer.writerow([row["job_id"], skill_id])
        count += 1
    buf.seek(0)
    cur.copy_from(buf, "fact_job_skill_demand", columns=["job_id", "skill_id"], null='None')
    conn.commit()
    cur.close()
    return count

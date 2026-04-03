"""
step3_enrich.py — Enrich job postings with NOC + seniority + skills via LLM.

2 prompt types based on NOC match status:
  - unmatched: NOC list + JD → NOC + seniority + skills (5/call)
  - matched: JD only → seniority + skills (10/call)

Seniority handled per-row within the prompt:
  - filled → "Seniority: {tier} (already determined - skip)"
  - missing → "Seniority: missing - determine from description"

Skills categorized: hard_skill, soft_skill, tool, certification.
Batches grouped by similarity (company + noc_id) for LLM accuracy.

Auth: Claude CLI with HOME=/tmp (host ~/.claude mounted to /tmp/.claude).
"""

import json
import re

from src.step2_seniority import SENIORITY_TIERS

# =============================================================================
# Config
# =============================================================================
SKILL_CATEGORIES = ["hard_skill", "soft_skill", "tool", "certification"]
MAX_DESC_CHARS = 1500

BATCH_SIZE_UNMATCHED = 5   # NOC list in prompt → larger prompt → fewer per call
BATCH_SIZE_MATCHED = 10    # No NOC list → smaller prompt → more per call

_SENIORITY_GUIDE = """Seniority levels:
- intern: internship, co-op, student placement
- entry_level: junior, graduate, trainee
- mid_level: intermediate, mid-level
- senior: senior, lead, principal, staff
- executive: director, VP, C-level, head of"""

_SKILL_CATEGORIES_GUIDE = """Skill categories:
- hard_skill: domain-specific technical knowledge, programming languages (e.g. Python, data modeling, welding)
- soft_skill: interpersonal and behavioral (e.g. communication, leadership, teamwork)
- tool: software, platform, system (e.g. Excel, SAP, Docker, AWS, Kubernetes)
- certification: formal credential or license (e.g. CPA, PMP, AWS Certified)"""


# =============================================================================
# Prompt Helpers
# =============================================================================
def _build_jobs_block(rows: list[dict]) -> str:
    """Build job listings with per-row seniority status."""
    lines = []
    for row in rows:
        desc = (row.get("description") or "")[:MAX_DESC_CHARS]
        seniority = row.get("seniority")
        lines.append(f"Job (ID: {row['job_id']}): {row['title']}")
        if seniority:
            lines.append(f"Seniority: {seniority} (already determined - skip)")
        else:
            lines.append("Seniority: missing - determine from description")
        lines.append(desc)
        lines.append("")
    return "\n".join(lines)


# =============================================================================
# 2 Prompt Builders
# =============================================================================
def build_unmatched_prompt(rows: list[dict], noc_list_str: str) -> str:
    """Unmatched rows: NOC + seniority(if missing) + skills.

    Args:
        rows: Row dicts with job_id, title, description, company_name, seniority.
        noc_list_str: Pre-formatted NOC category list string.
    """
    company = rows[0].get("company_name", "Unknown")
    jobs_block = _build_jobs_block(rows)

    return f"""For each job posting, determine:
1. Best NOC 2021 category from the list below (or null if none fit)
2. Seniority (only if marked as missing)
3. Skills with category

{_SENIORITY_GUIDE}

{_SKILL_CATEGORIES_GUIDE}

Company: {company}

{jobs_block}
Available NOC 2021 categories:
{noc_list_str}

Return JSON only:
{{"results": [{{"job_id": 123, "noc_title": "category or null", "seniority": "tier or null", "skills": [{{"name": "skill", "category": "type"}}]}}]}}"""


def build_matched_prompt(rows: list[dict]) -> str:
    """Matched rows: seniority(if missing) + skills. No NOC list.

    Args:
        rows: Row dicts with job_id, title, description, company_name, seniority.
    """
    company = rows[0].get("company_name", "Unknown")
    jobs_block = _build_jobs_block(rows)

    return f"""For each job posting, extract:
1. Seniority (only if marked as missing)
2. Skills with category

{_SENIORITY_GUIDE}

{_SKILL_CATEGORIES_GUIDE}

Company: {company}

{jobs_block}
Return JSON only:
{{"results": [{{"job_id": 123, "seniority": "tier or null", "skills": [{{"name": "skill", "category": "type"}}]}}]}}"""


# =============================================================================
# Response Parsing
# =============================================================================
def parse_enrich_response(
    response: str,
    noc_lookup: dict,
) -> list[dict]:
    """Parse LLM response into structured results.

    Args:
        response: Raw Claude CLI stdout.
        noc_lookup: Dict mapping NOC title → noc_id.

    Returns:
        List of dicts with job_id, noc_id, seniority, skills.
        Empty list on parse failure.
    """
    if not response or not response.strip():
        return []

    # Extract JSON from markdown block if present
    cleaned = response.strip()
    md_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', cleaned, re.DOTALL)
    if md_match:
        cleaned = md_match.group(1).strip()

    json_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if not json_match:
        return []

    try:
        parsed = json.loads(json_match.group())
    except json.JSONDecodeError:
        return []

    results_raw = parsed.get("results", [])
    if not isinstance(results_raw, list):
        return []

    results = []
    for item in results_raw:
        # NOC
        noc_title = item.get("noc_title")
        noc_id = None
        if noc_title and noc_title != "null" and noc_title in noc_lookup:
            noc_id = noc_lookup[noc_title]

        # Seniority
        seniority = item.get("seniority")
        if seniority not in SENIORITY_TIERS:
            seniority = None

        # Skills — validate category, lowercase name
        raw_skills = item.get("skills", [])
        if not isinstance(raw_skills, list):
            raw_skills = []
        skills = []
        for s in raw_skills:
            if not isinstance(s, dict):
                continue
            name = s.get("name", "")
            category = s.get("category", "")
            if name and category in SKILL_CATEGORIES:
                skills.append({"name": name.lower().strip(), "category": category})

        results.append({
            "job_id": item.get("job_id"),
            "noc_id": noc_id,
            "seniority": seniority,
            "skills": skills,
        })

    return results

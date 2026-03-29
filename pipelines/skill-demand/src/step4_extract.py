"""
step4_extract.py — Seniority + Skill Extraction via LLM.

Single Claude Haiku CLI call per JD extracts both:
- seniority: one of 5 tiers (intern, entry_level, mid_level, senior, executive)
- skills: list of lowercase technical skill strings

Same auth and rate limiting config as step3_noc_llm_fallback.py.
"""

import json
import os
import re
import subprocess

from src.step3_noc_llm_fallback import build_claude_env, CLAUDE_HOME

# =============================================================================
# Config
# =============================================================================
LLM_MODEL = "haiku"
SENIORITY_TIERS = ["intern", "entry_level", "mid_level", "senior", "executive"]

# Rate limiting — shared with step3
BATCH_SIZE = 10
BATCH_DELAY_SEC = 5
MAX_BATCHES_PER_RUN = 50


# =============================================================================
# Prompt
# =============================================================================
def build_extract_prompt(description: str) -> str:
    """Build prompt to extract seniority and skills from a JD.

    Args:
        description: Full job description text.

    Returns:
        Prompt string for Claude CLI.
    """
    tiers_str = ", ".join(SENIORITY_TIERS)
    return f"""Analyze this job description and extract:
1. seniority: one of [{tiers_str}]
2. skills: list of technical skills mentioned (lowercase)

Job Description:
{description[:3000]}

Return JSON only, no explanation:
{{"seniority": "tier", "skills": ["skill1", "skill2", ...]}}"""


# =============================================================================
# Response Parsing
# =============================================================================
def parse_extract_response(response: str) -> dict:
    """Parse Claude CLI response into seniority + skills.

    Returns:
        {"seniority": str|None, "skills": list[str]}
    """
    defaults = {"seniority": None, "skills": []}

    if not response or not response.strip():
        return defaults

    # Extract from markdown block if present
    cleaned = response.strip()
    md_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', cleaned, re.DOTALL)
    if md_match:
        cleaned = md_match.group(1).strip()

    json_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if not json_match:
        return defaults

    try:
        parsed = json.loads(json_match.group())
    except json.JSONDecodeError:
        return defaults

    # Validate seniority
    seniority = parsed.get("seniority")
    if seniority not in SENIORITY_TIERS:
        seniority = None

    # Extract and lowercase skills
    skills = parsed.get("skills", [])
    if not isinstance(skills, list):
        skills = []
    skills = [s.lower().strip() for s in skills if isinstance(s, str)]

    return {"seniority": seniority, "skills": skills}


# =============================================================================
# Single Batch Extraction
# =============================================================================
def extract_single_batch(rows: list[dict]) -> list[dict]:
    """Extract seniority + skills from a batch of JDs using Claude CLI.

    Args:
        rows: List of dicts with job_id, description.

    Returns:
        List of dicts with job_id, seniority, skills.
    """
    if not rows:
        return []

    results = []
    for row in rows:
        prompt = build_extract_prompt(row.get("description", ""))

        try:
            result = subprocess.run(
                ["claude", "--print", "--model", LLM_MODEL, "-"],
                input=prompt,
                capture_output=True,
                text=True,
                env=build_claude_env(),
            )

            if result.returncode != 0:
                results.append({"job_id": row["job_id"], "seniority": None, "skills": []})
                continue

            parsed = parse_extract_response(result.stdout)
            results.append({
                "job_id": row["job_id"],
                "seniority": parsed["seniority"],
                "skills": parsed["skills"],
            })

        except Exception:
            results.append({"job_id": row["job_id"], "seniority": None, "skills": []})

    return results

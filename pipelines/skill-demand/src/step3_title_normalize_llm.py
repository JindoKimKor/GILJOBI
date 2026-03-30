"""
step3_title_normalize_llm.py — LLM Title Normalization (company-grouped).

For unmatched rows from step2_noc_match_st, normalizes job titles
using Claude Haiku CLI with company context. Same company's titles
are sent together in one LLM call for consistency and efficiency.

Auth: Host ~/.claude mounted to /tmp/.claude in Spark container.
"""

import json
import os
import re
import subprocess

# =============================================================================
# Config
# =============================================================================
LLM_MODEL = "haiku"
CLAUDE_HOME = "/tmp"
DEFAULT_MAX_TITLES_PER_CALL = 50


# =============================================================================
# Claude CLI Environment
# =============================================================================
def build_claude_env() -> dict:
    env = {**os.environ}
    env["HOME"] = CLAUDE_HOME
    return env


# =============================================================================
# Prompt
# =============================================================================
def build_normalize_prompt(company: str, titles: list[str]) -> str:
    """Build prompt to normalize job titles for a company.

    Args:
        company: Company name for context.
        titles: List of raw job titles to normalize.

    Returns:
        Prompt string for Claude CLI.
    """
    titles_str = "\n".join(f"- {t}" for t in titles)
    return f"""Normalize these job titles to standard, clean job titles.
Remove location info, level numbers, internal codes, and company-specific jargon.
Keep the core role. Use the company name for context.

Company: {company}

Job titles to normalize:
{titles_str}

Return JSON only, no explanation:
{{"mappings": [{{"raw": "original title", "normalized": "standard job title"}}]}}"""


# =============================================================================
# Response Parsing
# =============================================================================
def parse_normalize_response(response: str) -> dict:
    """Parse Claude CLI response into raw→normalized title mapping.

    Returns:
        Dict mapping raw title → normalized title.
        Empty dict on parse failure.
    """
    if not response or not response.strip():
        return {}

    cleaned = response.strip()
    md_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', cleaned, re.DOTALL)
    if md_match:
        cleaned = md_match.group(1).strip()

    json_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if not json_match:
        return {}

    try:
        parsed = json.loads(json_match.group())
        mappings = parsed.get("mappings", [])
        return {m["raw"]: m["normalized"] for m in mappings if "raw" in m and "normalized" in m}
    except (json.JSONDecodeError, KeyError):
        return {}


# =============================================================================
# Company Batch Normalization
# =============================================================================
def normalize_company_batch(
    rows: list[dict],
    max_titles_per_call: int = DEFAULT_MAX_TITLES_PER_CALL,
) -> list[dict]:
    """Normalize titles for rows from the same company.

    Args:
        rows: List of dicts with job_id, title, company_name (+ any other fields).
        max_titles_per_call: Max titles per LLM call (batch internally for large companies).

    Returns:
        Same rows with added 'normalized_title' field.
    """
    if not rows:
        return []

    company = rows[0].get("company_name", "Unknown")
    all_titles = [r["title"] for r in rows]

    # Collect all mappings across batches
    all_mappings = {}
    for chunk_start in range(0, len(all_titles), max_titles_per_call):
        chunk_titles = all_titles[chunk_start:chunk_start + max_titles_per_call]
        # Deduplicate within chunk
        unique_titles = list(set(chunk_titles))

        prompt = build_normalize_prompt(company, unique_titles)

        try:
            result = subprocess.run(
                ["claude", "--print", "--model", LLM_MODEL, "-"],
                input=prompt,
                capture_output=True,
                text=True,
                env=build_claude_env(),
            )

            if result.returncode == 0:
                mappings = parse_normalize_response(result.stdout)
                all_mappings.update(mappings)
        except Exception:
            pass

    # Apply mappings — fallback to raw title if not mapped
    results = []
    for row in rows:
        new_row = dict(row)
        new_row["normalized_title"] = all_mappings.get(row["title"], row["title"])
        results.append(new_row)

    return results

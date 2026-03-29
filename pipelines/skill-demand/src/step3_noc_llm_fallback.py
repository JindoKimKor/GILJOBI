"""
step3_noc_llm_fallback.py — LLM fallback for sub-threshold NOC matches.

For job titles that Step 2 (Sentence Transformers) couldn't confidently
match to a NOC code, this step uses Claude Haiku CLI to read the full
JD description and determine the best NOC match.

Auth: Host ~/.claude mounted to /tmp/.claude in Spark container.
      HOME=/tmp so Claude CLI finds OAuth credentials.

Rate limiting: Subscription session limits require controlled batch
processing with delays between calls and a max batches per run cap.
Checkpoint pattern enables resume across runs.
"""

import json
import os
import re
import subprocess

import pandas as pd

# =============================================================================
# Config
# =============================================================================
LLM_MODEL = "haiku"
CLAUDE_HOME = "/tmp"

# Rate limiting — subscription session limit compliance
BATCH_SIZE = 10              # JDs per LLM call
BATCH_DELAY_SEC = 5          # seconds between batches
MAX_BATCHES_PER_RUN = 50     # stop after N batches (resume via checkpoints)


# =============================================================================
# Claude CLI Environment
# =============================================================================
def build_claude_env() -> dict:
    """Build environment dict for Claude CLI subprocess.

    Sets HOME=/tmp so Claude CLI reads credentials from /tmp/.claude
    (mounted from host ~/.claude in Docker).
    """
    env = {**os.environ}
    env["HOME"] = CLAUDE_HOME
    return env


# =============================================================================
# Prompt
# =============================================================================
def build_noc_prompt(title: str, description: str, noc_candidates: list[str]) -> str:
    """Build prompt for Claude to match a job to a NOC category.

    Args:
        title: Job title from posting.
        description: Full JD text.
        noc_candidates: List of NOC 2021 unit group title strings.

    Returns:
        Prompt string for Claude CLI.
    """
    candidates_str = "\n".join(f"- {c}" for c in noc_candidates)
    return f"""Given the following job posting, determine which NOC 2021 category best matches.
If none of the categories fit, return null.

Job Title: {title}

Job Description:
{description[:2000]}

Available NOC categories:
{candidates_str}

Return JSON only, no explanation:
{{"noc_title": "matching NOC category title or null"}}"""


# =============================================================================
# Response Parsing
# =============================================================================
def parse_llm_response(response: str) -> str | None:
    """Extract NOC title from Claude CLI response.

    Handles both raw JSON and markdown-wrapped JSON blocks.

    Returns:
        NOC title string, or None if parsing fails or value is null.
    """
    if not response or not response.strip():
        return None

    # Try to extract JSON from markdown code block
    cleaned = response.strip()
    md_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', cleaned, re.DOTALL)
    if md_match:
        cleaned = md_match.group(1).strip()

    # Try to find JSON object
    json_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if not json_match:
        return None

    try:
        parsed = json.loads(json_match.group())
        value = parsed.get("noc_title")
        if value is None or value == "null":
            return None
        return value
    except (json.JSONDecodeError, KeyError):
        return None


# =============================================================================
# Single Batch Match
# =============================================================================
def match_single_batch(rows: list[dict], noc_ref: pd.DataFrame) -> list[dict]:
    """Match a batch of rows to NOC codes using Claude CLI.

    Args:
        rows: List of dicts with job_id, title, description.
        noc_ref: NOC reference DataFrame with id, noc21_code, noc21_name.

    Returns:
        List of dicts with job_id, noc_id, noc_match_method.
    """
    if not rows:
        return []

    noc_candidates = noc_ref["noc21_name"].tolist()
    # Build name→id lookup
    name_to_id = {row["noc21_name"]: row["id"] for _, row in noc_ref.iterrows()}

    results = []
    for row in rows:
        prompt = build_noc_prompt(
            title=row["title"],
            description=row.get("description", ""),
            noc_candidates=noc_candidates,
        )

        try:
            result = subprocess.run(
                ["claude", "--print", "--model", LLM_MODEL, "-"],
                input=prompt,
                capture_output=True,
                text=True,
                env=build_claude_env(),
            )

            if result.returncode != 0:
                results.append({"job_id": row["job_id"], "noc_id": None, "noc_match_method": None})
                continue

            noc_title = parse_llm_response(result.stdout)
            noc_id = name_to_id.get(noc_title) if noc_title else None

            results.append({
                "job_id": row["job_id"],
                "noc_id": noc_id,
                "noc_match_method": "llm" if noc_id is not None else None,
            })

        except Exception:
            results.append({"job_id": row["job_id"], "noc_id": None, "noc_match_method": None})

    return results

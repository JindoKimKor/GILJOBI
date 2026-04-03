"""
step2_seniority.py — Seniority extraction from CSV + title keywords.

3-tier strategy (no LLM needed):
  1st: formatted_experience_level from CSV (76.3% filled)
  2nd: Title keyword matching (+6.2%)
  3rd: Remaining 18.7% → LLM in Step 3/4

Special case: "Mid-Senior level" defaults to mid_level,
but upgrades to senior if title contains senior-level keywords.

All keyword matching uses word boundary (\\b) to prevent false positives
(e.g. "sr" in "Israel", "lead" in "leading").
"""

import re

# =============================================================================
# Config
# =============================================================================
SENIORITY_TIERS = ["intern", "entry_level", "mid_level", "senior", "executive"]

CSV_SENIORITY_MAP = {
    "Internship": "intern",
    "Entry level": "entry_level",
    "Associate": "entry_level",
    "Mid-Senior level": "mid_level",
    "Director": "executive",
    "Executive": "executive",
}

# Ordered from highest to lowest priority for keyword matching
TITLE_KEYWORDS = {
    "executive": ["director", "vp", "vice president", "head of", "chief", "cto", "ceo", "cfo"],
    "senior": ["senior", "sr", "lead", "principal", "staff"],
    "mid_level": ["mid level", "mid-level", "intermediate"],
    "entry_level": ["junior", "jr", "entry level", "entry-level", "graduate", "trainee"],
    "intern": ["intern", "internship", "co-op", "coop"],
}

# Keywords that indicate senior level (used to refine Mid-Senior mapping)
_SENIOR_KEYWORDS = ["senior", "sr", "lead", "principal", "staff"]

# Pre-compile word boundary patterns
_KEYWORD_PATTERNS = {
    tier: [re.compile(rf'\b{re.escape(kw)}\b', re.IGNORECASE) for kw in keywords]
    for tier, keywords in TITLE_KEYWORDS.items()
}
_SENIOR_PATTERNS = [re.compile(rf'\b{re.escape(kw)}\b', re.IGNORECASE) for kw in _SENIOR_KEYWORDS]


# =============================================================================
# CSV Mapping
# =============================================================================
def map_experience_level(level: str | None) -> str | None:
    """Map LinkedIn formatted_experience_level to seniority tier.

    Args:
        level: Raw CSV value (e.g. "Mid-Senior level", "Entry level").

    Returns:
        Seniority tier string or None if unmappable.
    """
    if not level:
        return None
    return CSV_SENIORITY_MAP.get(level)


# =============================================================================
# Title Keyword Matching
# =============================================================================
def extract_seniority_from_title(title: str) -> str | None:
    """Extract seniority from job title keywords.

    Checks keywords in priority order: executive > senior > mid > entry > intern.
    Uses word boundary matching to avoid false positives.

    Args:
        title: Job title string.

    Returns:
        Seniority tier or None if no keywords found.
    """
    if not title:
        return None

    for tier, patterns in _KEYWORD_PATTERNS.items():
        for pattern in patterns:
            if pattern.search(title):
                return tier

    return None


# =============================================================================
# Combined Extraction
# =============================================================================
def extract_seniority(
    formatted_level: str | None,
    title: str,
) -> tuple[str | None, str | None]:
    """Extract seniority using CSV value first, then title keywords.

    Special case: "Mid-Senior level" checks title for senior keywords
    and upgrades to senior if found.

    Args:
        formatted_level: CSV formatted_experience_level value.
        title: Job title string.

    Returns:
        Tuple of (seniority, method) where method is:
        - "csv": direct CSV mapping
        - "csv_refined": Mid-Senior upgraded by title keyword
        - "title_keyword": no CSV value, matched by title
        - None: no seniority determined (needs LLM)
    """
    csv_tier = map_experience_level(formatted_level)

    if csv_tier is not None:
        # Special case: Mid-Senior can be refined by title keywords
        if formatted_level == "Mid-Senior level" and title:
            for pattern in _SENIOR_PATTERNS:
                if pattern.search(title):
                    return "senior", "csv_refined"
        return csv_tier, "csv"

    # Fallback to title keyword matching
    title_tier = extract_seniority_from_title(title)
    if title_tier is not None:
        return title_tier, "title_keyword"

    return None, None

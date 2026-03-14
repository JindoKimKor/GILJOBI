"""
SCRAPE stage — Extract CSV download URLs from the Open Data Portal.

The Canada Job Bank publishes monthly job posting CSVs on the Open Data Portal.
This module uses the CKAN API to retrieve all available CSV download links,
filtering for English-language files only.

The portal HTML is paginated (only shows ~5 recent files per page), so we use
the CKAN package_show API endpoint which returns all resources in one call.

API: https://open.canada.ca/data/api/3/action/package_show
Portal: https://open.canada.ca/data/en/dataset/ea639e28-c0fc-48bf-b5dd-b8899bd43072
"""

import json
import re
import urllib.request


# ============================================================
# Constants
# ============================================================

# CKAN API endpoint — returns all resources for the dataset in one call
DATASET_ID = "ea639e28-c0fc-48bf-b5dd-b8899bd43072"
API_URL = f"https://open.canada.ca/data/api/3/action/package_show?id={DATASET_ID}"

# Minimum expected English CSV URLs (Jan 2023 ~ Feb 2026 baseline)
MIN_EXPECTED_URLS = 37

# Maps month names (full + abbreviated) to two-digit month numbers.
# Used to parse filenames like "january2023" or "feb2025" into "YYYY-MM".
MONTH_MAP = {
    "january": "01", "jan": "01",
    "february": "02", "feb": "02",
    "march": "03", "mar": "03",
    "april": "04", "apr": "04",
    "may": "05",
    "june": "06", "jun": "06",
    "july": "07", "jul": "07",
    "august": "08", "aug": "08",
    "september": "09", "sep": "09",
    "october": "10", "oct": "10",
    "november": "11", "nov": "11",
    "december": "12", "dec": "12",
}


# ============================================================
# URL Filtering
# ============================================================

def filter_english_urls(urls: list[str]) -> list[str]:
    """Filter URLs to keep only English-language CSV files.

    The portal hosts both English and French CSVs. English files
    contain '-en-' or '-en.' in the filename.

    Args:
        urls: List of CSV download URLs.

    Returns:
        Filtered list containing only English CSV URLs.
    """
    return [u for u in urls if "-en-" in u or "-en." in u]


# ============================================================
# URL Parsing
# ============================================================

def parse_year_month(url: str) -> str:
    """Extract YYYY-MM from a CSV download URL.

    Parses the filename portion of the URL to find a month name
    (full or abbreviated) followed by a 4-digit year.
    Example: '.../en-january2023.csv' → '2023-01'

    Args:
        url: Full CSV download URL.

    Returns:
        Date string in 'YYYY-MM' format.

    Raises:
        ValueError: If month/year cannot be parsed from the URL.
    """
    filename = url.rsplit("/", 1)[-1].lower()

    # Match month name (full or abbreviated) followed by 4-digit year.
    # Optional hyphen between month and year to handle both formats:
    #   "january2023" and "june-2024"
    # Sorted by length (longest first) to match "september" before "sep"
    pattern = r"(" + "|".join(sorted(MONTH_MAP.keys(), key=len, reverse=True)) + r")-?(\d{4})"
    match = re.search(pattern, filename)
    if not match:
        raise ValueError(f"Cannot parse month/year from: {url}")

    month_str, year = match.group(1), match.group(2)
    month_num = MONTH_MAP[month_str]
    return f"{year}-{month_num}"


# ============================================================
# API Response Parsing
# ============================================================

def extract_csv_urls(api_response: str) -> list[str]:
    """Parse CKAN API response and return all English CSV download URLs.

    Extracts resource URLs from the JSON response, filters for .csv files,
    then keeps only English-language URLs. Validates that the result count
    meets the minimum threshold.

    Args:
        api_response: Raw JSON string from the CKAN package_show API.

    Returns:
        List of English CSV download URLs.

    Raises:
        ValueError: If fewer than MIN_EXPECTED_URLS English CSVs found,
            indicating a data issue or API change.
    """
    data = json.loads(api_response)
    resources = data["result"]["resources"]

    csv_urls = [r["url"] for r in resources if r["url"].endswith(".csv")]
    english_urls = filter_english_urls(csv_urls)

    if len(english_urls) < MIN_EXPECTED_URLS:
        raise ValueError(
            f"Expected >= {MIN_EXPECTED_URLS} English CSV URLs, found {len(english_urls)}"
        )

    return english_urls

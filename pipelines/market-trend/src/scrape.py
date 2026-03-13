"""
SCRAPE stage — Extract CSV download URLs from the Open Data Portal.
"""

import re
from html.parser import HTMLParser

PORTAL_URL = "https://open.canada.ca/data/en/dataset/ea639e28-c0fc-48bf-b5dd-b8899bd43072"
MIN_EXPECTED_URLS = 38

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


class _LinkExtractor(HTMLParser):
    """Extract all href values from <a> tags."""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.append(value)


def filter_english_urls(urls: list[str]) -> list[str]:
    """Keep only URLs whose filename contains '-en-' or '-en.'."""
    return [u for u in urls if "-en-" in u or "-en." in u]


def parse_year_month(url: str) -> str:
    """Extract YYYY-MM from a URL filename like '...en-january2023.csv'."""
    filename = url.rsplit("/", 1)[-1].lower()

    # Match month name (full or abbreviated) followed by 4-digit year
    pattern = r"(" + "|".join(sorted(MONTH_MAP.keys(), key=len, reverse=True)) + r")(\d{4})"
    match = re.search(pattern, filename)
    if not match:
        raise ValueError(f"Cannot parse month/year from: {url}")

    month_str, year = match.group(1), match.group(2)
    month_num = MONTH_MAP[month_str]
    return f"{year}-{month_num}"


def extract_csv_urls(html: str) -> list[str]:
    """
    Parse HTML and return English CSV download URLs.
    Raises ValueError if fewer than MIN_EXPECTED_URLS found.
    """
    parser = _LinkExtractor()
    parser.feed(html)

    csv_urls = [u for u in parser.links if u.endswith(".csv")]
    english_urls = filter_english_urls(csv_urls)

    if len(english_urls) < MIN_EXPECTED_URLS:
        raise ValueError(
            f"Expected >= {MIN_EXPECTED_URLS} English CSV URLs, found {len(english_urls)}"
        )

    return english_urls

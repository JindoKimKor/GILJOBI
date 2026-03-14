"""
Tests for market-trend pipeline SCRAPE stage.
Based on SPEC.md — Extraction Rules + Validation Rules.
"""

import json

import pytest

from src.scrape import extract_csv_urls, parse_year_month, filter_english_urls


# ============================================================
# URL Filtering
# ============================================================

class TestFilterEnglishUrls:
    """SPEC: Filter for English CSVs only (filename contains -en- or -en.)"""

    def test_keeps_english_urls(self):
        urls = [
            "https://example.com/download/job-bank-open-data-all-job-postings-en-jan2023.csv",
            "https://example.com/download/job-bank-open-data-all-job-postings-fr-jan2023.csv",
        ]
        result = filter_english_urls(urls)
        assert len(result) == 1
        assert "-en-" in result[0]

    def test_filters_out_french_urls(self):
        urls = [
            "https://example.com/download/job-bank-open-data-all-job-postings-fr-fev2026.csv",
            "https://example.com/download/job-bank-open-data-all-job-postings-fr-decembre2025.csv",
        ]
        result = filter_english_urls(urls)
        assert len(result) == 0

    def test_handles_en_dot_pattern(self):
        """Filenames might use -en. instead of -en-"""
        urls = [
            "https://example.com/download/some-file-en.csv",
        ]
        result = filter_english_urls(urls)
        assert len(result) == 1


# ============================================================
# Month/Year Parsing
# ============================================================

class TestParseYearMonth:
    """SPEC: Parse month/year from filename (e.g., january2023, feb2025)"""

    def test_full_month_name(self):
        url = "https://example.com/download/job-bank-open-data-all-job-postings-en-january2023.csv"
        assert parse_year_month(url) == "2023-01"

    def test_abbreviated_month(self):
        url = "https://example.com/download/job-bank-open-data-all-job-postings-en-feb2025.csv"
        assert parse_year_month(url) == "2025-02"

    def test_december(self):
        url = "https://example.com/download/job-bank-open-data-all-job-postings-en-december2025.csv"
        assert parse_year_month(url) == "2025-12"

    def test_all_months_parseable(self):
        months = [
            ("january2023", "2023-01"), ("february2023", "2023-02"),
            ("march2023", "2023-03"), ("april2023", "2023-04"),
            ("may2023", "2023-05"), ("june2023", "2023-06"),
            ("july2023", "2023-07"), ("august2023", "2023-08"),
            ("september2023", "2023-09"), ("october2023", "2023-10"),
            ("november2023", "2023-11"), ("december2023", "2023-12"),
        ]
        for month_str, expected in months:
            url = f"https://example.com/download/job-bank-en-{month_str}.csv"
            assert parse_year_month(url) == expected, f"Failed for {month_str}"


# ============================================================
# CKAN API Response Parsing
# ============================================================

def _make_api_response(urls: list[str]) -> str:
    """Helper: build a fake CKAN package_show JSON response."""
    resources = [{"url": url, "format": "CSV"} for url in urls]
    return json.dumps({"result": {"resources": resources}})


class TestExtractCsvUrls:
    """SPEC: Extracted URL count must be >= 37 (Jan 2023 ~ Feb 2026 baseline)"""

    def test_raises_if_fewer_than_37_urls(self):
        """Validation: minimum 37 English CSVs expected."""
        response = _make_api_response(["https://example.com/test.csv"])
        with pytest.raises(ValueError, match="Expected >= 37"):
            extract_csv_urls(response)

    def test_extracts_urls_from_api_response(self):
        """Should find all English .csv URLs from CKAN API response."""
        urls = []
        for i in range(40):
            month = f"january20{23 + i // 12}"
            urls.append(
                f"https://example.com/download/job-bank-en-{month}.csv"
            )
        response = _make_api_response(urls)
        result = extract_csv_urls(response)
        assert len(result) >= 37

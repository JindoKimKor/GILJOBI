"""
Tests for skill-demand pipeline STEP 3 — LLM Title Normalization.
Based on SPEC Pipeline Redesign — company-grouped Claude LLM title
normalization for unmatched rows from Step 2 (step2_noc_match_st).

Claude CLI runs inside Spark container only.
Tests mock subprocess.run to verify normalization logic.
"""

import json
import pandas as pd
import pytest
from unittest.mock import MagicMock

from src.step3_title_normalize_llm import (
    build_normalize_prompt,
    parse_normalize_response,
    normalize_company_batch,
)


# ============================================================
# Prompt Building
# ============================================================

class TestBuildPrompt:
    """Prompt includes company name and multiple titles."""

    def test_contains_company_name(self):
        prompt = build_normalize_prompt(
            company="Google",
            titles=["Software Engineer III", "Staff SWE"],
        )
        assert "Google" in prompt

    def test_contains_all_titles(self):
        titles = ["Software Engineer III", "Staff SWE, Cloud"]
        prompt = build_normalize_prompt(company="Google", titles=titles)
        for t in titles:
            assert t in prompt

    def test_asks_for_json(self):
        prompt = build_normalize_prompt(company="Google", titles=["SWE"])
        assert "json" in prompt.lower() or "JSON" in prompt

    def test_mentions_normalize(self):
        prompt = build_normalize_prompt(company="Google", titles=["SWE"])
        assert "normalize" in prompt.lower() or "standard" in prompt.lower()


# ============================================================
# Response Parsing
# ============================================================

class TestParseResponse:
    """Parse Claude CLI response into title mappings."""

    def test_parses_valid_response(self):
        response = json.dumps({
            "mappings": [
                {"raw": "Software Engineer III", "normalized": "Software Engineer"},
                {"raw": "Staff SWE", "normalized": "Software Engineer"},
            ]
        })
        result = parse_normalize_response(response)
        assert result["Software Engineer III"] == "Software Engineer"
        assert result["Staff SWE"] == "Software Engineer"

    def test_extracts_from_markdown_block(self):
        response = '```json\n{"mappings": [{"raw": "SWE", "normalized": "Software Engineer"}]}\n```'
        result = parse_normalize_response(response)
        assert result["SWE"] == "Software Engineer"

    def test_invalid_json_returns_empty(self):
        result = parse_normalize_response("not json")
        assert result == {}

    def test_empty_response_returns_empty(self):
        result = parse_normalize_response("")
        assert result == {}

    def test_missing_mappings_key(self):
        response = json.dumps({"something": "else"})
        result = parse_normalize_response(response)
        assert result == {}


# ============================================================
# Company Batch Normalization (mocked subprocess)
# ============================================================

class TestNormalizeCompanyBatch:
    """Normalize titles for a single company using mocked Claude CLI."""

    def test_successful_normalization(self, monkeypatch):
        rows = [
            {"job_id": 1, "title": "Software Engineer III", "company_name": "Google"},
            {"job_id": 2, "title": "Staff SWE", "company_name": "Google"},
        ]

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "mappings": [
                {"raw": "Software Engineer III", "normalized": "Software Engineer"},
                {"raw": "Staff SWE", "normalized": "Software Engineer"},
            ]
        })

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = normalize_company_batch(rows)
        assert len(results) == 2
        assert results[0]["normalized_title"] == "Software Engineer"
        assert results[1]["normalized_title"] == "Software Engineer"

    def test_preserves_original_title(self, monkeypatch):
        rows = [{"job_id": 1, "title": "SWE III", "company_name": "Google"}]

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "mappings": [{"raw": "SWE III", "normalized": "Software Engineer"}]
        })

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = normalize_company_batch(rows)
        assert results[0]["title"] == "SWE III"
        assert results[0]["normalized_title"] == "Software Engineer"

    def test_cli_failure_keeps_raw_title(self, monkeypatch):
        rows = [{"job_id": 1, "title": "SWE", "company_name": "Google"}]

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "error"

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = normalize_company_batch(rows)
        assert len(results) == 1
        assert results[0]["normalized_title"] == "SWE"

    def test_empty_batch_returns_empty(self):
        results = normalize_company_batch([])
        assert len(results) == 0

    def test_unmapped_title_keeps_raw(self, monkeypatch):
        """If LLM doesn't return a mapping for a title, use raw title."""
        rows = [
            {"job_id": 1, "title": "SWE", "company_name": "Google"},
            {"job_id": 2, "title": "Mystery Role", "company_name": "Google"},
        ]

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "mappings": [
                {"raw": "SWE", "normalized": "Software Engineer"},
            ]
        })

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = normalize_company_batch(rows)
        assert results[0]["normalized_title"] == "Software Engineer"
        assert results[1]["normalized_title"] == "Mystery Role"

    def test_large_company_batched(self, monkeypatch):
        """Companies with 50+ titles should be batched internally."""
        rows = [{"job_id": i, "title": f"Role {i}", "company_name": "BigCorp"} for i in range(60)]

        call_count = 0
        def mock_run(*a, **kw):
            nonlocal call_count
            call_count += 1
            mock = MagicMock()
            mock.returncode = 0
            mock.stdout = json.dumps({"mappings": []})
            return mock

        import subprocess
        monkeypatch.setattr(subprocess, "run", mock_run)

        results = normalize_company_batch(rows, max_titles_per_call=50)
        assert call_count >= 2
        assert len(results) == 60

    def test_preserves_all_original_fields(self, monkeypatch):
        rows = [{"job_id": 1, "title": "SWE", "company_name": "Google", "description": "Build stuff", "noc_match_score": 0.5}]

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "mappings": [{"raw": "SWE", "normalized": "Software Engineer"}]
        })

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = normalize_company_batch(rows)
        assert results[0]["job_id"] == 1
        assert results[0]["company_name"] == "Google"
        assert results[0]["description"] == "Build stuff"
        assert results[0]["noc_match_score"] == 0.5

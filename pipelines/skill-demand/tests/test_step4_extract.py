"""
Tests for skill-demand pipeline STEP 4 — Seniority + Skill Extraction.
Based on SPEC.md — Single LLM call per JD extracts both seniority AND skills.

Claude CLI runs inside Spark container only.
Tests mock subprocess.run to verify extraction logic.
Same rate limiting config as Step 3.
"""

import json
import pandas as pd
import pytest
from unittest.mock import MagicMock

from src.step4_extract import (
    build_extract_prompt,
    parse_extract_response,
    extract_single_batch,
    SENIORITY_TIERS,
    LLM_MODEL,
)


# ============================================================
# Prompt Building
# ============================================================

class TestBuildPrompt:
    """SPEC: Single LLM call extracts seniority + skills from JD."""

    def test_contains_description(self):
        prompt = build_extract_prompt("Build distributed systems with Java and AWS")
        assert "distributed systems" in prompt

    def test_mentions_seniority_tiers(self):
        prompt = build_extract_prompt("Some job description")
        for tier in SENIORITY_TIERS:
            assert tier in prompt

    def test_asks_for_skills(self):
        prompt = build_extract_prompt("Some job description")
        assert "skill" in prompt.lower()

    def test_asks_for_json(self):
        prompt = build_extract_prompt("Some job description")
        assert "json" in prompt.lower() or "JSON" in prompt


# ============================================================
# Response Parsing
# ============================================================

class TestParseResponse:
    """Parse Claude CLI response into seniority + skills."""

    def test_parses_valid_response(self):
        response = json.dumps({
            "seniority": "mid_level",
            "skills": ["python", "aws", "docker", "sql"]
        })
        result = parse_extract_response(response)
        assert result["seniority"] == "mid_level"
        assert result["skills"] == ["python", "aws", "docker", "sql"]

    def test_extracts_from_markdown_block(self):
        response = '```json\n{"seniority": "senior", "skills": ["java", "kubernetes"]}\n```'
        result = parse_extract_response(response)
        assert result["seniority"] == "senior"
        assert "java" in result["skills"]

    def test_lowercases_skills(self):
        response = json.dumps({
            "seniority": "entry_level",
            "skills": ["Python", "AWS", "Docker"]
        })
        result = parse_extract_response(response)
        assert result["skills"] == ["python", "aws", "docker"]

    def test_invalid_seniority_returns_none(self):
        response = json.dumps({
            "seniority": "wizard_level",
            "skills": ["python"]
        })
        result = parse_extract_response(response)
        assert result["seniority"] is None

    def test_empty_skills_returns_empty_list(self):
        response = json.dumps({
            "seniority": "mid_level",
            "skills": []
        })
        result = parse_extract_response(response)
        assert result["skills"] == []

    def test_invalid_json_returns_defaults(self):
        result = parse_extract_response("not json at all")
        assert result["seniority"] is None
        assert result["skills"] == []

    def test_empty_response_returns_defaults(self):
        result = parse_extract_response("")
        assert result["seniority"] is None
        assert result["skills"] == []

    def test_missing_fields_returns_defaults(self):
        response = json.dumps({"unrelated": "data"})
        result = parse_extract_response(response)
        assert result["seniority"] is None
        assert result["skills"] == []


# ============================================================
# Batch Extraction (mocked subprocess)
# ============================================================

class TestExtractSingleBatch:
    """Extract seniority + skills from a batch using mocked Claude CLI."""

    def test_successful_extraction(self, monkeypatch):
        rows = [{"job_id": 1, "description": "Senior Python developer with AWS experience"}]

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({
            "seniority": "senior",
            "skills": ["python", "aws"]
        })

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = extract_single_batch(rows)
        assert len(results) == 1
        assert results[0]["seniority"] == "senior"
        assert results[0]["skills"] == ["python", "aws"]

    def test_cli_failure_returns_defaults(self, monkeypatch):
        rows = [{"job_id": 1, "description": "Some job"}]

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "error"

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = extract_single_batch(rows)
        assert len(results) == 1
        assert results[0]["seniority"] is None
        assert results[0]["skills"] == []

    def test_empty_batch_returns_empty(self):
        results = extract_single_batch([])
        assert len(results) == 0

    def test_preserves_job_id(self, monkeypatch):
        rows = [{"job_id": 42, "description": "Some job"}]

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"seniority": "entry_level", "skills": ["sql"]})

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = extract_single_batch(rows)
        assert results[0]["job_id"] == 42

    def test_multiple_rows(self, monkeypatch):
        rows = [
            {"job_id": 1, "description": "Junior dev"},
            {"job_id": 2, "description": "Senior architect"},
        ]

        responses = iter([
            json.dumps({"seniority": "entry_level", "skills": ["python"]}),
            json.dumps({"seniority": "senior", "skills": ["java", "aws"]}),
        ])

        def mock_run(*a, **kw):
            mock = MagicMock()
            mock.returncode = 0
            mock.stdout = next(responses)
            return mock

        import subprocess
        monkeypatch.setattr(subprocess, "run", mock_run)

        results = extract_single_batch(rows)
        assert len(results) == 2
        assert results[0]["seniority"] == "entry_level"
        assert results[1]["seniority"] == "senior"


# ============================================================
# Config
# ============================================================

class TestConfig:
    def test_seniority_tiers(self):
        assert SENIORITY_TIERS == ["intern", "entry_level", "mid_level", "senior", "executive"]

    def test_model_is_haiku(self):
        assert "haiku" in LLM_MODEL.lower()

    def test_model_is_haiku(self):
        assert "haiku" in LLM_MODEL.lower()

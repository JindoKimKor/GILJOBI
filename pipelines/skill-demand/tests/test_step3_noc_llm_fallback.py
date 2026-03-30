"""
Tests for skill-demand pipeline STEP 3 — NOC LLM Fallback.
Based on SPEC.md — Claude Haiku CLI for sub-threshold NOC matching.

Claude CLI runs inside Spark container only (Dockerfile.spark-llm-onet-worker).
Auth: host ~/.claude mounted to /tmp/.claude, HOME=/tmp in subprocess env.
Tests mock subprocess.run to verify matching logic.
"""

import json
import os
import pandas as pd
import numpy as np
import pytest
from unittest.mock import MagicMock, call

from src.step3_noc_llm_fallback import (
    build_noc_prompt,
    parse_llm_response,
    match_single_batch,
    build_claude_env,
    LLM_MODEL,
    CLAUDE_HOME,
)


# ============================================================
# Prompt Building
# ============================================================

class TestBuildPrompt:
    """SPEC: LLM reads full JD description to determine NOC mapping."""

    def test_contains_job_title(self):
        prompt = build_noc_prompt(
            title="Full Stack Developer",
            description="Build web apps with React and Node.js",
            noc_candidates=["Web designers", "Software engineers"],
        )
        assert "Full Stack Developer" in prompt

    def test_contains_description(self):
        prompt = build_noc_prompt(
            title="SWE",
            description="Build distributed systems using Java and Kubernetes",
            noc_candidates=["Software engineers"],
        )
        assert "distributed systems" in prompt

    def test_contains_noc_candidates(self):
        candidates = ["Software engineers and designers", "Web developers and programmers"]
        prompt = build_noc_prompt(
            title="SWE",
            description="Build stuff",
            noc_candidates=candidates,
        )
        for c in candidates:
            assert c in prompt

    def test_asks_for_json_response(self):
        prompt = build_noc_prompt(
            title="SWE",
            description="Build stuff",
            noc_candidates=["Software engineers"],
        )
        assert "json" in prompt.lower() or "JSON" in prompt


# ============================================================
# Response Parsing
# ============================================================

class TestParseResponse:
    """Parse Claude CLI JSON response into NOC match."""

    def test_parses_valid_json(self):
        response = json.dumps({"noc_title": "Software engineers and designers"})
        result = parse_llm_response(response)
        assert result == "Software engineers and designers"

    def test_extracts_from_markdown_json_block(self):
        response = '```json\n{"noc_title": "Web developers"}\n```'
        result = parse_llm_response(response)
        assert result == "Web developers"

    def test_returns_none_on_invalid_json(self):
        result = parse_llm_response("I don't know what NOC this is")
        assert result is None

    def test_returns_none_on_null_value(self):
        response = json.dumps({"noc_title": None})
        result = parse_llm_response(response)
        assert result is None

    def test_returns_none_on_empty_response(self):
        result = parse_llm_response("")
        assert result is None


# ============================================================
# Claude CLI Auth
# ============================================================

class TestClaudeAuth:
    """Auth: ~/.claude mounted to /tmp/.claude, HOME=/tmp."""

    def test_claude_home_is_tmp(self):
        assert CLAUDE_HOME == "/tmp"

    def test_build_claude_env_sets_home(self):
        env = build_claude_env()
        assert env["HOME"] == CLAUDE_HOME

    def test_build_claude_env_preserves_existing_vars(self):
        env = build_claude_env()
        assert "PATH" in env


# ============================================================
# Batch Matching (mocked subprocess)
# ============================================================

class TestMatchSingleBatch:
    """Match a batch of sub-threshold rows using mocked Claude CLI."""

    def _make_noc_ref(self):
        return pd.DataFrame({
            "id": [1, 2, 3],
            "noc21_code": ["21231", "21232", "41200"],
            "noc21_name": [
                "Software engineers and designers",
                "Software developers and programmers",
                "University professors and lecturers",
            ],
        })

    def test_successful_match(self, monkeypatch):
        noc_ref = self._make_noc_ref()
        rows = [{"job_id": 1, "title": "Full Stack Wizard", "description": "Build React apps"}]

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"noc_title": "Software engineers and designers"})

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = match_single_batch(rows, noc_ref)
        assert len(results) == 1
        assert results[0]["noc_id"] == 1
        assert results[0]["noc_match_method"] == "llm"

    def test_no_match_returns_none(self, monkeypatch):
        noc_ref = self._make_noc_ref()
        rows = [{"job_id": 1, "title": "Alien Whisperer", "description": "Communicate with aliens"}]

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"noc_title": None})

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = match_single_batch(rows, noc_ref)
        assert len(results) == 1
        assert results[0]["noc_id"] is None

    def test_cli_failure_returns_none(self, monkeypatch):
        noc_ref = self._make_noc_ref()
        rows = [{"job_id": 1, "title": "SWE", "description": "Build stuff"}]

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "error"

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = match_single_batch(rows, noc_ref)
        assert len(results) == 1
        assert results[0]["noc_id"] is None

    def test_empty_batch_returns_empty(self):
        noc_ref = self._make_noc_ref()
        results = match_single_batch([], noc_ref)
        assert len(results) == 0

    def test_unrecognized_noc_title_returns_none(self, monkeypatch):
        noc_ref = self._make_noc_ref()
        rows = [{"job_id": 1, "title": "SWE", "description": "Build stuff"}]

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"noc_title": "Nonexistent NOC title"})

        import subprocess
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: mock_result)

        results = match_single_batch(rows, noc_ref)
        assert len(results) == 1
        assert results[0]["noc_id"] is None

    def test_uses_correct_model(self, monkeypatch):
        """Verify Claude CLI is called with the configured model."""
        noc_ref = self._make_noc_ref()
        rows = [{"job_id": 1, "title": "SWE", "description": "Build stuff"}]

        calls = []
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"noc_title": None})

        import subprocess
        def capture_run(*args, **kwargs):
            calls.append((args, kwargs))
            return mock_result
        monkeypatch.setattr(subprocess, "run", capture_run)

        match_single_batch(rows, noc_ref)
        assert len(calls) == 1
        cmd = calls[0][0][0]
        assert LLM_MODEL in cmd


# ============================================================
# Config
# ============================================================

class TestConfig:
    def test_model_is_haiku(self):
        assert "haiku" in LLM_MODEL.lower()

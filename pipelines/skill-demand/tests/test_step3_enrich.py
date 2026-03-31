"""
Tests for skill-demand pipeline STEP 3 — Enrich (NOC + Seniority + Skills).

2 prompt types based on NOC match status:
  - unmatched: NOC list + JD → NOC + seniority + skills (5/call)
  - matched: JD only → seniority + skills (10/call)

Seniority handled per-row within prompt (skip if filled, determine if missing).
Skills categorized: hard_skill, soft_skill, tool, certification.
"""

import json
import pytest

from src.step3_enrich import (
    SKILL_CATEGORIES,
    BATCH_SIZE_UNMATCHED,
    BATCH_SIZE_MATCHED,
    build_unmatched_prompt,
    build_matched_prompt,
    parse_enrich_response,
)


NOC_LIST = "- Web developers\n- Data engineers\n- Software engineers"


# ============================================================
# Prompt: Unmatched (NOC + seniority + skills)
# ============================================================

class TestBuildUnmatchedPrompt:

    def _rows(self, seniority=None):
        return [{"job_id": 1, "title": "SWE", "description": "Build systems with Python",
                 "company_name": "Google", "seniority": seniority}]

    def test_includes_noc_list(self):
        assert "Web developers" in build_unmatched_prompt(self._rows(), NOC_LIST)

    def test_includes_company(self):
        assert "Google" in build_unmatched_prompt(self._rows(), NOC_LIST)

    def test_includes_description(self):
        assert "Build systems" in build_unmatched_prompt(self._rows(), NOC_LIST)

    def test_includes_title(self):
        assert "SWE" in build_unmatched_prompt(self._rows(), NOC_LIST)

    def test_includes_seniority_guide(self):
        prompt = build_unmatched_prompt(self._rows(), NOC_LIST)
        assert "Seniority levels:" in prompt
        assert "intern:" in prompt

    def test_includes_skill_categories_guide(self):
        prompt = build_unmatched_prompt(self._rows(), NOC_LIST)
        assert "Skill categories:" in prompt
        for cat in SKILL_CATEGORIES:
            assert cat in prompt

    def test_seniority_filled_shows_skip(self):
        prompt = build_unmatched_prompt(self._rows(seniority="senior"), NOC_LIST)
        assert "already determined - skip" in prompt

    def test_seniority_missing_shows_determine(self):
        prompt = build_unmatched_prompt(self._rows(seniority=None), NOC_LIST)
        assert "missing - determine" in prompt

    def test_mixed_seniority_in_batch(self):
        rows = [
            {"job_id": 1, "title": "SWE", "description": "d1", "company_name": "A", "seniority": "senior"},
            {"job_id": 2, "title": "PM", "description": "d2", "company_name": "A", "seniority": None},
        ]
        prompt = build_unmatched_prompt(rows, NOC_LIST)
        assert "already determined - skip" in prompt
        assert "missing - determine" in prompt

    def test_truncates_description(self):
        rows = [{"job_id": 1, "title": "SWE", "description": "x" * 5000,
                 "company_name": "A", "seniority": None}]
        prompt = build_unmatched_prompt(rows, NOC_LIST)
        assert len(prompt) < 5000 + 15000


# ============================================================
# Prompt: Matched (seniority + skills, no NOC)
# ============================================================

class TestBuildMatchedPrompt:

    def _rows(self, seniority=None):
        return [{"job_id": 1, "title": "Data Scientist", "description": "Analyze data with SQL",
                 "company_name": "Meta", "seniority": seniority}]

    def test_no_noc_list(self):
        prompt = build_matched_prompt(self._rows())
        assert "NOC" not in prompt

    def test_includes_description(self):
        assert "Analyze data" in build_matched_prompt(self._rows())

    def test_includes_company(self):
        assert "Meta" in build_matched_prompt(self._rows())

    def test_includes_skill_categories_guide(self):
        prompt = build_matched_prompt(self._rows())
        assert "Skill categories:" in prompt
        for cat in SKILL_CATEGORIES:
            assert cat in prompt

    def test_includes_seniority_guide(self):
        prompt = build_matched_prompt(self._rows())
        assert "Seniority levels:" in prompt

    def test_seniority_filled_shows_skip(self):
        prompt = build_matched_prompt(self._rows(seniority="entry_level"))
        assert "already determined - skip" in prompt

    def test_seniority_missing_shows_determine(self):
        prompt = build_matched_prompt(self._rows(seniority=None))
        assert "missing - determine" in prompt

    def test_shorter_than_unmatched(self):
        """Matched prompt has no NOC list → shorter."""
        rows = self._rows()
        p_matched = build_matched_prompt(rows)
        p_unmatched = build_unmatched_prompt(rows, NOC_LIST)
        assert len(p_matched) < len(p_unmatched)


# ============================================================
# Response Parsing
# ============================================================

class TestParseEnrichResponse:

    def test_parses_full_response(self):
        response = json.dumps({"results": [{
            "job_id": 1,
            "noc_title": "Web developers",
            "seniority": "senior",
            "skills": [
                {"name": "python", "category": "hard_skill"},
                {"name": "leadership", "category": "soft_skill"},
                {"name": "Docker", "category": "tool"},
            ],
        }]})
        noc_lookup = {"Web developers": 42}
        results = parse_enrich_response(response, noc_lookup)
        assert len(results) == 1
        assert results[0]["noc_id"] == 42
        assert results[0]["seniority"] == "senior"
        assert len(results[0]["skills"]) == 3

    def test_parses_skills_only_response(self):
        response = json.dumps({"results": [{
            "job_id": 1,
            "skills": [{"name": "excel", "category": "tool"}],
        }]})
        results = parse_enrich_response(response, {})
        assert results[0]["noc_id"] is None
        assert results[0]["seniority"] is None
        assert results[0]["skills"] == [{"name": "excel", "category": "tool"}]

    def test_extracts_from_markdown_block(self):
        response = '```json\n{"results": [{"job_id": 1, "skills": [{"name": "sql", "category": "hard_skill"}]}]}\n```'
        results = parse_enrich_response(response, {})
        assert results[0]["skills"][0]["name"] == "sql"

    def test_invalid_noc_title_returns_none(self):
        response = json.dumps({"results": [{"job_id": 1, "noc_title": "Not real", "skills": []}]})
        results = parse_enrich_response(response, {"Web developers": 42})
        assert results[0]["noc_id"] is None

    def test_invalid_seniority_returns_none(self):
        response = json.dumps({"results": [{"job_id": 1, "seniority": "wizard", "skills": []}]})
        results = parse_enrich_response(response, {})
        assert results[0]["seniority"] is None

    def test_invalid_skill_category_filtered(self):
        response = json.dumps({"results": [{"job_id": 1, "skills": [
            {"name": "python", "category": "hard_skill"},
            {"name": "magic", "category": "superpower"},
        ]}]})
        results = parse_enrich_response(response, {})
        assert len(results[0]["skills"]) == 1

    def test_lowercases_skill_names(self):
        response = json.dumps({"results": [{"job_id": 1, "skills": [{"name": "Python", "category": "hard_skill"}]}]})
        results = parse_enrich_response(response, {})
        assert results[0]["skills"][0]["name"] == "python"

    def test_multiple_results(self):
        response = json.dumps({"results": [
            {"job_id": 1, "noc_title": "NOC A", "skills": [{"name": "a", "category": "tool"}]},
            {"job_id": 2, "noc_title": "NOC B", "skills": [{"name": "b", "category": "hard_skill"}]},
        ]})
        results = parse_enrich_response(response, {"NOC A": 10, "NOC B": 20})
        assert len(results) == 2
        assert results[0]["noc_id"] == 10
        assert results[1]["noc_id"] == 20

    def test_empty_response_returns_empty(self):
        assert parse_enrich_response("", {}) == []

    def test_malformed_json_returns_empty(self):
        assert parse_enrich_response("not json", {}) == []

    def test_noc_title_null_string(self):
        response = json.dumps({"results": [{"job_id": 1, "noc_title": "null", "skills": []}]})
        results = parse_enrich_response(response, {"null": 99})
        assert results[0]["noc_id"] is None


# ============================================================
# Config
# ============================================================

class TestConfig:
    def test_skill_categories(self):
        assert SKILL_CATEGORIES == ["hard_skill", "soft_skill", "tool", "certification"]

    def test_batch_size_unmatched_smaller(self):
        assert BATCH_SIZE_UNMATCHED < BATCH_SIZE_MATCHED

"""
Tests for skill-demand pipeline STEP 2 — NOC Normalization.
Based on SPEC.md — Cosine similarity against 510 NOC unit group titles.

Model (sentence-transformers) runs inside Spark container only.
Tests use mock embeddings to verify matching logic without PyTorch dependency.
"""

import numpy as np
import pandas as pd
import pytest

from src.step2_noc_normalize import (
    cosine_similarity_matrix,
    match_noc_titles,
    DEFAULT_THRESHOLD,
)


# ============================================================
# Cosine Similarity
# ============================================================

class TestCosineSimilarity:
    """Core math: cosine similarity between vectors."""

    def test_identical_vectors_score_1(self):
        a = np.array([[1.0, 0.0, 0.0]])
        b = np.array([[1.0, 0.0, 0.0]])
        scores = cosine_similarity_matrix(a, b)
        assert scores.shape == (1, 1)
        assert abs(scores[0, 0] - 1.0) < 1e-6

    def test_orthogonal_vectors_score_0(self):
        a = np.array([[1.0, 0.0]])
        b = np.array([[0.0, 1.0]])
        scores = cosine_similarity_matrix(a, b)
        assert abs(scores[0, 0]) < 1e-6

    def test_multiple_queries_against_multiple_refs(self):
        queries = np.array([[1.0, 0.0], [0.0, 1.0]])
        refs = np.array([[1.0, 0.0], [0.0, 1.0], [0.7, 0.7]])
        scores = cosine_similarity_matrix(queries, refs)
        assert scores.shape == (2, 3)
        assert np.argmax(scores[0]) == 0
        assert np.argmax(scores[1]) == 1

    def test_normalized_vectors(self):
        """Pre-normalized vectors should still work correctly."""
        a = np.array([[0.6, 0.8]])
        b = np.array([[0.6, 0.8]])
        scores = cosine_similarity_matrix(a, b)
        assert abs(scores[0, 0] - 1.0) < 1e-6


# ============================================================
# NOC Matching Logic (mock embeddings)
# ============================================================

class TestMatchNocTitles:
    """Match job titles to NOC codes using precomputed embeddings."""

    def _make_noc_data(self):
        noc_titles = pd.DataFrame({
            "id": [1, 2, 3],
            "noc21_code": ["21231", "21232", "41200"],
            "noc21_name": [
                "Software engineers and designers",
                "Software developers and programmers",
                "University professors and lecturers",
            ],
        })
        noc_embeddings = np.array([
            [0.9, 0.1, 0.0],
            [0.8, 0.2, 0.0],
            [0.0, 0.1, 0.9],
        ], dtype=np.float32)
        return noc_titles, noc_embeddings

    def _make_job_df(self):
        return pd.DataFrame({
            "job_id": [1, 2, 3],
            "title": ["Software Engineer", "Python Developer", "Professor of CS"],
            "company_name": ["Google", "Meta", "MIT"],
            "description": ["Build systems"] * 3,
        })

    def test_returns_expected_columns(self):
        noc_titles, noc_emb = self._make_noc_data()
        df = self._make_job_df()
        job_emb = np.array([
            [0.9, 0.1, 0.0],
            [0.8, 0.2, 0.0],
            [0.0, 0.1, 0.9],
        ], dtype=np.float32)

        result = match_noc_titles(df, job_emb, noc_titles, noc_emb, threshold=0.5)
        assert "noc_id" in result.columns
        assert "noc_match_score" in result.columns
        assert "noc_match_method" in result.columns

    def test_high_similarity_gets_matched(self):
        noc_titles, noc_emb = self._make_noc_data()
        df = self._make_job_df()
        job_emb = np.array([
            [0.9, 0.1, 0.0],
            [0.8, 0.2, 0.0],
            [0.0, 0.1, 0.9],
        ], dtype=np.float32)

        result = match_noc_titles(df, job_emb, noc_titles, noc_emb, threshold=0.5)
        assert result.iloc[0]["noc_id"] == 1
        assert result.iloc[0]["noc_match_method"] == "sentence_transformer"

    def test_below_threshold_gets_null(self):
        noc_titles, noc_emb = self._make_noc_data()
        df = self._make_job_df()
        job_emb = np.array([
            [0.3, 0.3, 0.3],
            [0.3, 0.3, 0.3],
            [0.3, 0.3, 0.3],
        ], dtype=np.float32)

        result = match_noc_titles(df, job_emb, noc_titles, noc_emb, threshold=0.95)
        assert pd.isna(result.iloc[0]["noc_id"])
        assert pd.isna(result.iloc[0]["noc_match_method"])

    def test_score_is_between_0_and_1(self):
        noc_titles, noc_emb = self._make_noc_data()
        df = self._make_job_df()
        job_emb = np.array([
            [0.9, 0.1, 0.0],
            [0.8, 0.2, 0.0],
            [0.0, 0.1, 0.9],
        ], dtype=np.float32)

        result = match_noc_titles(df, job_emb, noc_titles, noc_emb, threshold=0.5)
        for score in result["noc_match_score"].dropna():
            assert 0.0 < score <= 1.0

    def test_preserves_original_columns(self):
        noc_titles, noc_emb = self._make_noc_data()
        df = self._make_job_df()
        job_emb = np.array([
            [0.9, 0.1, 0.0],
            [0.8, 0.2, 0.0],
            [0.0, 0.1, 0.9],
        ], dtype=np.float32)

        result = match_noc_titles(df, job_emb, noc_titles, noc_emb, threshold=0.5)
        for col in ["job_id", "title", "company_name", "description"]:
            assert col in result.columns

    def test_empty_dataframe(self):
        noc_titles, noc_emb = self._make_noc_data()
        df = pd.DataFrame({
            "job_id": [], "title": [], "company_name": [], "description": [],
        })
        job_emb = np.empty((0, 3), dtype=np.float32)

        result = match_noc_titles(df, job_emb, noc_titles, noc_emb, threshold=0.5)
        assert len(result) == 0
        assert "noc_id" in result.columns

    def test_best_match_wins(self):
        """When multiple NOC titles are similar, pick the highest score."""
        noc_titles, noc_emb = self._make_noc_data()
        df = pd.DataFrame({
            "job_id": [1],
            "title": ["Software Engineer"],
            "company_name": ["Google"],
            "description": ["Build systems"],
        })
        # Embedding closer to NOC id=1 (0.9, 0.1, 0.0) than id=2 (0.8, 0.2, 0.0)
        job_emb = np.array([[0.95, 0.05, 0.0]], dtype=np.float32)

        result = match_noc_titles(df, job_emb, noc_titles, noc_emb, threshold=0.5)
        assert result.iloc[0]["noc_id"] == 1  # id=1 should win


# ============================================================
# Config
# ============================================================

class TestConfig:
    def test_default_threshold_is_reasonable(self):
        assert 0.5 <= DEFAULT_THRESHOLD <= 0.95

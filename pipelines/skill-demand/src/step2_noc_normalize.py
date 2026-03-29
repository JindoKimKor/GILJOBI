"""
step2_noc_normalize.py — NOC Normalization via Sentence Transformers.

Matches job titles to NOC 2021 unit group titles using cosine similarity
on sentence embeddings. Titles below threshold are left unmatched for
LLM fallback in step 3.

Embedding model (all-MiniLM-L6-v2) runs inside Spark container only —
this module exposes the matching logic separately so it can be tested
without PyTorch/sentence-transformers dependency.
"""

import numpy as np
import pandas as pd

# =============================================================================
# Config
# =============================================================================
DEFAULT_THRESHOLD = 0.75
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# =============================================================================
# Cosine Similarity
# =============================================================================
def cosine_similarity_matrix(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """
    Compute cosine similarity between every row in a and every row in b.

    Args:
        a: (N, D) query vectors
        b: (M, D) reference vectors

    Returns:
        (N, M) similarity matrix where [i, j] is cosine similarity
        between a[i] and b[j].
    """
    # Normalize rows to unit vectors
    a_norm = a / (np.linalg.norm(a, axis=1, keepdims=True) + 1e-10)
    b_norm = b / (np.linalg.norm(b, axis=1, keepdims=True) + 1e-10)
    return a_norm @ b_norm.T


# =============================================================================
# NOC Matching
# =============================================================================
def match_noc_titles(
    df: pd.DataFrame,
    job_embeddings: np.ndarray,
    noc_titles: pd.DataFrame,
    noc_embeddings: np.ndarray,
    threshold: float = DEFAULT_THRESHOLD,
) -> pd.DataFrame:
    """
    Match job titles to NOC codes using precomputed embeddings.

    Args:
        df: Job postings DataFrame (must have title column).
        job_embeddings: (N, D) embeddings for each job title.
        noc_titles: NOC reference DataFrame with id, noc21_code, noc21_name.
        noc_embeddings: (M, D) embeddings for each NOC title.
        threshold: Minimum cosine similarity to accept a match.

    Returns:
        Original df with added columns: noc_id, noc_match_score, noc_match_method.
        Unmatched rows have None/NaN in these columns.
    """
    result = df.copy()
    result["noc_id"] = None
    result["noc_match_score"] = np.nan
    result["noc_match_method"] = None

    if len(df) == 0:
        return result

    # Compute similarity matrix: (N jobs) × (M NOC titles)
    sim_matrix = cosine_similarity_matrix(job_embeddings, noc_embeddings)

    # For each job, find the best matching NOC
    best_indices = np.argmax(sim_matrix, axis=1)
    best_scores = np.max(sim_matrix, axis=1)

    for i in range(len(df)):
        if best_scores[i] >= threshold:
            result.at[result.index[i], "noc_id"] = int(noc_titles.iloc[best_indices[i]]["id"])
            result.at[result.index[i], "noc_match_score"] = float(best_scores[i])
            result.at[result.index[i], "noc_match_method"] = "sentence_transformer"

    return result

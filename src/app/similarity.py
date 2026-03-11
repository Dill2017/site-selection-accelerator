"""Cosine-similarity scoring between brand profile and candidate H3 cells."""

from __future__ import annotations

import h3
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity


def _locate_brand_cells(
    brand_locations: list[dict],
    resolution: int,
) -> list[int]:
    """Map brand lat/lon locations to H3 cell IDs.

    Parameters
    ----------
    brand_locations : list of dicts with keys ``lat`` and ``lon``.
    resolution : H3 resolution.

    Returns
    -------
    List of H3 cell IDs (BIGINT).
    """
    cells = []
    for loc in brand_locations:
        hex_str = h3.latlng_to_cell(loc["lat"], loc["lon"], resolution)
        cells.append(h3.str_to_int(hex_str))
    return cells


def compute_similarity(
    embeddings: pd.DataFrame,
    brand_locations: list[dict],
    resolution: int,
) -> pd.DataFrame:
    """Score every H3 cell by similarity to the brand profile.

    1. Identify H3 cells that contain the brand's existing locations.
    2. Average those embeddings → brand profile vector.
    3. Compute cosine similarity of every other cell to the profile.
    4. Exclude cells that already contain a brand location.

    Returns a DataFrame with columns:
        h3_cell, similarity, is_brand_cell
    sorted descending by similarity.
    """
    brand_cells = _locate_brand_cells(brand_locations, resolution)
    brand_cells_in_emb = [c for c in brand_cells if c in embeddings.index]

    if not brand_cells_in_emb:
        raise ValueError(
            "None of the brand locations fall within the analysed H3 cells. "
            "Check that your locations are inside the selected city."
        )

    brand_profile = embeddings.loc[brand_cells_in_emb].mean(axis=0).values.reshape(1, -1)
    all_vectors = embeddings.values
    scores = cosine_similarity(brand_profile, all_vectors).flatten()

    # Normalise to [0, 1] so colour mapping works for any embedding space
    s_min, s_max = scores.min(), scores.max()
    if s_max - s_min > 0:
        norm_scores = (scores - s_min) / (s_max - s_min)
    else:
        norm_scores = np.zeros_like(scores)

    result = pd.DataFrame(
        {
            "h3_cell": embeddings.index,
            "similarity": norm_scores,
            "is_brand_cell": embeddings.index.isin(brand_cells),
        }
    )
    result = result.sort_values("similarity", ascending=False).reset_index(drop=True)
    return result


def get_top_opportunities(
    scored: pd.DataFrame,
    top_n: int = 20,
) -> pd.DataFrame:
    """Return the top-N whitespace opportunities (excluding existing brand cells)."""
    return (
        scored[~scored["is_brand_cell"]]
        .head(top_n)
        .reset_index(drop=True)
    )

"""Cosine-similarity scoring between brand profile and candidate H3 cells."""

from __future__ import annotations

import h3
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

from brand_search import h3_int_to_hex


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
) -> tuple[pd.DataFrame, list[int]]:
    """Score every H3 cell by similarity to the brand profile.

    1. Identify H3 cells that contain the brand's existing locations.
    2. Average those embeddings -> brand profile vector.
    3. Compute cosine similarity of every other cell to the profile.
    4. Exclude cells that already contain a brand location.

    Returns
    -------
    scored : DataFrame with columns h3_cell, similarity, is_brand_cell
        sorted descending by similarity.
    brand_cells_in_emb : list of H3 cell IDs that form the brand profile.
    """
    brand_cells = _locate_brand_cells(brand_locations, resolution)
    brand_cells_in_emb = [c for c in brand_cells if c in embeddings.index]

    if not brand_cells_in_emb:
        raise ValueError(
            "None of the brand locations fall within the analysed H3 cells. "
            "This can happen when the brand-neighbourhood POI data is too "
            "sparse for the selected categories. Try selecting more POI "
            "categories or a coarser H3 resolution."
        )

    brand_profile = embeddings.loc[brand_cells_in_emb].mean(axis=0).values.reshape(1, -1)
    all_vectors = embeddings.values
    scores = cosine_similarity(brand_profile, all_vectors).flatten()

    result = pd.DataFrame(
        {
            "h3_cell": embeddings.index,
            "similarity": scores,
            "is_brand_cell": embeddings.index.isin(brand_cells),
        }
    )
    result = result.sort_values("similarity", ascending=False).reset_index(drop=True)
    return result, brand_cells_in_emb


def compute_opportunity_score(
    scored: pd.DataFrame,
    competition: pd.DataFrame,
    beta: float = 0.5,
) -> pd.DataFrame:
    """Apply competition penalty to similarity scores.

    opportunity_score = similarity * (1 - beta * competition_score)

    competition_score is normalised by the max competitor count found in
    any cell with competition data (not across all cells).

    Parameters
    ----------
    scored : DataFrame from compute_similarity (h3_cell, similarity, is_brand_cell)
    competition : DataFrame from find_competitors (h3_hex, competitor_count, top_competitors)
    beta : competition sensitivity (0 = ignore, 1 = max penalty)
    """
    scored = scored.copy()
    scored["h3_hex"] = scored["h3_cell"].apply(h3_int_to_hex)

    merged = scored.merge(
        competition[["h3_hex", "competitor_count", "top_competitors"]],
        on="h3_hex",
        how="left",
    )
    merged["competitor_count"] = merged["competitor_count"].fillna(0).astype(int)
    merged["top_competitors"] = merged["top_competitors"].fillna("")

    max_in_any_cell = competition["competitor_count"].max() if not competition.empty else 1
    cap = max(max_in_any_cell, 1)

    merged["competition_score"] = merged["competitor_count"] / cap

    merged["opportunity_score"] = merged["similarity"] * (
        1 - beta * merged["competition_score"]
    )

    return merged.sort_values(
        "opportunity_score", ascending=False
    ).reset_index(drop=True)


def get_top_opportunities(
    scored: pd.DataFrame,
    top_n: int = 20,
) -> pd.DataFrame:
    """Return the top-N whitespace opportunities (excluding existing brand cells).

    Sorts by opportunity_score (or similarity), using poi_density as a
    tiebreaker so denser areas rank higher among equally-scored cells.
    """
    sort_col = (
        "opportunity_score"
        if "opportunity_score" in scored.columns
        else "similarity"
    )
    sort_cols = [sort_col]
    if "poi_density" in scored.columns:
        sort_cols.append("poi_density")

    return (
        scored[~scored["is_brand_cell"]]
        .sort_values(sort_cols, ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

"""Cosine-similarity scoring between brand profile and candidate H3 cells."""

from __future__ import annotations

import h3
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
    alpha: float = 0.5,
) -> pd.DataFrame:
    """Score opportunities using similarity, demand, and competition.

    opportunity_score = similarity * demand_boost * competition_factor

    Where:
      demand_boost       = 1 + alpha * demand_score
      competition_factor = 1 - beta  * competition_score
      demand_score       = percentile_rank(poi_density)
      competition_score  = competitor_count / median(nonzero_counts), clipped to 1
      final score        = percentile_rank(raw)  (distribution-aware, outlier-resistant)

    Beta range [-1, +1] controls the competition strategy:
      beta > 0 : penalise (avoid saturated areas)
      beta = 0 : ignore competition
      beta < 0 : boost  (mirror / co-locate near competition)

    Parameters
    ----------
    scored : DataFrame from compute_similarity with poi_density column.
    competition : DataFrame from find_competitors or named competitor lookup.
    beta : competition sensitivity (-1 = full mirror, 0 = ignore, +1 = full penalty).
    alpha : demand sensitivity (0 = ignore demand, 1 = full boost).
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

    nonzero = competition.loc[competition["competitor_count"] > 0, "competitor_count"]
    cap = max(nonzero.median(), 1) if len(nonzero) > 0 else 1
    merged["competition_score"] = (merged["competitor_count"] / cap).clip(upper=1.0)

    if "poi_density" in merged.columns and merged["poi_density"].sum() > 0:
        merged["demand_score"] = merged["poi_density"].rank(pct=True)
    else:
        merged["demand_score"] = 0.0

    merged["demand_boost"] = 1 + alpha * merged["demand_score"]
    merged["competition_factor"] = 1 - beta * merged["competition_score"]
    raw = merged["similarity"] * merged["demand_boost"] * merged["competition_factor"]
    merged["opportunity_score"] = raw.rank(pct=True)

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

"""Explainability helpers for similarity scores.

Provides human-readable breakdowns of *why* a hexagon scores highly
against a brand profile, using the raw POI count vectors rather than the
learned Hex2Vec embeddings.
"""

from __future__ import annotations

import pandas as pd

from config import CATEGORY_GROUPS


def build_brand_profile(
    count_vectors: pd.DataFrame,
    brand_cells: list[int],
) -> dict:
    """Build an interpretable brand profile from POI count vectors.

    Returns
    -------
    dict with keys:
        avg   – Series of mean counts per category across brand cells
        cells – DataFrame of per-cell counts (subset of count_vectors)
    """
    brand_cv = count_vectors.loc[
        count_vectors.index.isin(brand_cells)
    ].copy()
    avg = brand_cv.mean(axis=0)
    return {"avg": avg, "cells": brand_cv}


def explain_opportunity(
    cell_id: int,
    count_vectors: pd.DataFrame,
    brand_avg: pd.Series,
) -> dict:
    """Compare a single opportunity cell to the brand average.

    Returns
    -------
    dict with keys:
        counts – Series of raw counts for the cell
        diff   – Series of (cell - brand_avg)
        top_matching – list of (category, cell_count, brand_avg) sorted
                       by smallest |diff|, limited to non-zero entries
        group_summary – dict[group_name -> float] average diff per group
    """
    if cell_id in count_vectors.index:
        counts = count_vectors.loc[cell_id]
    else:
        counts = pd.Series(0, index=brand_avg.index)

    diff = counts - brand_avg

    non_zero_mask = (counts > 0) | (brand_avg > 0)
    abs_diff = diff[non_zero_mask].abs().sort_values()
    top_matching = [
        (cat, int(counts[cat]), round(brand_avg[cat], 1))
        for cat in abs_diff.index[:5]
    ]

    group_summary = {}
    for group, cats in CATEGORY_GROUPS.items():
        cats_present = [c for c in cats if c in diff.index]
        if cats_present:
            group_summary[group] = round(diff[cats_present].mean(), 2)

    return {
        "counts": counts,
        "diff": diff,
        "top_matching": top_matching,
        "group_summary": group_summary,
    }


def summarise_explanation(explanation: dict) -> str:
    """One-line text summary of an opportunity explanation."""
    parts = []
    for group, avg_diff in explanation["group_summary"].items():
        if abs(avg_diff) < 0.05:
            continue
        direction = "above" if avg_diff > 0 else "below"
        parts.append(f"{group} {abs(avg_diff):+.1f} {direction} avg")
    if not parts:
        return "Category mix closely matches the brand profile."
    return "; ".join(parts)


def explain_competition(
    cell_id: int,
    scored: pd.DataFrame,
) -> dict | None:
    """Return competition breakdown for a cell, if available."""
    if "opportunity_score" not in scored.columns:
        return None
    row = scored[scored["h3_cell"] == cell_id]
    if row.empty:
        return None
    r = row.iloc[0]
    return {
        "vibe_score": round(float(r["similarity"]), 3),
        "competitor_count": int(r.get("competitor_count", 0)),
        "competition_score": round(float(r.get("competition_score", 0)), 3),
        "opportunity_score": round(float(r["opportunity_score"]), 3),
        "top_competitors": r.get("top_competitors", ""),
    }


def build_fingerprint_df(
    cell_id: int,
    count_vectors: pd.DataFrame,
    brand_avg: pd.Series,
) -> pd.DataFrame:
    """Build a full-category fingerprint comparison DataFrame.

    Returns a DataFrame with one row per category (including zeros),
    sorted by category group then alphabetically, with both raw counts
    and normalised (% of total) columns for shape comparison.
    """
    all_cats = count_vectors.columns.tolist()

    if cell_id in count_vectors.index:
        cell_counts = count_vectors.loc[cell_id]
    else:
        cell_counts = pd.Series(0, index=all_cats)

    brand_vals = brand_avg.reindex(all_cats, fill_value=0)

    group_lookup: dict[str, str] = {}
    group_order: dict[str, int] = {}
    for idx, (grp, cats) in enumerate(CATEGORY_GROUPS.items()):
        group_order[grp] = idx
        for c in cats:
            group_lookup[c] = grp

    df = pd.DataFrame({
        "category_raw": all_cats,
        "Category": [c.replace("_", " ").title() for c in all_cats],
        "Group": [group_lookup.get(c, "Other") for c in all_cats],
        "This Location": [float(cell_counts[c]) for c in all_cats],
        "Brand Average": [float(brand_vals[c]) for c in all_cats],
    })

    df["_group_order"] = df["Group"].map(
        lambda g: group_order.get(g, len(group_order))
    )
    df = df.sort_values(
        ["_group_order", "Category"], ascending=True
    ).drop(columns="_group_order").reset_index(drop=True)

    cell_total = df["This Location"].sum()
    brand_total = df["Brand Average"].sum()
    df["This Location (%)"] = (
        (df["This Location"] / cell_total * 100).round(1) if cell_total > 0
        else 0.0
    )
    df["Brand Average (%)"] = (
        (df["Brand Average"] / brand_total * 100).round(1) if brand_total > 0
        else 0.0
    )

    return df


def tooltip_snippet(
    cell_id: int,
    count_vectors: pd.DataFrame,
    brand_avg: pd.Series,
    max_cats: int = 4,
) -> str:
    """Short HTML snippet for map tooltip showing top category comparisons."""
    exp = explain_opportunity(cell_id, count_vectors, brand_avg)
    lines = []
    for cat, cell_val, avg_val in exp["top_matching"][:max_cats]:
        label = cat.replace("_", " ").title()
        lines.append(f"{label}: {cell_val} / {avg_val}")
    return "<br/>".join(lines)

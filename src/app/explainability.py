"""Explainability helpers for similarity scores.

Provides human-readable breakdowns of *why* a hexagon scores highly
against a brand profile, using the raw POI count vectors rather than the
learned Hex2Vec embeddings.
"""

from __future__ import annotations

import logging

import pandas as pd

from config import ALL_BUILDING_CATEGORIES, ALL_FEATURE_GROUPS

log = logging.getLogger(__name__)

FINGERPRINT_LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"


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
        counts       – Series of raw counts for the cell
        diff         – Series of (cell - brand_avg)
        top_matching – list of (category, cell_count, brand_avg) sorted
                       by smallest |diff|, limited to non-zero entries
        top_features – list of (category, cell_pct, avg_pct, pct_diff)
                       sorted by largest |pct_diff|, normalised within
                       POI / Building feature types independently
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

    # Percentage-normalised comparison within each feature type
    _bldg_set = set(ALL_BUILDING_CATEGORIES)
    cell_pct = pd.Series(0.0, index=counts.index)
    avg_pct = pd.Series(0.0, index=brand_avg.index)

    for is_bldg in (True, False):
        mask = counts.index.map(lambda c, _ib=is_bldg: (c in _bldg_set) == _ib)
        cell_total = counts[mask].sum()
        avg_total = brand_avg[mask].sum()
        if cell_total > 0:
            cell_pct[mask] = (counts[mask] / cell_total * 100).round(1)
        if avg_total > 0:
            avg_pct[mask] = (brand_avg[mask] / avg_total * 100).round(1)

    pct_diff = cell_pct - avg_pct
    ranked = pct_diff[non_zero_mask].abs().sort_values(ascending=False)
    top_features = [
        (cat, round(float(cell_pct[cat]), 1), round(float(avg_pct[cat]), 1),
         round(float(pct_diff[cat]), 1))
        for cat in ranked.index[:5]
    ]

    group_summary = {}
    for group, cats in ALL_FEATURE_GROUPS.items():
        cats_present = [c for c in cats if c in diff.index]
        if cats_present:
            group_summary[group] = round(diff[cats_present].mean(), 2)

    return {
        "counts": counts,
        "diff": diff,
        "top_matching": top_matching,
        "top_features": top_features,
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
    for idx, (grp, cats) in enumerate(ALL_FEATURE_GROUPS.items()):
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

    _bldg_set = set(ALL_BUILDING_CATEGORIES)
    df["Feature Type"] = df["category_raw"].apply(
        lambda c: "Building" if c in _bldg_set else "POI"
    )

    df["_group_order"] = df["Group"].map(
        lambda g: group_order.get(g, len(group_order))
    )
    df = df.sort_values(
        ["_group_order", "Category"], ascending=True
    ).drop(columns="_group_order").reset_index(drop=True)

    for col, pct_col in [
        ("This Location", "This Location (%)"),
        ("Brand Average", "Brand Average (%)"),
    ]:
        df[pct_col] = 0.0
        for ft in ("POI", "Building"):
            mask = df["Feature Type"] == ft
            type_total = df.loc[mask, col].sum()
            if type_total > 0:
                df.loc[mask, pct_col] = (
                    (df.loc[mask, col] / type_total * 100).round(1)
                )

    return df


def _build_fingerprint_prompt(fingerprint_df: pd.DataFrame) -> str | None:
    """Build an LLM prompt from fingerprint data, or return None if trivial."""
    non_zero = fingerprint_df[
        (fingerprint_df["This Location"] > 0) | (fingerprint_df["Brand Average"] > 0)
    ].copy()
    if non_zero.empty:
        return None

    non_zero["pct_diff"] = (
        non_zero["This Location (%)"] - non_zero["Brand Average (%)"]
    )

    over = non_zero[non_zero["pct_diff"] > 1.0].nlargest(5, "pct_diff")
    under = non_zero[non_zero["pct_diff"] < -1.0].nsmallest(5, "pct_diff")

    rows: list[str] = []
    for _, r in over.iterrows():
        rows.append(
            f"  {r['Category']} ({r['Group']}): {r['This Location (%)']:.1f}% here vs "
            f"{r['Brand Average (%)']:.1f}% brand avg (+{r['pct_diff']:.1f}pp)"
        )
    for _, r in under.iterrows():
        rows.append(
            f"  {r['Category']} ({r['Group']}): {r['This Location (%)']:.1f}% here vs "
            f"{r['Brand Average (%)']:.1f}% brand avg ({r['pct_diff']:.1f}pp)"
        )

    if not rows:
        return None

    data_block = "\n".join(rows)

    return f"""You are a site-selection analyst. A user is evaluating a location for a new store.
Below are the categories where this location's POI/building mix differs most from the brand's average existing locations. Percentages are share-of-type (POI or Building).

{data_block}

Write exactly 1–2 sentences (max 40 words) that explain what this means for the end user considering this site. Be specific about the categories, insightful, and practical. Do NOT use bullet points or lists. Do NOT repeat the numbers."""


def summarise_fingerprint(fingerprint_df: pd.DataFrame) -> str:
    """Generate an LLM-powered insight from fingerprint data.

    Uses Databricks ai_query() via the SQL warehouse to call a Foundation
    Model endpoint.  This works from Databricks Apps because the warehouse
    is already an app resource.  Falls back to a rule-based summary on error.
    """
    if fingerprint_df.empty:
        return ""

    prompt = _build_fingerprint_prompt(fingerprint_df)
    if prompt is None:
        return (
            "This location's category mix closely mirrors the brand average "
            "— no single category stands out significantly."
        )

    try:
        from db import execute_query

        safe_prompt = prompt.replace("\\", "\\\\").replace("'", "''")
        query = (
            f"SELECT ai_query('{FINGERPRINT_LLM_ENDPOINT}', "
            f"'{safe_prompt}') AS summary"
        )
        result_df = execute_query(query)
        if not result_df.empty:
            raw = str(result_df.iloc[0]["summary"]).strip()
            if raw.startswith('"') and raw.endswith('"'):
                raw = raw[1:-1]
            text = raw.strip()
            if text:
                log.info("Fingerprint LLM insight: %s", text[:80])
                return text
    except Exception as e:
        log.warning("Fingerprint ai_query failed: %s — using fallback", e)

    return _fallback_fingerprint_summary(fingerprint_df)


def _fallback_fingerprint_summary(fingerprint_df: pd.DataFrame) -> str:
    """Rule-based fallback when the LLM call is unavailable."""
    non_zero = fingerprint_df[
        (fingerprint_df["This Location"] > 0) | (fingerprint_df["Brand Average"] > 0)
    ].copy()
    if non_zero.empty:
        return "No points of interest or buildings detected in this area."

    non_zero["pct_diff"] = (
        non_zero["This Location (%)"] - non_zero["Brand Average (%)"]
    )
    over = non_zero[non_zero["pct_diff"] > 1.5].nlargest(3, "pct_diff")
    under = non_zero[non_zero["pct_diff"] < -1.5].nsmallest(3, "pct_diff")

    def _fmt(rows: pd.DataFrame) -> str:
        names = rows["Category"].tolist()
        if len(names) == 1:
            return names[0]
        return ", ".join(names[:-1]) + " and " + names[-1]

    parts: list[str] = []
    if not over.empty:
        parts.append(
            f"this area over-indexes on {_fmt(over)} compared to typical brand locations"
        )
    if not under.empty:
        parts.append(f"it under-indexes on {_fmt(under)}")

    if not parts:
        return (
            "This location's category mix closely mirrors the brand average "
            "— no single category stands out significantly."
        )

    sentence = "; ".join(parts) + "."
    return sentence[0].upper() + sentence[1:]


def tooltip_snippet(
    cell_id: int,
    count_vectors: pd.DataFrame,
    brand_avg: pd.Series,
    max_cats: int = 4,
) -> str:
    """Short HTML snippet for map tooltip showing top category comparisons."""
    exp = explain_opportunity(cell_id, count_vectors, brand_avg)
    lines = []
    for cat, cell_pct, avg_pct, diff_pct in exp["top_features"][:max_cats]:
        label = cat.replace("_", " ").title()
        arrow = "▲" if diff_pct > 0 else "▼" if diff_pct < 0 else "="
        lines.append(f"{label}: {cell_pct}% {arrow} (avg {avg_pct}%)")
    return "<br/>".join(lines)

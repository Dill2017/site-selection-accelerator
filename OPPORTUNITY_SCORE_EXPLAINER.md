# Opportunity Score Explainer

The opportunity score ranks every H3 hexagonal cell in a target city by how
well it matches a brand's ideal location profile, adjusted for local demand
and competitive landscape.

```
raw = similarity × demand_boost × competition_factor
opportunity_score = percentile_rank(raw)
```

A score of **95%** means the cell ranks higher than 95% of all candidate cells
in the analysis.

---

## 1. Similarity (base signal)

**What it measures:** how closely a cell's neighbourhood resembles the
neighbourhoods where the brand already operates.

**How it works:**

1. Each brand location is mapped to an H3 cell at the chosen resolution.
2. The Hex2Vec embedding for each brand cell is averaged to form a **brand
   profile vector** — a dense representation of the brand's typical
   neighbourhood (POI mix, building types, height profile).
3. Cosine similarity is computed between the brand profile and every other
   cell's embedding.

**Range:** 0–1. A value of 0.97 means the cell's neighbourhood is 97%
similar to the brand's average location.

**Implementation:** `compute_similarity()` in
[`src/app/similarity.py`](src/app/similarity.py)

---

## 2. Demand Boost

**What it measures:** how commercially active an area is, based on the
density of Points of Interest and buildings.

```
demand_score = percentile_rank(poi_density)
demand_boost = 1 + α × demand_score
```

- **`poi_density`** — total count of POI and building features in the cell.
  This includes all selected POI categories (restaurants, shops, services,
  etc.) and building features (residential, commercial, height profile) when
  buildings are enabled.
- **`demand_score`** — percentile rank of `poi_density` across all cells.
  A cell at the 90th percentile has more activity than 90% of cells.
- **`α` (alpha)** — user-controlled via the **Demand boost (α)** slider.

| α value | Effect | demand_boost range |
|---------|--------|-------------------|
| 0.0 | Demand ignored | Always 1.0 |
| 0.5 (default) | Moderate boost | 1.0 – 1.5 |
| 1.0 | Full boost | 1.0 – 2.0 |

**Why it matters:** Two cells may have identical similarity scores, but a
cell in a busy commercial district with hundreds of POIs is a stronger
opportunity than one in a quiet residential area with only a handful.

---

## 3. Competition Factor

**What it measures:** the presence of a specific competitor brand (e.g.
"Subway") in each cell and its neighbours.

```
competition_score = min(competitor_count / median(nonzero_counts), 1.0)
competition_factor = 1 − β × competition_score
```

### Competition score normalisation

Competitor counts are normalised by the **median** of cells that have at
least one competitor — not the maximum. This is **outlier-resistant**: if
one cell happens to have 4 Subway locations while most cells have 1–2, it
does not compress every other cell's score.

| Competitor count | Median = 2 | Score |
|-----------------|-----------|-------|
| 0 | — | 0.0 |
| 1 | 1 / 2 | 0.5 |
| 2 | 2 / 2 | 1.0 |
| 4 (outlier) | 4 / 2, clipped | 1.0 |

### β (beta) — competition strategy

The user controls how competition affects the score via the **β slider**:

| β value | Strategy | Effect |
|---------|----------|--------|
| +1.0 | **Penalise** (far right) | Cells with competitors score much lower — find true whitespace |
| +0.5 | Moderate penalty | Mild downweight for competitor presence |
| 0.0 | **Ignore** (centre) | Competition has no effect on scores |
| −0.5 | Moderate mirror | Mild boost for competitor presence |
| −1.0 | **Mirror** (far left) | Cells with competitors score higher — co-locate near competition |

**Example — Penalise (β = 1.0):**
A cell with `competition_score = 1.0` gets `competition_factor = 0.0`,
eliminating it from contention. A cell with no competitors gets
`competition_factor = 1.0`, preserving its full score.

**Example — Mirror (β = −1.0):**
A cell with `competition_score = 1.0` gets `competition_factor = 2.0`,
doubling its raw score. This is useful when the strategy is to open near
existing competitors (e.g. fast food clusters).

### No competitor brand specified

If the competitor brand text box is left empty, the app uses category-based
market saturation instead — counting any business in the same POI categories
as the brand, not a specific competitor name.

---

## 4. Final Normalisation — Percentile Rank

The raw composite score (`similarity × demand_boost × competition_factor`)
is converted to a **percentile rank** across all candidate cells.

### Why percentile rank?

| Method | Problem |
|--------|---------|
| **Min-max** | A single outlier cell stretches the range, compressing all other scores into a narrow band (e.g. everything between 50–60%) |
| **Percentile rank** | Distribution-aware — scores spread naturally across the full 0–100% range regardless of outliers |

### Interpretation

| Score | Meaning |
|-------|---------|
| 95–100% | Top-tier opportunity — among the best-matched, highest-demand, optimally-positioned cells |
| 80–95% | Strong opportunity |
| 50–80% | Above average |
| 20–50% | Below average |
| 0–20% | Poor match, low demand, or heavy competition (depending on β) |

---

## 5. Tiebreaking

When multiple cells share the same opportunity score, **`poi_density`** is
used as a secondary sort key. Denser areas rank higher among equally-scored
cells.

---

## Summary

```
                  ┌─────────────┐
                  │  Hex2Vec     │
                  │  Embeddings  │
                  └──────┬──────┘
                         │
                         ▼
               ┌─────────────────┐
               │   Similarity    │  cosine(brand_profile, cell_embedding)
               │    (0 – 1)      │
               └────────┬────────┘
                        │
                        × ──── demand_boost = 1 + α × percentile(poi_density)
                        │
                        × ──── competition_factor = 1 − β × min(count/median, 1)
                        │
                        ▼
               ┌─────────────────┐
               │   Raw Score     │
               └────────┬────────┘
                        │
                  percentile_rank
                        │
                        ▼
               ┌─────────────────┐
               │  Opportunity    │  "better than X% of all cells"
               │  Score (0–100%) │
               └─────────────────┘
```

**Implementation:** `compute_opportunity_score()` in
[`src/app/similarity.py`](src/app/similarity.py)

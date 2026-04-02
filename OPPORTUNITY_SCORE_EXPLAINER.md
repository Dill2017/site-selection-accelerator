# Opportunity Score Explainer

The opportunity score ranks every H3 hexagonal cell in a target city by how
well it matches a brand's ideal location profile, adjusted for the
competitive landscape.

```
raw               = similarity × (1 − β × competition_score)
opportunity_score = percentile_rank(raw)
```

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

## 2. Competition Factor

**What it measures:** the presence of a specific competitor brand (e.g.
"Subway") in each cell and its neighbours.

```
competition_score = min(competitor_count / max(nonzero_counts), 1.0)
competition_factor = 1 − β × competition_score
```

### Competition score normalisation

Competitor counts are normalised by the **maximum** non-zero count across
all cells. The most saturated cell gets `competition_score = 1.0`; cells
with fewer competitors are penalised proportionally less. This avoids
over-penalising cells when most competitor cells only have 1 location.

| Competitor count | Max = 4 | Score |
|-----------------|---------|-------|
| 0 | — | 0.0 |
| 1 | 1 / 4 | 0.25 |
| 2 | 2 / 4 | 0.5 |
| 4 (max) | 4 / 4 | 1.0 |

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

## 3. Interpretation

| Score range | Meaning |
|-------------|---------|
| High | Strong neighbourhood match with favourable competitive position |
| Medium | Decent match or good match offset by competition |
| Low | Poor neighbourhood match, or heavy competition (depending on β) |

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
                        × ──── (1 − β × min(count/max, 1))
                        │
                        ▼
               ┌─────────────────┐
               │  Opportunity    │
               │  Score          │
               └─────────────────┘
```

**Implementation:** `compute_opportunity_score()` in
[`src/app/similarity.py`](src/app/similarity.py)

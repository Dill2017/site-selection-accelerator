# How Hex2Vec Embeddings Power Site Selection

This document explains the geospatial embedding pipeline at the heart of the
Site Selection Accelerator — from raw Points of Interest (POI) data all the
way to a ranked list of whitespace opportunities for a brand.

---

## Table of Contents

1. [The Intuition](#the-intuition)
2. [Step 1 — Discretising Geography with H3](#step-1--discretising-geography-with-h3)
3. [Step 2 — Counting POI Features per Cell](#step-2--counting-poi-features-per-cell)
4. [Step 3 — From Count Vectors to Dense Embeddings](#step-3--from-count-vectors-to-dense-embeddings)
5. [Step 4 — How Hex2Vec Learns](#step-4--how-hex2vec-learns)
6. [Step 5 — Brand Profiling and Cosine Similarity](#step-5--brand-profiling-and-cosine-similarity)
7. [Step 6 — Interpreting the Scores](#step-6--interpreting-the-scores)
8. [Worked Example](#worked-example)
9. [Why Not Just Use Count Vectors?](#why-not-just-use-count-vectors)
10. [References](#references)

---

## The Intuition

Every neighbourhood has a character: a cluster of coffee shops and bookstores
feels different from a strip of car dealerships and fast-food restaurants.
If we can capture that character numerically, we can answer the question
**"Which neighbourhoods feel like the ones where my brand already thrives?"**

Hex2Vec does exactly this. It learns a compact numeric fingerprint (an
*embedding*) for every hexagonal cell on a map, based on the types of places
(POIs) found inside and around it.

---

## Step 1 — Discretising Geography with H3

Before analysing the landscape, we need a uniform spatial grid. The
accelerator uses [Uber's H3 indexing system](https://h3geo.org/), which
partitions the Earth's surface into hexagonal cells at multiple resolutions.

| Resolution | Avg. edge length | Avg. cell area |
|:---:|:---:|:---:|
| 7 | ~1.22 km | ~5.16 km² |
| 8 | ~0.46 km | ~0.74 km² |
| 9 | ~0.17 km | ~0.11 km² |
| 10 | ~0.07 km | ~0.015 km² |

The city polygon (or a fallback bounding box) is filled with H3 cells using
the Databricks SQL function `h3_polyfillash3()`:

```sql
SELECT explode(h3_polyfillash3(geom_wkt, 9)) AS h3_cell
FROM city_poly
```

Each cell is identified by a unique 64-bit integer. Hexagons tile the plane
with equal-area cells and consistent adjacency (every cell has exactly six
neighbours), making them ideal for spatial analysis.

---

## Step 2 — Counting POI Features per Cell

Every POI — restaurant, bank, gym, etc. — is assigned to the H3 cell it
falls within using `h3_longlatash3()`:

```sql
SELECT
    p.id AS poi_id,
    p.categories.primary AS category,
    h3_longlatash3(ST_X(ST_GeomFromWKB(p.geom)),
                   ST_Y(ST_GeomFromWKB(p.geom)),
                   9) AS h3_cell
FROM carto_overture_maps_places.carto.place p
WHERE p.categories.primary IN ('restaurant', 'cafe', 'bank', ...)
```

The result is pivoted into a **count-vector matrix** — one row per H3 cell,
one column per POI category, values are the number of that POI type in the
cell:

| h3_cell | restaurant | cafe | bank | gym | ... |
|:---:|:---:|:---:|:---:|:---:|:---:|
| 617700169483624447 | 3 | 2 | 1 | 0 | ... |
| 617700169483624448 | 0 | 0 | 2 | 1 | ... |
| 617700169520979967 | 5 | 4 | 0 | 2 | ... |

This is the raw feature representation. A cell with 3 restaurants, 2 cafes,
and 1 bank has a different character from a cell with 2 banks and 1 gym.

---

## Step 3 — From Count Vectors to Dense Embeddings

Count vectors work, but they have limitations:

- **Sparse**: most cells have zero counts for most categories.
- **No neighbourhood context**: a cell's character depends not just on what's
  inside it, but what's *around* it.
- **No learned similarity**: a cell with 3 restaurants and a cell with
  3 cafes are equidistant in count space, even though they are functionally
  similar (both are food-oriented).

**Hex2Vec** solves all three by learning a dense, low-dimensional embedding
that encodes each cell's character *in the context of its neighbours*.

The pipeline constructs three inputs for the SRAI `Hex2VecEmbedder`:

```
┌──────────────────────────────────────────────────────────────────┐
│                         SRAI Inputs                              │
│                                                                  │
│  regions_gdf    H3 cell polygons, indexed by hex-string ID       │
│                 ┌───────────────────────────────────────────┐     │
│                 │  region_id       │  geometry              │     │
│                 │  891f1d48177ffff │  POLYGON((...))        │     │
│                 │  891f1d4812fffff │  POLYGON((...))        │     │
│                 └───────────────────────────────────────────┘     │
│                                                                  │
│  features_gdf   POI points with one-hot category columns         │
│                 ┌─────────────────────────────────────────────┐   │
│                 │  feature_id │ geometry   │ restaurant │ cafe │   │
│                 │  poi_001    │ POINT(...) │     1      │  0   │   │
│                 │  poi_002    │ POINT(...) │     0      │  1   │   │
│                 └─────────────────────────────────────────────┘   │
│                                                                  │
│  joint_gdf      Which features belong to which region            │
│                 ┌──────────────────────────────────────┐          │
│                 │  region_id       │  feature_id       │          │
│                 │  891f1d48177ffff │  poi_001          │          │
│                 │  891f1d48177ffff │  poi_002          │          │
│                 └──────────────────────────────────────┘          │
│                                                                  │
│  neighbourhood  H3 adjacency graph (6 neighbours per cell)       │
└──────────────────────────────────────────────────────────────────┘
```

---

## Step 4 — How Hex2Vec Learns

Hex2Vec is inspired by Word2Vec (specifically the Skip-gram model from NLP),
adapted for hexagonal geography. The core idea:

> **Nearby hexagons should have similar embeddings;
> distant hexagons should have different embeddings.**

### Training Objective

For each H3 cell (the "anchor"), Hex2Vec:

1. **Aggregates its POI features** — sums the one-hot category vectors of
   all POIs in the cell to form a feature vector.
2. **Samples a positive neighbour** — picks one of the anchor's immediate
   H3 neighbours (the "positive" example).
3. **Samples a negative cell** — picks a random cell from elsewhere in
   the region (the "negative" example).
4. **Trains a neural encoder** — passes the anchor's feature vector through
   a small feed-forward network to produce a dense embedding. The loss
   function pushes the anchor's embedding *closer* to the positive
   neighbour's embedding and *further* from the negative cell's embedding.

This is a contrastive learning setup (similar to triplet loss):

```
                ┌──────────────┐
  Feature       │   Encoder    │      Embedding
  vector   ───► │  [15, 10]    │ ───►  (10-dim)
  (sparse)      │  dense layers│      (dense)
                └──────────────┘

  Loss = -log σ(sim(anchor, positive)) - log σ(-sim(anchor, negative))

  where sim = dot product of embeddings
        σ   = sigmoid function
```

### Architecture

The encoder is a small feed-forward network with configurable hidden layer
sizes. In this accelerator, the default is `[15, 10]`:

```
Input layer:  N features (one per POI category, e.g. 28)
     │
     ▼
Hidden 1:     15 neurons + ReLU
     │
     ▼
Hidden 2:     10 neurons ← this is the embedding dimension
     │
     ▼
Output:       10-dimensional embedding vector
```

The encoder is shared between anchor and neighbour — both cells are
encoded by the same weights, ensuring that cells with similar POI
surroundings end up near each other in the 10-dimensional embedding space.

### What the Model Captures

After training (default: 5 epochs over the cell neighbourhood graph),
the 10-dimensional embedding for each cell encodes:

- **Local POI mix** — what types of places are in the cell.
- **Neighbourhood context** — what types of places surround the cell.
- **Learned category relationships** — restaurants and cafes are treated as
  more similar to each other than to, say, gas stations, even though all
  three are separate columns in the count vector.

---

## Step 5 — Brand Profiling and Cosine Similarity

Once every H3 cell has an embedding, we can quantify how well each cell
matches a brand's existing locations.

### 1. Identify Brand Cells

Each user-provided brand location (lat/lon or geocoded address) is mapped
to its H3 cell:

```python
hex_str = h3.latlng_to_cell(lat, lon, resolution)
cell_id = h3.str_to_int(hex_str)
```

### 2. Compute the Brand Profile

The embeddings of all brand cells are averaged element-wise to form a
single **brand profile vector** — a 10-dimensional fingerprint of the
kind of neighbourhood the brand prefers:

```
Brand has locations in cells A, B, C

  embedding(A) = [0.3, -0.1, 0.8, ...]
  embedding(B) = [0.4,  0.0, 0.7, ...]
  embedding(C) = [0.2, -0.2, 0.9, ...]

  brand_profile = mean([A, B, C]) = [0.3, -0.1, 0.8, ...]
```

### 3. Score Every Other Cell

**Cosine similarity** measures the angle between the brand profile vector
and each candidate cell's embedding:

```
                 brand_profile · cell_embedding
cos(θ) = ─────────────────────────────────────────
          ‖brand_profile‖  ×  ‖cell_embedding‖
```

- **cos(θ) = 1** → the cell's neighbourhood character is identical to the
  brand profile.
- **cos(θ) = 0** → no relationship.
- **cos(θ) = -1** → the cell is the polar opposite of the brand profile.

The raw cosine scores are then min-max normalised to [0, 1] for display
on the map.

### 4. Exclude and Rank

Cells where the brand already has a location are excluded. The remaining
cells are ranked by descending similarity — the top-ranked cells are the
**whitespace opportunities**.

```
┌──────────────────────────────────────────────────────────────┐
│  All H3 cells in city                                        │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  Cells with embeddings                                 │  │
│  │                                                        │  │
│  │  ┌──────────────────┐  ┌────────────────────────────┐  │  │
│  │  │ Brand cells (3)  │  │ Candidate cells (997)      │  │  │
│  │  │ → EXCLUDED       │  │ → Scored & ranked          │  │  │
│  │  │                  │  │ → Top 20 = opportunities   │  │  │
│  │  └──────────────────┘  └────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## Step 6 — Interpreting the Scores

| Score Range | Interpretation |
|:---:|---|
| **0.8 – 1.0** | Very strong match — the neighbourhood closely mirrors the brand's existing locations. High-priority expansion candidates. |
| **0.5 – 0.8** | Moderate match — similar character with some differences. Worth investigating. |
| **0.2 – 0.5** | Weak match — the neighbourhood has a different character. |
| **0.0 – 0.2** | Poor match — very different from the brand's preferred surroundings. |

Keep in mind:

- Scores are **relative within a single run**. A score of 0.75 in one city
  is not directly comparable to 0.75 in another city (unless they were
  analysed together).
- The embedding model is trained **fresh each time** with only 5 epochs.
  For production use, consider training on a larger dataset and saving the
  model for reuse.
- **More brand locations = better profile**. A single location gives a
  noisy profile; 5+ locations give a more stable signal.

---

## Worked Example

Suppose you are expanding a coffee chain with 3 existing locations in central
London. Here is the pipeline, step by step:

### Input

- **Brand locations**: 3 addresses geocoded to lat/lon.
- **City**: London, GB.
- **H3 resolution**: 9 (~170m edge, ~0.11 km² cells).
- **POI categories**: restaurant, cafe, coffee_shop, bar, bakery, bank,
  clothing_store, gym.

### Processing

1. **H3 tessellation**: London's bounding polygon is filled with ~8,000 H3
   cells at resolution 9.

2. **POI extraction**: ~12,000 POIs in the selected categories within the
   London bounding box are each assigned to their H3 cell.

3. **Count vectors**: An 8,000 x 8 matrix (cells x categories) is built.
   Most cells have at least a few POIs; central cells might have dozens.

4. **Hex2Vec training**: The encoder trains for 5 epochs over the H3
   neighbourhood graph. Each cell gets a 10-dimensional embedding.

   *Cell near Shoreditch (many cafes, bars, clothing stores)*:
   `[0.8, -0.2, 0.5, 0.9, 0.1, -0.3, 0.7, 0.4, -0.1, 0.6]`

   *Cell in the City of London (many banks, few cafes)*:
   `[0.1, 0.7, -0.4, 0.2, 0.8, 0.5, -0.2, 0.1, 0.6, -0.3]`

5. **Brand profile**: The 3 brand cells' embeddings are averaged. The
   profile might emphasise high cafe density, moderate restaurant density,
   and walkable mixed-use neighbourhoods.

6. **Scoring**: All ~8,000 cells are scored against the brand profile.
   Cells in areas like Notting Hill, Camden, or Brixton (similar cafe/bar
   mix) score high. Industrial areas or residential-only zones score low.

7. **Output**: The top 20 cells are plotted as green dots on the map.
   The user can hover to see the similarity score and a nearby address.

---

## Why Not Just Use Count Vectors?

| Property | Count Vectors | Hex2Vec Embeddings |
|---|---|---|
| **Dimensionality** | One dimension per category (can be 30+) | Fixed low dimension (e.g. 10) regardless of categories |
| **Sparsity** | Many zeros — most cells lack most POI types | Dense — every dimension carries information |
| **Neighbourhood awareness** | None — each cell is independent | Built in — the model is trained on adjacent cells |
| **Category relationships** | All categories are orthogonal (cafe ≠ restaurant) | Related categories (cafe, coffee_shop) end up near each other in embedding space |
| **Noise tolerance** | Sensitive to individual POI counts | Smoothed by training over neighbourhoods |
| **Computational cost** | Instant (just a SQL pivot) | Requires model training (seconds to minutes) |

Count vectors are useful for quick exploratory analysis. Hex2Vec embeddings
are better for similarity scoring because they capture the *contextual
character* of a location, not just the raw counts.

---

## References

- **Hex2Vec paper**: Szymon Woźniak, Piotr Szymański. *Hex2vec: Context-Aware
  Representation Learning for Spatial Data.* 2021.
  [arXiv:2111.00970](https://arxiv.org/abs/2111.00970)

- **SRAI library**: Kraina AI. *Spatial Representations for Artificial
  Intelligence.*
  [Documentation](https://kraina-ai.github.io/srai/) |
  [GitHub](https://github.com/kraina-ai/srai)

- **H3**: Uber. *H3: A Hexagonal Hierarchical Geospatial Indexing System.*
  [h3geo.org](https://h3geo.org/)

- **Word2Vec** (the NLP inspiration): Tomas Mikolov et al. *Efficient
  Estimation of Word Representations in Vector Space.* 2013.
  [arXiv:1301.3781](https://arxiv.org/abs/1301.3781)

- **Cosine similarity**: A standard metric in information retrieval and
  machine learning for comparing the orientation of two vectors, independent
  of their magnitude.

- **Overture Maps Foundation**: The POI data source.
  [overturemaps.org](https://overturemaps.org/)

- **Databricks SQL H3 functions**:
  [Documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-h3-geospatial-functions.html)

# Text Embedding Integration Plan — Checkpoint

Extends the site-selection-accelerator with POI text-feature embeddings to
enable **brand search by name** and **competition-aware whitespace scoring**.

> **Status:** In progress. Brand search, competition analysis, opportunity
> scoring, LLM intent detection, brand refinement, POI density tiebreaking,
> and single-pass normalisation are implemented and running locally.

---

## Existing Data Assets

| Asset | Location | Contents |
|:---|:---|:---|
| Enriched POI table | `beatrice_liew.geospatial.site_selection_embedding` | Overture POIs with enriched fields + H3 cell assignments (hex strings) |
| Vector Search index | `beatrice_liew.geospatial.site_embeddings` | Text-feature embeddings on concatenated POI string |
| Gold tables | `beatrice_liew.geospatial.gold_cities`, `gold_places` | Copied to `e2-demo-field-eng` workspace for local dev |

**Geographic scope:** UK (current data coverage). Default country is GB,
default city is London.

---

## Architecture (Implemented)

```
                          ┌─────────────────────────┐
                          │   User Input             │
                          │   "Starbucks"            │
                          │   or lat/lon (existing)  │
                          └────────────┬─────────────┘
                                       │
                          ┌────────────▼──────────────────┐
                          │  Brand Discovery               │
                          │  (brand_search.py)             │
                          │                                │
                          │  1. LLM intent detection       │
                          │     → target category filter   │
                          │  2. Vector Search on           │
                          │     site_embeddings index      │
                          │     (pre-filtered by category) │
                          │  3. Refine brand POIs:         │
                          │     a. brand_name match        │
                          │     b. dominant category       │
                          │  → extract lat/lon + H3 cells  │
                          └──┬─────────────────────────────┘
                             │
          ┌──────────────────▼──────────────┐
          │  EXISTING PIPELINE (unchanged)  │
          │                                 │
          │  H3 tessellation (DBSQL)        │
          │  → POI count vectors            │
          │  → Hex2Vec embeddings (SRAI)    │
          │  → brand profile (mean emb)     │
          │  → vibe_score (cosine sim)      │
          └──────────────┬──────────────────┘
                         │
          ┌──────────────▼───────────────────────────────┐
          │  Competition Analysis                         │
          │  (brand_search.find_competitors_in_similar    │
          │   _cells)                                     │
          │                                               │
          │  1. Take all cells above min_similarity       │
          │  2. Convert h3_cell ints → hex strings        │
          │     (h3_int_to_hex handles signed BIGINTs)    │
          │  3. Filter brand categories:                  │
          │     a. Frequency gate (≥5% of brand POIs)     │
          │     b. LLM industry filter (same vertical)    │
          │  4. SQL: query enriched table for POIs in     │
          │     those cells matching those categories     │
          │  5. Exclude the brand itself by name          │
          │  6. Aggregate: comp_per_cell (h3_hex,         │
          │     competitor_count, top_competitors)         │
          └──────────────┬───────────────────────────────┘
                         │
          ┌──────────────▼───────────────────────────────┐
          │  Opportunity Scoring                          │
          │  (similarity.compute_opportunity_score)       │
          │                                               │
          │  scored["h3_hex"] = h3_int_to_hex(h3_cell)   │
          │  LEFT JOIN competition ON h3_hex              │
          │                                               │
          │  opportunity = vibe_score                     │
          │             × (1 − β × comp_score)            │
          └──────────────┬───────────────────────────────┘
                         │
          ┌──────────────▼───────────────────────────────┐
          │  Map + Debug Tables                           │
          │  (map_viz.py, app.py)                         │
          │                                               │
          │  • H3 heatmap coloured by similarity           │
          │  • Blue dots = brand locations                │
          │  • Green dots = top 2% opportunities          │
          │    (sorted by opp score + POI density)        │
          │  • Tooltip: opp score, similarity, POI        │
          │    density, competitor count, top 3 names,    │
          │    POI mix vs brand avg                       │
          │  • Debug expanders: merged table, comp/cell   │
          └──────────────────────────────────────────────┘
```

---

## Scoring Formula

```
opportunity_score = vibe_score × (1 − β × competition_score)
```

| Term | Source | Range |
|:---|:---|:---|
| **similarity** | Hex2Vec cosine similarity (single-pass min-max normalised across city cells only) | 0–1 |
| **competition_score** | `competitor_count / max(competitor_count)` across cells with competition | 0–1 |
| **β** | User slider in sidebar (default 1.0) | 0–1 |

### Examples (β = 0.5)

| Cell | Vibe | Competitors | Comp Score | Opportunity |
|:---|:---:|:---:|:---:|:---:|
| Good vibe, empty | 0.90 | 0 | 0.00 | **0.90** |
| Good vibe, light comp | 0.85 | 2 | 0.20 | **0.77** |
| Good vibe, saturated | 0.92 | 10 | 1.00 | **0.46** |
| Bad vibe, empty | 0.20 | 0 | 0.00 | **0.20** |

---

## H3 Format Normalisation

Databricks SQL H3 functions (`h3_polyfillash3`, `h3_longlatash3`) return
**signed BIGINT**. The h3-py library's `str_to_int()` returns **unsigned**
integers. For resolution ≥ 8, the MSB is set, so the same cell has different
Python int values depending on which produced it.

**Solution:** merge on **hex strings**, not integers. A utility function
`h3_int_to_hex()` handles both signed and unsigned inputs:

```python
def h3_int_to_hex(val: int) -> str:
    if val < 0:
        val = val + (1 << 64)
    return h3.int_to_str(val)
```

This is used in:
- `similarity.compute_opportunity_score` — adds `h3_hex` to `scored` before
  merging with `comp_per_cell`
- `brand_search.find_competitors_in_similar_cells` — converts candidate cell
  ints to hex strings for the SQL `WHERE h3 IN (...)` query
- `map_viz._h3_int_to_hex` — converts cell ints to hex strings for pydeck's
  `H3HexagonLayer`

---

## LLM Intent Detection (Pre-Filter)

Before calling Vector Search, an LLM identifies the target business category
from the user's free-text query. This pre-filters Vector Search by
`basic_category`, scoping results to the correct business type:

- `"budget hotels"` → LLM returns `hotel` → VS only searches hotels
- `"Premier Inn"` → LLM returns `hotel` → no convenience stores
- `"artisan coffee shops"` → LLM returns `cafe` → no car rentals

**Fallback:** if the filtered search returns fewer than 5 results (LLM
guessed wrong or category name mismatch), retries without the category
filter.

Implemented in `brand_search._detect_category_intent()`.

---

## Brand POI Refinement

Vector Search matches on shared tokens (e.g. "Premier Inn" also returns
"Premier" convenience stores). After the threshold filter, a two-stage
refinement cleans up false positives:

### Stage 1: Brand Name Match

If `brand_name_primary` is available, keep only POIs where the query is a
substring of the brand name (e.g. `"premier inn" in "Premier Inn London
Waterloo"` → kept; `"premier inn" in "Premier"` → excluded). Requires ≥3
matches to activate.

### Stage 2: Dominant Category

If brand name matching doesn't apply or gives too few hits, identify the
most common category from the top 20 results by score, and keep only POIs
matching that category (e.g. if 80% are "hotel", drop the rest).

Implemented in `brand_search._refine_brand_pois()`.

---

## Similarity Normalisation (Single-Pass)

**Problem:** The original code normalised cosine similarity twice:

1. `similarity.compute_similarity()` — across ALL cells (city + rural
   brand neighbourhood cells)
2. `app.py` — re-normalised after filtering to city cells only

The double normalisation squashed score spread: rural cells anchored the
"0" end, compressing all city cells into a narrow band near 1.0.

**Fix:** Removed the first normalisation pass. Raw cosine scores now flow
to `app.py`, where a single min-max normalisation runs against city cells
only. This gives the full [0, 1] range to differentiate between city
locations.

---

## POI Density Tiebreaking

When multiple cells have identical opportunity scores (common in cities
where many areas share similar category mixes), **POI density** (total
amenity count per cell from `count_vectors`) is used as a secondary sort
key. Denser areas (busier high streets) rank higher among equally-scored
cells.

Used in:
- `similarity.get_top_opportunities()` — secondary sort
- `map_viz.build_map()` — top 2% green dot selection
- Map tooltip — displayed as "POI Density: N"

---

## Category Filtering (Two-Stage)

The competitor search needs to know which POI categories to include. Raw
brand POI data can contain noise (e.g. a Starbucks distributor hub tagged
as `b2b_supplier_distributor`).

### Stage 1: Frequency Gate

Count category occurrences across brand POIs (both `basic_category` and
`poi_primary_category`). Drop any category appearing in fewer than 5% of
POIs. This removes low-frequency noise from false-positive vector search
matches.

### Stage 2: LLM Industry Filter

Pass the brand name, the **top 3 dominant categories** (by count), and all
above-threshold categories to an LLM. The prompt asks it to keep only
categories that represent a **competitor in the same industry vertical**,
with concrete examples:

> *If the brand is a cafe, competitors are cafes, coffee shops, bakeries —
> NOT hair salons, gyms, or distributors.*

This is flexible across retail verticals — works for coffee chains, gyms,
pharmacies, etc. without hardcoded rules.

**Fallback:** if the LLM call fails, use the top 3 dominant categories.

---

## Authentication

The Databricks SDK's `databricks-cli` OAuth auth causes `SIGSEGV` crashes
when run inside Streamlit's multi-threaded environment (the Go-compiled CLI
binary segfaults in a subprocess).

**Solution:** conditional auth in both `brand_search._get_workspace_client()`
and `db._create_connection()`:

```python
if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("IS_DATABRICKS_APP"):
    # On Databricks: service principal (default)
    cfg = Config()
else:
    # Local Streamlit: PAT from [DEFAULT] profile
    cfg = Config(profile="DEFAULT")
```

This ensures portability: works locally with PAT and deploys to Databricks
Apps with service principal auth.

---

## File Change Summary

| File | Status | What changed |
|:---|:---:|:---|
| `brand_search.py` | **NEW** | `search_pois()`, `discover_brand_locations()`, `_detect_category_intent()` (LLM pre-filter), `_refine_brand_pois()` (brand name + dominant category cleanup), `find_competitors_in_similar_cells()`, `_filter_categories()` (frequency + LLM), `_llm_industry_filter()` (hotel example added), `h3_int_to_hex()`, `_get_workspace_client()` |
| `similarity.py` | MODIFIED | Removed first-pass min-max normalisation (raw cosine scores flow through); `compute_opportunity_score()` merges on `h3_hex`; `get_top_opportunities()` sorts by `[opportunity_score, poi_density]` |
| `app.py` | MODIFIED | Brand name input mode, country/city defaults (GB/London), adjustable thresholds (brand=0.45, competitor=0.45), β slider (default 1.0), competition pipeline, POI density merge from `count_vectors`, brand search debug expander, removed red competitor blob |
| `map_viz.py` | MODIFIED | `_h3_int_to_hex` handles signed BIGINTs, heatmap coloured by similarity, green dots = top 2% by opp score + density tiebreak, tooltip shows opp score + similarity + POI density + competitor count + top 3 names + POI mix, blue/green dots non-pickable for tooltip pass-through |
| `explainability.py` | MODIFIED | `explain_competition()` for detail panel |
| `config.py` | MODIFIED | `VS_INDEX_NAME`, `ENRICHED_TABLE`, `VS_COLUMNS`, `BRAND_THRESHOLD=0.45`, `COMPETITOR_THRESHOLD=0.45` |
| `db.py` | MODIFIED | `_create_connection()` uses conditional `Config(profile="DEFAULT")` for local dev |
| `.env` | MODIFIED | `DATABRICKS_WAREHOUSE_ID`, `GOLD_CATALOG=beatrice_liew`, `GOLD_SCHEMA=geospatial` |
| `embeddings.py` | — | No changes (SRAI captures density via POI row count per cell) |
| `pipeline.py` | — | No changes (`build_count_vectors` now used for density tiebreak) |

---

## Key Decisions & Trade-offs

1. **Data-driven competitor identification** (not pure vector search).
   Initial approach used vector search to find competitors directly, but
   results were saturated with the brand itself. Current approach: use
   Hex2Vec to find similar *areas*, then query the enriched POI table for
   businesses in those areas matching the brand's categories.

2. **LLM for category filtering, not competitor naming.** The LLM doesn't
   guess competitor brand names — it only decides which POI *categories*
   belong to the same industry vertical. This is more reliable and
   data-grounded.

3. **LLM intent detection as VS pre-filter.** Vector Search on
   concatenated text fields is noisy (e.g. "budget hotels" returns "Budget
   Car Rental"). The LLM identifies the target category and adds it as a
   metadata filter on the VS call, scoping results to the right business
   type while keeping free-text flexibility.

4. **Brand POI refinement post-VS.** Even with intent filtering, shared
   brand names cause noise (e.g. "Premier" convenience stores for "Premier
   Inn"). A two-stage post-filter uses brand_name_primary matching then
   dominant-category filtering to clean up results.

5. **Hex string merge** instead of integer merge. Avoids the signed/unsigned
   BIGINT mismatch between Databricks SQL and h3-py without changing the
   existing pipeline's integer-based data flow.

6. **Frequency gate before LLM.** Reduces the number of categories the LLM
   sees, making it less likely to keep irrelevant ones, and provides a
   sensible fallback if the LLM is unavailable.

7. **Single-pass normalisation.** Removed the first min-max normalisation
   in `compute_similarity()` to avoid squashing score spread. City-only
   normalisation in `app.py` gives the full [0, 1] range for meaningful
   differentiation.

8. **POI density as tiebreaker.** Among cells with identical opportunity
   scores, denser areas (more amenities) rank higher. Uses existing
   `count_vectors` data — no additional computation needed.

---

## Open Issues

- [x] ~~Verify competitor counts appear correctly on map tooltips~~
- [x] ~~Fix double normalisation squashing score spread~~
- [x] ~~Fix "Premier" convenience stores appearing for "Premier Inn"~~
- [x] ~~Remove orange debug layer and red competitor blob~~
- [ ] Validate category filter quality across different brand types
  (coffee, pharmacy, gym, hotel, etc.)
- [ ] Consider tuning the top N% cutoff (currently 2%) or making it
  a slider
- [ ] Remove debug expanders before final demo
- [ ] Test with non-UK data when enriched table is expanded

---

## Future Enhancements (not in v1)

1. **Semantic cell embedding** — average POI text embeddings per H3 cell
   and fuse with Hex2Vec vibe score for richer affinity matching.
2. **Expand to non-UK** — extend the enriched table and VS index to cover
   more Overture data.
3. **DBSQL brand fallback** — for exact brand name matches, query
   `site_selection_embedding` directly by `brand_name_primary` instead of
   relying on Vector Search similarity thresholds.
4. **Catchment area analysis** — k-ring scoring around candidate sites for
   complementary POIs and foot traffic proxies.

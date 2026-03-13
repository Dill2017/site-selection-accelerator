# Integration Plan — Genie + H3 Polyfill Architecture

Extends the site-selection-accelerator with **Databricks Genie** for natural
language brand discovery and **h3_polyfillash3** for fast spatial filtering,
replacing the previous Vector Search approach.

> **Status:** Implemented. Genie-based brand discovery, h3_polyfillash3
> spatial filtering, competition analysis (all input modes), auto-provisioned
> Genie Space, and density-aware brand location visualisation are running.

---

## Data Assets

| Asset | Location | Contents |
|:---|:---|:---|
| Enriched POI table | `{catalog}.{schema}.gold_places_enriched` | Flattened CARTO place data with names, categories, brands, addresses, coordinates |
| City polygons | `{catalog}.{schema}.gold_cities` | City metadata with WKT polygons (real or bbox fallback), bounding boxes |
| Original POIs | `{catalog}.{schema}.gold_places` | Simpler POI table with extracted coords and primary category |
| App config | `{catalog}.{schema}.app_config` | Key-value store for `GENIE_SPACE_ID` |
| Genie Space | Auto-created or env var | NL→SQL interface on gold_places_enriched + gold_cities |

**Geographic scope:** Global (CARTO Overture Maps). Default country is GB,
default city is London.

---

## Architecture

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
                          │  1. _ensure_genie_space()      │
                          │     → find/create Genie Space  │
                          │  2. _ask_genie(question)       │
                          │     → Genie generates SQL      │
                          │     → execute via DBSQL        │
                          │  3. SQL pattern:               │
                          │     h3_polyfillash3(geom_wkt)  │
                          │     → H3 cell membership IN    │
                          │     → ILIKE brand matching     │
                          │  → brand locations + H3 hex    │
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
          │  3. Filter brand categories:                  │
          │     a. Frequency gate (≥5% of brand POIs)     │
          │     b. LLM industry filter (same vertical)    │
          │  4. SQL: gold_places_enriched WHERE           │
          │     h3_h3tostring(...) IN (hex_list)           │
          │  5. Exclude the brand itself by name          │
          │  6. Aggregate: comp_per_cell                  │
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
          │  Map Visualisation                            │
          │  (map_viz.py)                                 │
          │                                               │
          │  • H3 heatmap coloured by similarity           │
          │  • Blue dots = brand locations (snapped to    │
          │    H3 centres, light→dark by density)         │
          │  • Green dots = top 2% opportunities          │
          │  • Contextual tooltips per layer              │
          └──────────────────────────────────────────────┘
```

---

## Spatial Filtering: h3_polyfillash3 (not ST_CONTAINS)

The previous approach used `JOIN ... ON country` + per-row `ST_CONTAINS`,
which created an expensive cross-join (every POI checked against every city
polygon in the country). This has been replaced with `h3_polyfillash3`:

### Pattern

```sql
WITH city_h3 AS (
    SELECT explode(h3_polyfillash3(geom_wkt, 9)) AS h3_cell
    FROM gold_cities
    WHERE country = 'GB' AND city_name = 'London'
)
SELECT p.*, h3_h3tostring(h3_longlatash3(p.lon, p.lat, 9)) AS h3_cell
FROM gold_places_enriched p
WHERE h3_longlatash3(p.lon, p.lat, 9) IN (SELECT h3_cell FROM city_h3)
  AND p.lon IS NOT NULL AND p.lat IS NOT NULL
```

**Why this is faster:**
1. `h3_polyfillash3` runs once on the city polygon WKT string (not GEOMETRY)
2. Produces a finite set of H3 BIGINT cells
3. POI filtering becomes a simple integer `IN` subquery — no per-row geometry
4. `h3_h3tostring` converts to hex only for the output column

**Important:** `h3_polyfillash3` accepts WKT STRING or WKB BINARY, not
GEOMETRY. Pass `geom_wkt` directly — do NOT wrap in `ST_GeomFromText()`.

**Note:** `gold_cities.has_polygon` indicates whether a real polygon exists
(vs a synthetic bbox fallback). Both produce valid WKT for `h3_polyfillash3`,
so the `has_polygon` filter is NOT used in queries.

---

## Genie Space Integration

### Auto-Provisioning

The `GENIE_SPACE_ID` is resolved in this order:
1. `GENIE_SPACE_ID` environment variable
2. `app_config` table (`config_key = 'GENIE_SPACE_ID'`)
3. Find existing space by name ("Site Selection - Brand & Competition Explorer")
4. Create a new space via `setup_genie_space.py`

This means end users can:
- Set the env var for immediate use
- Run the ETL job to auto-create and persist the ID
- Let the app auto-provision on first use

### Genie Space Configuration

The `setup_genie_space.py` script builds a `serialized_space` JSON with:

- **Tables**: `gold_cities` and `gold_places_enriched` (sorted alphabetically — required by the API)
- **Text instructions**: spatial filtering pattern using `h3_polyfillash3`, H3 output as hex strings, brand matching via ILIKE
- **Example question-SQL pairs**: concrete Starbucks/London example showing the full CTE pattern

### Genie → DBSQL Execution

The `_ask_genie()` function extracts the SQL that Genie generates and executes
it directly via the DBSQL connector (`db.execute_query`). This bypasses known
SDK issues with data truncation in the Genie result API.

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

Genie returns H3 cells as hex strings via `h3_h3tostring()`, so the brand
discovery path already produces hex strings directly.

---

## Competition Analysis

### For brand name input

1. Genie discovers brand POIs → extract categories
2. Two-stage category filter (frequency gate + LLM industry filter)
3. Query `gold_places_enriched` for competitors in high-similarity cells
   matching those categories (direct SQL, no Genie — parameters are structured)

### For lat/lon or address input

1. `infer_location_categories()` reverse-looks up POIs in the same H3 cells
   as the input locations (filtered by city polygon via h3_polyfillash3)
2. Same two-stage category filter + competitor query

### Competition query pattern

The competition query doesn't need a city polygon filter because it already
has a specific list of H3 hex cells from the similarity scoring:

```sql
SELECT p.poi_id AS id,
       h3_h3tostring(h3_longlatash3(p.lon, p.lat, 9)) AS h3, ...
FROM gold_places_enriched p
WHERE h3_h3tostring(h3_longlatash3(p.lon, p.lat, 9)) IN ({h3_list})
  AND (p.basic_category IN ({cat_list}) OR p.poi_primary_category IN ({cat_list}))
```

---

## Map Visualisation Changes

### Brand location dots

- **Snapped to H3 cell centres** — aligns with the hexagon grid
- **Colour gradient** — light blue (1 location per cell) to dark navy
  (max locations per cell), showing store density
- **Fixed radius** — all dots same size for clean visuals
- **Pickable** — hover shows H3 cell ID and brand location count

### Tooltip system

pydeck uses a single tooltip template for all pickable layers. Each layer's
DataFrame is populated with all tooltip fields (empty string for inapplicable
ones). CSS `:has(span:empty)` hides rows with no value, so:

- **Hexagon hover**: shows address, opp score, similarity, POI count,
  competitors, POI mix
- **Brand dot hover**: shows H3 cell ID and brand count only

---

## Category Filtering (Two-Stage)

### Stage 1: Frequency Gate

Count category occurrences across brand POIs (both `basic_category` and
`poi_primary_category`). Drop any category appearing in fewer than 5% of
POIs.

### Stage 2: LLM Industry Filter

Pass the brand name, top 3 dominant categories, and all above-threshold
categories to an LLM. The prompt asks it to keep only categories representing
a **competitor in the same industry vertical**.

**Fallback:** if the LLM call fails, use the top 3 dominant categories.

---

## Authentication

Conditional auth in both `brand_search._get_workspace_client()` and
`db._create_connection()`:

```python
if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("IS_DATABRICKS_APP"):
    # On Databricks: service principal (default)
    client = WorkspaceClient()
else:
    # Local Streamlit: PAT from [DEFAULT] profile
    client = WorkspaceClient(profile="DEFAULT")
```

---

## File Change Summary

| File | Status | What changed |
|:---|:---:|:---|
| `brand_search.py` | **REWRITTEN** | Replaced Vector Search with Genie (`_ask_genie`, `_ensure_genie_space`). h3_polyfillash3 for inference query. Direct SQL for competition (no city polygon — uses specific h3_list). `infer_location_categories()` for lat/lon input. LLM category filter retained. |
| `setup_genie_space.py` | **NEW** | Auto-create/update Genie Space with h3_polyfillash3 instructions, example SQL, sample questions. Persist space ID to app_config table. Runs as DABs task or standalone. |
| `gold_places_enriched.sql` | **NEW** | CTAS flattening raw CARTO place data into comprehensive POI table for Genie and competition queries. |
| `config.py` | MODIFIED | Added `GOLD_PLACES_ENRICHED`, `APP_CONFIG_TABLE`, `GENIE_SPACE_ID` (resolved from env/table). Removed `VS_INDEX_NAME`, `ENRICHED_TABLE`, `VS_COLUMNS`, `BRAND_THRESHOLD`, `COMPETITOR_THRESHOLD`. |
| `pipeline.py` | MODIFIED | `get_enriched_pois_in_city()` uses h3_polyfillash3 CTE instead of ST_CONTAINS JOIN. Removed `has_polygon` filter. |
| `map_viz.py` | MODIFIED | Brand dots snapped to H3 cell centres with density colour gradient (light→dark blue). Fixed radius. Pickable with brand count tooltip. Contextual tooltip via CSS `:has(span:empty)` hiding. |
| `app.py` | MODIFIED | Removed brand/competitor threshold sliders. Competition runs for all input modes. Uses `infer_location_categories` for lat/lon. |
| `app.yaml` | MODIFIED | Added `GENIE_SPACE_ID` env var. |
| `geospatial_etl_job.yml` | MODIFIED | Added `create_gold_places_enriched` SQL task and `setup_genie_space` Python task. |
| `.env` | MODIFIED | Added `GENIE_SPACE_ID`. |
| `similarity.py` | — | No changes |
| `embeddings.py` | — | No changes |

---

## Key Decisions & Trade-offs

1. **Genie over Vector Search.** Genie generates precise SQL from natural
   language, leveraging the full schema of `gold_places_enriched`. This
   eliminates the need for a separate Vector Search index and gives more
   control over spatial filtering via instructions.

2. **h3_polyfillash3 over ST_CONTAINS.** Converting city polygons to H3 cells
   once and filtering by integer membership is orders of magnitude faster than
   per-row geometry checks. Accepts WKT strings directly (not GEOMETRY).

3. **Genie SQL re-execution.** Genie's SDK data retrieval has known truncation
   issues. The app extracts Genie's generated SQL and executes it directly via
   DBSQL for complete, reliable results.

4. **Auto-provisioned Genie Space.** The GENIE_SPACE_ID is optional — if not
   set, the app finds an existing space by name or creates one. This makes
   the app self-contained for end users while allowing pre-provisioning via
   the ETL job.

5. **No has_polygon filter.** All `gold_cities` rows have valid WKT (either
   real polygons from `division_area` or synthetic bbox fallbacks).
   `h3_polyfillash3` works with both, so the filter is unnecessary and was
   causing London (which has `has_polygon = FALSE`) to return 0 results.

6. **H3 as hex strings.** Genie returns `h3_h3tostring(h3_longlatash3(...))`
   hex strings, matching the rest of the codebase's merge-on-hex pattern.

7. **Brand dots snapped to H3 centres.** Raw POI coordinates appeared
   misaligned from the hexagon grid. Snapping to cell centres and adding a
   density colour gradient (light→dark) provides cleaner visualisation.

8. **Competition for all input modes.** For lat/lon and address inputs,
   `infer_location_categories()` reverse-looks up nearby POIs to determine
   relevant categories, enabling competition analysis without a brand name.

---

## Future Enhancements

1. **Semantic cell embedding** — average POI text embeddings per H3 cell
   and fuse with Hex2Vec vibe score for richer affinity matching.
2. **Expand to non-UK** — the enriched table already covers global CARTO
   data; validate with non-UK cities.
3. **Catchment area analysis** — k-ring scoring around candidate sites for
   complementary POIs and foot traffic proxies.
4. **Genie conversation follow-ups** — use `create_message_and_wait` to
   ask refinement questions within an existing Genie conversation.

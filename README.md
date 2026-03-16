# Site Selection Accelerator — Brand Site Matching

A Databricks solution accelerator that helps retail operations teams identify
**whitespace expansion opportunities** for their brand. Users can search by
**brand name** (e.g. "Starbucks", "Premier Inn") or enter coordinates directly.
The app discovers existing locations via **Databricks Genie** (natural language
to SQL), builds a geospatial profile using **Hex2Vec** embeddings from both
**POI categories** and **building data** (type, height), scores every H3
hexagonal cell in the target city by neighbourhood similarity, and applies a
**competition penalty** based on co-located competitors — surfacing the areas
that best match the brand's surroundings but aren't yet saturated.

Brand locations can be in **any city** — the tool learns what kind of
neighbourhoods a brand thrives in and finds similar areas in the target market,
enabling cross-city expansion analysis.

**Use cases:** franchise expansion, competitive gap analysis, new market entry.

> **Detailed technical notes:** see
> [INTEGRATION_PLAN.md](INTEGRATION_PLAN.md) for architecture diagrams,
> scoring formula, category filtering details, and file change summary.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                        Streamlit Application                         │
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  ┌───────────┐  │
│  │ Brand name   │  │ Target city  │  │ H3         │  │ POI       │  │
│  │ or lat/lon   │  │ & country    │  │ resolution │  │ categories│  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬─────┘  └─────┬─────┘  │
│         └────────┬────────┴─────────────────┴───────────────┘        │
│                  ▼                                                    │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │        Brand Discovery (brand_search.py)                     │    │
│  │                                                              │    │
│  │  1. Databricks Genie: NL → SQL on gold_places_enriched      │    │
│  │     (auto-provisions Genie Space if ID not set)              │    │
│  │  2. h3_polyfillash3 city polygon → H3 cell filter            │    │
│  │  3. Brand matching via ILIKE on brand_name / poi_name        │    │
│  │  → brand locations (lat/lon + H3 hex cells)                  │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                             ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │         DBSQL Queries on Gold Tables (pipeline.py)           │    │
│  │                                                              │    │
│  │  1. City polygon from gold_cities    (real boundary WKT)      │    │
│  │  2. H3 tessellation via h3_polyfillash3 (polygon fill)       │    │
│  │  3. POI lookup from gold_places (polygon-filtered via H3)    │    │
│  │  4. Building lookup from gold_buildings (bbox + H3 join)     │    │
│  │  5. Cross-city brand neighbourhood   (for external brands)   │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                             ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │            SRAI Hex2Vec Embeddings (embeddings.py)           │    │
│  │                                                              │    │
│  │  • regions_gdf from H3 polygons                              │    │
│  │  • features_gdf with one-hot POI + building categories       │    │
│  │  • joint_gdf from DBSQL H3 assignment                        │    │
│  │  • Hex2VecEmbedder.fit_transform()                           │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                             ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │   Cosine Similarity + Opportunity Scoring (similarity.py)    │    │
│  │                                                              │    │
│  │  • Average brand-cell embeddings → brand profile             │    │
│  │  • Cosine similarity vs all target-city cells                │    │
│  │  • Competition penalty: opp = sim × (1 − β × comp_score)    │    │
│  │  • POI density tiebreaking for equal scores                  │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                             │                                        │
│          ┌──────────────────┤                                        │
│          ▼                  ▼                                        │
│  ┌────────────────┐  ┌──────────────────────────────────────────┐    │
│  │ Competition     │  │  Score Explainability (explainability.py)│    │
│  │ Analysis        │  │                                          │    │
│  │ (brand_search)  │  │  • Brand profile (avg POI counts)        │    │
│  │                 │  │  • Category comparison vs brand average   │    │
│  │ • Similar cells │  │  • Competition detail panel               │    │
│  │ • gold_places   │  └──────────────────────────┬───────────────┘    │
│  │   _enriched     │                             │                   │
│  │ • LLM category  │                             │                   │
│  │   filtering     │                             │                   │
│  └────────┬────────┘                             │                   │
│           └──────────────────┬───────────────────┘                   │
│                              ▼                                       │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │             pydeck Map Visualisation (map_viz.py)            │    │
│  │                                                              │    │
│  │  • CARTO basemap                                             │    │
│  │  • GeoJsonLayer — city polygon boundary outline              │    │
│  │  • H3HexagonLayer — similarity heatmap                       │    │
│  │  • ScatterplotLayer — existing locations (blue, density      │    │
│  │    gradient: light→dark by brand count per cell)             │    │
│  │  • ScatterplotLayer — top 2% opportunities (green)           │    │
│  │  • Tooltips: contextual per layer (brand count, opp score,   │    │
│  │    similarity, competitors, POI mix)                          │    │
│  └──────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────────┘
                              │
     ┌────────────────────────┼────────────────────────┐
     ▼                        ▼                         ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│  Gold Tables      │ │  Genie Space     │ │  Databricks SQL      │
│  (Unity Catalog)  │ │  (auto-created)  │ │  Warehouse           │
│                   │ │                  │ │                      │
│  • gold_cities    │ │  NL → SQL on:    │ │  H3 functions:       │
│  • gold_places    │ │  • gold_places   │ │   h3_polyfillash3    │
│  • gold_places    │ │    _enriched     │ │   h3_longlatash3     │
│    _enriched      │ │  • gold_cities   │ │   h3_h3tostring      │
│  • gold_buildings │ │                  │ │   h3_centerasgeojson │
│  • app_config     │ │                  │ │                      │
└──────────────────┘ └──────────────────┘ └──────────────────────┘
         ▲
┌───────────────────────┐
│  ETL Job               │
│  (SQL + Python tasks)  │
│                        │
│  CARTO Overture Maps   │
│  → gold_cities         │
│  → gold_places         │
│  → gold_buildings      │
│  → gold_places_enriched│
│  → Genie Space setup   │
└───────────────────────┘
```

---

## Quick Start

### Prerequisites

| Requirement | Details |
|---|---|
| Databricks workspace | With access to a SQL Warehouse |
| Databricks CLI | v0.239.0+ (for Asset Bundle deployment) |
| CARTO Overture Maps catalogs | `carto_overture_maps_places`, `carto_overture_maps_divisions`, `carto_overture_maps_buildings` mounted via Delta Sharing |
| Python | 3.11+ (used by the Databricks App runtime) |

### 1. Clone the repository

```bash
git clone <repo-url> site-selection-accelerator
cd site-selection-accelerator
```

### 2. Configure your Databricks profile

Make sure you have a working Databricks CLI profile:

```bash
databricks auth login --host https://<workspace-url>
```

### 3. Configure catalog, schema, and warehouse

Edit `databricks.yml` to set your target catalog and schema (defaults shown):

```yaml
variables:
  catalog:
    default: "dilshad_shawki"   # Change to your catalog
  schema:
    default: "geospatial"       # Change to your schema
  warehouse_id:
    lookup:
      warehouse: "Shared Endpoint"  # Change to your warehouse name
```

Also update `src/app/app.yaml` to match:

```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql-warehouse
  - name: GOLD_CATALOG
    value: "dilshad_shawki"     # Must match catalog above
  - name: GOLD_SCHEMA
    value: "geospatial"         # Must match schema above
  - name: GENIE_SPACE_ID
    value: ""                   # Optional — auto-created if empty
```

### 4. Deploy with Asset Bundles

```bash
# Validate the bundle
databricks bundle validate

# Deploy resources (app + ETL job)
databricks bundle deploy
```

### 5. Run the ETL job to populate gold tables

This creates `gold_cities`, `gold_places`, `gold_buildings`, and
`gold_places_enriched` tables in your catalog/schema, then provisions a Genie
Space with spatial instructions.

```bash
databricks bundle run geospatial_etl_job
```

### 6. Start the application

```bash
databricks bundle run site_selection_app
```

After deployment, the Databricks Apps UI will show the application URL.
Open it in your browser to start finding whitespace opportunities.

---

## How It Works

### Step 1 — Gold Table ETL (one-time setup)

A Databricks job with SQL and Python tasks pre-processes the raw CARTO
Overture Maps data into gold tables and provisions the Genie Space:

| Gold Table | Source | What it does |
|---|---|---|
| `gold_cities` | `division` + `division_area` | Joins city metadata with polygons, extracts WKT geometry, and computes bounding boxes. Uses a multi-level fallback: locality area first, then `ST_Union` of all same-name region/county/neighborhood/macrohood areas (covers city-states like Hamburg/Berlin and metro areas like Manchester/Birmingham), finally a synthetic bbox polygon |
| `gold_places` | `place` | Extracts lon/lat from WKB geometry, flattens `categories.primary` and `addresses[0].freeform`, filters to supported POI categories |
| `gold_buildings` | `building` | Extracts centroids from footprint polygons, derives `building_category` (prefixed subtype/class, e.g. `bldg_residential`), `height_bin` (low/mid/high-rise/skyscraper), pre-computes H3 cell at resolution 9, Z-ordered by bbox for fast spatial scans |
| `gold_places_enriched` | `place` | Comprehensive POI table with flattened names, categories, brands, addresses, coordinates, and bounding boxes for Genie and competition queries |
| `app_config` | — | Key-value store for `GENIE_SPACE_ID` and other runtime config |

The ETL job also runs `setup_genie_space.py`, which creates (or updates) a
Genie Space configured with `h3_polyfillash3`-based spatial instructions and
example SQL, then persists the space ID to `app_config`.

### Step 2 — User Input

The Streamlit sidebar collects:

- **Brand name** (e.g. "Starbucks", "Premier Inn") — discovered via Genie.
  The Genie Space uses `h3_polyfillash3` to fill the city polygon with H3
  cells and filters POIs by cell membership for fast spatial queries.
- **Or brand locations**: `lat, lon` pairs or street addresses (geocoded via
  Nominatim/geopy). These can be in **any city** — not just the target city.
- **H3 resolution** (7–10): controls hexagon granularity.
- **Target country and city**: cascading dropdowns populated from `gold_cities`
  (defaults to GB / London).
- **POI categories**: multi-select grouped by theme (Food & Drink, Shopping,
  Services, Entertainment, Commercial).
- **Building features toggle**: include/exclude building type and height data
  in the embeddings (enabled by default).
- **Competition sensitivity (beta)**: slider from 0 to 1 controlling how
  heavily competition penalises the opportunity score.

### Step 3 — Brand Discovery via Genie (`brand_search.py`)

1. The app sends a natural language question to the Genie Space (e.g. "Find
   all Starbucks within London, GB using h3_polyfillash3").
2. Genie generates optimised SQL following the configured instructions —
   using `h3_polyfillash3` to convert the city polygon into H3 cells,
   then filtering POIs by H3 cell membership (no expensive ST_CONTAINS).
3. The generated SQL is executed via DBSQL for full, reliable results.
4. H3 cells are returned as hex strings via `h3_h3tostring`.
5. If `GENIE_SPACE_ID` is not set, the app auto-provisions one on first use.

### Step 4 — Cross-City Brand Profiling (`pipeline.py`)

When brand locations are outside the target city:

1. Each external location gets an H3 neighbourhood (center cell + k-ring=2).
2. POIs are fetched for those neighbourhoods from `gold_places`.
3. The neighbourhood cells and POIs are merged with the target city data.
4. Hex2Vec trains on the combined dataset, placing brand cells and target city
   cells in the same embedding space.
5. After scoring, only target city cells are shown as opportunities.

### Step 5 — SRAI Hex2Vec Embeddings (`embeddings.py`)

Using the [SRAI](https://kraina-ai.github.io/srai/) library:

1. Builds a `regions_gdf` of H3 cell polygons (via the `h3` Python library).
2. Fetches buildings from `gold_buildings` and normalises each into up to two
   feature rows: one for building type (`bldg_residential`, `bldg_commercial`,
   etc.) and one for height bin (`height_low_rise`, `height_mid_rise`, etc.).
3. Merges POIs and buildings into a unified features table with generic
   `feature_id` / `category` columns. Builds a `features_gdf` with one-hot
   encoded category columns spanning both POI and building categories.
4. Constructs a `joint_gdf` (region-feature mapping) directly from the DBSQL
   H3 assignment.
5. Trains a **Hex2VecEmbedder** (encoder sizes `[15, 10]`, 5 epochs, CPU)
   on the H3 neighbourhood graph to produce dense embeddings per cell.

Building data enriches the embeddings with land-use signals (residential vs.
commercial vs. industrial) and urbanisation density (height profile) that POIs
alone cannot capture.

> **Deep dive:** See [HEX2VEC_EXPLAINER.md](HEX2VEC_EXPLAINER.md) for a
> full explanation of how Hex2Vec works.

### Step 6 — Cosine Similarity & Opportunity Scoring (`similarity.py`)

1. Maps each brand location to its H3 cell at the chosen resolution.
2. Averages those cell embeddings to form a **brand profile vector**.
3. Computes cosine similarity between the brand profile and every target city
   cell's embedding.
4. Re-normalises scores to [0, 1] within the target city for colour contrast.
5. Excludes cells where the brand already has a location.
6. **Competition analysis** (all input modes): queries `gold_places_enriched`
   for businesses in high-similarity cells matching the brand's categories.
   Categories are filtered via a frequency gate and LLM industry filter.
   For lat/lon input, categories are inferred from nearby POIs.
7. Computes **opportunity score**:
   `opportunity = similarity × (1 − β × competition_score)` where
   `competition_score = competitor_count / max(competitor_count)`.
8. Ranks cells by opportunity score, using **POI density** as a tiebreaker.

### Step 7 — Map Visualisation (`map_viz.py`)

Rendered with [pydeck](https://deckgl.readthedocs.io/) on a CARTO Positron
basemap:

| Layer | Description |
|---|---|
| **GeoJsonLayer** | City polygon boundary outline (when real polygon data is available) |
| **H3HexagonLayer** | All candidate cells coloured by similarity score (red = high, blue = low) |
| **ScatterplotLayer (blue)** | Existing brand locations snapped to H3 cell centres; colour gradient from light blue (1 location) to dark navy (max locations per cell). Pickable with brand count tooltip. |
| **ScatterplotLayer (green)** | Top 2% opportunity locations by opportunity score + POI density |

Hovering over any H3 cell shows a tooltip with:
- H3 cell ID and nearest address
- Opportunity score and similarity percentage
- POI count (total amenities in the cell)
- Competitor count and top 3 competitor names (ranked by popularity)
- Category breakdown comparing the cell's POI mix against the brand average

Hovering over a brand location dot shows:
- H3 cell ID
- Number of brand locations in that cell

### Step 8 — Score Explainability (`explainability.py`)

Rather than showing only a similarity percentage, the app provides interpretable
explanations using the raw POI count vectors:

- **Brand Location Profile** — displayed before the map as a horizontal bar
  chart of average feature distributions across all brand cells, faceted by
  feature type (POI vs. Building) with independent scales. Values are shown as
  **% within type** so building counts don't overshadow POI counts.
- **Enhanced tooltips** — hovering any hexagon on the map shows the top 4
  categories in that cell compared to the brand average.
- **Category Fingerprint** — clicking any row in the Top 20 table reveals a
  fingerprint chart showing **all** feature categories (POI + building) for the
  selected location versus the brand average. Users can toggle between **line
  chart** (to compare distribution shapes) and **bar chart** modes, and between
  raw **counts** and **% within type** (normalised) views. The normalised view
  computes percentages independently for POIs and buildings, so neither type
  dominates the chart.

---

## Project Structure

```
site_selection_accelerator/
├── databricks.yml                    # Asset Bundle config (catalog, schema, warehouse)
├── README.md                         # This file
├── HEX2VEC_EXPLAINER.md             # Deep dive into the Hex2Vec algorithm
├── INTEGRATION_PLAN.md              # Detailed technical notes
├── resources/
│   ├── site_selection_app.yml        # Databricks App resource definition
│   └── geospatial_etl_job.yml        # ETL job: SQL + Python tasks
└── src/
    ├── app/
    │   ├── app.yaml                  # App runtime config (command, env vars)
    │   ├── requirements.txt          # Python dependencies
    │   ├── .env                      # Local dev env vars (not committed)
    │   ├── app.py                    # Streamlit UI + orchestration
    │   ├── config.py                 # Gold table refs, categories, Genie Space ID
    │   ├── db.py                     # SQL execution via Databricks SDK Statement API
    │   ├── pipeline.py               # DBSQL queries on gold tables + polygon-aware POI filter
    │   ├── embeddings.py             # SRAI Hex2Vec embedding pipeline
    │   ├── similarity.py             # Cosine similarity + opportunity scoring
    │   ├── brand_search.py           # Brand discovery (Genie) + competition analysis
    │   ├── explainability.py         # Score explainability + competition detail
    │   └── map_viz.py                # pydeck map (heatmap + city boundary outline)
    └── pipeline/
        ├── setup_genie_space.py      # Genie Space provisioning (create/update/persist)
        └── transformations/
            ├── setup_schema.sql      # CREATE SCHEMA IF NOT EXISTS
            ├── gold_cities.sql       # CTAS: cities + ST_Union'd polygons + bboxes
            ├── gold_places.sql       # CTAS: flattened POIs with extracted coords
            ├── gold_buildings.sql    # CTAS: building centroids + categories + H3 + ZORDER
            └── gold_places_enriched.sql  # CTAS: comprehensive POI table for Genie
```

---

## Key Libraries and Functions

### Databricks SQL Geospatial & H3 Functions

| Function | Purpose |
|---|---|
| `h3_polyfillash3(wkt, res)` | Tessellate a polygon (WKT string) into H3 cells |
| `h3_longlatash3(lon, lat, res)` | Assign a point to its H3 cell |
| `h3_h3tostring(cell)` | Convert H3 BIGINT to hex string |
| `h3_centerasgeojson(cell)` | Get the centre point of an H3 cell |
| `ST_GeomFromWKB(wkb)` | Parse WKB into geometry (used in gold_cities ETL) |
| `ST_Union(geom1, geom2)` | Merge two geometries into one (used to combine multi-level polygons) |
| `ST_AsText(geom)` | Convert geometry to WKT string |

### Python Libraries

| Library | Purpose |
|---|---|
| `srai` (Hex2VecEmbedder) | Learned dense geospatial embeddings from POI tag patterns |
| `h3` | Client-side H3 cell ↔ polygon conversions, k-ring neighbourhoods |
| `geopandas` / `shapely` | GeoDataFrame construction for SRAI |
| `scikit-learn` | `cosine_similarity` for scoring |
| `altair` | Vega-Lite charts for brand profile and explainability panels |
| `pydeck` | Deck.gl map rendering in Streamlit |
| `geopy` | Optional address geocoding via Nominatim |
| `databricks-sdk` | Workspace auth, SQL Statement Execution API, Genie API, Foundation Model API |
| `python-dotenv` | Load `.env` variables for local development |

---

## Extending the Accelerator

- **Change catalog/schema** — update `databricks.yml` variables and
  `src/app/app.yaml` env vars (`GOLD_CATALOG`, `GOLD_SCHEMA`), then re-run
  the ETL job.
- **Add new POI categories** — edit `CATEGORY_GROUPS` in `config.py` and the
  `WHERE` clause in `gold_places.sql`, then re-run the ETL job.
- **Add new building categories** — edit `BUILDING_CATEGORY_GROUPS` in
  `config.py` and the `building_category`/`height_bin` logic in
  `gold_buildings.sql`, then re-run the ETL job.
- **Refresh gold tables** — run `databricks bundle run geospatial_etl_job`
  whenever the upstream CARTO data updates. This also updates the Genie Space
  instructions.
- **Custom Genie instructions** — edit `setup_genie_space.py` to add
  domain-specific instructions or example question-SQL pairs.
- **Scale Hex2Vec training** — for large regions, offload training to a
  Databricks Job / notebook with GPU cluster.
- **Alternative embedders** — swap `Hex2VecEmbedder` for SRAI's
  `CountEmbedder` (no training needed, faster) or
  `ContextualCountEmbedder` (neighbourhood-aware counts).

---

## Troubleshooting

| Issue | Resolution |
|---|---|
| "None of the brand locations fall within the analysed H3 cells" | Brand-neighbourhood POI data may be too sparse. Try selecting more POI categories or a coarser H3 resolution. |
| "No POIs found" | The category filter may be too restrictive. Broaden the category selection. |
| Gold tables don't exist | Run the ETL job first: `databricks bundle run geospatial_etl_job` |
| Genie returns empty results | Check that `gold_places_enriched` exists and the Genie Space has correct instructions. Run `setup_genie_space.py` to recreate. |
| Slow embedding training | Reduce H3 resolution (fewer cells) or reduce `max_epochs` in `embeddings.py`. |
| SQL warehouse timeout | Increase the timeout on your warehouse or use a larger warehouse size. |
| App deployment fails | Check the Logs tab in the Databricks Apps UI. Verify `DATABRICKS_WAREHOUSE_ID` and gold table env vars are set. |

---

## License

This solution accelerator is provided as-is for demonstration purposes.

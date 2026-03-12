# Site Selection Accelerator — Brand Site Matching

A Databricks solution accelerator that helps retail operations teams identify
**whitespace expansion opportunities** for their brand. Given a set of existing
store locations and a target city, the application builds a geospatial profile of
the brand, then scores every H3 hexagonal cell in the target city by similarity
to that profile — surfacing the areas that best match the brand's preferred
surroundings but don't yet have a presence.

Brand locations can be in **any city** — the tool learns what kind of
neighbourhoods a brand thrives in and finds similar areas in the target market,
enabling cross-city expansion analysis.

**Use cases:** franchise expansion, competitive gap analysis, new market entry.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                        Streamlit Application                         │
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  ┌───────────┐  │
│  │ Brand        │  │ Target city  │  │ H3         │  │ POI       │  │
│  │ locations    │  │ & country    │  │ resolution │  │ categories│  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬─────┘  └─────┬─────┘  │
│         └────────┬────────┴─────────────────┴───────────────┘        │
│                  ▼                                                    │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │         DBSQL Queries on Gold Tables (pipeline.py)           │    │
│  │                                                              │    │
│  │  1. City polygon from gold_cities    (pre-computed WKT)      │    │
│  │  2. H3 tessellation                  (h3_polyfillash3)       │    │
│  │  3. POI lookup from gold_places      (pre-flattened coords)  │    │
│  │  4. Cross-city brand neighbourhood   (for external brands)   │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                             ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │            SRAI Hex2Vec Embeddings (embeddings.py)           │    │
│  │                                                              │    │
│  │  • regions_gdf from H3 polygons                              │    │
│  │  • features_gdf with one-hot POI categories                  │    │
│  │  • joint_gdf from DBSQL H3 assignment                        │    │
│  │  • Hex2VecEmbedder.fit_transform()                           │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                             ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │         Cosine Similarity Scoring (similarity.py)            │    │
│  │                                                              │    │
│  │  • Average brand-cell embeddings → brand profile             │    │
│  │  • Cosine similarity vs all target-city cells                │    │
│  │  • Exclude existing locations, rank by score                 │    │
│  └──────────────────────────┬───────────────────────────────────┘    │
│                             ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │             pydeck Map Visualisation (map_viz.py)            │    │
│  │                                                              │    │
│  │  • CARTO basemap                                             │    │
│  │  • H3HexagonLayer — similarity heatmap                       │    │
│  │  • ScatterplotLayer — existing locations (blue)              │    │
│  │  • ScatterplotLayer — top opportunities (green)              │    │
│  └──────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────────┘
                              │
           ┌──────────────────┴──────────────────┐
           ▼                                     ▼
  ┌───────────────────────┐          ┌──────────────────────┐
  │  Gold Tables           │          │  Databricks SQL      │
  │  (Unity Catalog)       │          │  Warehouse           │
  │                        │          │                      │
  │  • gold_cities         │          │  H3 functions:       │
  │    (pre-joined polys,  │          │   h3_polyfillash3    │
  │     bboxes, WKT)       │          │   h3_longlatash3     │
  │  • gold_places         │          │   h3_centerasgeojson │
  │    (flattened coords,  │          │                      │
  │     categories)        │          │  Populated by ETL    │
  └───────────────────────┘          │  job from CARTO      │
           ▲                          │  Overture Maps       │
           │                          └──────────────────────┘
  ┌───────────────────────┐
  │  ETL Job               │
  │  (SQL tasks on DBSQL)  │
  │                        │
  │  CARTO Overture Maps   │
  │  → gold_cities         │
  │  → gold_places         │
  └───────────────────────┘
```

---

## Quick Start

### Prerequisites

| Requirement | Details |
|---|---|
| Databricks workspace | With access to a SQL Warehouse |
| Databricks CLI | v0.239.0+ (for Asset Bundle deployment) |
| CARTO Overture Maps catalogs | `carto_overture_maps_places`, `carto_overture_maps_divisions` mounted via Delta Sharing |
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
```

### 4. Deploy with Asset Bundles

```bash
# Validate the bundle
databricks bundle validate

# Deploy resources (app + ETL job)
databricks bundle deploy
```

### 5. Run the ETL job to populate gold tables

This creates the `gold_cities` and `gold_places` tables in your catalog/schema
by pre-processing the raw CARTO Overture Maps data. It runs as SQL tasks on
your SQL Warehouse and typically completes in under a minute.

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

A Databricks job with SQL tasks pre-processes the raw CARTO Overture Maps data
into two gold tables optimised for the app:

| Gold Table | Source | What it does |
|---|---|---|
| `gold_cities` | `division` + `division_area` | Joins city metadata with polygons, extracts WKT geometry, computes bounding boxes, generates fallback rectangular polygons for cities without geometry |
| `gold_places` | `place` | Extracts lon/lat from WKB geometry, flattens `categories.primary` and `addresses[0].freeform`, filters to supported POI categories |

This eliminates all JSON/array unnesting and WKB→geometry conversion at query
time, making the app significantly faster.

### Step 2 — User Input

The Streamlit sidebar collects:

- **Brand locations**: either as `lat, lon` pairs or street addresses
  (geocoded via Nominatim/geopy). These can be in **any city** — not just the
  target city.
- **H3 resolution** (7–10): controls hexagon granularity.
- **Target country and city**: cascading dropdowns populated from `gold_cities`.
- **POI categories**: multi-select grouped by theme (Food & Drink, Shopping,
  Services, Entertainment, Commercial).

### Step 3 — Cross-City Brand Profiling (`pipeline.py`)

When brand locations are outside the target city:

1. Each external location gets an H3 neighbourhood (center cell + k-ring=2).
2. POIs are fetched for those neighbourhoods from `gold_places`.
3. The neighbourhood cells and POIs are merged with the target city data.
4. Hex2Vec trains on the combined dataset, placing brand cells and target city
   cells in the same embedding space.
5. After scoring, only target city cells are shown as opportunities.

### Step 4 — SRAI Hex2Vec Embeddings (`embeddings.py`)

Using the [SRAI](https://kraina-ai.github.io/srai/) library:

1. Builds a `regions_gdf` of H3 cell polygons (via the `h3` Python library).
2. Builds a `features_gdf` of POI point geometries with one-hot encoded
   category columns.
3. Constructs a `joint_gdf` (region-feature mapping) directly from the DBSQL
   H3 assignment.
4. Trains a **Hex2VecEmbedder** (encoder sizes `[15, 10]`, 5 epochs, CPU)
   on the H3 neighbourhood graph to produce dense embeddings per cell.

> **Deep dive:** See [HEX2VEC_EXPLAINER.md](HEX2VEC_EXPLAINER.md) for a
> full explanation of how Hex2Vec works.

### Step 5 — Cosine Similarity (`similarity.py`)

1. Maps each brand location to its H3 cell at the chosen resolution.
2. Averages those cell embeddings to form a **brand profile vector**.
3. Computes cosine similarity between the brand profile and every target city
   cell's embedding.
4. Re-normalises scores to [0, 1] within the target city for colour contrast.
5. Excludes cells where the brand already has a location.
6. Ranks remaining cells descending by similarity — these are the
   **whitespace opportunities**.

### Step 6 — Map Visualisation (`map_viz.py`)

Rendered with [pydeck](https://deckgl.readthedocs.io/) on a CARTO Positron
basemap:

| Layer | Description |
|---|---|
| **H3HexagonLayer** | All candidate cells coloured by similarity score (red = high, blue = low) |
| **ScatterplotLayer (blue)** | Existing brand locations |
| **ScatterplotLayer (green)** | Top 20 recommended opportunity locations (cell centres) |

Hovering over any H3 cell shows a tooltip with the similarity percentage,
nearest address, and cell ID.

---

## Project Structure

```
site_selection_accelerator/
├── databricks.yml                    # Asset Bundle config (catalog, schema, warehouse)
├── README.md                         # This file
├── HEX2VEC_EXPLAINER.md             # Deep dive into the Hex2Vec algorithm
├── resources/
│   ├── site_selection_app.yml        # Databricks App resource definition
│   └── geospatial_etl_job.yml        # ETL job: SQL tasks to build gold tables
└── src/
    ├── app/
    │   ├── app.yaml                  # App runtime config (command, env vars)
    │   ├── requirements.txt          # Python dependencies
    │   ├── app.py                    # Streamlit UI + orchestration
    │   ├── config.py                 # Gold table references, categories, resolutions
    │   ├── db.py                     # DBSQL connection (cached, auto-reconnect)
    │   ├── pipeline.py               # DBSQL queries on gold tables + cross-city logic
    │   ├── embeddings.py             # SRAI Hex2Vec embedding pipeline
    │   ├── similarity.py             # Cosine similarity scoring
    │   └── map_viz.py                # pydeck map construction
    └── pipeline/
        └── transformations/
            ├── setup_schema.sql      # CREATE SCHEMA IF NOT EXISTS
            ├── gold_cities.sql       # CTAS: flattened cities + polygons + bboxes
            └── gold_places.sql       # CTAS: flattened POIs with extracted coords
```

---

## Key Libraries and Functions

### Databricks SQL Geospatial Functions

| Function | Purpose |
|---|---|
| `h3_polyfillash3(geog, res)` | Tessellate a polygon into H3 cells |
| `h3_longlatash3(lon, lat, res)` | Assign a point to its H3 cell |
| `h3_centerasgeojson(cell)` | Get the centre point of an H3 cell |

### Python Libraries

| Library | Purpose |
|---|---|
| `srai` (Hex2VecEmbedder) | Learned dense geospatial embeddings from POI tag patterns |
| `h3` | Client-side H3 cell ↔ polygon conversions, k-ring neighbourhoods |
| `geopandas` / `shapely` | GeoDataFrame construction for SRAI |
| `scikit-learn` | `cosine_similarity` for scoring |
| `pydeck` | Deck.gl map rendering in Streamlit |
| `geopy` | Optional address geocoding via Nominatim |
| `databricks-sql-connector` | DBSQL query execution |
| `databricks-sdk` | Workspace authentication (Config) |

---

## POI Categories (Overture Maps)

This accelerator uses the
[Overture Maps](https://overturemaps.org/) `categories.primary` taxonomy
rather than raw OSM `key=value` tags. The Overture categories are derived
from OpenStreetMap (among other sources) and provide a cleaner, normalised
classification. The mapping is roughly:

| OSM Tag | Overture Category |
|---|---|
| `amenity=restaurant` | `restaurant` |
| `amenity=fast_food` | `fast_food_restaurant` |
| `amenity=cafe` | `cafe`, `coffee_shop` |
| `amenity=bar` / `pub` | `bar` |
| `amenity=bank` | `bank` |
| `amenity=pharmacy` | `pharmacy` |
| `amenity=fuel` | `gas_station` |
| `shop=supermarket` | `grocery_store`, `supermarket` |
| `shop=clothes` | `clothing_store` |
| `shop=convenience` | `convenience_store` |

To add custom categories, edit the `CATEGORY_GROUPS` dictionary in
`src/app/config.py` and re-run the ETL job to include them in the gold table.

---

## Extending the Accelerator

- **Change catalog/schema** — update `databricks.yml` variables and
  `src/app/app.yaml` env vars (`GOLD_CATALOG`, `GOLD_SCHEMA`), then re-run
  the ETL job.
- **Add new POI categories** — edit `CATEGORY_GROUPS` in `config.py` and the
  `WHERE` clause in `src/pipeline/transformations/gold_places.sql`, then re-run
  the ETL job.
- **Refresh gold tables** — run `databricks bundle run geospatial_etl_job`
  whenever the upstream CARTO data updates. Add a cron schedule to the job
  resource for automatic refreshes.
- **Scale Hex2Vec training** — for large regions, offload training to a
  Databricks Job / notebook with GPU cluster. Save the model with
  `embedder.save()` and load it in the app with `Hex2VecEmbedder.load()`.
- **Alternative embedders** — swap `Hex2VecEmbedder` for SRAI's
  `CountEmbedder` (no training needed, faster) or
  `ContextualCountEmbedder` (neighbourhood-aware counts).
- **Brand detection** — the Overture places table has a `brand.names.primary`
  field. You can auto-detect existing brand locations by querying this field
  instead of requiring manual input.

---

## Troubleshooting

| Issue | Resolution |
|---|---|
| "None of the brand locations fall within the analysed H3 cells" | Brand-neighbourhood POI data may be too sparse. Try selecting more POI categories or a coarser H3 resolution. |
| "No POIs found" | The bounding box or category filter may be too restrictive. Broaden the category selection. |
| Gold tables don't exist | Run the ETL job first: `databricks bundle run geospatial_etl_job` |
| Slow embedding training | Reduce H3 resolution (fewer cells) or reduce `max_epochs` in `embeddings.py`. |
| SQL warehouse timeout | Increase the timeout on your warehouse or use a larger warehouse size. |
| App deployment fails | Check the Logs tab in the Databricks Apps UI. Verify `DATABRICKS_WAREHOUSE_ID` and gold table env vars are set. |

---

## License

This solution accelerator is provided as-is for demonstration purposes.

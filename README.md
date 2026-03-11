# Site Selection Accelerator — Brand Site Matching

A Databricks solution accelerator that helps retail operations teams identify
**whitespace expansion opportunities** for their brand. Given a set of existing
store locations and a target city, the application builds a geospatial profile of
the brand, then scores every H3 hexagonal cell in the target city by similarity
to that profile — surfacing the areas that best match the brand's preferred
surroundings but don't yet have a presence.

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
│  │            DBSQL Geospatial Queries (pipeline.py)            │    │
│  │                                                              │    │
│  │  1. Get city polygon      (division_area + ST_GeogFromWKB)   │    │
│  │  2. H3 tessellation       (h3_polyfillash3)                  │    │
│  │  3. POI extraction + H3   (h3_longlatash3, ST_X/ST_Y)       │    │
│  │  4. Count-vector aggregation                                 │    │
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
│  │  • Cosine similarity vs all other cells                      │    │
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
│  │  • Tooltips: similarity %, address, cell ID                  │    │
│  └──────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────────┘
                              │
           ┌──────────────────┴──────────────────┐
           ▼                                     ▼
  ┌─────────────────────┐           ┌──────────────────────┐
  │  CARTO Overture Maps │           │  Databricks SQL      │
  │  (Delta Sharing)     │           │  Warehouse           │
  │                      │           │                      │
  │  • places            │           │  H3 functions:       │
  │  • divisions         │           │   h3_polyfillash3    │
  │  • division_area     │           │   h3_longlatash3     │
  └─────────────────────┘           │   h3_centerasgeojson │
                                    │  ST functions:        │
                                    │   ST_GeogFromWKB     │
                                    │   ST_GeomFromWKB     │
                                    │   ST_X / ST_Y        │
                                    └──────────────────────┘
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

Make sure you have a working Databricks CLI profile. The Asset Bundle uses
the default profile unless overridden:

```bash
databricks auth login --host https://<workspace-url>
```

### 3. Set the SQL Warehouse

Edit `src/app/app.yaml` and set `DATABRICKS_WAREHOUSE_ID` to the ID of your
SQL Warehouse, or configure it as an app resource in the Databricks UI after
deployment.

### 4. Deploy with Asset Bundles

```bash
# Validate the bundle
databricks bundle validate

# Deploy to the dev target
databricks bundle deploy

# Start the application
databricks bundle run site_selection_app
```

### 5. Open the application

After deployment, the Databricks Apps UI will show the application URL.
Open it in your browser to start finding whitespace opportunities.

---

## How It Works

### Step 1 — User Input

The Streamlit sidebar collects:

- **Brand locations**: either as `lat, lon` pairs or street addresses
  (geocoded via Nominatim/geopy).
- **H3 resolution** (7-10): controls hexagon granularity.
- **Target country and city**: cascading dropdowns populated live from the
  Overture Maps divisions catalog.
- **POI categories**: multi-select grouped by theme (Food & Drink, Shopping,
  Services, Entertainment, Commercial).

### Step 2 — DBSQL Geospatial Pipeline (`pipeline.py`)

All spatial heavy-lifting runs server-side on the SQL Warehouse:

1. **City polygon retrieval** — joins `division_area` to `division` to get
   the city boundary as a `GEOGRAPHY` via `ST_GeogFromWKB()`.
2. **H3 tessellation** — fills the city polygon with hexagonal cells using
   `h3_polyfillash3(geog, resolution)`, then extracts centre coordinates
   with `h3_centerasgeojson()`.
3. **POI extraction** — queries `carto_overture_maps_places.carto.place`
   filtered by the city bounding box and selected categories, assigning
   each POI to its H3 cell via `h3_longlatash3(lon, lat, resolution)`.
4. **Count vectors** — pivots the POI data into a matrix of
   *H3 cell x category* counts.

### Step 3 — SRAI Hex2Vec Embeddings (`embeddings.py`)

Using the [SRAI](https://kraina-ai.github.io/srai/) library:

1. Builds a `regions_gdf` of H3 cell polygons (via the `h3` Python library).
2. Builds a `features_gdf` of POI point geometries with one-hot encoded
   category columns.
3. Constructs a `joint_gdf` (region-feature mapping) directly from the DBSQL
   H3 assignment — this replaces the usual `IntersectionJoiner` step.
4. Trains a **Hex2VecEmbedder** (encoder sizes `[15, 10]`, 5 epochs, CPU)
   on the H3 neighbourhood graph to produce dense embeddings per cell.

> **Deep dive:** See [HEX2VEC_EXPLAINER.md](HEX2VEC_EXPLAINER.md) for a
> full explanation of how Hex2Vec works — the training objective, neural
> architecture, how POI features become embeddings, and how those embeddings
> drive the similarity scores.

### Step 4 — Cosine Similarity (`similarity.py`)

1. Maps each brand location to its H3 cell at the chosen resolution.
2. Averages those cell embeddings to form a **brand profile vector**.
3. Computes cosine similarity between the brand profile and every other
   cell's embedding.
4. Excludes cells where the brand already has a location.
5. Ranks remaining cells descending by similarity — these are the
   **whitespace opportunities**.

### Step 5 — Map Visualisation (`map_viz.py`)

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
├── databricks.yml                # Asset Bundle configuration
├── README.md                     # This file
├── resources/
│   └── site_selection_app.yml    # Databricks App resource definition
└── src/
    └── app/
        ├── app.yaml              # App runtime config (command, env vars)
        ├── requirements.txt      # Python dependencies
        ├── app.py                # Streamlit UI + orchestration
        ├── config.py             # Constants: categories, tables, resolutions
        ├── db.py                 # DBSQL connection via databricks-sdk Config()
        ├── pipeline.py           # DBSQL geospatial query functions
        ├── embeddings.py         # SRAI Hex2Vec embedding pipeline
        ├── similarity.py         # Cosine similarity scoring
        └── map_viz.py            # pydeck map construction
```

---

## Key Libraries and Functions

### Databricks SQL Geospatial Functions

| Function | Purpose |
|---|---|
| `h3_polyfillash3(geog, res)` | Tessellate a polygon into H3 cells |
| `h3_longlatash3(lon, lat, res)` | Assign a point to its H3 cell |
| `h3_centerasgeojson(cell)` | Get the centre point of an H3 cell |
| `ST_GeogFromWKB(binary)` | Convert WKB to GEOGRAPHY |
| `ST_GeomFromWKB(binary)` | Convert WKB to GEOMETRY |
| `ST_X(geom)` / `ST_Y(geom)` | Extract longitude / latitude |
| `ST_XMin/XMax/YMin/YMax(geom)` | Bounding box extraction |

### Python Libraries

| Library | Purpose |
|---|---|
| `srai` (Hex2VecEmbedder) | Learned dense geospatial embeddings from POI tag patterns |
| `h3` | Client-side H3 cell ↔ polygon conversions |
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
`src/app/config.py`.

---

## Extending the Accelerator

- **Add new POI categories** — edit `CATEGORY_GROUPS` in `config.py`. Any
  value that exists in `carto_overture_maps_places.carto.place.categories.primary`
  can be used.
- **Multiple cities** — the pipeline functions accept a single city; loop
  over multiple cities and concatenate results before embedding.
- **Scale Hex2Vec training** — for large regions, offload training to a
  Databricks Job / notebook with GPU cluster. Save the model with
  `embedder.save()` and load it in the app with `Hex2VecEmbedder.load()`.
- **Alternative embedders** — swap `Hex2VecEmbedder` for SRAI's
  `CountEmbedder` (no training needed, faster) or
  `ContextualCountEmbedder` (neighbourhood-aware counts).
- **Brand detection** — the Overture places table has a `brand.names.primary`
  field. You can auto-detect existing brand locations by querying this field
  instead of requiring manual input.
- **Custom data sources** — replace the CARTO Overture Maps tables with your
  own POI data in Unity Catalog. Adjust table names in `config.py`.

---

## Troubleshooting

| Issue | Resolution |
|---|---|
| "No polygon found for city" | Verify the city name matches `names.primary` in the divisions table. Try a nearby larger city. |
| "No POIs found" | The bounding box or category filter may be too restrictive. Broaden the category selection. |
| "None of the brand locations fall within the analysed H3 cells" | Ensure your brand lat/lon coordinates are inside the selected city boundary. |
| Slow embedding training | Reduce H3 resolution (fewer cells) or reduce `max_epochs` in `embeddings.py`. |
| SQL warehouse timeout | Increase the `timeout` on your warehouse or use a larger warehouse size. |
| App deployment fails | Check logs: `databricks apps logs site-selection-dev`. Verify `DATABRICKS_WAREHOUSE_ID` is set. |

---

## License

This solution accelerator is provided as-is for demonstration purposes.

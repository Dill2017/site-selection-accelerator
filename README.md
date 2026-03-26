# Site Selection Accelerator

A Databricks solution accelerator that helps retail and operations teams
identify **whitespace expansion opportunities** for their brand. The app
discovers existing locations, builds a geospatial profile using **Hex2Vec**
embeddings from POI and building data, scores every H3 hexagonal cell in a
target city by neighbourhood similarity, and applies a competition penalty to
surface areas that best match the brand but are not yet saturated.

Everything the app produces is persisted so the end user walks away with a
complete set of assets: the **ETL pipeline**, the **pretrained model**, the
**gold tables**, the **analysis results in Delta**, and a **Genie Space** for
continued natural-language exploration.

**Use cases:** franchise expansion, competitive gap analysis, new market entry,
cross-market transfer learning.

### Core Capabilities

- **Brand search** — enter a brand name (e.g. "Starbucks") and Genie resolves
  all locations within the target city boundary using spatial SQL.
- **Address / coordinate disambiguation** — enter addresses or lat/lon pairs;
  the app geocodes them, resolves matching POIs, and presents a multi-select
  checklist so you can pick exactly which anchor POIs to use before analysis.
- **Cross-city / cross-country analysis** — source locations can be in a
  different city or country to the target market. The app infers the category
  fingerprint from the source locations and finds similar areas in the target.
- **Competition analysis** — detects competitor brands in high-similarity cells
  and applies a configurable penalty (β sensitivity) to surface true whitespace.

---

## What You Get

| Asset | Description |
|---|---|
| **Databricks App** | Interactive full-stack app (React + FastAPI) for running brand site analysis |
| **ETL Pipeline** | DABs job that builds gold tables from CARTO Overture Maps and trains the Hex2Vec model |
| **Gold Tables** | `gold_cities`, `gold_places`, `gold_buildings`, `gold_places_enriched` in Unity Catalog |
| **Analysis Tables** | `analyses`, `analysis_hexagons`, `analysis_brand_profiles`, `analysis_fingerprints`, `analysis_competitors` — every analysis run persisted to Delta |
| **Hex2Vec Model** | Multi-city pretrained geospatial embedding model stored in a UC Volume |
| **Genie Space** | Natural-language SQL exploration over both the base data and your analysis results |

After running an analysis and clicking **Save Analysis**, all results are
written to Delta tables keyed by a unique `analysis_id`. The **Assets** button
in the app links directly to the workspace, pipeline, tables, Genie Space, and
model.

---

## Quick Start

### Prerequisites

| Requirement | Details |
|---|---|
| Databricks workspace | With a SQL Warehouse (Serverless or Pro recommended) |
| Databricks CLI | v0.239.0+ |
| CARTO Overture Maps | `carto_overture_maps_places`, `carto_overture_maps_divisions`, `carto_overture_maps_buildings` available via Databricks Marketplace / Delta Sharing |
| APX CLI | [Install from GitHub](https://github.com/databricks-solutions/apx) — required for building and running the full-stack app |
| Python 3.11+ | With [uv](https://docs.astral.sh/uv/) installed |

### 1. Clone and install

```bash
git clone https://github.com/Dill2017/site-selection-accelerator.git
cd site-selection-accelerator
uv sync
```

### 2. Authenticate with Databricks

```bash
databricks auth login --host https://<your-workspace-url>
```

### 3. Configure your catalog, schema, and warehouse

Edit **`databricks.yml`** — set your target catalog, schema, and warehouse:

```yaml
variables:
  catalog:
    default: "my_catalog"          # <-- your Unity Catalog catalog
  schema:
    default: "site_selection"      # <-- your schema (will be created)
  warehouse_id:
    lookup:
      warehouse: "My SQL Warehouse"  # <-- your SQL warehouse name
```

Edit **`packages/app/app.yml`** — set the same catalog and schema as environment
variables for the running app:

```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql-warehouse
  - name: GOLD_CATALOG
    value: "my_catalog"            # <-- must match catalog above
  - name: GOLD_SCHEMA
    value: "site_selection"        # <-- must match schema above
  - name: GENIE_SPACE_ID
    value: ""                      # leave empty — auto-created by ETL
```

### 4. Deploy

```bash
uv run apx build
databricks bundle deploy
```

### 5. Run the ETL pipeline

This creates the schema, gold tables, analysis tables, Genie Space, and
pretrained Hex2Vec model — everything the app needs:

```bash
databricks bundle run geospatial_etl_job
```

The job takes ~10 minutes. You can monitor it from the Jobs UI link printed in
the terminal.

### 6. Launch the app

```bash
databricks bundle run site_selection_app
```

The app URL will appear in the Databricks Apps UI. Open it in your browser.

---

## Required Permissions

The deploying user and the app service principal need the following access:

| Resource | Permission | Why |
|---|---|---|
| **Catalog** (e.g. `my_catalog`) | `USE CATALOG`, `CREATE SCHEMA` | ETL creates the schema and tables |
| **Schema** (e.g. `my_catalog.site_selection`) | `USE SCHEMA`, `CREATE TABLE`, `SELECT`, `MODIFY` | Read gold tables, write analysis results |
| **SQL Warehouse** | `CAN_USE` | All queries and `ai_query()` LLM calls |
| **CARTO Overture Maps catalogs** | `SELECT` | ETL reads source data (`carto_overture_maps_places`, `_divisions`, `_buildings`) |
| **UC Volume** (`/Volumes/{catalog}/{schema}/models/`) | `READ FILES`, `WRITE FILES` | Store and load the pretrained Hex2Vec model |
| **Genie Space** | `CAN_RUN` | Auto-granted to the app service principal by the ETL |
| **Foundation Model endpoints** | Access via SQL warehouse | `ai_query()` calls for LLM-powered fingerprint insights and competition filtering |

The ETL job's `setup_genie_space` task automatically grants `CAN_RUN` on the
Genie Space to the app's service principal.

---

## Using the App

### Running an Analysis

1. **Select a target market** — choose country and city from the dropdowns.
2. **Choose your brand** — enter a brand name, lat/lon coordinates, street
   addresses, or draw locations on the map.
   - **Brand mode** — enter a brand name; Genie finds all matching locations.
   - **Address mode** — enter addresses; the app geocodes them and shows a POI
     checklist for disambiguation when multiple POIs share an address.
   - **Coordinate mode** — paste lat/lon pairs directly.
   - **Map drawing** — drop points or draw polygons on the map.
3. **Tune parameters** — adjust H3 resolution, POI categories, building
   features, and competition sensitivity (β).
4. **Click "Find Opportunities"** — the pipeline runs with live progress,
   then the map fills with scored hexagons.
5. **Explore** — hover hexagons for quick stats, click any hexagon for a
   detailed fingerprint with LLM-generated insight, view the brand profile.

### Saving Results to Delta

Click **Save Analysis** in the sidebar after an analysis completes. This
persists all outputs to five Delta tables, each keyed by a unique
`analysis_id`:

| Table | Contents |
|---|---|
| `analyses` | Registry of every run — brand, city, parameters, timestamp |
| `analysis_hexagons` | Every scored H3 cell with similarity, opportunity score, POI/competitor counts |
| `analysis_brand_profiles` | Average category distribution across brand locations |
| `analysis_fingerprints` | Per-hexagon category comparison with LLM insight (top 20 cells) |
| `analysis_competitors` | Competitor POIs found in high-similarity cells |

The Genie Space includes these tables, so you can ask questions like
*"Show me the top 10 hexagons by similarity for Starbucks in London"* in
natural language.

### Accessing All Assets

Click the **Assets** button (bottom-right corner) to open a dialog with direct
links to:

- Databricks Workspace
- Genie Space (Brand & Competition Explorer)
- Hex2Vec pretrained model (UC Volume)
- All gold and analysis Delta tables
- Recently saved analyses

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        Databricks App (apx)                              │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │  React + TypeScript Frontend (deck.gl, MapLibre, shadcn/ui)       │   │
│  │                                                                    │   │
│  │  OpportunityMap ─ ConfigSidebar ─ BrandProfileDialog               │   │
│  │  FingerprintPanel ─ AssetsPopover                                  │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                               │ /api/*                                    │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │  FastAPI Backend                                                   │   │
│  │                                                                    │   │
│  │  POST /analyze             ─ SSE pipeline (tessellate, embed, score)│   │
│  │  POST /resolve-addresses  ─ geocode + POI lookup for disambiguation│   │
│  │  GET  /results/{id}       ─ cached analysis for map rendering      │   │
│  │  POST /results/{id}/persist ─ save analysis to Delta tables        │   │
│  │  GET  /assets             ─ links to all workspace assets          │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                               │                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │  Python Modules                                                    │   │
│  │  pipeline ─ embeddings ─ similarity ─ brand_search                 │   │
│  │  explainability ─ persist ─ config ─ db                            │   │
│  └────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
                               │
          ┌────────────────────┼─────────────────────┐
          ▼                    ▼                      ▼
  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────┐
  │ Unity Catalog   │  │ Genie Space     │  │ SQL Warehouse    │
  │                 │  │                 │  │                  │
  │ Gold tables     │  │ NL → SQL over   │  │ H3 functions     │
  │ Analysis tables │  │ gold + analysis │  │ ai_query() LLM   │
  │ Hex2Vec model   │  │ tables          │  │                  │
  └────────────────┘  └─────────────────┘  └──────────────────┘
          ▲
  ┌────────────────┐
  │ ETL Job (DABs) │
  │                │
  │ CARTO Overture │
  │ → gold tables  │
  │ → analysis DDL │
  │ → Genie Space  │
  │ → Hex2Vec      │
  └────────────────┘
```

---

## Project Structure

```
site-selection-accelerator/
├── databricks.yml                        # Bundle config — catalog, schema, warehouse
├── pyproject.toml                        # Root uv workspace
├── resources/
│   ├── site_selection_app.yml            # Databricks App resource
│   └── geospatial_etl_job.yml            # ETL job (SQL + Python tasks)
├── packages/app/                         # Full-stack application (apx)
│   ├── app.yml                           # App runtime config (env vars)
│   ├── pyproject.toml                    # Python dependencies
│   ├── package.json                      # Frontend dependencies
│   └── src/site_selection/
│       ├── backend/
│       │   ├── app.py                    # FastAPI entry point
│       │   ├── router.py                 # API routes
│       │   ├── models.py                 # Pydantic models
│       │   └── cache.py                  # In-memory session cache
│       └── ui/
│           ├── routes/index.tsx          # Main page
│           ├── components/
│           │   ├── map/                  # deck.gl map layers
│           │   ├── sidebar/              # Config panel with Save Analysis
│           │   ├── brand-profile/        # Brand profile dialog
│           │   ├── fingerprint/          # Hexagon detail panel
│           │   └── assets/               # Assets dialog
│           └── lib/
│               ├── types.ts              # TypeScript interfaces
│               └── use-analyze.ts        # SSE analysis hook
└── src/
    ├── app/                              # Python modules
    │   ├── config.py                     # Table refs, categories, constants
    │   ├── db.py                         # DBSQL execution via SDK
    │   ├── pipeline.py                   # Geospatial queries
    │   ├── embeddings.py                 # SRAI Hex2Vec pipeline
    │   ├── similarity.py                 # Cosine similarity + opportunity scoring
    │   ├── brand_search.py               # Genie brand discovery + competition
    │   ├── explainability.py             # Score explanations + LLM insights
    │   └── persist.py                    # Delta table persistence
    └── pipeline/
        ├── setup_genie_space.py          # Genie Space provisioning
        ├── train_hex2vec.py              # Multi-city Hex2Vec training
        └── transformations/
            ├── setup_schema.sql
            ├── gold_cities.sql
            ├── gold_places.sql
            ├── gold_buildings.sql
            ├── gold_places_enriched.sql
            └── analysis_tables.sql       # Analysis result DDL
```

---

## How It Works

### ETL Pipeline (one-time setup)

The `geospatial_etl_job` runs these tasks in order:

1. **Create schema** — `CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}`
2. **Gold tables** — transform CARTO Overture Maps into optimised lookup tables
   (`gold_cities`, `gold_places`, `gold_buildings`, `gold_places_enriched`)
3. **Analysis tables** — create the five analysis result tables
4. **Genie Space** — provision a Genie Space with H3 spatial instructions,
   analysis table instructions, and example queries; grant the app service
   principal `CAN_RUN` access
5. **Hex2Vec training** — train an embedding model on 37 cities and save to a
   UC Volume

### Analysis Pipeline (per run)

1. **Brand resolution** — discover locations via Genie (brand name), geocoding
   with POI disambiguation (addresses), or direct input (coordinates / map
   drawing). Source locations can be outside the target city/country for
   cross-market analysis.
2. **Tessellation** — fill the target city polygon with H3 cells
3. **Feature assembly** — query POIs and buildings, merge into a unified feature
   table
4. **Hex2Vec embeddings** — transform features using the pretrained model (or
   train from scratch as fallback)
5. **Similarity scoring** — cosine similarity between brand profile and every
   city cell
6. **Competition scoring** — find competitor POIs in high-similarity cells and
   apply `similarity * (1 - β * competition_score)` to surface true whitespace.
7. **Persist (optional)** — save all results to Delta when the user clicks
   Save Analysis

---

## Local Development

```bash
uv sync
uv run apx bun install

# Create packages/app/.env:
#   DATABRICKS_CONFIG_PROFILE=DEFAULT
#   GOLD_CATALOG=my_catalog
#   GOLD_SCHEMA=site_selection
#   DATABRICKS_WAREHOUSE_ID=<your-warehouse-id>

uv run apx dev start    # backend + frontend + OpenAPI watcher
uv run apx dev status   # check health
uv run apx dev logs     # view logs
uv run apx dev check    # type checks (TypeScript + Python)
uv run apx dev stop     # stop servers
```

---

## Configuration Reference

All configuration is in two files:

| File | Setting | Purpose |
|---|---|---|
| `databricks.yml` | `variables.catalog` | Unity Catalog catalog for all tables |
| `databricks.yml` | `variables.schema` | Schema for all tables |
| `databricks.yml` | `variables.warehouse_id` | SQL warehouse (by name lookup) |
| `packages/app/app.yml` | `GOLD_CATALOG` env | Must match `variables.catalog` |
| `packages/app/app.yml` | `GOLD_SCHEMA` env | Must match `variables.schema` |
| `packages/app/app.yml` | `GENIE_SPACE_ID` env | Leave empty for auto-provisioning |

To change catalog/schema: update both files, then re-run `databricks bundle deploy`
and `databricks bundle run geospatial_etl_job`.

---

## Extending the Accelerator

- **Add POI categories** — edit `CATEGORY_GROUPS` in `src/app/config.py` and
  the `WHERE` clause in `gold_places.sql`, then re-run the ETL.
- **Add building categories** — edit `BUILDING_CATEGORY_GROUPS` in `config.py`
  and the logic in `gold_buildings.sql`.
- **Retrain Hex2Vec** — edit `HEX2VEC_TRAINING_CITIES` in `config.py`, then
  re-run the ETL. The app picks up the new model on next restart.
- **Custom Genie instructions** — edit `src/pipeline/setup_genie_space.py`.
- **Refresh data** — run `databricks bundle run geospatial_etl_job` whenever
  the upstream CARTO data updates.

---

## Troubleshooting

| Issue | Resolution |
|---|---|
| Gold tables don't exist | Run `databricks bundle run geospatial_etl_job` |
| Genie returns empty results | Verify `gold_places_enriched` exists; re-run `setup_genie_space.py` |
| "No POIs found" | Broaden category selection or use a coarser H3 resolution |
| Save Analysis fails | Check that analysis tables exist (run ETL, or the app creates them on first save) |
| Fingerprint shows fallback text | SQL warehouse may lack Foundation Model access; check backend logs |
| Dropdowns empty in deployed app | Verify `GOLD_CATALOG` and `GOLD_SCHEMA` in `app.yml` match your tables |
| `bundle deploy` skips build | Ensure `sync.include: [packages/app/.build]` is in `databricks.yml` |

---

## License

This solution accelerator is provided as-is for demonstration purposes.

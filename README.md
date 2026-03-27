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

## Prerequisites

Before deploying, ensure your environment meets these requirements:

### Workspace & Compute

| Requirement | Details |
|---|---|
| **Databricks workspace** | Any cloud (AWS, Azure, GCP) with **Unity Catalog** enabled |
| **SQL Warehouse** | Serverless or Pro recommended. The deploying user and the app service principal both need `CAN_USE` permission |
| **Foundation Models** | Access via the SQL warehouse for `ai_query()` LLM calls (fingerprint insights). Not required for core functionality |

### CARTO Overture Maps (Databricks Marketplace)

The ETL pipeline reads geospatial data from three CARTO catalogs that must be
installed from the **Databricks Marketplace** before running the job:

| Marketplace Listing | Expected Catalog Name |
|---|---|
| CARTO Overture Maps — Places | `carto_overture_maps_places` |
| CARTO Overture Maps — Divisions | `carto_overture_maps_divisions` |
| CARTO Overture Maps — Buildings | `carto_overture_maps_buildings` |

To install: **Workspace → Marketplace → search "CARTO Overture Maps" → Get**.

If your workspace uses different catalog names for these datasets, override the
`carto_*_catalog` variables in `databricks.yml`.

### Local Tools

| Tool | Version | Install |
|---|---|---|
| **Databricks CLI** | v0.239.0+ | [Install guide](https://docs.databricks.com/dev-tools/cli/install.html) |
| **APX CLI** | Latest | [GitHub](https://github.com/databricks-solutions/apx) |
| **Python** | 3.11+ | — |
| **uv** | Latest | [Install](https://docs.astral.sh/uv/) |
| **bun** | Latest | [Install](https://bun.sh) |

### Cloud-Specific: Node Type for Hex2Vec Training

The ETL job trains Hex2Vec on a single-node cluster. You **must** set
`node_type_id` in `databricks.yml` to a type available on your cloud:

| Cloud | Recommended `node_type_id` |
|---|---|
| **AWS** | `i3.xlarge` |
| **Azure** | `Standard_DS3_v2` |
| **GCP** | `n1-standard-4` |

---

## Configuration

All user-specific settings are in **two files** that must be kept in sync:

### 1. `databricks.yml` — Bundle Variables

```yaml
variables:
  catalog:
    default: "my_catalog"             # Your Unity Catalog catalog
  schema:
    default: "site_selection"         # Schema (created by ETL job)
  warehouse_id:
    lookup:
      warehouse: "My SQL Warehouse"   # Your SQL warehouse display name
  node_type_id:
    default: "i3.xlarge"              # Cloud-specific (see table above)
```

### 2. `packages/app/app.yml` — App Environment Variables

```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql-warehouse
  - name: GOLD_CATALOG
    value: "my_catalog"               # Must match catalog above
  - name: GOLD_SCHEMA
    value: "site_selection"           # Must match schema above
  - name: GENIE_SPACE_ID
    value: ""                         # Leave empty — auto-created by ETL
```

> **Important:** `GOLD_CATALOG` and `GOLD_SCHEMA` in `app.yml` must exactly
> match the `catalog` and `schema` variables in `databricks.yml`. If they
> don't match, the app will fail to find its tables at runtime.

### Optional: CARTO Catalog Overrides

If your CARTO Marketplace catalogs have non-default names:

```yaml
# In databricks.yml under variables:
  carto_divisions_catalog:
    default: "my_custom_carto_divisions"
  carto_places_catalog:
    default: "my_custom_carto_places"
  carto_buildings_catalog:
    default: "my_custom_carto_buildings"
```

---

## Deploy

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

### 3. Configure

Edit `databricks.yml` and `packages/app/app.yml` as described in the
[Configuration](#configuration) section above.

### 4. Build and deploy

```bash
uv run apx build
databricks bundle deploy
```

> **Note:** `apx build` requires PyPI access to download build dependencies
> (`hatchling`, `uv-dynamic-versioning`). If your environment blocks PyPI,
> see [Build in Restricted Environments](#build-in-restricted-environments).

### 5. Run the ETL pipeline

This creates the schema, gold tables, analysis tables, Genie Space, and
pretrained Hex2Vec model — everything the app needs:

```bash
databricks bundle run geospatial_etl_job
```

**What it does (in order):**

| Task | Duration | Description |
|---|---|---|
| `setup_schema` | ~5s | Creates `{catalog}.{schema}` if it doesn't exist |
| `create_gold_cities` | ~1 min | Builds city polygon lookup from CARTO Divisions |
| `create_gold_places` | ~2 min | Extracts POI coordinates and categories from CARTO Places |
| `create_gold_buildings` | ~3 min | Extracts building footprints from CARTO Buildings |
| `create_gold_places_enriched` | ~2 min | Full POI table with brands, addresses, coordinates |
| `create_analysis_tables` | ~5s | DDL for the five analysis result tables |
| `setup_genie_space` | ~30s | Creates/updates Genie Space, grants app SP access, writes config |
| `train_hex2vec` | ~5 min | Trains Hex2Vec embedding model on 37 cities |

Monitor the job from the Databricks Jobs UI (link printed in terminal).

### 6. Launch the app

```bash
databricks bundle run site_selection_app
```

Open the app URL from the Databricks Apps UI. On first load you should see:
- Country dropdown populated with country codes
- City dropdown populated after selecting a country
- All POI categories checked by default

If dropdowns are empty or you see errors, check [Troubleshooting](#troubleshooting).

---

## Required Permissions

### Deploying User

The user running `databricks bundle deploy` and `databricks bundle run` needs:

| Resource | Permission |
|---|---|
| Workspace | Ability to create apps, jobs |
| Target catalog | `USE CATALOG`, `CREATE SCHEMA` |
| CARTO catalogs | `USE CATALOG`, `USE SCHEMA`, `SELECT` |

### App Service Principal

The Databricks App runs under an automatically-created service principal. The
ETL job's `setup_genie_space` task automatically grants:

| Resource | Permission | Granted By |
|---|---|---|
| SQL Warehouse | `CAN_USE` | Bundle resource binding (`site_selection_app.yml`) |
| Genie Space | `CAN_RUN` | `setup_genie_space.py` |
| Target catalog | `USE CATALOG` | `setup_genie_space.py` |
| Target schema | `USE SCHEMA`, `SELECT`, `MODIFY` | `setup_genie_space.py` |

If the automatic grants fail (e.g. due to permission restrictions), you can
grant them manually. Find the app service principal name in the Databricks Apps
UI, then run:

```sql
-- Replace <sp_name> with the app's service principal display name
-- Replace <catalog>.<schema> with your values

GRANT USE CATALOG ON CATALOG `<catalog>` TO `<sp_name>`;
GRANT USE SCHEMA ON SCHEMA `<catalog>`.`<schema>` TO `<sp_name>`;
GRANT SELECT ON SCHEMA `<catalog>`.`<schema>` TO `<sp_name>`;
GRANT MODIFY ON SCHEMA `<catalog>`.`<schema>` TO `<sp_name>`;
```

For the SQL warehouse, use the Databricks CLI:

```bash
# Find the warehouse ID
databricks warehouses list

# Grant CAN_USE (replace <warehouse_id> and <sp_client_id>)
databricks warehouses update-permissions <warehouse_id> \
  --json '{"access_control_list": [{"service_principal_name": "<sp_client_id>", "permission_level": "CAN_USE"}]}'
```

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
   principal `CAN_RUN` access and `SELECT`/`MODIFY` on the schema
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
| `databricks.yml` | `variables.node_type_id` | Cloud-specific instance type for Hex2Vec training |
| `databricks.yml` | `variables.carto_*_catalog` | Override CARTO Marketplace catalog names |
| `packages/app/app.yml` | `GOLD_CATALOG` env | Must match `variables.catalog` |
| `packages/app/app.yml` | `GOLD_SCHEMA` env | Must match `variables.schema` |
| `packages/app/app.yml` | `GENIE_SPACE_ID` env | Leave empty for auto-provisioning |

To change catalog/schema: update both files, then re-run `databricks bundle deploy`
and `databricks bundle run geospatial_etl_job`.

---

## Build in Restricted Environments

The `apx build` command uses `hatchling` and `uv-dynamic-versioning` as Python
build-system dependencies, which are fetched from PyPI at build time.

If your environment **blocks PyPI access** (corporate firewall, air-gapped
network), the build will fail with a connection error. Options:

1. **Pre-install build deps** — run `uv pip install hatchling uv-dynamic-versioning`
   before building, then set `UV_NO_BUILD_ISOLATION=1`:
   ```bash
   UV_NO_BUILD_ISOLATION=1 uv run apx build
   ```

2. **Use a PyPI mirror** — set `UV_INDEX_URL` to your internal mirror:
   ```bash
   UV_INDEX_URL=https://pypi.internal.example.com/simple uv run apx build
   ```

3. **Build on a machine with internet** — run `uv run apx build` on a connected
   machine, then copy `packages/app/.build/` to the restricted environment
   before running `databricks bundle deploy`.

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

### Deployment Errors

| Symptom | Cause | Fix |
|---|---|---|
| `apx build` fails with "connection refused" or PyPI timeout | PyPI access blocked | See [Build in Restricted Environments](#build-in-restricted-environments) |
| `bundle deploy` fails with "warehouse not found" | `warehouse_id.lookup.warehouse` name doesn't match | Run `databricks warehouses list` and use the exact display name |
| `bundle deploy` fails with empty `node_type_id` | Variable not set | Add your cloud-specific node type to `databricks.yml` (see [Prerequisites](#cloud-specific-node-type-for-hex2vec-training)) |

### ETL Job Errors

| Symptom | Cause | Fix |
|---|---|---|
| SQL tasks fail with "table not found" | CARTO catalogs not installed | Install from Marketplace (see [Prerequisites](#carto-overture-maps-databricks-marketplace)) |
| `train_hex2vec` fails to provision cluster | Wrong `node_type_id` for your cloud | Update `databricks.yml` with the correct instance type |
| `setup_genie_space` fails with permission error | Insufficient privileges | Ensure deploying user has `USE CATALOG` on target catalog |

### App Runtime Errors

| Symptom | Cause | Fix |
|---|---|---|
| Empty country/city dropdowns | Gold tables don't exist | Run `databricks bundle run geospatial_etl_job` |
| `Countries API: 500` | App can't connect to SQL warehouse | Check that `DATABRICKS_WAREHOUSE_ID` is set in `app.yml` via the `sql-warehouse` resource binding |
| `500 — You do not have permission to use the SQL Warehouse` | App service principal lacks `CAN_USE` | Grant manually — see [Required Permissions](#app-service-principal) |
| `500 — GOLD_CATALOG is not configured` | `GOLD_CATALOG` in `app.yml` still set to `CHANGE_ME` | Update `app.yml` with your actual catalog name, redeploy |
| `500 — Table not found` | Catalog/schema mismatch between `app.yml` and `databricks.yml` | Ensure `GOLD_CATALOG` and `GOLD_SCHEMA` in `app.yml` match the bundle variables |
| Save Analysis fails with "Failed to create analysis tables" | App SP can't create tables | Grant `MODIFY` on the schema to the app SP |
| "No POIs found" | No data for selected categories in that city | Broaden category selection or use a coarser H3 resolution |
| Fingerprint shows fallback text | SQL warehouse may lack Foundation Model access | Check backend logs; `ai_query()` requires a model serving endpoint |
| App crashes immediately after deploy | `__version__` import error in wheel | Rebuild with `uv run apx build` (ensures correct `__init__.py`) |

---

## License

This solution accelerator is provided as-is for demonstration purposes.

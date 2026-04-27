#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Site Selection Accelerator — Interactive Configuration
#
# This script updates both databricks.yml and packages/app/app.yml so they
# stay in sync.  Run it once before your first deploy.
# ---------------------------------------------------------------------------

BUNDLE_FILE="databricks.yml"
APP_YML="packages/app/app.yml"

if [[ ! -f "$BUNDLE_FILE" ]]; then
  echo "ERROR: $BUNDLE_FILE not found.  Run this script from the repository root."
  exit 1
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   Site Selection Accelerator — Configuration                ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Workspace ─────────────────────────────────────────────────────────────────
echo "Enter the full URL of your Databricks workspace."
echo "  e.g. https://my-workspace.cloud.databricks.com"
echo "  e.g. https://adb-1234567890.11.azuredatabricks.net"
echo ""
read -rp "Workspace URL: " WORKSPACE_HOST
if [[ -z "$WORKSPACE_HOST" ]]; then
  echo "ERROR: workspace URL is required."
  exit 1
fi
WORKSPACE_HOST="${WORKSPACE_HOST%/}"

echo ""

# ── Catalog ──────────────────────────────────────────────────────────────────
read -rp "Unity Catalog catalog name (e.g. my_catalog): " CATALOG
if [[ -z "$CATALOG" ]]; then
  echo "ERROR: catalog name is required."
  exit 1
fi

# ── Schema ───────────────────────────────────────────────────────────────────
read -rp "Schema name [geospatial]: " SCHEMA
SCHEMA="${SCHEMA:-geospatial}"

# ── Warehouse ────────────────────────────────────────────────────────────────
echo ""
echo "Enter the DISPLAY NAME of your SQL warehouse exactly as shown in"
echo "the Databricks UI (Compute → SQL Warehouses)."
echo "Run 'databricks warehouses list' to see all warehouse names."
echo ""
read -rp "SQL warehouse display name: " WAREHOUSE
if [[ -z "$WAREHOUSE" ]]; then
  echo "ERROR: warehouse name is required."
  exit 1
fi

# ── Node type (cloud-specific) ───────────────────────────────────────────────
echo ""
echo "Pick the instance type for the Hex2Vec training cluster:"
echo "  AWS:   i3.xlarge"
echo "  Azure: Standard_DS3_v2"
echo "  GCP:   n1-standard-4"
echo ""
read -rp "node_type_id [Standard_DS3_v2]: " NODE_TYPE
NODE_TYPE="${NODE_TYPE:-Standard_DS3_v2}"

# ── CARTO Marketplace Data ────────────────────────────────────────────────────
echo ""
echo "This accelerator requires three datasets from the Databricks Marketplace:"
echo "  • CARTO Overture Maps - Buildings"
echo "  • CARTO Overture Maps - Places"
echo "  • CARTO Overture Maps - Divisions"
echo ""
echo "Install them before running the ETL job."
echo "See: https://marketplace.databricks.com (search \"CARTO Overture\")"
echo ""
read -rp "Did you install all three into your main catalog ($CATALOG)? [Y/n] " CARTO_SAME
if [[ "$(printf '%s' "$CARTO_SAME" | tr '[:upper:]' '[:lower:]')" == "n" ]]; then
  echo ""
  read -rp "CARTO Buildings catalog  [carto_overture_maps_buildings]: " CARTO_BUILDINGS
  CARTO_BUILDINGS="${CARTO_BUILDINGS:-carto_overture_maps_buildings}"
  read -rp "CARTO Places catalog     [carto_overture_maps_places]: " CARTO_PLACES
  CARTO_PLACES="${CARTO_PLACES:-carto_overture_maps_places}"
  read -rp "CARTO Divisions catalog  [carto_overture_maps_divisions]: " CARTO_DIVISIONS
  CARTO_DIVISIONS="${CARTO_DIVISIONS:-carto_overture_maps_divisions}"
else
  CARTO_BUILDINGS="$CATALOG"
  CARTO_PLACES="$CATALOG"
  CARTO_DIVISIONS="$CATALOG"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Workspace:            $WORKSPACE_HOST"
echo "  Catalog:              $CATALOG"
echo "  Schema:               $SCHEMA"
echo "  Warehouse:            $WAREHOUSE"
echo "  Node type:            $NODE_TYPE"
echo "  CARTO Buildings:      $CARTO_BUILDINGS"
echo "  CARTO Places:         $CARTO_PLACES"
echo "  CARTO Divisions:      $CARTO_DIVISIONS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
read -rp "Apply these settings? [Y/n] " CONFIRM
if [[ "$(printf '%s' "$CONFIRM" | tr '[:upper:]' '[:lower:]')" == "n" ]]; then
  echo "Aborted."
  exit 0
fi

# ── Portable sed -i (works on macOS and Linux) ───────────────────────────────
_sed_i() {
  if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' "$@"
  else
    sed -i "$@"
  fi
}

# ── Update databricks.yml ────────────────────────────────────────────────────
echo "Updating $BUNDLE_FILE …"

# Use Python for reliable YAML-aware replacement
python3 - "$BUNDLE_FILE" "$CATALOG" "$SCHEMA" "$WAREHOUSE" "$NODE_TYPE" \
        "$CARTO_BUILDINGS" "$CARTO_PLACES" "$CARTO_DIVISIONS" \
        "$WORKSPACE_HOST" <<'PYEOF'
import sys, re

path, catalog, schema, warehouse, node_type, \
    carto_buildings, carto_places, carto_divisions, \
    workspace_host = sys.argv[1:10]

with open(path) as f:
    content = f.read()

def replace_default(text, var_name, new_val):
    """Replace the default: value under a variables.X block."""
    pattern = rf'({var_name}:\s*\n(?:.*\n)*?\s*default:\s*)"[^"]*"'
    return re.sub(pattern, rf'\1"{new_val}"', text)

def replace_warehouse_lookup(text, new_name):
    pattern = r'(warehouse:\s*)"[^"]*"'
    return re.sub(pattern, rf'\1"{new_name}"', text, count=1)

def replace_workspace_host(text, new_host):
    pattern = r'(host:\s*)"[^"]*"'
    return re.sub(pattern, rf'\1"{new_host}"', text)

content = replace_default(content, "catalog", catalog)
content = replace_default(content, "schema", schema)
content = replace_default(content, "node_type_id", node_type)
content = replace_default(content, "carto_divisions_catalog", carto_divisions)
content = replace_default(content, "carto_places_catalog", carto_places)
content = replace_default(content, "carto_buildings_catalog", carto_buildings)
content = replace_warehouse_lookup(content, warehouse)
content = replace_workspace_host(content, workspace_host)

with open(path, "w") as f:
    f.write(content)
PYEOF

# ── Update packages/app/app.yml ─────────────────────────────────────────────
echo "Updating $APP_YML …"

python3 - "$APP_YML" "$CATALOG" "$SCHEMA" <<'PYEOF'
import sys, re

path, catalog, schema = sys.argv[1:4]

with open(path) as f:
    lines = f.readlines()

out = []
for line in lines:
    if "GOLD_CATALOG" in "".join(out[-3:]) if len(out) >= 3 else False:
        pass
    out.append(line)

with open(path) as f:
    content = f.read()

def replace_env_value(text, env_name, new_val):
    pattern = rf'(- name: {env_name}\s*\n\s*value:\s*)"[^"]*"'
    return re.sub(pattern, rf'\1"{new_val}"', text)

content = replace_env_value(content, "GOLD_CATALOG", catalog)
content = replace_env_value(content, "GOLD_SCHEMA", schema)

with open(path, "w") as f:
    f.write(content)
PYEOF

echo ""
echo "Done! Both config files are now in sync."
echo ""
echo "Next steps:"
echo "  1. Authenticate:   databricks auth login --host $WORKSPACE_HOST"
echo "  2. Build:          uv run apx build"
echo "  3. Deploy:         databricks bundle deploy"
echo "  4. Run ETL:        databricks bundle run geospatial_etl_job"
echo "  5. Launch app:     databricks bundle run site_selection_app"
echo ""

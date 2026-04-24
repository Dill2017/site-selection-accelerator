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

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Catalog:       $CATALOG"
echo "  Schema:        $SCHEMA"
echo "  Warehouse:     $WAREHOUSE"
echo "  Node type:     $NODE_TYPE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
read -rp "Apply these settings? [Y/n] " CONFIRM
if [[ "${CONFIRM,,}" == "n" ]]; then
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
python3 - "$BUNDLE_FILE" "$CATALOG" "$SCHEMA" "$WAREHOUSE" "$NODE_TYPE" <<'PYEOF'
import sys, re

path, catalog, schema, warehouse, node_type = sys.argv[1:6]

with open(path) as f:
    content = f.read()

def replace_default(text, var_name, new_val):
    """Replace the default: value under a variables.X block."""
    pattern = rf'({var_name}:\s*\n(?:.*\n)*?\s*default:\s*)"[^"]*"'
    return re.sub(pattern, rf'\1"{new_val}"', text)

def replace_warehouse_lookup(text, new_name):
    pattern = r'(warehouse:\s*)"[^"]*"'
    return re.sub(pattern, rf'\1"{new_name}"', text, count=1)

content = replace_default(content, "catalog", catalog)
content = replace_default(content, "schema", schema)
content = replace_default(content, "node_type_id", node_type)
content = replace_warehouse_lookup(content, warehouse)

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
echo "  1. Authenticate:   databricks auth login --host https://<your-workspace>"
echo "  2. Build:          uv run apx build"
echo "  3. Deploy:         databricks bundle deploy"
echo "  4. Run ETL:        databricks bundle run geospatial_etl_job"
echo "  5. Launch app:     databricks bundle run site_selection_app"
echo ""

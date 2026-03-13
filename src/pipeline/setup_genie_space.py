"""Provision (or locate) the Genie Space for the Site Selection app.

Runs as a DABs job task after the gold tables are created.
If a Genie Space with the expected name already exists, its ID is
reused and its instructions are updated; otherwise a new one is created.

The space_id is written to a small key-value table so the app can
read it at startup without hard-coding IDs.

Uses the REST API for space CRUD (create/update) since the SDK's
GenieAPI only covers conversations, not space management.

End users can also run this script directly:
    python setup_genie_space.py <catalog> <schema> <warehouse_id>
Or with env vars (loads from .env):
    python setup_genie_space.py
"""

import json
import logging
import uuid

from databricks.sdk import WorkspaceClient

log = logging.getLogger(__name__)

GENIE_DISPLAY_NAME = "Site Selection - Brand & Competition Explorer"

GENIE_DESCRIPTION = (
    "Explore POI data for brand site selection and competitive analysis. "
    "Uses H3 polygon fill (h3_polyfillash3) with city boundaries from "
    "gold_cities for fast spatial filtering."
)


def _build_serialized_space(catalog: str, schema: str) -> str:
    """Build the full serialized_space JSON with tables, instructions,
    and sample question-SQL pairs."""
    enriched_table = f"{catalog}.{schema}.gold_places_enriched"
    cities_table = f"{catalog}.{schema}.gold_cities"

    return json.dumps({
        "version": 2,
        "data_sources": {
            "tables": sorted([
                {"identifier": cities_table},
                {"identifier": enriched_table},
            ], key=lambda t: t["identifier"]),
        },
        "config": {
            "sample_questions": [
                {
                    "id": uuid.uuid4().hex[:32],
                    "question": [
                        "Find all Starbucks locations within the city "
                        "boundary of London, GB"
                    ],
                },
                {
                    "id": uuid.uuid4().hex[:32],
                    "question": [
                        "Count businesses by basic_category within the "
                        "London, GB city polygon"
                    ],
                },
                {
                    "id": uuid.uuid4().hex[:32],
                    "question": [
                        "List distinct brand_name_primary for hotels "
                        "within the Manchester, GB city polygon"
                    ],
                },
            ],
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": uuid.uuid4().hex[:32],
                    "content": [
                        "CITY/LOCATION FILTERING — use H3 polygon fill "
                        "for fast spatial queries:\n",
                        "\n",
                        "Step 1: Build the set of H3 cells covering the "
                        "city polygon (CTE). Filter gold_cities by both "
                        "country AND city_name to pick the right polygon.\n",
                        "Step 2: JOIN gold_places_enriched to gold_cities "
                        "on TRIM(p.country) = TRIM(c.country) AND "
                        "c.city_name = '{city}'.\n",
                        "Step 3: The H3 cell membership check MUST come "
                        "first as the primary WHERE predicate — this is "
                        "the fast filter.\n",
                        "\n",
                        "Pattern (replace {city}, {country}, {resolution}):\n",
                        "  WITH city_h3 AS (\n",
                        "      SELECT explode(h3_polyfillash3(\n",
                        "          c.geom_wkt, {resolution}\n",
                        "      )) AS h3_cell\n",
                        f"      FROM {cities_table} c\n",
                        "      WHERE c.country = '{country}'\n",
                        "        AND c.city_name = '{city}'\n",
                        "  )\n",
                        f"  SELECT p.*, h3_h3tostring(\n",
                        "      h3_longlatash3(p.lon, p.lat, {resolution})\n",
                        "  ) AS h3_cell\n",
                        f"  FROM {enriched_table} p\n",
                        "  WHERE h3_longlatash3(p.lon, p.lat, {resolution})\n",
                        "        IN (SELECT h3_cell FROM city_h3)\n",
                        "    AND p.lon IS NOT NULL AND p.lat IS NOT NULL\n",
                        "\n",
                        "NEVER use ST_CONTAINS to filter individual POIs "
                        "row-by-row — it is too slow.\n",
                        "NEVER filter by the locality text column — it is "
                        "unreliable (e.g. London POIs may have locality = "
                        "'Westminster', 'Camden', etc.)\n",
                        "\n",
                        "H3 OUTPUT: always return h3 as hex string via "
                        "h3_h3tostring(h3_longlatash3(lon, lat, "
                        "{resolution})) AS h3_cell\n",
                        "\n",
                        "BRAND MATCHING: match on brand_name_primary "
                        "(ILIKE) first, fall back to poi_primary_name "
                        "ILIKE. Use OR:\n",
                        "  (p.brand_name_primary ILIKE '%Brand%' "
                        "OR p.poi_primary_name ILIKE '%Brand%')\n",
                    ],
                },
            ],
            "example_question_sqls": [
                {
                    "id": uuid.uuid4().hex[:32],
                    "question": [
                        "Find all Starbucks locations within the city "
                        "boundary of London, GB. Return poi_id, "
                        "poi_primary_name, basic_category, "
                        "brand_name_primary, lon, lat, h3_cell"
                    ],
                    "sql": [
                        "WITH city_h3 AS (\n",
                        "    SELECT explode(h3_polyfillash3(\n",
                        "        geom_wkt, 9\n",
                        "    )) AS h3_cell\n",
                        f"    FROM {cities_table}\n",
                        "    WHERE country = 'GB'\n",
                        "      AND city_name = 'London'\n",
                        ")\n",
                        "SELECT p.poi_id, p.poi_primary_name, "
                        "p.basic_category, p.brand_name_primary, "
                        "p.lon, p.lat,\n",
                        "       h3_h3tostring(h3_longlatash3(p.lon, "
                        "p.lat, 9)) AS h3_cell\n",
                        f"FROM {enriched_table} p\n",
                        "WHERE h3_longlatash3(p.lon, p.lat, 9) "
                        "IN (SELECT h3_cell FROM city_h3)\n",
                        "  AND (p.brand_name_primary ILIKE '%Starbucks%' "
                        "OR p.poi_primary_name ILIKE '%Starbucks%')\n",
                        "  AND p.lon IS NOT NULL AND p.lat IS NOT NULL\n",
                    ],
                },
            ],
        },
    })


def _api(w: WorkspaceClient, method: str, path: str, body: dict | None = None):
    """Thin wrapper around the SDK's API client for REST calls."""
    resp = w.api_client.do(method, path, body=body)
    if isinstance(resp, bytes):
        return json.loads(resp)
    return resp


def _find_existing_space(w: WorkspaceClient) -> str | None:
    """Return the space_id of an existing Genie Space matching our name."""
    try:
        resp = _api(w, "GET", "/api/2.0/genie/spaces")
        for space in resp.get("spaces", []):
            if space.get("title") == GENIE_DISPLAY_NAME:
                sid = space.get("space_id") or space.get("id")
                log.info("Found existing Genie Space: %s", sid)
                return sid
    except Exception as e:
        log.warning("Could not list Genie Spaces: %s", e)
    return None


def _create_space(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    warehouse_id: str,
) -> str:
    """Create a new Genie Space via REST API and return its ID."""
    serialized = _build_serialized_space(catalog, schema)

    resp = _api(w, "POST", "/api/2.0/genie/spaces", {
        "warehouse_id": warehouse_id,
        "serialized_space": serialized,
        "title": GENIE_DISPLAY_NAME,
        "description": GENIE_DESCRIPTION,
    })
    space_id = resp.get("space_id") or resp.get("id")
    log.info("Created Genie Space: %s", space_id)
    return space_id


def _update_space_instructions(
    w: WorkspaceClient,
    space_id: str,
    catalog: str,
    schema: str,
) -> None:
    """Update an existing Genie Space with the latest instructions."""
    serialized = _build_serialized_space(catalog, schema)
    try:
        _api(w, "PATCH", f"/api/2.0/genie/spaces/{space_id}", {
            "serialized_space": serialized,
        })
        log.info("Updated instructions for Genie Space: %s", space_id)
    except Exception as e:
        log.warning("Could not update space instructions: %s", e)


def _persist_space_id(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    space_id: str,
    warehouse_id: str,
) -> None:
    """Write the space_id into a small config table so the app can read it."""
    from databricks.sdk.service.sql import StatementState

    table = f"{catalog}.{schema}.app_config"
    statements = [
        f"""CREATE TABLE IF NOT EXISTS {table} (
            config_key STRING NOT NULL,
            config_value STRING NOT NULL
        ) USING DELTA""",
        f"""MERGE INTO {table} t
        USING (SELECT 'GENIE_SPACE_ID' AS config_key,
                      '{space_id}' AS config_value) s
        ON t.config_key = s.config_key
        WHEN MATCHED THEN UPDATE SET config_value = s.config_value
        WHEN NOT MATCHED THEN INSERT (config_key, config_value)
            VALUES (s.config_key, s.config_value)""",
    ]
    for stmt in statements:
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            statement=stmt,
        )
        if resp.status and resp.status.state == StatementState.FAILED:
            raise RuntimeError(
                f"SQL failed: {resp.status.error and resp.status.error.message}"
            )

    log.info("Persisted GENIE_SPACE_ID=%s to %s", space_id, table)


def _grant_app_sp_access(w: WorkspaceClient, space_id: str, app_name: str = "site-selection-accelerator") -> None:
    """Grant the Databricks App's service principal CAN_RUN on the Genie Space."""
    try:
        app = w.apps.get(app_name)
        sp_client_id = app.service_principal_client_id
        if not sp_client_id:
            log.warning("App %s has no service_principal_client_id", app_name)
            return

        _api(w, "PATCH", f"/api/2.0/permissions/genie/{space_id}", {
            "access_control_list": [
                {
                    "service_principal_name": sp_client_id,
                    "permission_level": "CAN_RUN",
                }
            ],
        })
        log.info("Granted CAN_RUN on Genie Space %s to SP %s", space_id, sp_client_id)
    except Exception as e:
        log.warning("Could not grant SP access to Genie Space: %s", e)


def main(catalog: str, schema: str, warehouse_id: str) -> str:
    """Ensure the Genie Space exists, update instructions, and return its ID."""
    w = WorkspaceClient()

    space_id = _find_existing_space(w)
    if space_id:
        _update_space_instructions(w, space_id, catalog, schema)
    else:
        space_id = _create_space(w, catalog, schema, warehouse_id)

    _grant_app_sp_access(w, space_id)
    _persist_space_id(w, catalog, schema, space_id, warehouse_id)
    return space_id


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    try:
        from databricks.sdk.runtime import dbutils  # type: ignore[import]
        catalog = dbutils.widgets.get("catalog")
        schema = dbutils.widgets.get("schema")
        warehouse_id = dbutils.widgets.get("warehouse_id")
    except Exception:
        if len(sys.argv) >= 4:
            catalog, schema, warehouse_id = sys.argv[1], sys.argv[2], sys.argv[3]
        else:
            import os
            from dotenv import load_dotenv
            load_dotenv()
            catalog = os.getenv("GOLD_CATALOG", "dilshad_shawki")
            schema = os.getenv("GOLD_SCHEMA", "geospatial")
            warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

    sid = main(catalog, schema, warehouse_id)
    print(f"GENIE_SPACE_ID={sid}")

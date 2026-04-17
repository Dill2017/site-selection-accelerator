"""Radiance orchestration for the app runtime.

Provides three capabilities:
  1. get_radiance_for_city()  — fast SQL lookup from gold_radiance
  2. submit_radiance_job()    — trigger a serverless job for a cache-miss city
  3. check_radiance_job()     — poll whether the job has finished

The heavy raster computation (rasterio + h3ronpy) runs exclusively on the
serverless job cluster — this module never touches the VIIRS GeoTIFF directly.
"""

from __future__ import annotations

import logging

import pandas as pd
from databricks.sdk.service.sql import StatementParameterListItem

from config import GOLD_RADIANCE_TABLE, RADIANCE_JOB_ID
from db import _get_client, execute_query

log = logging.getLogger(__name__)


def get_radiance_for_city(
    country: str,
    city: str,
    resolution: int = 9,
) -> pd.DataFrame | None:
    """Read precomputed radiance from gold_radiance.  Returns None on miss."""
    if resolution != 9:
        return None

    try:
        df = execute_query(
            f"""
            SELECT h3_cell, radiance
            FROM {GOLD_RADIANCE_TABLE}
            WHERE country = :country AND city_name = :city
            """,
            params=[
                StatementParameterListItem(name="country", value=country),
                StatementParameterListItem(name="city", value=city),
            ],
        )
        if not df.empty:
            log.info("Radiance cache hit: %d cells for %s, %s", len(df), city, country)
            return df
    except Exception as e:
        log.debug("gold_radiance query failed: %s", e)

    return None


def submit_radiance_job(
    country: str,
    city: str,
    resolution: int = 9,
) -> int | None:
    """Submit a serverless run of the on-demand radiance job.

    Returns the run_id, or None if the job could not be triggered.
    """
    if not RADIANCE_JOB_ID:
        log.debug("RADIANCE_JOB_ID not configured — skipping submission")
        return None

    client = _get_client()
    try:
        run = client.jobs.run_now(
            job_id=int(RADIANCE_JOB_ID),
            job_parameters={
                "country": country,
                "city": city,
                "resolution": str(resolution),
            },
        )
        log.info(
            "Submitted radiance job run_id=%s for %s, %s (res %d)",
            run.run_id, city, country, resolution,
        )
        return run.run_id
    except Exception as e:
        log.warning("Failed to submit radiance job: %s", e)
        return None


def check_radiance_job(run_id: int) -> str:
    """Poll the status of a radiance job run.

    Returns one of: "COMPLETED", "RUNNING", "FAILED".
    """
    client = _get_client()
    try:
        run = client.jobs.get_run(run_id)
        state = run.state
        if state and state.life_cycle_state:
            lcs = state.life_cycle_state.value
            if lcs == "TERMINATED":
                result = state.result_state.value if state.result_state else "UNKNOWN"
                return "COMPLETED" if result == "SUCCESS" else "FAILED"
            if lcs in ("INTERNAL_ERROR", "SKIPPED"):
                return "FAILED"
            return "RUNNING"
    except Exception as e:
        log.warning("Could not check radiance run %s: %s", run_id, e)

    return "FAILED"

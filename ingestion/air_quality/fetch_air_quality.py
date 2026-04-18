"""
Air-quality ingestion from the OpenAQ v3 API.

OpenAQ v3 requires a free API key in the X-API-Key header — grab one at
https://explore.openaq.org and put it in the OPENAQ_API_KEY env var.

The flow is two-step because OpenAQ v3 splits locations and measurements:

  1.  /v3/locations?coordinates=lat,lon&radius=25000  → find monitoring
      stations within a 25 km radius of each city centre
  2.  /v3/sensors/{sensor_id}/measurements/daily      → pull recent daily
      aggregates from each sensor at each location

Using the daily aggregate endpoint (instead of raw hourly measurements) keeps
volume manageable and aligns with how the dbt marts roll the data up anyway.
For cities with sparse monitoring we still capture what's available — the
downstream dbt tests flag any city whose row count drops below a threshold.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, "/app")
from utils.cities import CITIES, City  # noqa: E402
from utils.gcs_helper import upload_dataframe_as_parquet, resolve_bucket  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openaq.org/v3"
SEARCH_RADIUS_M = 25_000  # 25 km around each city centre
POLLUTANTS_OF_INTEREST = {"pm25", "pm10", "o3", "no2", "so2", "co"}


def _build_session() -> requests.Session:
    api_key = os.environ.get("OPENAQ_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAQ_API_KEY is not set. Get a free key at https://explore.openaq.org "
            "and export it before running."
        )
    session = requests.Session()
    session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})
    retry = Retry(
        total=5,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def discover_sensors(session: requests.Session, city: City) -> list[dict[str, Any]]:
    """
    Return a flat list of sensors near the city that measure pollutants we care about.
    Each record has: location_id, location_name, sensor_id, parameter_name, unit.
    """
    url = f"{BASE_URL}/locations"
    params = {
        "coordinates": f"{city.latitude},{city.longitude}",
        "radius": SEARCH_RADIUS_M,
        "limit": 100,
    }
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    locations = resp.json().get("results", [])

    sensors: list[dict[str, Any]] = []
    for loc in locations:
        for sensor in loc.get("sensors", []):
            param = sensor.get("parameter", {})
            name = param.get("name")
            if name in POLLUTANTS_OF_INTEREST:
                sensors.append(
                    {
                        "location_id": loc["id"],
                        "location_name": loc.get("name"),
                        "sensor_id": sensor["id"],
                        "parameter_name": name,
                        "parameter_units": param.get("units"),
                    }
                )
    logger.info("Found %d relevant sensors near %s", len(sensors), city.name)
    return sensors


def fetch_daily_measurements(
    session: requests.Session,
    sensor_id: int,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    """Hit /v3/sensors/{id}/measurements/daily and page through the results."""
    url = f"{BASE_URL}/sensors/{sensor_id}/measurements/daily"
    params = {
        "datetime_from": f"{start.isoformat()}T00:00:00Z",
        "datetime_to": f"{end.isoformat()}T23:59:59Z",
        "limit": 1000,
        "page": 1,
    }
    out: list[dict[str, Any]] = []
    while True:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 404:
            # Sensor has no data in the requested window — skip quietly
            return []
        resp.raise_for_status()
        body = resp.json()
        results = body.get("results", [])
        out.extend(results)
        meta = body.get("meta", {})
        if len(results) < params["limit"] or meta.get("found", 0) <= params["page"] * params["limit"]:
            break
        params["page"] += 1
    return out


def normalise(records: list[dict[str, Any]], city: City, sensor: dict[str, Any]) -> pd.DataFrame:
    """Flatten the nested OpenAQ response into one row per measurement."""
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        # OpenAQ returns datetime objects like {"utc": "...", "local": "..."}
        period = r.get("period", {}) or {}
        dt_from = (period.get("datetimeFrom") or {}).get("utc")
        rows.append(
            {
                "city_id": city.city_id,
                "city_name": city.name,
                "location_id": sensor["location_id"],
                "location_name": sensor["location_name"],
                "sensor_id": sensor["sensor_id"],
                "parameter_name": sensor["parameter_name"],
                "parameter_units": sensor["parameter_units"],
                "observation_date": dt_from[:10] if dt_from else None,
                "value": r.get("value"),
                "min_value": (r.get("summary") or {}).get("min"),
                "max_value": (r.get("summary") or {}).get("max"),
                "avg_value": (r.get("summary") or {}).get("avg"),
                "measurement_count": r.get("coverage", {}).get("observedCount"),
            }
        )
    df = pd.DataFrame(rows)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def run() -> None:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=7)  # 7-day sliding window per run; late-arriving data gets picked up

    bucket = resolve_bucket()
    session = _build_session()

    all_frames: list[pd.DataFrame] = []
    for city in CITIES:
        sensors = discover_sensors(session, city)
        for sensor in sensors:
            records = fetch_daily_measurements(session, sensor["sensor_id"], start, end)
            df = normalise(records, city, sensor)
            if not df.empty:
                all_frames.append(df)
            time.sleep(0.2)

    if not all_frames:
        logger.warning("No air-quality measurements returned for window %s–%s", start, end)
        return

    combined = pd.concat(all_frames, ignore_index=True)
    blob_path = (
        f"raw/air_quality/year={end.year:04d}"
        f"/month={end.month:02d}"
        f"/day={end.day:02d}"
        f"/data.parquet"
    )
    upload_dataframe_as_parquet(combined, bucket, blob_path)
    logger.info("Uploaded %d air-quality rows covering %s → %s", len(combined), start, end)


if __name__ == "__main__":
    run()

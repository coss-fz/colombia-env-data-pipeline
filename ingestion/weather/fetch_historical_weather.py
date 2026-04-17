"""
Historical weather backfill for all 8 Colombian cities.

Calls the Open-Meteo /v1/archive endpoint (no API key required), reshapes the
hourly JSON response into a tall dataframe, and writes one parquet file per
(city, year-month) partition to GCS.

Why partition by year-month? The historical window spans multiple years. A
single parquet file for all of it would be too big for BigQuery's external-
table limits to be comfortable, and it would prevent incremental reprocessing
if we ever want to re-pull a single month.

Usage (run inside the ingestion container or locally with GOOGLE_APPLICATION_CREDENTIALS set):

    python -m weather.fetch_historical_weather \\
        --start-date 2020-01-01 \\
        --end-date   2024-12-31
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime
from typing import Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Allow running this file as `python -m weather.fetch_historical_weather` from /app
sys.path.insert(0, "/app")
from utils.cities import CITIES, City  # noqa: E402
from utils.gcs_helper import upload_dataframe_as_parquet, resolve_bucket  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "shortwave_radiation",
]


def _build_session() -> requests.Session:
    """HTTP session with retry/backoff. Open-Meteo is reliable but rate-limits bursts."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_city_window(
    session: requests.Session,
    city: City,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Fetch a single (city, date-range) window from Open-Meteo.

    Returns a dataframe with one row per hour. Each column is a weather variable
    listed in HOURLY_VARS, plus metadata columns (city_id, city_name, etc.).
    """
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": city.timezone,
    }
    logger.info("GET archive for %s: %s → %s", city.name, start_date, end_date)
    resp = session.get(ARCHIVE_URL, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    hourly = payload.get("hourly", {})
    if not hourly or "time" not in hourly:
        logger.warning("No hourly data for %s %s–%s", city.name, start_date, end_date)
        return pd.DataFrame()

    df = pd.DataFrame(hourly)
    # Open-Meteo returns timestamps as naive strings in the requested tz. Tagging
    # them explicitly avoids nasty surprises downstream when joining with other
    # datasets that ARE tz-aware.
    df["observation_ts"] = pd.to_datetime(df["time"])
    df = df.drop(columns=["time"])

    df.insert(0, "city_id", city.city_id)
    df.insert(1, "city_name", city.name)
    df.insert(2, "latitude", city.latitude)
    df.insert(3, "longitude", city.longitude)
    df.insert(4, "elevation_m", city.elevation_m)
    df["ingested_at"] = pd.Timestamp.utcnow()

    return df


def month_windows(start: date, end: date) -> Iterable[tuple[date, date]]:
    """Yield (month_start, month_end) pairs covering [start, end]."""
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        if cursor.month == 12:
            next_month = date(cursor.year + 1, 1, 1)
        else:
            next_month = date(cursor.year, cursor.month + 1, 1)
        window_start = max(cursor, start)
        window_end = min(next_month - pd.Timedelta(days=1).to_pytimedelta(), end)
        yield window_start, window_end
        cursor = next_month


def run(start_date: date, end_date: date) -> None:
    bucket = resolve_bucket()
    session = _build_session()

    total_rows = 0
    for city in CITIES:
        for w_start, w_end in month_windows(start_date, end_date):
            df = fetch_city_window(session, city, w_start, w_end)
            if df.empty:
                continue

            # Layout: raw/weather/city_id=BOG/year=2024/month=03/data.parquet
            # This hive-style layout makes BigQuery external tables trivially
            # partitionable later and is also friendly to Spark readers.
            blob_path = (
                f"raw/weather/city_id={city.city_id}"
                f"/year={w_start.year:04d}"
                f"/month={w_start.month:02d}"
                f"/data.parquet"
            )
            upload_dataframe_as_parquet(df, bucket, blob_path)
            total_rows += len(df)
            # Be polite: Open-Meteo allows ~10k requests/day but we don't need to burn through it.
            time.sleep(0.3)

    logger.info("Historical backfill complete: %s rows across %d cities", f"{total_rows:,}", len(CITIES))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(
        datetime.strptime(args.start_date, "%Y-%m-%d").date(),
        datetime.strptime(args.end_date, "%Y-%m-%d").date(),
    )

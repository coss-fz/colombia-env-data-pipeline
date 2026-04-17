"""
Daily incremental weather ingestion.

Grabs yesterday's hourly observations for every city in one go. This is what
Kestra runs every night at 02:00 (see kestra/flows/02_ingest_weather_daily.yaml).
The output path is deliberately date-scoped so re-runs are idempotent — an
accidental double-trigger just overwrites the same file with the same data.

Why yesterday, not today? ERA5-based archive data isn't published in real time;
Open-Meteo typically has the previous day available by the next UTC morning.
Running at 02:00 local Colombia time (~07:00 UTC) is well within that window.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd

sys.path.insert(0, "/app")
from utils.cities import CITIES  # noqa: E402
from utils.gcs_helper import upload_dataframe_as_parquet, resolve_bucket  # noqa: E402
from weather.fetch_historical_weather import fetch_city_window, _build_session  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _target_date() -> date:
    """Default target date is yesterday. Overridable via TARGET_DATE env var for backfill."""
    override = os.environ.get("TARGET_DATE")
    if override:
        return datetime.strptime(override, "%Y-%m-%d").date()
    return date.today() - timedelta(days=1)


def run() -> None:
    target = _target_date()
    bucket = resolve_bucket()
    session = _build_session()

    logger.info("Daily weather pull for %s across %d cities", target, len(CITIES))
    all_frames = []
    for city in CITIES:
        df = fetch_city_window(session, city, target, target)
        if not df.empty:
            all_frames.append(df)
        time.sleep(0.3)

    if not all_frames:
        logger.error("No data returned for any city on %s — aborting without writing", target)
        raise SystemExit(1)

    combined = pd.concat(all_frames, ignore_index=True)
    # One combined file per day — small, dense, easy for BigQuery to scan.
    blob_path = (
        f"raw/weather_daily/year={target.year:04d}"
        f"/month={target.month:02d}"
        f"/day={target.day:02d}"
        f"/data.parquet"
    )
    upload_dataframe_as_parquet(combined, bucket, blob_path)
    logger.info("Wrote %d rows to gs://%s/%s", len(combined), bucket, blob_path)


if __name__ == "__main__":
    run()

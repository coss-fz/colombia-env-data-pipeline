"""
Thin wrapper around google-cloud-storage used by every ingestion script.

Why this module exists: Kestra flows pass a GCS bucket name in via env vars
and every ingestion script ends with "write the dataframe as parquet to GCS".
Centralising that here means we only have one place to tweak (compression,
credentials, path conventions) when something changes.
"""
from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)


def _get_client() -> storage.Client:
    """Build a GCS client. Respects GOOGLE_APPLICATION_CREDENTIALS when set."""
    return storage.Client()


def upload_dataframe_as_parquet(
    df: pd.DataFrame,
    bucket_name: str,
    blob_path: str,
    compression: str = "snappy",
) -> str:
    """
    Write a pandas DataFrame to GCS as parquet.

    Returns the full gs:// URI on success. Uses snappy compression by default
    because BigQuery external tables read it natively and it's a good
    compression/speed trade-off for this data volume.
    """
    if df.empty:
        logger.warning("Refusing to upload empty dataframe to gs://%s/%s", bucket_name, blob_path)
        return ""

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression=compression, index=False)
    buffer.seek(0)

    client = _get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")

    uri = f"gs://{bucket_name}/{blob_path}"
    logger.info("Uploaded %d rows (%.1f KB) to %s", len(df), buffer.getbuffer().nbytes / 1024, uri)
    return uri


def upload_file(local_path: str | Path, bucket_name: str, blob_path: str) -> str:
    """Upload an arbitrary file from disk to GCS. Used by the Spark job."""
    client = _get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{blob_path}"


def resolve_bucket() -> str:
    """Read the bucket name from env. Fails loudly if missing — silent defaults are bugs."""
    bucket = os.environ.get("GCS_BUCKET")
    if not bucket:
        raise RuntimeError("GCS_BUCKET env var is not set")
    return bucket

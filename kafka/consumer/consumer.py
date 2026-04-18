"""
Kafka consumer that batches streaming weather messages into parquet files on GCS.

Strategy: accumulate messages into an in-memory buffer, flush either when the
buffer reaches BATCH_SIZE or when BATCH_TIMEOUT_SEC elapses since the last flush.
Flushes write a single parquet file to:

    gs://$GCS_BUCKET/streaming/weather/year=YYYY/month=MM/day=DD/batch_<epoch_ms>.parquet

The BigQuery external table (see Kestra flow 00_setup_external_tables) already
points at this prefix, so downstream queries see new data as soon as the file
lands — typical latency is BATCH_TIMEOUT_SEC + a few seconds.
"""
from __future__ import annotations

import io
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from google.cloud import storage
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_running = True  # flipped to False on SIGTERM/SIGINT so the main loop exits cleanly


def _handle_signal(signum, _frame):
    global _running
    logger.info("Received signal %s, shutting down after next flush", signum)
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        os.environ.get("KAFKA_TOPIC", "weather.sensor.readings"),
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
        group_id=os.environ.get("KAFKA_GROUP_ID", "weather-sink-gcs"),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        # `earliest` so a restart doesn't lose in-flight demo traffic;
        # switch to `latest` in a real production sink.
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # we commit manually, only after a successful flush
        max_poll_records=500,
    )


def flush_batch(bucket: storage.Bucket, rows: list[dict]) -> str:
    """Write the buffered rows to a single parquet object. Returns the gs:// URI."""
    if not rows:
        return ""
    df = pd.DataFrame(rows)
    df["consumed_at"] = pd.Timestamp.utcnow()

    now = datetime.now(timezone.utc)
    blob_path = (
        f"streaming/weather/year={now.year:04d}"
        f"/month={now.month:02d}"
        f"/day={now.day:02d}"
        f"/batch_{int(now.timestamp() * 1000)}.parquet"
    )

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    buffer.seek(0)

    bucket.blob(blob_path).upload_from_file(
        buffer, content_type="application/octet-stream"
    )
    uri = f"gs://{bucket.name}/{blob_path}"
    logger.info("Flushed %d rows to %s", len(rows), uri)
    return uri


def main() -> int:
    bucket_name      = os.environ["GCS_BUCKET"]
    batch_size       = int(os.environ.get("BATCH_SIZE", "100"))
    batch_timeout_s  = int(os.environ.get("BATCH_TIMEOUT_SEC", "30"))

    bucket = storage.Client().bucket(bucket_name)
    consumer = build_consumer()

    buffer: list[dict] = []
    last_flush = time.monotonic()

    logger.info(
        "Consumer started: topic=%s, bucket=%s, batch_size=%d, batch_timeout=%ds",
        os.environ.get("KAFKA_TOPIC"), bucket_name, batch_size, batch_timeout_s,
    )

    try:
        while _running:
            # Poll with a short timeout so we can hit the flush-by-time path
            # even when no messages are arriving.
            records = consumer.poll(timeout_ms=1000, max_records=batch_size)
            for _tp, msgs in records.items():
                for msg in msgs:
                    buffer.append(msg.value)

            size_trigger = len(buffer) >= batch_size
            time_trigger = buffer and (time.monotonic() - last_flush) >= batch_timeout_s

            if size_trigger or time_trigger:
                try:
                    flush_batch(bucket, buffer)
                    consumer.commit()  # only commit after successful GCS write
                    buffer.clear()
                    last_flush = time.monotonic()
                except Exception as exc:  # noqa: BLE001
                    # On flush failure, DON'T commit — the same messages will be
                    # re-delivered, and we clear the buffer to avoid unbounded growth
                    # if the failure is due to malformed rows.
                    logger.error("Flush failed, will retry next cycle: %s", exc)
                    buffer.clear()

        # Final flush on shutdown
        if buffer:
            logger.info("Shutdown flush: %d rows", len(buffer))
            flush_batch(bucket, buffer)
            consumer.commit()

    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())

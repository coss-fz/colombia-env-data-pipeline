"""
Simulated real-time weather sensor producer.

Fetches the current observed weather for each of the 8 cities from Open-Meteo's
live /v1/forecast endpoint (current weather block) and publishes one message
per city every EMIT_INTERVAL_SEC. This is what a fleet of IoT stations would
look like if we actually had them — same schema, same topic, same downstream
plumbing.

Why not use simulated noise? Two reasons:
  1. Keeping the values real means the streaming dashboard actually tells you
     something (current conditions in each city), not just a demo number.
  2. The pipeline integration is identical whether messages are generated
     locally or from real hardware — the Kafka topic is the contract.
"""
from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Inlined city list so this container has no dependency on the ingestion module.
CITIES = [
    ("BOG", "Bogota",        4.7110, -74.0721),
    ("MDE", "Medellin",      6.2442, -75.5812),
    ("CLO", "Cali",          3.4516, -76.5320),
    ("BAQ", "Barranquilla", 10.9639, -74.7964),
    ("CTG", "Cartagena",    10.3910, -75.4794),
    ("BGA", "Bucaramanga",   7.1193, -73.1227),
    ("PEI", "Pereira",       4.8133, -75.6961),
    ("SMR", "Santa Marta",  11.2408, -74.1990),
]

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_producer() -> KafkaProducer:
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Durability > throughput for a demo: wait for all in-sync replicas.
        # (With one broker that just means "wait for broker".)
        acks="all",
        linger_ms=20,
        retries=5,
        max_in_flight_requests_per_connection=1,  # guarantee ordering per partition
    )


def fetch_current(city_id: str, lat: float, lon: float) -> dict | None:
    """Hit Open-Meteo's current weather. Returns None on transient failures."""
    try:
        resp = requests.get(
            FORECAST_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "current": (
                    "temperature_2m,relative_humidity_2m,apparent_temperature,"
                    "precipitation,wind_speed_10m,wind_direction_10m,pressure_msl,"
                    "cloud_cover,wind_gusts_10m"
                ),
                "timezone": "America/Bogota",
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("current")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Fetch failed for %s: %s", city_id, exc)
        return None


def build_payload(city_id: str, city_name: str, lat: float, lon: float, current: dict) -> dict:
    """Shape the message. Schema is stable — dbt + the consumer depend on it."""
    # Add a pinch of sensor noise so we can tell the streaming feed apart from
    # the batch feed when both are present (and it looks more realistic).
    noise = random.uniform(-0.3, 0.3)

    return {
        "event_id": f"{city_id}-{int(time.time() * 1000)}",
        "event_ts_utc": datetime.now(timezone.utc).isoformat(),
        "city_id": city_id,
        "city_name": city_name,
        "latitude": lat,
        "longitude": lon,
        "observed_ts": current.get("time"),
        "temperature_c": _nudge(current.get("temperature_2m"), noise),
        "apparent_temperature_c": _nudge(current.get("apparent_temperature"), noise),
        "relative_humidity_pct": current.get("relative_humidity_2m"),
        "precipitation_mm": current.get("precipitation"),
        "wind_speed_kmh": current.get("wind_speed_10m"),
        "wind_direction_deg": current.get("wind_direction_10m"),
        "wind_gusts_kmh": current.get("wind_gusts_10m"),
        "pressure_hpa": current.get("pressure_msl"),
        "cloud_cover_pct": current.get("cloud_cover"),
        "sensor_kind": "simulated_iot",
    }


def _nudge(value: float | None, delta: float) -> float | None:
    return None if value is None else round(value + delta, 2)


def main() -> None:
    topic         = os.environ.get("KAFKA_TOPIC", "weather.sensor.readings")
    interval_sec  = float(os.environ.get("EMIT_INTERVAL_SEC", "2"))
    producer      = build_producer()

    logger.info("Producer started → topic=%s, interval=%ss", topic, interval_sec)
    try:
        while True:
            for city_id, city_name, lat, lon in CITIES:
                current = fetch_current(city_id, lat, lon)
                if not current:
                    continue
                payload = build_payload(city_id, city_name, lat, lon, current)
                future = producer.send(topic, key=city_id, value=payload)
                try:
                    meta = future.get(timeout=10)
                    logger.info("Sent %s → %s:%s@%s", city_id, meta.topic, meta.partition, meta.offset)
                except KafkaError as exc:
                    logger.error("Publish failed for %s: %s", city_id, exc)
                time.sleep(interval_sec)
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)


if __name__ == "__main__":
    sys.exit(main())

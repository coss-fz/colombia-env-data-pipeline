"""
Spark job: multi-year historical weather aggregations.

Reads the partitioned parquet landing zone (raw/weather/...), computes rollups
that are too heavy to run as regular dbt models every day, and writes the
result back to GCS where dbt's downstream marts read it via external tables.
The same aggregates are also loaded into BigQuery as native tables in the
reporting warehouse dataset.

What we compute:
  1. Monthly climate normals per city (avg, p10, p90, extremes)
  2. 30-day rolling averages for every variable
  3. Year-over-year temperature anomalies vs. the city's own long-run baseline
  4. Heat-wave detection (3+ consecutive days where Tmax > city p95)

Why Spark here and not just BigQuery?
  - The partition-scanning pattern (needing the full multi-year history to
    compute anomalies for the latest month) is awkward to express efficiently
    in pure SQL without scanning the whole fact table repeatedly.
  - It's the right tool for a windowed transformation over a data lake of
    parquet, which is exactly what this pipeline has.

Usage:
    spark-submit \\
        --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.23,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \\
        jobs/weather_aggregations.py \\
        --bucket          $GCS_BUCKET \\
        --input-prefix    raw/weather \\
        --output-prefix   processed/weather_monthly \\
        --gcp-project     $GCP_PROJECT \\
        --bq-dataset      colombia_env_warehouse_reporting \\
        --bq-location     US \\
        --bq-temp-bucket  $GCS_BUCKET
"""
from __future__ import annotations

import argparse
import logging

from google.cloud import bigquery
from pyspark.sql import SparkSession, DataFrame, functions as F, Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_spark(app_name: str = "colombia-env-weather-aggregations") -> SparkSession:
    """Create a Spark session wired up to read from / write to GCS."""
    # The GCS connector jar must be on the classpath — supplied via --packages at submit time.
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def read_weather(spark: SparkSession, gcs_path: str) -> DataFrame:
    """
    Load the hive-partitioned parquet dataset.

    The ingestion side currently writes observation_ts as INT64 TIMESTAMP(NANOS),
    which Spark's parquet reader refuses by default. We enable
    spark.sql.legacy.parquet.nanosAsLong at submit time so those columns come
    through as bigint nanoseconds-since-epoch, then cast back to a proper
    timestamp here. Once the writer is switched to microsecond precision the
    guard below becomes a no-op and can be removed.
    """
    logger.info("Reading parquet from %s", gcs_path)
    df = spark.read.parquet(gcs_path)

    if dict(df.dtypes).get("observation_ts") == "bigint":
        logger.info("observation_ts arrived as bigint ns; casting to timestamp")
        df = df.withColumn(
            "observation_ts",
            (F.col("observation_ts") / F.lit(1_000_000_000)).cast("timestamp"),
        )
    return df


def compute_daily(df: DataFrame) -> DataFrame:
    """Roll hourly → daily inside Spark so we don't pay to scan hourly grain every time."""
    return (
        df
        .withColumn("observation_date", F.to_date("observation_ts"))
        .groupBy("city_id", "city_name", "observation_date")
        .agg(
            F.avg("temperature_2m").alias("avg_temperature_c"),
            F.min("temperature_2m").alias("min_temperature_c"),
            F.max("temperature_2m").alias("max_temperature_c"),
            F.avg("relative_humidity_2m").alias("avg_humidity_pct"),
            F.sum("precipitation").alias("total_precipitation_mm"),
            F.avg("pressure_msl").alias("avg_pressure_hpa"),
            F.avg("wind_speed_10m").alias("avg_wind_speed_kmh"),
            F.max("wind_gusts_10m").alias("max_wind_gusts_kmh"),
            F.count("*").alias("hour_count"),
        )
    )


def compute_monthly_normals(daily: DataFrame) -> DataFrame:
    """
    Monthly climate normals. `climate normal` in meteorology usually means a
    30-year average — we compute over whatever window we have, labelled so the
    dashboard can show the sample size honestly.
    """
    normal_window = Window.partitionBy("city_id", "month_of_year")

    return (
        daily
        .withColumn("month_of_year", F.month("observation_date"))
        .groupBy("city_id", "city_name", "month_of_year",
                 F.year("observation_date").alias("year"))
        .agg(
            F.avg("avg_temperature_c").alias("month_avg_temp_c"),
            F.min("min_temperature_c").alias("month_min_temp_c"),
            F.max("max_temperature_c").alias("month_max_temp_c"),
            F.sum("total_precipitation_mm").alias("month_total_precip_mm"),
            F.avg("avg_humidity_pct").alias("month_avg_humidity_pct"),
        )
        # Per-city, per-calendar-month long-run normal across all years
        .withColumn("climate_normal_temp_c",
                    F.avg("month_avg_temp_c").over(normal_window))
        .withColumn("climate_normal_precip_mm",
                    F.avg("month_total_precip_mm").over(normal_window))
        # Anomaly = this year's value minus the long-run normal
        .withColumn("temp_anomaly_c",
                    F.col("month_avg_temp_c") - F.col("climate_normal_temp_c"))
        .withColumn("precip_anomaly_mm",
                    F.col("month_total_precip_mm") - F.col("climate_normal_precip_mm"))
        .withColumn("years_in_sample",
                    F.count("year").over(normal_window))
    )


def compute_rolling_30d(daily: DataFrame) -> DataFrame:
    """30-day rolling averages. Window by days, not rows, so missing days don't distort."""
    # Use unix-timestamp-of-day so rangeBetween can operate in seconds.
    days_seconds = 30 * 24 * 60 * 60
    w = (
        Window
        .partitionBy("city_id")
        .orderBy(F.col("obs_unix"))
        .rangeBetween(-days_seconds, 0)
    )

    return (
        daily
        .withColumn("obs_unix", F.unix_timestamp("observation_date"))
        .withColumn("rolling_30d_avg_temp_c", F.avg("avg_temperature_c").over(w))
        .withColumn("rolling_30d_total_precip_mm", F.sum("total_precipitation_mm").over(w))
        .withColumn("rolling_30d_max_temp_c", F.max("max_temperature_c").over(w))
        .drop("obs_unix")
    )


def detect_heat_waves(daily: DataFrame) -> DataFrame:
    """
    Mark heat-wave days. Definition used: 3+ consecutive days with max temp
    above the 95th percentile for that city, computed across all historical data.

    The percentile is per-city because a "hot day" in Bogotá (2640 m altitude,
    average high ~20°C) is very different from one in Barranquilla.
    """
    # Per-city p95 of max temp
    tagged = (
        daily
        .withColumn("city_tmax_p95",
                    F.expr("percentile_cont(0.95) within group (order by max_temperature_c) over (partition by city_id)"))
        .withColumn("is_heat_day", F.col("max_temperature_c") > F.col("city_tmax_p95"))
    )

    # Gap-and-islands pattern: identify runs of consecutive heat days.
    order_w = Window.partitionBy("city_id").orderBy("observation_date")
    with_runs = (
        tagged
        .withColumn("rn_all",
                    F.row_number().over(order_w))
        .withColumn("rn_heat",
                    F.sum(F.when(F.col("is_heat_day"), 1).otherwise(0)).over(
                        order_w.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("run_group",
                    F.col("rn_all") - F.col("rn_heat"))
    )

    # Only count heat days inside each run_group — a non-heat day and the
    # following heat day can share a run_group by construction, so a naive
    # count(*) would overstate the run length by 1. Summing the 0/1 flag
    # avoids that trap.
    run_window = Window.partitionBy("city_id", "run_group")
    return (
        with_runs
        .withColumn("heat_run_length",
                    F.when(F.col("is_heat_day"),
                           F.sum(F.when(F.col("is_heat_day"), 1).otherwise(0)).over(run_window))
                     .otherwise(0))
        .withColumn("is_heat_wave_day",
                    F.col("heat_run_length") >= 3)
        .drop("rn_all", "rn_heat", "run_group")
    )


def write_parquet(df: DataFrame, gcs_path: str, partition_cols: list[str] | None = None) -> None:
    logger.info("Writing %s (partitioned by %s)", gcs_path, partition_cols)
    writer = df.write.mode("overwrite").format("parquet").option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(gcs_path)


def ensure_dataset(project: str, dataset: str, location: str) -> None:
    """Create the BigQuery dataset if it doesn't exist. Idempotent."""
    client = bigquery.Client(project=project)
    ds_ref = bigquery.Dataset(f"{project}.{dataset}")
    ds_ref.location = location
    client.create_dataset(ds_ref, exists_ok=True)
    logger.info("Dataset %s.%s ready (location=%s)", project, dataset, location)


def write_bigquery(
    df: DataFrame,
    table_fqn: str,
    temp_bucket: str,
    *,
    partition_field: str | None = None,
    partition_type: str = "DAY",
    cluster_fields: list[str] | None = None,
    mode: str = "overwrite",
) -> None:
    """
    Write a DataFrame to a native BigQuery table via the spark-bigquery connector.

    `mode="overwrite"` maps to WRITE_TRUNCATE in BQ: the table gets replaced on
    every run, which matches the semantics of rollups recomputed over the full
    history. For a real append-only pattern (per-run snapshots) pass mode="append",
    but you'll also need a batch-run column to avoid duplicates across runs.

    The connector uses the indirect write path: Spark stages parquet in
    `temp_bucket` and BigQuery runs a load job from there. Cheaper and more
    robust than streaming inserts for batches of this size.
    """
    logger.info("Writing to BigQuery %s (mode=%s)", table_fqn, mode)
    writer = (
        df.write
        .format("bigquery")
        .option("table", table_fqn)
        .option("temporaryGcsBucket", temp_bucket)
        .option("writeMethod", "indirect")
        .option("createDisposition", "CREATE_IF_NEEDED")
    )
    if partition_field:
        writer = (
            writer
            .option("partitionField", partition_field)
            .option("partitionType", partition_type)
        )
    if cluster_fields:
        writer = writer.option("clusteredFields", ",".join(cluster_fields))
    writer.mode(mode).save()


def main(
    bucket: str,
    input_prefix: str,
    output_prefix: str,
    gcp_project: str,
    bq_dataset: str,
    bq_location: str,
    bq_temp_bucket: str,
) -> None:
    spark = build_spark()
    try:
        raw_path = f"gs://{bucket}/{input_prefix}"
        out_root = f"gs://{bucket}/{output_prefix}"

        hourly = read_weather(spark, raw_path)
        logger.info("Raw rows: %s", hourly.count())

        daily = compute_daily(hourly).cache()

        monthly_normals = compute_monthly_normals(daily)
        rolling_30d     = compute_rolling_30d(daily)
        heat_waves      = detect_heat_waves(daily)

        # --- Landing parquet on GCS (unchanged) ---
        write_parquet(monthly_normals, f"{out_root}/monthly_normals", ["city_id"])
        write_parquet(rolling_30d,     f"{out_root}/rolling_30d",     ["city_id"])
        write_parquet(heat_waves,      f"{out_root}/heat_waves",      ["city_id"])

        # --- Warehouse (BigQuery native tables) ---
        ensure_dataset(gcp_project, bq_dataset, bq_location)

        write_bigquery(
            monthly_normals,
            f"{gcp_project}.{bq_dataset}.weather_monthly_normals",
            temp_bucket=bq_temp_bucket,
            cluster_fields=["city_id", "month_of_year"],
        )
        write_bigquery(
            rolling_30d,
            f"{gcp_project}.{bq_dataset}.weather_rolling_30d",
            temp_bucket=bq_temp_bucket,
            partition_field="observation_date",
            partition_type="DAY",
            cluster_fields=["city_id"],
        )
        write_bigquery(
            heat_waves,
            f"{gcp_project}.{bq_dataset}.weather_heat_waves",
            temp_bucket=bq_temp_bucket,
            partition_field="observation_date",
            partition_type="DAY",
            cluster_fields=["city_id"],
        )

        logger.info("Spark aggregations + BigQuery load complete")
    finally:
        spark.stop()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket",         required=True)
    p.add_argument("--input-prefix",   default="raw/weather")
    p.add_argument("--output-prefix",  default="processed/weather_monthly")
    p.add_argument("--gcp-project",    required=True)
    p.add_argument("--bq-dataset",     default="colombia_env_warehouse_reporting")
    p.add_argument("--bq-location",    default="us-central1")
    p.add_argument("--bq-temp-bucket", required=True,
                   help="GCS bucket the connector uses to stage parquet "
                        "before the BQ load job. Can be the same as --bucket.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(
        args.bucket,
        args.input_prefix,
        args.output_prefix,
        args.gcp_project,
        args.bq_dataset,
        args.bq_location,
        args.bq_temp_bucket,
    )
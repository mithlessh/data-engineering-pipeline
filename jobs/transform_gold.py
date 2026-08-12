from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    sum,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "processed" / "gold"


def main():

    spark = (
        SparkSession.builder
        .appName("DataEngineeringPipeline-Gold")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("GOLD TRANSFORMATION STARTED")
    print("=" * 60)

    # =========================================================
    # 1. READ SILVER
    # =========================================================

    print(f"Reading Silver data from:")
    print(SILVER_PATH)

    df = spark.read.parquet(str(SILVER_PATH))

    silver_count = df.count()

    print(f"Silver rows: {silver_count:,}")

    # =========================================================
    # 2. DAILY METRICS
    # =========================================================

    print("\nCreating daily metrics...")

    daily_metrics = (
        df.groupBy("pickup_date")
        .agg(
            count("*").alias("total_trips"),
            sum("passenger_count").alias("total_passengers"),
            avg("trip_duration").alias("avg_trip_duration_sec"),
            avg("trip_distance_km").alias("avg_trip_distance_km"),
            avg("trip_speed_kmh").alias("avg_trip_speed_kmh"),
        )
        .orderBy("pickup_date")
    )

    daily_path = GOLD_PATH / "daily_metrics"

    (
        daily_metrics
        .write
        .mode("overwrite")
        .parquet(str(daily_path))
    )

    print(f"Daily metrics written to:")
    print(daily_path)

    # =========================================================
    # 3. HOURLY METRICS
    # =========================================================

    print("\nCreating hourly metrics...")

    hourly_metrics = (
        df.groupBy("pickup_hour")
        .agg(
            count("*").alias("total_trips"),
            sum("passenger_count").alias("total_passengers"),
            avg("trip_duration").alias("avg_trip_duration_sec"),
            avg("trip_distance_km").alias("avg_trip_distance_km"),
            avg("trip_speed_kmh").alias("avg_trip_speed_kmh"),
        )
        .orderBy("pickup_hour")
    )

    hourly_path = GOLD_PATH / "hourly_metrics"

    (
        hourly_metrics
        .write
        .mode("overwrite")
        .parquet(str(hourly_path))
    )

    print(f"Hourly metrics written to:")
    print(hourly_path)

    # =========================================================
    # 4. VENDOR METRICS
    # =========================================================

    print("\nCreating vendor metrics...")

    vendor_metrics = (
        df.groupBy("vendor_id")
        .agg(
            count("*").alias("total_trips"),
            sum("passenger_count").alias("total_passengers"),
            avg("trip_duration").alias("avg_trip_duration_sec"),
            avg("trip_distance_km").alias("avg_trip_distance_km"),
            avg("trip_speed_kmh").alias("avg_trip_speed_kmh"),
        )
        .orderBy("vendor_id")
    )

    vendor_path = GOLD_PATH / "vendor_metrics"

    (
        vendor_metrics
        .write
        .mode("overwrite")
        .parquet(str(vendor_path))
    )

    print(f"Vendor metrics written to:")
    print(vendor_path)

    # =========================================================
    # 5. DISPLAY RESULTS
    # =========================================================

    print("\n" + "=" * 60)
    print("DAILY METRICS SAMPLE")
    print("=" * 60)

    daily_metrics.show(
        10,
        truncate=False,
    )

    print("\n" + "=" * 60)
    print("HOURLY METRICS")
    print("=" * 60)

    hourly_metrics.show(
        24,
        truncate=False,
    )

    print("\n" + "=" * 60)
    print("VENDOR METRICS")
    print("=" * 60)

    vendor_metrics.show(
        truncate=False,
    )

    # =========================================================
    # FINAL STATUS
    # =========================================================

    print("\n" + "=" * 60)
    print("GOLD TRANSFORMATION COMPLETE")
    print("=" * 60)

    print("Gold datasets written successfully to:")
    print(GOLD_PATH)

    print("\nGenerated datasets:")
    print("- daily_metrics")
    print("- hourly_metrics")
    print("- vendor_metrics")

    spark.stop()


if __name__ == "__main__":
    main()

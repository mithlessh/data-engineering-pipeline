from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    dayofweek,
    month,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    lit,
    when,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = PROJECT_ROOT / "data" / "processed" / "bronze"
SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"


def main():

    spark = (
        SparkSession.builder
        .appName("DataEngineeringPipeline-Silver")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("SILVER TRANSFORMATION STARTED")
    print("=" * 60)

    # =========================================================
    # 1. READ BRONZE
    # =========================================================

    print(f"Reading Bronze data from:")
    print(BRONZE_PATH)

    df = spark.read.parquet(str(BRONZE_PATH))

    bronze_count = df.count()

    print(f"Bronze rows: {bronze_count:,}")

    # =========================================================
    # 2. REMOVE NULL RECORDS FROM REQUIRED COLUMNS
    # =========================================================

    print("\nApplying required-field null checks...")

    df = df.dropna(
        subset=[
            "id",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
            "trip_duration",
        ]
    )

    # =========================================================
    # 3. VALIDATE BASIC BUSINESS RULES
    # =========================================================

    print("Applying passenger and duration validation...")

    df = df.filter(
        (col("passenger_count") > 0)
        & (col("passenger_count") <= 8)
        & (col("trip_duration") > 0)
        & (col("trip_duration") <= 24 * 60 * 60)
    )

    # =========================================================
    # 4. VALIDATE GEOGRAPHIC COORDINATES
    # =========================================================

    print("Applying geographic coordinate validation...")

    df = df.filter(
        (col("pickup_longitude") >= -180)
        & (col("pickup_longitude") <= 180)
        & (col("dropoff_longitude") >= -180)
        & (col("dropoff_longitude") <= 180)
        & (col("pickup_latitude") >= -90)
        & (col("pickup_latitude") <= 90)
        & (col("dropoff_latitude") >= -90)
        & (col("dropoff_latitude") <= 90)
    )

    # =========================================================
    # 5. VALIDATE DATETIME RELATIONSHIP
    # =========================================================

    print("Applying pickup/dropoff datetime validation...")

    df = df.filter(
        col("dropoff_datetime") > col("pickup_datetime")
    )

    # =========================================================
    # 6. CALCULATE TRIP DISTANCE
    # =========================================================

    print("Calculating trip distance using Haversine formula...")

    pickup_lat = radians(col("pickup_latitude"))
    dropoff_lat = radians(col("dropoff_latitude"))

    lat_difference = radians(
        col("dropoff_latitude") - col("pickup_latitude")
    )

    lon_difference = radians(
        col("dropoff_longitude") - col("pickup_longitude")
    )

    haversine_a = (
        sin(lat_difference / 2) ** 2
        + cos(pickup_lat)
        * cos(dropoff_lat)
        * sin(lon_difference / 2) ** 2
    )

    earth_radius_km = lit(6371.0)

    trip_distance_km = (
        2
        * earth_radius_km
        * asin(sqrt(haversine_a))
    )

    df = df.withColumn(
        "trip_distance_km",
        trip_distance_km,
    )

    # =========================================================
    # 7. CALCULATE TRIP SPEED
    # =========================================================

    print("Calculating trip speed...")

    df = df.withColumn(
        "trip_speed_kmh",
        when(
            col("trip_duration") > 0,
            col("trip_distance_km")
            / (col("trip_duration") / 3600.0),
        ).otherwise(None),
    )

    # =========================================================
    # 8. ADD DATE/TIME FEATURES
    # =========================================================

    print("Creating date and time dimensions...")

    df = (
        df
        .withColumn(
            "pickup_date",
            to_date(col("pickup_datetime")),
        )
        .withColumn(
            "pickup_hour",
            hour(col("pickup_datetime")),
        )
        .withColumn(
            "pickup_day_of_week",
            dayofweek(col("pickup_datetime")),
        )
        .withColumn(
            "pickup_month",
            month(col("pickup_datetime")),
        )
    )

    # =========================================================
    # 9. VALIDATE DERIVED METRICS
    # =========================================================

    print("Applying derived metric validation...")

    df = df.filter(
        (col("trip_distance_km") >= 0)
        & (col("trip_distance_km") <= 500)
        & (col("trip_speed_kmh") >= 0)
        & (col("trip_speed_kmh") <= 200)
    )

    # =========================================================
    # 10. SELECT FINAL SILVER COLUMNS
    # =========================================================

    silver_df = df.select(
        "id",
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_longitude",
        "dropoff_latitude",
        "store_and_fwd_flag",
        "trip_duration",
        "trip_distance_km",
        "trip_speed_kmh",
        "pickup_date",
        "pickup_hour",
        "pickup_day_of_week",
        "pickup_month",
    )

    # =========================================================
    # 11. COUNT SILVER RECORDS
    # =========================================================

    silver_count = silver_df.count()

    rows_removed = bronze_count - silver_count

    print("\n=== TRANSFORMATION SUMMARY ===")

    print(f"Bronze rows:  {bronze_count:,}")
    print(f"Silver rows:  {silver_count:,}")
    print(f"Rows removed: {rows_removed:,}")

    # =========================================================
    # 12. SILVER SCHEMA
    # =========================================================

    print("\n=== SILVER SCHEMA ===")

    silver_df.printSchema()

    # =========================================================
    # 13. SILVER SAMPLE
    # =========================================================

    print("\n=== SILVER SAMPLE ===")

    silver_df.show(
        10,
        truncate=False,
    )

    # =========================================================
    # 14. WRITE SILVER PARQUET
    # =========================================================

    print("\nWriting Silver data to:")

    print(SILVER_PATH)

    (
        silver_df
        .write
        .mode("overwrite")
        .parquet(str(SILVER_PATH))
    )

    # =========================================================
    # FINAL STATUS
    # =========================================================

    print("\n" + "=" * 60)
    print("SILVER TRANSFORMATION COMPLETE")
    print("=" * 60)

    print("Silver data written successfully to:")
    print(SILVER_PATH)

    spark.stop()


if __name__ == "__main__":
    main()

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    hour,
    sum as spark_sum,
    to_date,
)

from src.data_quality import missing_required_columns
from src.schemas import taxi_schema
from src.spark_session import get_spark_session
from src.transformations import (
    add_time_features,
    add_trip_distance,
    add_trip_speed,
    validate_business_rules,
    validate_derived_metrics,
    validate_required_columns,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_PATH = PROJECT_ROOT / "data" / "raw" / "train.csv"

BRONZE_PATH = PROJECT_ROOT / "data" / "processed" / "bronze"
SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"

GOLD_PATH = PROJECT_ROOT / "data" / "processed" / "gold"

DAILY_GOLD_PATH = GOLD_PATH / "daily_metrics"
HOURLY_GOLD_PATH = GOLD_PATH / "hourly_metrics"
VENDOR_GOLD_PATH = GOLD_PATH / "vendor_metrics"


def create_bronze(spark: SparkSession) -> DataFrame:
    """
    Read raw CSV data and write the Bronze layer.
    """

    print("\n[BRONZE] Reading raw data...")
    print(f"[BRONZE] Input: {RAW_PATH}")

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(taxi_schema)
        .csv(str(RAW_PATH))
    )

    print(f"[BRONZE] Records: {df.count():,}")

    (
        df.write
        .mode("overwrite")
        .parquet(str(BRONZE_PATH))
    )

    print(f"[BRONZE] Written to: {BRONZE_PATH}")

    return df


def create_silver(df: DataFrame) -> DataFrame:
    """
    Validate and transform Bronze data into Silver.
    """

    print("\n[SILVER] Checking required columns...")

    missing = missing_required_columns(df)

    if missing:
        raise ValueError(
            "Missing required columns: "
            + ", ".join(missing)
        )

    print("[SILVER] Required columns: OK")

    df = validate_required_columns(df)

    print(
        f"[SILVER] After null validation: "
        f"{df.count():,}"
    )

    df = validate_business_rules(df)

    print(
        f"[SILVER] After business rules: "
        f"{df.count():,}"
    )

    df = add_trip_distance(df)

    df = add_trip_speed(df)

    df = add_time_features(df)

    df = validate_derived_metrics(df)

    print(
        f"[SILVER] Final records: "
        f"{df.count():,}"
    )

    (
        df.write
        .mode("overwrite")
        .parquet(str(SILVER_PATH))
    )

    print(f"[SILVER] Written to: {SILVER_PATH}")

    return df


def create_daily_metrics(df: DataFrame) -> DataFrame:
    """
    Create daily Gold metrics.
    """

    print("\n[GOLD] Creating daily metrics...")

    result = (
        df.groupBy(
            to_date("pickup_datetime").alias("pickup_date")
        )
        .agg(
            count("*").alias("total_trips"),
            spark_sum("passenger_count").alias(
                "total_passengers"
            ),
            avg("trip_duration").alias(
                "avg_trip_duration_sec"
            ),
            avg("trip_distance_km").alias(
                "avg_trip_distance_km"
            ),
            avg("trip_speed_kmh").alias(
                "avg_trip_speed_kmh"
            ),
        )
        .orderBy("pickup_date")
    )

    (
        result.write
        .mode("overwrite")
        .parquet(str(DAILY_GOLD_PATH))
    )

    print(
        f"[GOLD] Daily metrics: "
        f"{result.count():,} rows"
    )

    return result


def create_hourly_metrics(df: DataFrame) -> DataFrame:
    """
    Create hourly Gold metrics.
    """

    print("\n[GOLD] Creating hourly metrics...")

    result = (
        df.groupBy(
            to_date("pickup_datetime").alias(
                "pickup_date"
            ),
            hour("pickup_datetime").alias(
                "pickup_hour"
            ),
        )
        .agg(
            count("*").alias("total_trips"),
            spark_sum("passenger_count").alias(
                "total_passengers"
            ),
            avg("trip_duration").alias(
                "avg_trip_duration_sec"
            ),
            avg("trip_distance_km").alias(
                "avg_trip_distance_km"
            ),
            avg("trip_speed_kmh").alias(
                "avg_trip_speed_kmh"
            ),
        )
        .orderBy(
            "pickup_date",
            "pickup_hour",
        )
    )

    (
        result.write
        .mode("overwrite")
        .parquet(str(HOURLY_GOLD_PATH))
    )

    print(
        f"[GOLD] Hourly metrics: "
        f"{result.count():,} rows"
    )

    return result


def create_vendor_metrics(df: DataFrame) -> DataFrame:
    """
    Create vendor Gold metrics.
    """

    print("\n[GOLD] Creating vendor metrics...")

    result = (
        df.groupBy("vendor_id")
        .agg(
            count("*").alias("total_trips"),
            spark_sum("passenger_count").alias(
                "total_passengers"
            ),
            avg("trip_duration").alias(
                "avg_trip_duration_sec"
            ),
            avg("trip_distance_km").alias(
                "avg_trip_distance_km"
            ),
            avg("trip_speed_kmh").alias(
                "avg_trip_speed_kmh"
            ),
        )
        .orderBy("vendor_id")
    )

    (
        result.write
        .mode("overwrite")
        .parquet(str(VENDOR_GOLD_PATH))
    )

    print(
        f"[GOLD] Vendor metrics: "
        f"{result.count():,} rows"
    )

    return result


def run_pipeline(
    spark: SparkSession | None = None,
) -> None:
    """
    Run Bronze -> Silver -> Gold.

    If Spark is supplied by pytest, reuse it and do not stop it.

    If Spark is not supplied, create a Spark session and stop it
    when the pipeline finishes.
    """

    owns_spark = spark is None

    if spark is None:
        spark = get_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("DATA ENGINEERING PIPELINE")
    print("=" * 60)

    try:
        bronze_df = create_bronze(spark)

        silver_df = create_silver(bronze_df)

        create_daily_metrics(silver_df)

        create_hourly_metrics(silver_df)

        create_vendor_metrics(silver_df)

        print("\n" + "=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)

    except Exception:
        print("\n" + "=" * 60)
        print("PIPELINE FAILED")
        print("=" * 60)
        raise

    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    run_pipeline()
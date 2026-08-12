from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "processed" / "gold"

DAILY_PATH = GOLD_PATH / "daily_metrics"
HOURLY_PATH = GOLD_PATH / "hourly_metrics"
VENDOR_PATH = GOLD_PATH / "vendor_metrics"


def main():

    spark = (
        SparkSession.builder
        .appName("GoldVerification")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("GOLD VERIFICATION")
    print("=" * 60)

    # =========================================================
    # 1. READ SILVER
    # =========================================================

    silver_df = spark.read.parquet(
        str(SILVER_PATH)
    )

    silver_count = silver_df.count()

    print(f"\nSilver rows: {silver_count:,}")

    # =========================================================
    # 2. READ GOLD DATASETS
    # =========================================================

    daily_df = spark.read.parquet(
        str(DAILY_PATH)
    )

    hourly_df = spark.read.parquet(
        str(HOURLY_PATH)
    )

    vendor_df = spark.read.parquet(
        str(VENDOR_PATH)
    )

    # =========================================================
    # 3. GOLD ROW COUNTS
    # =========================================================

    daily_count = daily_df.count()
    hourly_count = hourly_df.count()
    vendor_count = vendor_df.count()

    print("\n=== GOLD ROW COUNTS ===")

    print(f"Daily metrics rows:  {daily_count:,}")
    print(f"Hourly metrics rows: {hourly_count:,}")
    print(f"Vendor metrics rows: {vendor_count:,}")

    # =========================================================
    # 4. SCHEMAS
    # =========================================================

    print("\n=== DAILY METRICS SCHEMA ===")

    daily_df.printSchema()

    print("\n=== HOURLY METRICS SCHEMA ===")

    hourly_df.printSchema()

    print("\n=== VENDOR METRICS SCHEMA ===")

    vendor_df.printSchema()

    # =========================================================
    # 5. SAMPLES
    # =========================================================

    print("\n=== DAILY METRICS SAMPLE ===")

    daily_df.show(
        10,
        truncate=False,
    )

    print("\n=== HOURLY METRICS ===")

    hourly_df.orderBy(
        "pickup_hour"
    ).show(
        24,
        truncate=False,
    )

    print("\n=== VENDOR METRICS ===")

    vendor_df.orderBy(
        "vendor_id"
    ).show(
        truncate=False,
    )

    # =========================================================
    # 6. NULL CHECKS
    # =========================================================

    print("\n=== NULL CHECKS ===")

    # IMPORTANT:
    # sum(isNull()) counts actual null values.
    # count(isNull()) would count every row because
    # the boolean expression itself is non-null.

    daily_nulls = daily_df.select(
        *[
            sum(
                col(c).isNull().cast("int")
            ).alias(c)
            for c in daily_df.columns
        ]
    ).collect()[0]

    hourly_nulls = hourly_df.select(
        *[
            sum(
                col(c).isNull().cast("int")
            ).alias(c)
            for c in hourly_df.columns
        ]
    ).collect()[0]

    vendor_nulls = vendor_df.select(
        *[
            sum(
                col(c).isNull().cast("int")
            ).alias(c)
            for c in vendor_df.columns
        ]
    ).collect()[0]

    print("\nDaily null counts:")

    print(
        daily_nulls.asDict()
    )

    print("\nHourly null counts:")

    print(
        hourly_nulls.asDict()
    )

    print("\nVendor null counts:")

    print(
        vendor_nulls.asDict()
    )

    # =========================================================
    # 7. RECONCILIATION CHECKS
    # =========================================================

    print("\n=== RECONCILIATION CHECKS ===")

    daily_trip_total = (
        daily_df
        .agg(
            sum("total_trips")
        )
        .collect()[0][0]
    )

    hourly_trip_total = (
        hourly_df
        .agg(
            sum("total_trips")
        )
        .collect()[0][0]
    )

    vendor_trip_total = (
        vendor_df
        .agg(
            sum("total_trips")
        )
        .collect()[0][0]
    )

    print(
        f"Silver total trips:  {silver_count:,}"
    )

    print(
        f"Daily total trips:   {daily_trip_total:,}"
    )

    print(
        f"Hourly total trips:  {hourly_trip_total:,}"
    )

    print(
        f"Vendor total trips:  {vendor_trip_total:,}"
    )

    # =========================================================
    # 8. RECONCILIATION STATUS
    # =========================================================

    daily_reconciles = (
        daily_trip_total == silver_count
    )

    hourly_reconciles = (
        hourly_trip_total == silver_count
    )

    vendor_reconciles = (
        vendor_trip_total == silver_count
    )

    if daily_reconciles:
        print(
            "PASS: Daily trips reconcile with Silver."
        )
    else:
        print(
            "FAIL: Daily trips do NOT reconcile with Silver."
        )

    if hourly_reconciles:
        print(
            "PASS: Hourly trips reconcile with Silver."
        )
    else:
        print(
            "FAIL: Hourly trips do NOT reconcile with Silver."
        )

    if vendor_reconciles:
        print(
            "PASS: Vendor trips reconcile with Silver."
        )
    else:
        print(
            "FAIL: Vendor trips do NOT reconcile with Silver."
        )

    # =========================================================
    # 9. HOURLY VALIDATION
    # =========================================================

    invalid_hours = hourly_df.filter(
        (col("pickup_hour") < 0)
        | (col("pickup_hour") > 23)
        | col("pickup_hour").isNull()
    ).count()

    print(
        f"\nInvalid hourly records: {invalid_hours}"
    )

    if invalid_hours == 0:
        print(
            "PASS: All pickup hours are between 0 and 23."
        )
    else:
        print(
            "FAIL: Invalid pickup hours found."
        )

    # =========================================================
    # 10. VENDOR VALIDATION
    # =========================================================

    invalid_vendors = vendor_df.filter(
        col("vendor_id").isNull()
    ).count()

    print(
        f"Invalid vendor records: {invalid_vendors}"
    )

    if invalid_vendors == 0:
        print(
            "PASS: Vendor IDs are populated."
        )
    else:
        print(
            "FAIL: Null vendor IDs found."
        )

    # =========================================================
    # 11. METRIC SANITY CHECKS
    # =========================================================

    invalid_daily_metrics = daily_df.filter(
        (col("total_trips") <= 0)
        | (col("total_passengers") <= 0)
        | (col("avg_trip_duration_sec") <= 0)
        | (col("avg_trip_distance_km") < 0)
        | (col("avg_trip_speed_kmh") < 0)
        | col("total_trips").isNull()
        | col("total_passengers").isNull()
        | col("avg_trip_duration_sec").isNull()
        | col("avg_trip_distance_km").isNull()
        | col("avg_trip_speed_kmh").isNull()
    ).count()

    invalid_hourly_metrics = hourly_df.filter(
        (col("total_trips") <= 0)
        | (col("total_passengers") <= 0)
        | (col("avg_trip_duration_sec") <= 0)
        | (col("avg_trip_distance_km") < 0)
        | (col("avg_trip_speed_kmh") < 0)
        | col("total_trips").isNull()
        | col("total_passengers").isNull()
        | col("avg_trip_duration_sec").isNull()
        | col("avg_trip_distance_km").isNull()
        | col("avg_trip_speed_kmh").isNull()
    ).count()

    invalid_vendor_metrics = vendor_df.filter(
        (col("total_trips") <= 0)
        | (col("total_passengers") <= 0)
        | (col("avg_trip_duration_sec") <= 0)
        | (col("avg_trip_distance_km") < 0)
        | (col("avg_trip_speed_kmh") < 0)
        | col("total_trips").isNull()
        | col("total_passengers").isNull()
        | col("avg_trip_duration_sec").isNull()
        | col("avg_trip_distance_km").isNull()
        | col("avg_trip_speed_kmh").isNull()
    ).count()

    print("\n=== METRIC SANITY ===")

    print(
        f"Invalid daily metric rows:  "
        f"{invalid_daily_metrics}"
    )

    print(
        f"Invalid hourly metric rows: "
        f"{invalid_hourly_metrics}"
    )

    print(
        f"Invalid vendor metric rows: "
        f"{invalid_vendor_metrics}"
    )

    # =========================================================
    # 12. NULL SUMMARY
    # =========================================================

    daily_has_nulls = any(
        value > 0
        for value in daily_nulls.asDict().values()
        if value is not None
    )

    hourly_has_nulls = any(
        value > 0
        for value in hourly_nulls.asDict().values()
        if value is not None
    )

    vendor_has_nulls = any(
        value > 0
        for value in vendor_nulls.asDict().values()
        if value is not None
    )

    null_checks_passed = (
        not daily_has_nulls
        and not hourly_has_nulls
        and not vendor_has_nulls
    )

    if null_checks_passed:
        print(
            "\nPASS: No null values found in Gold datasets."
        )
    else:
        print(
            "\nFAIL: Null values found in Gold datasets."
        )

    # =========================================================
    # 13. FINAL STATUS
    # =========================================================

    all_checks_passed = (
        daily_reconciles
        and hourly_reconciles
        and vendor_reconciles
        and invalid_hours == 0
        and invalid_vendors == 0
        and invalid_daily_metrics == 0
        and invalid_hourly_metrics == 0
        and invalid_vendor_metrics == 0
        and null_checks_passed
    )

    print("\n" + "=" * 60)

    if all_checks_passed:

        print(
            "GOLD VERIFICATION COMPLETE - ALL CHECKS PASSED"
        )

    else:

        print(
            "GOLD VERIFICATION COMPLETE - CHECK FAILURES ABOVE"
        )

    print("=" * 60)

    spark.stop()

    return all_checks_passed


if __name__ == "__main__":
    main()

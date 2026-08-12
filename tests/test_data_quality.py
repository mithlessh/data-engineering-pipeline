from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def test_missing_required_columns(spark):
    from src.data_quality import missing_required_columns

    df = spark.createDataFrame(
        [
            (1, 1),
        ],
        ["id", "vendor_id"],
    )

    missing = missing_required_columns(df)

    assert "pickup_datetime" in missing
    assert "dropoff_datetime" in missing
    assert "passenger_count" in missing


def test_required_columns_present(spark):
    from src.data_quality import REQUIRED_COLUMNS
    from src.data_quality import missing_required_columns

    # Spark cannot infer types from an all-None row.
    # Provide an explicit schema instead.
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("trip_duration", IntegerType(), True),
    ])

    df = spark.createDataFrame(
        [
            [None] * len(REQUIRED_COLUMNS)
        ],
        schema=schema,
    )

    assert missing_required_columns(df) == []


def test_null_counts(spark):
    from src.data_quality import null_counts

    df = spark.createDataFrame(
        [
            (1, "A"),
            (None, "B"),
            (3, None),
        ],
        ["id", "value"],
    )

    counts = null_counts(df)

    assert counts["id"] == 1
    assert counts["value"] == 1


def test_has_nulls_detects_null_values(spark):
    from src.data_quality import has_nulls

    df = spark.createDataFrame(
        [
            (1,),
            (None,),
        ],
        ["value"],
    )

    assert has_nulls(df) is True


def test_has_nulls_returns_false_for_clean_data(spark):
    from src.data_quality import has_nulls

    df = spark.createDataFrame(
        [
            (1,),
            (2,),
            (3,),
        ],
        ["value"],
    )

    assert has_nulls(df) is False


def test_invalid_pickup_hours(spark):
    from src.data_quality import invalid_pickup_hours

    df = spark.createDataFrame(
        [
            (0,),
            (12,),
            (23,),
            (24,),
            (-1,),
            (None,),
        ],
        ["pickup_hour"],
    )

    assert invalid_pickup_hours(df) == 3


def test_invalid_vendor_ids(spark):
    from src.data_quality import invalid_vendor_ids

    df = spark.createDataFrame(
        [
            (1,),
            (2,),
            (None,),
            (0,),
        ],
        ["vendor_id"],
    )

    assert invalid_vendor_ids(df) == 2


def test_valid_gold_metrics(spark):
    from src.data_quality import validate_gold_metrics

    df = spark.createDataFrame(
        [
            (100, 150, 600.0, 5.0, 30.0),
        ],
        [
            "total_trips",
            "total_passengers",
            "avg_trip_duration_sec",
            "avg_trip_distance_km",
            "avg_trip_speed_kmh",
        ],
    )

    assert validate_gold_metrics(df) == 0


def test_invalid_gold_metrics(spark):
    from src.data_quality import validate_gold_metrics

    df = spark.createDataFrame(
        [
            (0, 150, 600.0, 5.0, 30.0),
            (100, 0, 600.0, 5.0, 30.0),
            (100, 150, 0.0, 5.0, 30.0),
            (100, 150, 600.0, -1.0, 30.0),
            (100, 150, 600.0, 5.0, -1.0),
        ],
        [
            "total_trips",
            "total_passengers",
            "avg_trip_duration_sec",
            "avg_trip_distance_km",
            "avg_trip_speed_kmh",
        ],
    )

    assert validate_gold_metrics(df) == 5
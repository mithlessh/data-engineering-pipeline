from datetime import datetime

import pytest

from src.transformations import (
    add_time_features,
    add_trip_distance,
    add_trip_speed,
    validate_business_rules,
    validate_derived_metrics,
    validate_required_columns,
)


def test_add_trip_distance(spark):
    df = spark.createDataFrame(
        [
            (
                40.7128,
                -74.0060,
                40.7306,
                -73.9352,
            )
        ],
        [
            "pickup_latitude",
            "pickup_longitude",
            "dropoff_latitude",
            "dropoff_longitude",
        ],
    )

    result = add_trip_distance(df).collect()[0]

    assert result["trip_distance_km"] > 0
    assert result["trip_distance_km"] < 20


def test_add_trip_distance_same_location(spark):
    df = spark.createDataFrame(
        [
            (
                40.7128,
                -74.0060,
                40.7128,
                -74.0060,
            )
        ],
        [
            "pickup_latitude",
            "pickup_longitude",
            "dropoff_latitude",
            "dropoff_longitude",
        ],
    )

    result = add_trip_distance(df).collect()[0]

    assert abs(result["trip_distance_km"]) < 0.001


def test_add_trip_speed(spark):
    df = spark.createDataFrame(
        [
            (10.0, 1800),
        ],
        [
            "trip_distance_km",
            "trip_duration",
        ],
    )

    result = add_trip_speed(df).collect()[0]

    assert result["trip_speed_kmh"] == pytest.approx(20.0)


def test_add_trip_speed_zero_duration(spark):
    df = spark.createDataFrame(
        [
            (10.0, 0),
        ],
        [
            "trip_distance_km",
            "trip_duration",
        ],
    )

    result = add_trip_speed(df).collect()[0]

    assert result["trip_speed_kmh"] is None


def test_add_time_features(spark):
    df = spark.createDataFrame(
        [
            (
                datetime(2016, 4, 4, 22, 10, 50),
            )
        ],
        ["pickup_datetime"],
    )

    result = add_time_features(df).collect()[0]

    assert str(result["pickup_date"]) == "2016-04-04"
    assert result["pickup_hour"] == 22
    assert result["pickup_month"] == 4
    assert result["pickup_day_of_week"] == 2


def test_validate_required_columns_removes_nulls(spark):
    df = spark.createDataFrame(
        [
            ("id1", 1, 600),
            ("id2", None, 700),
        ],
        [
            "id",
            "vendor_id",
            "trip_duration",
        ],
    )

    # The reusable validation function expects the complete
    # required schema, so create the full DataFrame below.
    from src.data_quality import REQUIRED_COLUMNS

    rows = [
        [
            "id1",
            1,
            datetime(2016, 1, 1, 10),
            datetime(2016, 1, 1, 10, 10),
            1,
            -73.99,
            40.75,
            -73.98,
            40.76,
            "N",
            600,
        ],
        [
            "id2",
            None,
            datetime(2016, 1, 1, 10),
            datetime(2016, 1, 1, 10, 10),
            1,
            -73.99,
            40.75,
            -73.98,
            40.76,
            "N",
            600,
        ],
    ]

    full_df = spark.createDataFrame(
        rows,
        REQUIRED_COLUMNS + ["store_and_fwd_flag"],
    )

    result = validate_required_columns(full_df)

    assert result.count() == 1
    assert result.first()["id"] == "id1"


def test_validate_business_rules(spark):
    rows = [
        (
            "valid",
            1,
            datetime(2016, 1, 1, 10),
            datetime(2016, 1, 1, 10, 10),
            2,
            -73.99,
            40.75,
            -73.98,
            40.76,
            "N",
            600,
        ),
        (
            "bad_passengers",
            0,
            datetime(2016, 1, 1, 10),
            datetime(2016, 1, 1, 10, 10),
            2,
            -73.99,
            40.75,
            -73.98,
            40.76,
            "N",
            600,
        ),
        (
            "bad_duration",
            1,
            datetime(2016, 1, 1, 10),
            datetime(2016, 1, 1, 10, 10),
            2,
            -73.99,
            40.75,
            -73.98,
            40.76,
            "N",
            0,
        ),
    ]

    columns = [
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
    ]

    df = spark.createDataFrame(rows, columns)

    result = validate_business_rules(df)

    assert result.count() == 1
    assert result.first()["id"] == "valid"


def test_validate_derived_metrics(spark):
    df = spark.createDataFrame(
        [
            (3.5, 20.0),
            (-1.0, 20.0),
            (3.5, 250.0),
        ],
        [
            "trip_distance_km",
            "trip_speed_kmh",
        ],
    )

    result = validate_derived_metrics(df)

    assert result.count() == 1

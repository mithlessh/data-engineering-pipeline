from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum


REQUIRED_COLUMNS = [
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


def missing_required_columns(
    df: DataFrame,
) -> list[str]:
    """
    Return required columns that are missing from a DataFrame.
    """

    return [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]


def null_counts(
    df: DataFrame,
) -> dict[str, int]:
    """
    Return the number of null values for every DataFrame column.
    """

    expressions = [
        spark_sum(
            col(column)
            .isNull()
            .cast("int")
        ).alias(column)
        for column in df.columns
    ]

    row = df.select(expressions).collect()[0]

    return {
        column: int(row[column] or 0)
        for column in df.columns
    }


def has_nulls(
    df: DataFrame,
) -> bool:
    """
    Return True when any DataFrame column contains nulls.
    """

    counts = null_counts(df)

    return any(
        count > 0
        for count in counts.values()
    )


def invalid_pickup_hours(
    df: DataFrame,
) -> int:
    """
    Count records with invalid pickup hours.
    """

    return df.filter(
        (col("pickup_hour") < 0)
        | (col("pickup_hour") > 23)
        | col("pickup_hour").isNull()
    ).count()


def invalid_vendor_ids(
    df: DataFrame,
) -> int:
    """
    Count records with invalid vendor IDs.

    Vendor IDs must be positive and non-null.
    """

    return df.filter(
        col("vendor_id").isNull()
        | (col("vendor_id") <= 0)
    ).count()


def validate_gold_metrics(
    df: DataFrame,
) -> int:
    """
    Count invalid aggregated metric records.
    """

    return df.filter(
        (col("total_trips") <= 0)
        | (col("total_passengers") <= 0)
        | (col("avg_trip_duration_sec") <= 0)
        | (col("avg_trip_distance_km") < 0)
        | (col("avg_trip_speed_kmh") < 0)
    ).count()
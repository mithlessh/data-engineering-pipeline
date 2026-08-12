from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    asin,
    col,
    cos,
    dayofweek,
    hour,
    lit,
    month,
    radians,
    sin,
    sqrt,
    to_date,
    when,
)


def add_trip_distance(df: DataFrame) -> DataFrame:
    """
    Calculate great-circle trip distance using the Haversine formula.

    Returns:
        DataFrame with trip_distance_km column added.
    """

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

    return df.withColumn(
        "trip_distance_km",
        trip_distance_km,
    )


def add_trip_speed(df: DataFrame) -> DataFrame:
    """
    Calculate trip speed in kilometres per hour.
    """

    return df.withColumn(
        "trip_speed_kmh",
        when(
            col("trip_duration") > 0,
            col("trip_distance_km")
            / (col("trip_duration") / 3600.0),
        ).otherwise(None),
    )


def add_time_features(df: DataFrame) -> DataFrame:
    """
    Add commonly used pickup datetime features.
    """

    return (
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


def validate_required_columns(
    df: DataFrame,
) -> DataFrame:
    """
    Remove rows missing fields required for trip analysis.
    """

    required_columns = [
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

    return df.dropna(subset=required_columns)


def validate_business_rules(
    df: DataFrame,
) -> DataFrame:
    """
    Apply core business and geographic validation rules.

    Valid records must have:
    - A positive vendor ID
    - Passenger count between 1 and 8
    - Trip duration between 1 second and 24 hours
    - Valid longitude and latitude ranges
    - Dropoff after pickup
    """

    return df.filter(
        # Vendor ID must be present and positive.
        col("vendor_id").isNotNull()
        & (col("vendor_id") > 0)

        # Passenger count must be between 1 and 8.
        & col("passenger_count").isNotNull()
        & (col("passenger_count") > 0)
        & (col("passenger_count") <= 8)

        # Trip duration must be between 1 second and 24 hours.
        & col("trip_duration").isNotNull()
        & (col("trip_duration") > 0)
        & (col("trip_duration") <= 24 * 60 * 60)

        # Pickup longitude.
        & col("pickup_longitude").isNotNull()
        & (col("pickup_longitude") >= -180)
        & (col("pickup_longitude") <= 180)

        # Dropoff longitude.
        & col("dropoff_longitude").isNotNull()
        & (col("dropoff_longitude") >= -180)
        & (col("dropoff_longitude") <= 180)

        # Pickup latitude.
        & col("pickup_latitude").isNotNull()
        & (col("pickup_latitude") >= -90)
        & (col("pickup_latitude") <= 90)

        # Dropoff latitude.
        & col("dropoff_latitude").isNotNull()
        & (col("dropoff_latitude") >= -90)
        & (col("dropoff_latitude") <= 90)

        # Dropoff must occur after pickup.
        & col("dropoff_datetime").isNotNull()
        & col("pickup_datetime").isNotNull()
        & (col("dropoff_datetime") > col("pickup_datetime"))
    )


def validate_derived_metrics(
    df: DataFrame,
) -> DataFrame:
    """
    Remove impossible derived distance and speed values.
    """

    return df.filter(
        (col("trip_distance_km") >= 0)
        & (col("trip_distance_km") <= 500)
        & (col("trip_speed_kmh") >= 0)
        & (col("trip_speed_kmh") <= 200)
    )
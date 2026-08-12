from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


taxi_schema = StructType([
    StructField(
        "id",
        StringType(),
        True,
    ),

    StructField(
        "vendor_id",
        IntegerType(),
        True,
    ),

    StructField(
        "pickup_datetime",
        TimestampType(),
        True,
    ),

    StructField(
        "dropoff_datetime",
        TimestampType(),
        True,
    ),

    StructField(
        "passenger_count",
        IntegerType(),
        True,
    ),

    StructField(
        "pickup_longitude",
        DoubleType(),
        True,
    ),

    StructField(
        "pickup_latitude",
        DoubleType(),
        True,
    ),

    StructField(
        "dropoff_longitude",
        DoubleType(),
        True,
    ),

    StructField(
        "dropoff_latitude",
        DoubleType(),
        True,
    ),

    StructField(
        "store_and_fwd_flag",
        StringType(),
        True,
    ),

    StructField(
        "trip_duration",
        IntegerType(),
        True,
    ),
])

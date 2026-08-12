import os
import sys

from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """
    Create and return the Spark session used by the pipeline.

    Configures PySpark to use the current Python interpreter
    and runs Spark locally with all available CPU cores.
    """

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    return (
        SparkSession.builder
        .appName("DataEngineeringPipeline")
        .master("local[*]")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

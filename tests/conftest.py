import os
import sys
from pathlib import Path

import pytest


# ============================================================
# Project configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# Python configuration for PySpark
# ============================================================
#
# Force Spark to use the exact same Python interpreter that
# pytest is currently running.
#

PYTHON_EXECUTABLE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE


# ============================================================
# Spark fixture
# ============================================================

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("DataEngineeringPipelineTests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")

        # Explicitly tell Spark which Python executable to use
        # for both driver and worker processes.
        .config("spark.pyspark.python", PYTHON_EXECUTABLE)
        .config("spark.pyspark.driver.python", PYTHON_EXECUTABLE)

        .getOrCreate()
    )

    # Keep test output clean.
    spark.sparkContext.setLogLevel("ERROR")

    # Diagnostic information. Useful while fixing the
    # Windows/PySpark Python-worker configuration.
    print("\n" + "=" * 60)
    print("Spark test configuration")
    print("=" * 60)
    print(f"Python executable : {PYTHON_EXECUTABLE}")
    print(f"Python version    : {sys.version.split()[0]}")
    print(f"PySpark version   : {spark.version}")
    print(
        "Spark Python      : "
        f"{spark.conf.get('spark.pyspark.python', 'not set')}"
    )
    print(
        "Driver Python     : "
        f"{spark.conf.get('spark.pyspark.driver.python', 'not set')}"
    )
    print("=" * 60)

    yield spark

    # Cleanly shut down Spark after the entire test session.
    spark.stop()

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "bronze"
)

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
)

GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
)

DAILY_PATH = GOLD_PATH / "daily_metrics"
HOURLY_PATH = GOLD_PATH / "hourly_metrics"
VENDOR_PATH = GOLD_PATH / "vendor_metrics"


def test_pipeline_outputs_exist(spark):
    """
    Run the complete pipeline and verify that all expected
    Bronze, Silver, and Gold outputs are created.
    """

    from src.pipeline import run_pipeline

    run_pipeline(spark)

    assert BRONZE_PATH.exists()
    assert SILVER_PATH.exists()

    assert DAILY_PATH.exists()
    assert HOURLY_PATH.exists()
    assert VENDOR_PATH.exists()

    assert (BRONZE_PATH / "_SUCCESS").exists()
    assert (SILVER_PATH / "_SUCCESS").exists()

    assert (DAILY_PATH / "_SUCCESS").exists()
    assert (HOURLY_PATH / "_SUCCESS").exists()
    assert (VENDOR_PATH / "_SUCCESS").exists()
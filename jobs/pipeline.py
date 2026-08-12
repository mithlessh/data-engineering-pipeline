import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from jobs.ingest import main as ingest_main
from jobs.transform import main as transform_main
from jobs.aggregate import main as aggregate_main
from jobs.verify_bronze import main as verify_bronze_main
from jobs.verify_silver import main as verify_silver_main
from jobs.verify_gold import main as verify_gold_main


def run_stage(stage_name, stage_function):
    """
    Execute one pipeline stage and report execution time.
    """

    print("\n")
    print("=" * 70)
    print(f"PIPELINE STAGE: {stage_name}")
    print("=" * 70)

    start_time = time.time()

    stage_function()

    elapsed = time.time() - start_time

    print("\n" + "-" * 70)
    print(f"COMPLETED: {stage_name}")
    print(f"Execution time: {elapsed:.2f} seconds")
    print("-" * 70)


def main():

    pipeline_start = time.time()

    print("\n")
    print("=" * 70)
    print("NYC TAXI DATA ENGINEERING ETL PIPELINE")
    print("=" * 70)

    print("\nProject root:")
    print(PROJECT_ROOT)

    print("\nPipeline:")
    print("RAW CSV")
    print("  -> BRONZE")
    print("  -> SILVER")
    print("  -> GOLD")
    print("  -> DATA QUALITY VERIFICATION")

    try:

        # =====================================================
        # 1. INGESTION
        # =====================================================

        run_stage(
            "BRONZE INGESTION",
            ingest_main,
        )

        # =====================================================
        # 2. SILVER TRANSFORMATION
        # =====================================================

        run_stage(
            "SILVER TRANSFORMATION",
            transform_main,
        )

        # =====================================================
        # 3. GOLD AGGREGATION
        # =====================================================

        run_stage(
            "GOLD AGGREGATION",
            aggregate_main,
        )

        # =====================================================
        # 4. BRONZE VERIFICATION
        # =====================================================

        run_stage(
            "BRONZE VERIFICATION",
            verify_bronze_main,
        )

        # =====================================================
        # 5. SILVER VERIFICATION
        # =====================================================

        run_stage(
            "SILVER VERIFICATION",
            verify_silver_main,
        )

        # =====================================================
        # 6. GOLD VERIFICATION
        # =====================================================

        run_stage(
            "GOLD VERIFICATION",
            verify_gold_main,
        )

        # =====================================================
        # FINAL STATUS
        # =====================================================

        total_time = time.time() - pipeline_start

        print("\n")
        print("=" * 70)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print(f"\nTotal pipeline execution time: {total_time:.2f} seconds")

        print("\nData flow completed:")

        print("  RAW CSV")
        print("     ↓")
        print("  BRONZE PARQUET")
        print("     ↓")
        print("  SILVER PARQUET")
        print("     ↓")
        print("  GOLD METRICS")
        print("     ↓")
        print("  QUALITY VERIFICATION")

        print("\nGold datasets produced:")

        print("  - daily_metrics")
        print("  - hourly_metrics")
        print("  - vendor_metrics")

        print("\nFinal validation:")

        print("  - Bronze ingestion verified")
        print("  - Silver transformation verified")
        print("  - Gold reconciliation verified")
        print("  - Metric sanity checks passed")

        print("\n" + "=" * 70)
        print("ALL ETL STAGES PASSED")
        print("=" * 70)

    except Exception as exc:

        total_time = time.time() - pipeline_start

        print("\n")
        print("=" * 70)
        print("ETL PIPELINE FAILED")
        print("=" * 70)

        print(f"\nFailure: {type(exc).__name__}")
        print(f"Message: {exc}")
        print(f"Elapsed time: {total_time:.2f} seconds")

        raise


if __name__ == "__main__":
    main()

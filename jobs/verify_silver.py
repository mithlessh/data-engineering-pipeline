from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"


def main():

    spark = (
        SparkSession.builder
        .appName("SilverVerification")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("SILVER VERIFICATION")
    print("=" * 60)

    print("\nReading Silver data from:")

    print(SILVER_PATH)

    df = spark.read.parquet(str(SILVER_PATH))

    # =========================================================
    # ROW COUNT
    # =========================================================

    row_count = df.count()

    print(f"\nRows: {row_count:,}")

    # =========================================================
    # COLUMNS / SCHEMA
    # =========================================================

    print("\n=== COLUMNS ===")

    for field in df.schema.fields:
        print(f"- {field.name}: {field.dataType}")

    # =========================================================
    # SAMPLE DATA
    # =========================================================

    print("\n=== SAMPLE DATA ===")

    df.show(
        10,
        truncate=False,
    )

    # =========================================================
    # FINAL STATUS
    # =========================================================

    print("\n" + "=" * 60)
    print("SILVER VERIFICATION COMPLETE")
    print("=" * 60)

    spark.stop()

    return row_count


if __name__ == "__main__":
    main()

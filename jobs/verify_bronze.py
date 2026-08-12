import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.spark_session import get_spark_session


def main():

    spark = get_spark_session()

    bronze_dir = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "bronze"
    )

    print("\n" + "=" * 60)
    print("BRONZE DATA VERIFICATION")
    print("=" * 60)

    print("\nReading from:")

    print(bronze_dir)

    df = spark.read.parquet(
        str(bronze_dir)
    )

    # =========================================================
    # SCHEMA
    # =========================================================

    print("\n=== SCHEMA ===")

    df.printSchema()

    # =========================================================
    # ROW COUNT
    # =========================================================

    print("\n=== ROW COUNT ===")

    row_count = df.count()

    print(
        f"Rows: {row_count:,}"
    )

    # =========================================================
    # COLUMNS
    # =========================================================

    print("\n=== COLUMNS ===")

    for column in df.columns:
        print(f"- {column}")

    # =========================================================
    # SAMPLE
    # =========================================================

    print("\n=== SAMPLE DATA ===")

    df.show(
        10,
        truncate=False,
    )

    # =========================================================
    # DATA TYPES
    # =========================================================

    print("\n=== DATA TYPES ===")

    for field in df.schema.fields:
        print(
            f"- {field.name}: {field.dataType}"
        )

    # =========================================================
    # FINAL STATUS
    # =========================================================

    print("\n" + "=" * 60)
    print("BRONZE VERIFICATION COMPLETE")
    print("=" * 60)

    spark.stop()

    return row_count


if __name__ == "__main__":
    main()

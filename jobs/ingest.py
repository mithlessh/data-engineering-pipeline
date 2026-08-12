import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.spark_session import get_spark_session
from src.schemas import taxi_schema


def main():

    spark = get_spark_session()

    input_file = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "train.csv"
    )

    output_dir = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "bronze"
    )

    print("=" * 60)
    print("BRONZE INGESTION STARTED")
    print("=" * 60)

    print("\nReading raw data from:")

    print(input_file)

    print("\nWriting Bronze data to:")

    print(output_dir)

    if not input_file.exists():

        raise FileNotFoundError(
            f"Input file not found: {input_file}"
        )

    # =========================================================
    # READ CSV WITH EXPLICIT SCHEMA
    # =========================================================

    df = (
        spark.read
        .option("header", True)
        .schema(taxi_schema)
        .csv(str(input_file))
    )

    # =========================================================
    # INPUT SUMMARY
    # =========================================================

    row_count = df.count()

    print(
        f"\nRaw rows read: {row_count:,}"
    )

    print("\n=== RAW SCHEMA ===")

    df.printSchema()

    # =========================================================
    # WRITE BRONZE
    # =========================================================

    print("\nStarting Bronze Parquet write...")

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_dir))
    )

    print("\n" + "=" * 60)
    print("BRONZE INGESTION COMPLETE")
    print("=" * 60)

    print(
        f"Bronze rows written: {row_count:,}"
    )

    print(
        f"Bronze path: {output_dir}"
    )

    spark.stop()

    return row_count


if __name__ == "__main__":
    main()

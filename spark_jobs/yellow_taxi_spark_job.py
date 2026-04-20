import argparse
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, to_date, unix_timestamp, year

load_dotenv()

DATA_DIR = os.environ["DATA_DIR"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--load-type", choices=["full", "incremental"], default="full")
    parser.add_argument("--process-month", required=False)
    return parser.parse_args()


def resolve_paths(load_type: str, process_month: str | None) -> tuple[str, str]:
    if load_type == "full":
        input_path = os.path.join(DATA_DIR, "yellow_tripdata_2023-*.parquet")
        output_path = os.path.join(DATA_DIR, "yellow_tripdata_2023_silver_preview.parquet")
        return input_path, output_path

    if not process_month:
        raise ValueError("process_month is required for incremental load")

    input_path = os.path.join(DATA_DIR, f"yellow_tripdata_{process_month}.parquet")
    output_path = os.path.join(DATA_DIR, f"yellow_tripdata_{process_month}_silver_preview.parquet")
    return input_path, output_path


def main() -> None:
    args = parse_args()
    input_path, output_path = resolve_paths(args.load_type, args.process_month)

    print("Starting Spark job...")
    print(f"Load type: {args.load_type}")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")

    spark = (
        SparkSession.builder
        .appName("yellow-taxi-spark-job")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(input_path)

        cleaned_df = (
            df
            .filter(col("trip_distance") > 0)
            .filter(col("fare_amount") > 0)
            .filter(col("tpep_pickup_datetime") <= col("tpep_dropoff_datetime"))
            .filter(col("tpep_pickup_datetime") >= "2023-01-01")
            .filter(col("tpep_pickup_datetime") < "2024-01-01")
            .filter(col("tpep_dropoff_datetime") >= "2023-01-01")
            .filter(col("tpep_dropoff_datetime") < "2024-01-01")
            .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
            .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
            .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
            .withColumn(
                "trip_duration_minutes",
                (
                    unix_timestamp(col("tpep_dropoff_datetime"))
                    - unix_timestamp(col("tpep_pickup_datetime"))
                ) / 60.0
            )
        )

        cleaned_df.write.mode("overwrite").parquet(output_path)

        print(f"Spark job finished successfully. Output written to: {output_path}")

    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()

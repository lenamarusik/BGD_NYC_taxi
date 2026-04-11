import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    month,
    to_date,
    unix_timestamp,
    year,
)

load_dotenv()

DATA_DIR = os.environ["DATA_DIR"]

INPUT_PATH = os.path.join(DATA_DIR, "yellow_tripdata_2023-12.parquet")
OUTPUT_PATH = os.path.join(DATA_DIR, "yellow_tripdata_2023-12_silver_preview.parquet")


def main() -> None:
    print("Starting Spark job...")
    print(f"Input path: {INPUT_PATH}")
    print(f"Output path: {OUTPUT_PATH}")

    spark = (
        SparkSession.builder
        .appName("yellow-taxi-spark-job")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(INPUT_PATH)

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

        cleaned_df.write.mode("overwrite").parquet(OUTPUT_PATH)

        print(f"Spark job finished successfully. Output written to: {OUTPUT_PATH}")

    finally:
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()

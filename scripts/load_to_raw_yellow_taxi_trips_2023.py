from pathlib import Path
from datetime import datetime
from io import StringIO
import argparse
import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.environ["DATA_DIR"])
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

TARGET_COLUMNS = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "source_file",
    "load_timestamp",
]

RENAME_MAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pulocationid",
    "DOLocationID": "dolocationid",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "Airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--load-type", choices=["full", "incremental"], default="full")
    parser.add_argument("--process-month", required=False)
    return parser.parse_args()


def resolve_files(load_type: str, process_month: str | None) -> list[Path]:
    if load_type == "full":
        return sorted(DATA_DIR.glob("yellow_tripdata_2023-*.parquet"))

    if not process_month:
        raise ValueError("process_month is required for incremental load")

    file_path = DATA_DIR / f"yellow_tripdata_{process_month}.parquet"
    return [file_path]


def main() -> None:
    args = parse_args()
    files = resolve_files(args.load_type, args.process_month)

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    print(f"Load type: {args.load_type}")
    print(f"Found {len(files)} files")

    for i, file_path in enumerate(files, start=1):
        print(f"[{i}/{len(files)}] Loading {file_path.name}")

        if not file_path.exists():
            raise FileNotFoundError(f"File does not exist: {file_path}")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM raw.yellow_taxi_trips_2023
                WHERE source_file = %s
                LIMIT 1;
                """,
                (str(file_path),)
            )
            exists = cur.fetchone()

        if exists:
            print(f"Skipping {file_path.name} (already loaded)")
            continue

        df = pd.read_parquet(file_path)
        df = df.rename(columns=RENAME_MAP)

        df["source_file"] = str(file_path)
        df["load_timestamp"] = datetime.now()

        for col in TARGET_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df = df[TARGET_COLUMNS]

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep=",", na_rep="")
        buffer.seek(0)

        with conn.cursor() as cur:
            cur.copy_expert(
                """
                COPY raw.yellow_taxi_trips_2023 (
                    vendor_id,
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    ratecode_id,
                    store_and_fwd_flag,
                    pulocationid,
                    dolocationid,
                    payment_type,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    improvement_surcharge,
                    total_amount,
                    congestion_surcharge,
                    airport_fee,
                    cbd_congestion_fee,
                    source_file,
                    load_timestamp
                )
                FROM STDIN WITH (
                    FORMAT CSV,
                    DELIMITER ',',
                    NULL ''
                )
                """,
                buffer,
            )
        conn.commit()

        print(f"Inserted {len(df)} rows from {file_path.name}")

    conn.close()
    print("Done")


if __name__ == "__main__":
    main()

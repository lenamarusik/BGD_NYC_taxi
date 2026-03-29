from pathlib import Path
from datetime import datetime
from io import StringIO
import os

import pandas as pd
import psycopg2

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

FILES = [DATA_DIR / "yellow_tripdata_2023-12.parquet"]

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
)

print(f"Found {len(FILES)} files")

for i, file_path in enumerate(FILES, start=1):
    print(f"[{i}/{len(FILES)}] Loading {file_path.name}")

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

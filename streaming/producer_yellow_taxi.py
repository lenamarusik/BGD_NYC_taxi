from pathlib import Path
import argparse
import json
import os
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

DATA_DIR = Path(os.environ["DATA_DIR"])
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_YELLOW_TAXI = os.environ.get("KAFKA_TOPIC_YELLOW_TAXI", "yellow_taxi_trips")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--process-month", required=True, help="Month in format YYYY-MM, e.g. 2023-12")
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Optional limit of records to publish to Kafka"
    )
    return parser.parse_args()


def normalize_value(value: Any) -> Any:
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    return value


def main() -> None:
    args = parse_args()

    file_path = DATA_DIR / f"yellow_tripdata_{args.process_month}.parquet"
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC_YELLOW_TAXI}")
    print(f"Reading file: {file_path}")

    df = pd.read_parquet(file_path)

    if args.max_records is not None:
        df = df.head(args.max_records)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
    )

    sent_count = 0

    try:
        for row_number, (_, row) in enumerate(df.iterrows(), start=1):
            payload = {column: normalize_value(row[column]) for column in df.columns}
            payload["source_file"] = str(file_path)
            payload["stream_row_number"] = row_number

            message_key = f"{file_path.name}:{row_number}"

            producer.send(
                KAFKA_TOPIC_YELLOW_TAXI,
                key=message_key,
                value=payload,
            )
            sent_count += 1

        producer.flush()
        print(f"Published {sent_count} records to Kafka topic '{KAFKA_TOPIC_YELLOW_TAXI}'")

    finally:
        producer.close()


if __name__ == "__main__":
    main()

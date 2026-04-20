import argparse
import json
import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_YELLOW_TAXI = os.environ.get("KAFKA_TOPIC_YELLOW_TAXI", "yellow_taxi_trips")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "yellow_taxi_raw_loader")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Optional limit of messages to consume before stopping"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC_YELLOW_TAXI}")
    print(f"Kafka consumer group: {KAFKA_CONSUMER_GROUP}")

    consumer = KafkaConsumer(
        KAFKA_TOPIC_YELLOW_TAXI,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        key_deserializer=lambda x: x.decode("utf-8") if x else None,
    )

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    insert_sql = """
        INSERT INTO raw.yellow_taxi_trips_2023 (
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
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    processed_count = 0

    try:
        with conn.cursor() as cur:
            for message in consumer:
                data = message.value

                source_file = data.get("source_file", f"kafka_topic:{KAFKA_TOPIC_YELLOW_TAXI}")

                cur.execute(
                    insert_sql,
                    (
                        data.get("VendorID") if data.get("VendorID") is not None else data.get("vendor_id"),
                        data.get("tpep_pickup_datetime"),
                        data.get("tpep_dropoff_datetime"),
                        data.get("passenger_count"),
                        data.get("trip_distance"),
                        data.get("RatecodeID") if data.get("RatecodeID") is not None else data.get("ratecode_id"),
                        data.get("store_and_fwd_flag"),
                        data.get("PULocationID") if data.get("PULocationID") is not None else data.get("pulocationid"),
                        data.get("DOLocationID") if data.get("DOLocationID") is not None else data.get("dolocationid"),
                        data.get("payment_type"),
                        data.get("fare_amount"),
                        data.get("extra"),
                        data.get("mta_tax"),
                        data.get("tip_amount"),
                        data.get("tolls_amount"),
                        data.get("improvement_surcharge"),
                        data.get("total_amount"),
                        data.get("congestion_surcharge"),
                        data.get("Airport_fee") if data.get("Airport_fee") is not None else data.get("airport_fee"),
                        data.get("cbd_congestion_fee"),
                        source_file,
                        datetime.now(),
                    ),
                )

                conn.commit()
                processed_count += 1

                if processed_count % 100 == 0:
                    print(f"Processed {processed_count} messages")

                if args.max_messages is not None and processed_count >= args.max_messages:
                    print(f"Reached max_messages={args.max_messages}, stopping consumer")
                    break

    finally:
        consumer.close()
        conn.close()

    print(f"Finished. Inserted {processed_count} messages into raw.yellow_taxi_trips_2023")


if __name__ == "__main__":
    main()

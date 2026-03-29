CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS raw.taxi_zone_lookup;

CREATE TABLE raw.taxi_zone_lookup (
    locationid INTEGER,
    borough TEXT,
    zone TEXT,
    service_zone TEXT,
    source_file TEXT,
    load_timestamp TIMESTAMP
);

DROP TABLE IF EXISTS raw.yellow_taxi_trips_2023;

CREATE TABLE raw.yellow_taxi_trips_2023 (
    vendor_id INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count FLOAT8,
    trip_distance FLOAT8,
    ratecode_id FLOAT8,
    store_and_fwd_flag TEXT,
    pulocationid INTEGER,
    dolocationid INTEGER,
    payment_type INTEGER,
    fare_amount FLOAT8,
    extra FLOAT8,
    mta_tax FLOAT8,
    tip_amount FLOAT8,
    tolls_amount FLOAT8,
    improvement_surcharge FLOAT8,
    total_amount FLOAT8,
    congestion_surcharge FLOAT8,
    airport_fee FLOAT8,
    cbd_congestion_fee FLOAT8,
    source_file TEXT,
    load_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_trips_2023_cleaned (
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
    load_timestamp TIMESTAMP,
    pickup_date DATE,
    pickup_year INTEGER,
    pickup_month INTEGER,
    trip_duration_minutes FLOAT8
);

INSERT INTO silver.yellow_taxi_trips_2023_cleaned (
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
    load_timestamp,
    pickup_date,
    pickup_year,
    pickup_month,
    trip_duration_minutes
)
SELECT
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
    load_timestamp,
    DATE(tpep_pickup_datetime) AS pickup_date,
    EXTRACT(YEAR FROM tpep_pickup_datetime)::INT AS pickup_year,
    EXTRACT(MONTH FROM tpep_pickup_datetime)::INT AS pickup_month,
    (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0)::FLOAT8 AS trip_duration_minutes
FROM raw.yellow_taxi_trips_2023
WHERE trip_distance > 0
  AND fare_amount > 0
  AND tpep_pickup_datetime <= tpep_dropoff_datetime
  AND tpep_pickup_datetime >= TIMESTAMP '2023-01-01'
  AND tpep_pickup_datetime < TIMESTAMP '2024-01-01'
  AND tpep_dropoff_datetime >= TIMESTAMP '2023-01-01'
  AND tpep_dropoff_datetime < TIMESTAMP '2024-01-01'
  AND load_timestamp > (
        SELECT COALESCE(MAX(load_timestamp), TIMESTAMP '1900-01-01')
        FROM silver.yellow_taxi_trips_2023_cleaned
  );

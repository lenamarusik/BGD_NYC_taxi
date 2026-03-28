DROP TABLE IF EXISTS silver.yellow_taxi_trips_2023_cleaned;

CREATE TABLE silver.yellow_taxi_trips_2023_cleaned_1 AS
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
  AND tpep_dropoff_datetime < TIMESTAMP '2024-01-01';

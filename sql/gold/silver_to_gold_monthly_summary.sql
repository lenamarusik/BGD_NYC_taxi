CREATE TABLE IF NOT EXISTS gold.monthly_summary_2023 (
    pickup_year INTEGER,
    pickup_month INTEGER,
    total_trips BIGINT,
    total_revenue FLOAT8,
    avg_total_amount FLOAT8,
    avg_fare_amount FLOAT8,
    avg_tip_amount FLOAT8,
    avg_trip_distance FLOAT8,
    avg_trip_duration_minutes FLOAT8
);

WITH affected_months AS (
    SELECT DISTINCT pickup_year, pickup_month
    FROM silver.yellow_taxi_trips_2023_cleaned
    WHERE pickup_year IS NOT NULL
      AND pickup_month IS NOT NULL
),
deleted_rows AS (
    DELETE FROM gold.monthly_summary_2023
    WHERE (pickup_year, pickup_month) IN (
        SELECT pickup_year, pickup_month
        FROM affected_months
    )
    RETURNING pickup_year, pickup_month
)
INSERT INTO gold.monthly_summary_2023 (
    pickup_year,
    pickup_month,
    total_trips,
    total_revenue,
    avg_total_amount,
    avg_fare_amount,
    avg_tip_amount,
    avg_trip_distance,
    avg_trip_duration_minutes
)
SELECT
    pickup_year,
    pickup_month,
    COUNT(*) AS total_trips,
    SUM(total_amount)::FLOAT8 AS total_revenue,
    AVG(total_amount)::FLOAT8 AS avg_total_amount,
    AVG(fare_amount)::FLOAT8 AS avg_fare_amount,
    AVG(tip_amount)::FLOAT8 AS avg_tip_amount,
    AVG(trip_distance)::FLOAT8 AS avg_trip_distance,
    AVG(trip_duration_minutes)::FLOAT8 AS avg_trip_duration_minutes
FROM silver.yellow_taxi_trips_2023_cleaned
WHERE (pickup_year, pickup_month) IN (
    SELECT pickup_year, pickup_month
    FROM affected_months
)
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;

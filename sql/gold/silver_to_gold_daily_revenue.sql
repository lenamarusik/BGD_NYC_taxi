CREATE TABLE IF NOT EXISTS gold.daily_revenue_2023 (
    pickup_date DATE,
    total_trips BIGINT,
    total_revenue FLOAT8,
    avg_trip_revenue FLOAT8,
    avg_trip_distance FLOAT8,
    avg_trip_duration_minutes FLOAT8
);

WITH affected_dates AS (
    SELECT DISTINCT pickup_date
    FROM silver.yellow_taxi_trips_2023_cleaned
    WHERE pickup_date IS NOT NULL
),
deleted_rows AS (
    DELETE FROM gold.daily_revenue_2023
    WHERE pickup_date IN (
        SELECT pickup_date
        FROM affected_dates
    )
    RETURNING pickup_date
)
INSERT INTO gold.daily_revenue_2023 (
    pickup_date,
    total_trips,
    total_revenue,
    avg_trip_revenue,
    avg_trip_distance,
    avg_trip_duration_minutes
)
SELECT
    pickup_date,
    COUNT(*) AS total_trips,
    SUM(total_amount)::FLOAT8 AS total_revenue,
    AVG(total_amount)::FLOAT8 AS avg_trip_revenue,
    AVG(trip_distance)::FLOAT8 AS avg_trip_distance,
    AVG(trip_duration_minutes)::FLOAT8 AS avg_trip_duration_minutes
FROM silver.yellow_taxi_trips_2023_cleaned
WHERE pickup_date IN (
    SELECT pickup_date
    FROM affected_dates
)
GROUP BY pickup_date
ORDER BY pickup_date;

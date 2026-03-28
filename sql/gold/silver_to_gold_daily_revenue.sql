DROP TABLE IF EXISTS gold.daily_revenue_2023;

CREATE TABLE gold.daily_revenue_2023 AS
SELECT
    pickup_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount)::FLOAT8 AS avg_trip_revenue,
    AVG(trip_distance)::FLOAT8 AS avg_trip_distance,
    AVG(trip_duration_minutes)::FLOAT8 AS avg_trip_duration_minutes
FROM silver.yellow_taxi_trips_2023_cleaned
GROUP BY pickup_date
ORDER BY pickup_date;

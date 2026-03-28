DROP TABLE IF EXISTS gold.monthly_summary_2023;

CREATE TABLE gold.monthly_summary_2023 AS
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
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;

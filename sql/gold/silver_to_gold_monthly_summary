DROP TABLE IF EXISTS gold.monthly_summary_2023;

CREATE TABLE gold.monthly_summary_2023 AS
SELECT
    pickup_year,
    pickup_month,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_total_amount,
    AVG(fare_amount) AS avg_fare_amount,
    AVG(tip_amount) AS avg_tip_amount,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes
FROM silver.yellow_taxi_trips_2023_cleaned
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;

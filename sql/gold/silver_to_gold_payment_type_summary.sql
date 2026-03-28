DROP TABLE IF EXISTS gold.payment_type_summary_2023;

CREATE TABLE gold.payment_type_summary_2023 AS
SELECT
    payment_type,
    COUNT(*) AS total_trips,
    SUM(total_amount)::FLOAT8 AS total_revenue,
    AVG(total_amount)::FLOAT8 AS avg_total_amount,
    AVG(tip_amount)::FLOAT8 AS avg_tip_amount
FROM silver.yellow_taxi_trips_2023_cleaned
GROUP BY payment_type
ORDER BY total_trips DESC;

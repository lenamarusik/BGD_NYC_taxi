CREATE TABLE IF NOT EXISTS gold.payment_type_summary_2023 (
    payment_type INTEGER,
    total_trips BIGINT,
    total_revenue FLOAT8,
    avg_total_amount FLOAT8,
    avg_tip_amount FLOAT8
);

DELETE FROM gold.payment_type_summary_2023;

INSERT INTO gold.payment_type_summary_2023 (
    payment_type,
    total_trips,
    total_revenue,
    avg_total_amount,
    avg_tip_amount
)
SELECT
    payment_type,
    COUNT(*) AS total_trips,
    SUM(total_amount)::FLOAT8 AS total_revenue,
    AVG(total_amount)::FLOAT8 AS avg_total_amount,
    AVG(tip_amount)::FLOAT8 AS avg_tip_amount
FROM silver.yellow_taxi_trips_2023_cleaned
GROUP BY payment_type
ORDER BY total_trips DESC;

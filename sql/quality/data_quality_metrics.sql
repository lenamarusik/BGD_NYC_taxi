-- =====================================================
-- Data Quality Metrics for NYC Yellow Taxi Analytics 2023
-- =====================================================

-- Metric 1: Completeness of pickup_date
SELECT
    'Completeness of pickup_date' AS metric_name,
    '% of rows where pickup_date is not null' AS metric_definition,
    ROUND(
        100.0 * COUNT(pickup_date) / COUNT(*),
        2
    ) || '%' AS current_value,
    '> 99%' AS expected_threshold,
    'Every pipeline run' AS update_cadence
FROM silver.yellow_taxi_trips_2023_cleaned;


-- Metric 2: Validity of trip values
SELECT
    'Validity of trip values' AS metric_name,
    '% of rows with trip_distance > 0, fare_amount > 0 and pickup datetime before dropoff datetime' AS metric_definition,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE trip_distance > 0
              AND fare_amount > 0
              AND tpep_pickup_datetime <= tpep_dropoff_datetime
        ) / COUNT(*),
        2
    ) || '%' AS current_value,
    '100%' AS expected_threshold,
    'Every pipeline run' AS update_cadence
FROM silver.yellow_taxi_trips_2023_cleaned;


-- Metric 3: Row count in daily revenue table
SELECT
    'Row count in daily revenue table' AS metric_name,
    'Number of rows in gold.daily_revenue_2023' AS metric_definition,
    COUNT(*)::TEXT AS current_value,
    '365 rows expected for full 2023 load' AS expected_threshold,
    'Every pipeline run' AS update_cadence
FROM gold.daily_revenue_2023;


-- Metric 4: Data freshness
SELECT
    'Data freshness' AS metric_name,
    'Time since latest successful load_timestamp' AS metric_definition,
    (NOW() - MAX(load_timestamp))::TEXT AS current_value,
    '< 1 day after pipeline run' AS expected_threshold,
    'Every pipeline run' AS update_cadence
FROM silver.yellow_taxi_trips_2023_cleaned;

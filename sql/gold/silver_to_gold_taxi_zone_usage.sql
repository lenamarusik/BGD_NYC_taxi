DROP TABLE IF EXISTS gold.taxi_zone_usage;

CREATE TABLE gold.taxi_zone_usage AS
WITH pickup_stats AS (
    SELECT
        pulocationid AS locationid,
        COUNT(*)::INT8 AS total_pickups,
        SUM(total_amount)::FLOAT8 AS pickup_revenue
    FROM silver.yellow_taxi_trips_2023_cleaned
    GROUP BY pulocationid
),
dropoff_stats AS (
    SELECT
        dolocationid AS locationid,
        COUNT(*)::INT8 AS total_dropoffs,
        SUM(total_amount)::FLOAT8 AS dropoff_revenue
    FROM silver.yellow_taxi_trips_2023_cleaned
    GROUP BY dolocationid
)
SELECT
    z.locationid,
    z.borough,
    z.zone,
    z.service_zone,
    COALESCE(p.total_pickups, 0)::INT8 AS total_pickups,
    COALESCE(d.total_dropoffs, 0)::INT8 AS total_dropoffs,
    COALESCE(p.pickup_revenue, 0)::FLOAT8 AS pickup_revenue,
    COALESCE(d.dropoff_revenue, 0)::FLOAT8 AS dropoff_revenue
FROM silver.taxi_zone_lookup_cleaned z
LEFT JOIN pickup_stats p
    ON z.locationid = p.locationid
LEFT JOIN dropoff_stats d
    ON z.locationid = d.locationid
ORDER BY total_pickups DESC;

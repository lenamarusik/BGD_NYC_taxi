DROP TABLE IF EXISTS silver.taxi_zone_lookup_cleaned;

CREATE TABLE silver.taxi_zone_lookup_cleaned AS
SELECT
    locationid,
    TRIM(borough) AS borough,
    TRIM(zone) AS zone,
    TRIM(service_zone) AS service_zone,
    source_file,
    load_timestamp
FROM raw.taxi_zone_lookup
WHERE locationid IS NOT NULL
  AND borough IS NOT NULL
  AND zone IS NOT NULL;

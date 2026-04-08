CREATE TABLE IF NOT EXISTS silver.taxi_zone_lookup_cleaned (
    locationid INTEGER,
    borough TEXT,
    zone TEXT,
    service_zone TEXT,
    source_file TEXT,
    load_timestamp TIMESTAMP
);

INSERT INTO silver.taxi_zone_lookup_cleaned (
    locationid,
    borough,
    zone,
    service_zone,
    source_file,
    load_timestamp
)
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

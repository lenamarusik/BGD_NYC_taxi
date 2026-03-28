-- =========================
-- PRIMARY KEYS
-- =========================

ALTER TABLE gold.daily_revenue_2023
ADD CONSTRAINT pk_daily_revenue_2023
PRIMARY KEY (pickup_date);

ALTER TABLE gold.monthly_summary_2023
ADD CONSTRAINT pk_monthly_summary_2023
PRIMARY KEY (pickup_year, pickup_month);

ALTER TABLE gold.payment_type_summary_2023
ADD CONSTRAINT pk_payment_type_summary_2023
PRIMARY KEY (payment_type);

ALTER TABLE gold.taxi_zone_usage
ADD CONSTRAINT pk_taxi_zone_usage
PRIMARY KEY (locationid);


-- =========================
-- NOT NULL - daily_revenue_2023
-- PK: pickup_date -> already is NOT NULL (PK)
-- =========================

ALTER TABLE gold.daily_revenue_2023
ALTER COLUMN total_trips SET NOT NULL;

ALTER TABLE gold.daily_revenue_2023
ALTER COLUMN total_revenue SET NOT NULL;

ALTER TABLE gold.daily_revenue_2023
ALTER COLUMN avg_trip_revenue SET NOT NULL;

ALTER TABLE gold.daily_revenue_2023
ALTER COLUMN avg_trip_distance SET NOT NULL;

ALTER TABLE gold.daily_revenue_2023
ALTER COLUMN avg_trip_duration_minutes SET NOT NULL;


-- =========================
-- NOT NULL - monthly_summary_2023
-- PK composite: (pickup_year, pickup_month) -> already is NOT NULL (PK)
-- =========================

ALTER TABLE gold.monthly_summary_2023
ALTER COLUMN total_trips SET NOT NULL;

ALTER TABLE gold.monthly_summary_2023
ALTER COLUMN total_revenue SET NOT NULL;

ALTER TABLE gold.monthly_summary_2023
ALTER COLUMN avg_total_amount SET NOT NULL;

ALTER TABLE gold.monthly_summary_2023
ALTER COLUMN avg_fare_amount SET NOT NULL;

ALTER TABLE gold.monthly_summary_2023
ALTER COLUMN avg_tip_amount SET NOT NULL;

ALTER TABLE gold.monthly_summary_2023
ALTER COLUMN avg_trip_distance SET NOT NULL;

ALTER TABLE gold.monthly_summary_2023
ALTER COLUMN avg_trip_duration_minutes SET NOT NULL;


-- =========================
-- NOT NULL - payment_type_summary_2023
-- PK: payment_type -> already is NOT NULL (PK)
-- =========================

ALTER TABLE gold.payment_type_summary_2023
ALTER COLUMN total_trips SET NOT NULL;

ALTER TABLE gold.payment_type_summary_2023
ALTER COLUMN total_revenue SET NOT NULL;

ALTER TABLE gold.payment_type_summary_2023
ALTER COLUMN avg_total_amount SET NOT NULL;

ALTER TABLE gold.payment_type_summary_2023
ALTER COLUMN avg_tip_amount SET NOT NULL;


-- =========================
-- NOT NULL - taxi_zone_usage
-- PK: locationid -> already is NOT NULL (PK)
-- =========================

ALTER TABLE gold.taxi_zone_usage
ALTER COLUMN borough SET NOT NULL;

ALTER TABLE gold.taxi_zone_usage
ALTER COLUMN zone SET NOT NULL;

ALTER TABLE gold.taxi_zone_usage
ALTER COLUMN service_zone SET NOT NULL;

ALTER TABLE gold.taxi_zone_usage
ALTER COLUMN total_pickups SET NOT NULL;

ALTER TABLE gold.taxi_zone_usage
ALTER COLUMN total_dropoffs SET NOT NULL;

ALTER TABLE gold.taxi_zone_usage
ALTER COLUMN pickup_revenue SET NOT NULL;

ALTER TABLE gold.taxi_zone_usage
ALTER COLUMN dropoff_revenue SET NOT NULL;

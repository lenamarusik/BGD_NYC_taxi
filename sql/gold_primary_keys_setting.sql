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

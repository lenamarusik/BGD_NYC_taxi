# BGD_NYC_taxi

Data warehouse project based on NYC Taxi Trip Records from 2023 and Taxi Zones Lookup Table.

## Objective
To build a simple data warehouse with the following layers:
- raw (bronze)
- silver (cleaned)
- gold (curated)

## Data Source
Data source link: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Data Scope
- Dataset: NYC Yellow Taxi Trip Records + Taxi Zones
- Time range: 2023-01 to 2023-12
- Total data size: approximately 8.8 GB

## Analytical Goal
The goal of this project is to enable analysis of NYC taxi operations in 2023 by building a structured data warehouse.

The gold layer supports key analytical use cases such as:
- tracking daily and monthly revenue trends,
- analyzing the number of trips over time,
- understanding customer payment behavior (payment type distribution),
- evaluating average trip metrics such as distance, duration, and fare,
- analyzing taxi activity and revenue distribution across different zones (pickup and dropoff),
- enabling high-level business reporting and dashboarding through aggregated, ready-to-use datasets.

The project demonstrates how raw transactional data can be transformed into meaningful business insights using an ELT pipeline and medallion architecture.

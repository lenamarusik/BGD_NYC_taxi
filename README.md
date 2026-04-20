# BGD_NYC_taxi

Data warehouse project based on NYC Taxi Trip Records from 2023 and Taxi Zones Lookup Table.

---

## Objective

To build a simple data warehouse using medallion architecture with the following layers:

* RAW (bronze)
* SILVER (cleaned)
* GOLD (curated)

---

## Data Source

Data source:
[https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## Data Scope

* Dataset: NYC Yellow Taxi Trip Records + Taxi Zones
* Time range: 2023-01 to 2023-12
* Total data size: approximately 8.8 GB

---

## Analytical Goal

The goal of this project is to enable analysis of NYC taxi operations in 2023 by building a structured data warehouse.

The gold layer supports key analytical use cases such as:

* tracking daily and monthly revenue trends,
* analyzing the number of trips over time,
* understanding customer payment behavior,
* evaluating average trip metrics,
* analyzing taxi activity across zones,
* enabling business reporting and dashboards.

---

## Architecture

The project follows the **Medallion Architecture** (Bronze → Silver → Gold):

- **Bronze (RAW)**: Raw ingested data with minimal transformation
- **Silver**: Cleaned, validated and enriched data
- **Gold**: Aggregated, business-ready analytical tables

### Data Flow

1. **Ingestion** – Python scripts load CSV + Parquet files into Bronze layer (with idempotency check using `source_file`)
2. **Spark Processing** – PySpark job performs scalable cleaning, filtering and enrichment, saving result as temporary Parquet file (`yellow_tripdata_*_silver_preview.parquet`)
3. **RAW → Silver** – SQL scripts transform data from Bronze + use Spark's cleaned Parquet output
4. **Silver → Gold** – SQL scripts create analytical aggregations (daily revenue, monthly summary, payment type, zone usage)
5. **Constraints** – Primary keys and NOT NULL constraints are applied on Gold tables

The entire pipeline is **idempotent** and supports both `full` and `incremental` loads.

---

## Pipeline Orchestration

The pipeline is orchestrated using **Prefect** and has a single entry point:

```
python orchestration/prefect_flow.py --load-type full
# or
python orchestration/prefect_flow.py --load-type incremental --process-month 2023-12
```

This script executes the pipeline in the correct order:

* creates schemas and raw tables
* loads source files into RAW
* runs the PySpark job
* transforms RAW to SILVER
* transforms SILVER to GOLD
* applies constraints to GOLD tables

This ensures consistent, reproducible end-to-end execution.

---

## Scalable Processing Engine

The project includes a runnable PySpark job:

```
spark-submit spark_jobs/yellow_taxi_spark_job.py
```

The PySpark job:

* reads parquet taxi data
* performs data cleaning and validation
* enriches data with analytical columns
* writes cleaned parquet output

This component demonstrates scalable data processing outside the database.

### Pipeline Execution

Install dependencies:

```
pip install -r requirements.txt
```

Run the full pipeline:
```
python orchestration/prefect_flow.py --load-type full
```

Run the pipeline in incremental mode (for a selected month):
```
python orchestration/prefect_flow.py --load-type incremental --process-month 2023-12
```

Requirements:
- PostgreSQL database running and accessible via environment variables
- Spark installed and available (spark-submit command)
- Input data available in the directory defined by DATA_DIR

---

## Incremental Strategy

- **Bronze (RAW)**: File-level deduplication using `source_file` column. Already loaded files are skipped.
- **Silver**: True incremental load – only records with newer `load_timestamp` are inserted.
- **Gold**: Hybrid approach:
  - `daily_revenue_2023` and `monthly_summary_2023` → incremental (DELETE affected dates/months + INSERT)
  - `payment_type_summary_2023` and `taxi_zone_usage` → full refresh (`DELETE + INSERT`)

---

## Idempotency

The pipeline is **idempotent** and can be safely re-executed at any time:

- No duplicate data is created in Bronze thanks to `source_file` checks.
- Silver only appends new data based on `load_timestamp`.
- Gold tables are refreshed using incremental logic for time-based aggregations and full rebuild for global aggregates.
- Prefect orchestration + task retries ensure reliable execution even after failures.

---

## Repository Structure

```
BGD_NYC_taxi/
├── orchestration/                  # Prefect orchestration
│   └── prefect_flow.py
├── scripts/                        # Ingestion to Bronze layer
│   ├── load_to_raw_taxi_zone_lookup.py
│   └── load_to_raw_yellow_taxi_trips_2023.py
├── spark_jobs/                     # Scalable Spark processing
│   └── yellow_taxi_spark_job.py
├── sql/
│   ├── raw/
│   │   └── create_schemas_and_raw_tables.sql
│   ├── silver/
│   │   ├── raw_to_silver_taxi_zone_lookup.sql
│   │   └── raw_to_silver_yellow_taxi_trips_2023.sql
│   └── gold/
│       ├── silver_to_gold_daily_revenue.sql
│       ├── silver_to_gold_monthly_summary.sql
│       ├── silver_to_gold_payment_type_summary.sql
│       ├── silver_to_gold_taxi_zone_usage.sql
│       └── adding_pk_setting_nn.sql
├── assets/                         # Architecture diagrams
├── docs/
├── .env.example
├── requirements.txt
└── README.md
```

---

## Architecture Diagrams

### Pipeline Architecture
![Pipeline Architecture](assets/PipelineArchitectureDiagram.png)

---

### High Level Architecture
![High Level Architecture](assets/HighLevelArchitecture.png)

---

### ERD
![ERD](assets/ERD.png)

---

## Data Quality Risks

1. Invalid trip durations
2. Non-positive values
3. Missing zone mappings

---

## Tech Stack

- **Orchestration**: Prefect
- **Database**: PostgreSQL
- **Ingestion**: Python + psycopg2 + pandas
- **Scalable Processing**: PySpark
- **Transformations**: SQL (raw → silver → gold)
- **Environment**: dotenv

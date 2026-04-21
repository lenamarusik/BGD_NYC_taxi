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

---

### Data Flow

The pipeline supports **two ingestion modes**:

### 1. Batch ingestion (historical load)

1. **Ingestion** – Python scripts load CSV + Parquet files into Bronze layer (with idempotency check using `source_file`)
2. **Spark Processing** – PySpark job performs scalable cleaning, filtering and enrichment
3. **RAW → Silver** – SQL transformations
4. **Silver → Gold** – Aggregations and business logic

### 2. Streaming ingestion (incremental / fresh data)

1. **Producer** – reads Parquet data and publishes records to Kafka topic
2. **Kafka** – acts as a queue system (decoupling ingestion from processing)
3. **Consumer** – reads messages from Kafka and inserts them into RAW tables
4. **Spark + SQL** – same processing as batch (Spark + SQL transformations), executed in incremental mode after data is written to RAW

This approach ensures:
- support for large historical loads (batch)
- real-time / near-real-time ingestion (streaming)
- decoupled and scalable architecture
  
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

---

## Streaming (Kafka)

The project includes a **streaming ingestion layer** using Apache Kafka.

### Components

- **Producer** (`streaming/producer_yellow_taxi.py`)
  - reads Parquet files
  - sends records to Kafka topic

- **Consumer** (`streaming/consumer_yellow_taxi.py`)
  - reads messages from Kafka
  - inserts data into `raw.yellow_taxi_trips_2023`

- **Kafka Topic**
  - acts as a queue system between ingestion and processing

### Purpose

- decoupling ingestion from processing
- enabling near real-time data ingestion
- supporting incremental data loads

Batch ingestion remains available for **historical data loading**, while streaming is used for **fresh data ingestion**.

---

## Pipeline Orchestration

The pipeline is orchestrated using **Prefect** and has a single entry point:

### Batch mode

```
python orchestration/prefect_flow.py --ingestion-mode batch --load-type full
```

### Or incremental

```
python orchestration/prefect_flow.py --ingestion-mode batch --load-type incremental --process-month 2023-12
```

---

### Streaming mode (Kafka)

```
python orchestration/prefect_flow.py --ingestion-mode streaming --process-month 2023-12
```

Prefect acts as a **trigger and orchestrator**, executing:

- ingestion (batch or streaming)
- Spark processing
- SQL transformations (RAW → SILVER → GOLD)
- constraints

Streaming ingestion is triggered via Prefect, which orchestrates Kafka producer and consumer as part of the pipeline execution.

---

### Pipeline Execution

Install dependencies:

```
pip install -r requirements.txt
```

### Run batch pipeline (historical load)

```
python orchestration/prefect_flow.py --ingestion-mode batch --load-type full
```

### Run incremental batch

```
python orchestration/prefect_flow.py --ingestion-mode batch --load-type incremental --process-month 2023-12
```

### Run streaming pipeline (Kafka)

```
python orchestration/prefect_flow.py --ingestion-mode streaming --process-month 2023-12
```

Requirements:
- PostgreSQL database running
- Spark installed (`spark-submit`)
- Kafka running (`localhost:9092`)
- Input data available in `DATA_DIR`

---

## Incremental Strategy

- **Bronze (RAW)**:
  - batch → file-level deduplication using `source_file`
  - streaming → continuous ingestion via Kafka

- **Silver**:
  - incremental load based on `load_timestamp`

- **Gold**:
  - time-based aggregations → incremental (DELETE affected partitions + INSERT)
  - global aggregations → full refresh (DELETE + INSERT)

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
├── streaming/                      # Kafka streaming layer
│   ├── producer_yellow_taxi.py
│   └── consumer_yellow_taxi.py
├── assets/                         # Architecture diagrams
├── docs/
├── .env.example
├── requirements.txt
└── README.md
```

---

## Architecture Diagrams

---

### Pipeline Architecture (before Streaming assignment)
![Pipeline Architecture](assets/PipelineArchitectureDiagram.png)

---

### Pipeline Architecture (after Streaming assignment)
![Pipeline Architecture](assets/PipelineArchitectureDiagramWithStreaming.png)

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

- **Streaming / Queue**: Apache Kafka
- **Orchestration**: Prefect
- **Database**: PostgreSQL
- **Ingestion**: Python + psycopg2 + pandas
- **Scalable Processing**: PySpark
- **Transformations**: SQL (raw → silver → gold)
- **Environment**: dotenv

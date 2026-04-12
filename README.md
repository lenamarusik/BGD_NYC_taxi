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

The project follows a medallion architecture:

* RAW – ingested data
* SILVER – cleaned data
* GOLD – aggregated data

Schemas:

* raw
* silver
* gold

---

## Pipeline Orchestration

The pipeline is executed from a single entry point:

```bash
python pipeline/run_pipeline.py
```

This script executes the pipeline in the correct order:

* creates schemas and raw tables
* loads source files into RAW
* runs the PySpark job
* transforms RAW to SILVER
* transforms SILVER to GOLD
* applies constraints to GOLD tables

This ensures full end-to-end reproducibility.

---

## Scalable Processing Engine

The project includes a runnable PySpark job:

```bash
spark-submit spark_jobs/yellow_taxi_spark_job.py
```

The PySpark job:

* reads parquet taxi data
* performs data cleaning and validation
* enriches data with analytical columns
* writes cleaned parquet output

This component demonstrates scalable data processing outside the database.

---

## Pipeline Execution

Install dependencies:

```bash
pip install -r requirements.txt
```

Run pipeline:

```bash
python pipeline/run_pipeline.py
```

---

## Incremental Strategy

RAW:

* file-level deduplication using source_file

SILVER:

* incremental load using load_timestamp

GOLD:

* full refresh using TRUNCATE + INSERT

---

## Idempotency

Pipeline can be safely re-run:

* RAW avoids duplicate loads
* SILVER processes only new data
* GOLD refreshes consistently

---

## Repository Structure

```
BGD_NYC_taxi/
├── assets/
├── docs/
├── pipeline/
├── scripts/
├── spark_jobs/
├── sql/
├── README.md
├── requirements.txt
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

* PostgreSQL
* Python
* Pandas
* PySpark
* SQL
* GitHub

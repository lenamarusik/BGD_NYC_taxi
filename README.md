# BGD_NYC_taxi

Data warehouse project based on NYC Taxi Trip Records.

## Objective
To build a simple data warehouse with the following layers:
- raw (bronze)
- silver (cleaned)
- gold (curated)

## Data Scope
Dataset: NYC Yellow Taxi Trip Records  
Time range: 2015-01 to 2025-11  
Total data size: approximately 10 GB

## Plan
1. Load parquet data into the raw layer
2. Clean and transform data into the silver layer
3. Create business-level aggregations in the gold layer
4. Identify key data quality issues
5. Provide scripts and documentation in a Git repository
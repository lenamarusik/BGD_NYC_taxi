# Data issues

## Data Quality Issues – NYC Taxi Dataset (2023)

During the exploratory analysis of the raw dataset, several data quality issues were identified. Below are three key anomalies detected in the data.

---

### 1. Zero or Negative Trip Distance

Some records contain trips with a distance equal to zero or even negative values.

This is problematic because:

* A taxi trip should always have a positive distance
* Zero distance may indicate canceled rides or incorrect measurements
* Negative values are clearly invalid and suggest data corruption

These records were identified using the following logic:

```sql
WHERE trip_distance <= 0
```

![Trip distance issue](../assets/trip_distance.png)

---

### 2. Non-positive Fare Amount

There are trips where the fare amount is zero or negative.

Possible reasons include:

* Data entry errors
* Refunds or adjustments incorrectly recorded
* Test or invalid transactions

Such values are not valid for standard taxi operations, as every trip should generate a positive fare.

Detection logic:

```sql
WHERE fare_amount <= 0
```

![Fare amount issue](../assets/fare_amount.png)

---

### 3. Invalid Timestamp Order (Pickup After Dropoff)

Some trips have inconsistent timestamps where the pickup time occurs after the dropoff time.

This is logically impossible and indicates:

* Data ingestion errors
* Corrupted timestamps
* System synchronization issues

Detection logic:

```sql
WHERE tpep_pickup_datetime > tpep_dropoff_datetime
```

![Timestamp issue](../assets/timestamp.png)

## Additional Data Quality Risks

Beyond the detected anomalies in the dataset, several potential risks were identified during the ELT process:

### 1. Large Data Volume (Scalability Risk)

The dataset is relatively large (~8–9 GB), which can lead to:
- storage limitations (as observed during loading),
- slower query performance,
- increased resource usage in local environments.

This highlights the need for efficient storage management and potential use of partitioning or indexing in production systems.

---

### 2. Risk of Duplicate Data Loads

Since data is loaded from external parquet files without strict deduplication logic, there is a risk that:
- re-running the ingestion script could insert duplicate records,
- historical data may be duplicated if not properly controlled.

In a production system, this should be handled using:
- primary keys,
- deduplication logic,
- or incremental loading strategies.

---

### 3. Schema Inconsistencies Across Files

Different parquet files may contain:
- missing columns,
- differently named fields (e.g. `Airport_fee` vs `airport_fee`),
- schema evolution over time.

This can lead to ingestion errors or incorrect data mapping, which is why a column normalization step was required in the raw loading process.

---

### 4. Timestamp Quality and Consistency

Timestamp fields may contain:
- incorrect ordering (pickup after dropoff),
- inconsistent formats,
- timezone-related issues.

These problems can significantly affect time-based aggregations in the gold layer.

---

### 5. Impact of Data Cleaning on Aggregations

Filtering invalid records in the silver layer (e.g. removing negative fares or invalid trips) may:
- slightly change total counts,
- impact revenue calculations,
- introduce differences between raw and analytical outputs.

This highlights the importance of clearly defining business rules for data cleaning.



---

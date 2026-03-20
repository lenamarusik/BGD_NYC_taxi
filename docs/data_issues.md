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


---

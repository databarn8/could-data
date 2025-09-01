# PySpark Hive Table Writes: saveAsTable vs insertInto

## Overview
This note compares the behavior of **`saveAsTable`** and **`insertInto`** in PySpark when writing to Hive tables, especially with partitioned tables, schema mismatches, and dynamic partition overwrite mode.

---

## Comparison Table

| Aspect | `saveAsTable(...)` | `insertInto(...)` |
|--------|---------------------|-------------------|
| **Table creation** | Can **create new table** if it does not exist. | Requires table to already exist; cannot create. |
| **Column resolution** | Uses **column names** → safe if column order differs. | Uses **column positions** → unsafe if schema order mismatched. |
| **Schema mismatch – `mode("overwrite")`** | Drops existing table and **recreates** it with DataFrame’s schema → **all data & partitions lost**. Dynamic partition setting is irrelevant because table is redefined. | Will overwrite rows in target partitions if schemas align. If mismatched → **can corrupt or error** because no schema validation. |
| **Schema mismatch – `mode("append")`** | **Fails with error** if schema differs. Schema compatibility required. | Appends data **positionally** → schema mismatch may silently corrupt data (no strong validation). |
| **Schema mismatch – `mode("errorIfExists")`** | Default. Throws error if table exists (schema irrelevant). | N/A (must specify mode). |
| **Schema mismatch – `mode("ignore")`** | Does nothing if table exists (schema irrelevant). | N/A. |
| **Partition overwrite (dynamic vs static)** | With `mode("overwrite")` → recreates whole table regardless of `spark.sql.sources.partitionOverwriteMode`. Dynamic partition overwrite setting doesn’t apply if schema mismatch triggers table recreation. | With `mode("overwrite")` and `spark.sql.sources.partitionOverwriteMode="dynamic"` → **only overwrites partitions present in the DataFrame** (leaves other partitions intact). With static (default) → overwrites all partitions. |
| **Partition creation (`append`)** | Creates new partitions when writing if keys exist in DataFrame. | Creates new partitions when writing if keys exist in DataFrame. |
| **Best suited for** | - Initial table creation.<br>- Cases where schema evolution is expected (column name safety).<br>- Controlled overwrites of entire datasets. | - Incremental **partition updates**.<br>- Efficient for append/overwrite into large partitioned Hive tables **when schema is stable**. |

---

## General Behavior with Schema Mismatch and Dynamic Partition Overwrite

### `saveAsTable`
- **SaveMode.Overwrite**: Drops the existing table and recreates it with the DataFrame schema. **All data is lost** and replaced. Dynamic partition overwrite setting is ignored in this case.
- **SaveMode.Append**: Fails with schema mismatch.
- **SaveMode.ErrorIfExists (default)**: Errors if table exists.
- **SaveMode.Ignore**: Does nothing if table exists.

### `insertInto`
- **Append**: Inserts rows into existing partitions or creates new partitions. Sensitive to column position.
- **Overwrite with dynamic partitioning**:  
  ```python
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  df.write.mode("overwrite").insertInto("my_partitioned_table")
  ```
  Overwrites only the partitions present in the DataFrame. Other partitions remain untouched.

---

## Key Considerations

- **Partition Columns**: The DataFrame must contain the partition columns of the target table.
- **Schema Mismatch**:  
  - `saveAsTable(overwrite)` → recreates table with new schema.  
  - `saveAsTable(append)` → fails if schema differs.  
  - `insertInto` → may silently corrupt data if schemas differ (column position-based).

---

## Best Practices

- Use **`saveAsTable`** for creating tables, schema evolution, or when you want column name safety.
- Use **`insertInto`** for stable schemas and efficient incremental updates, especially with dynamic partition overwrite.

---

## Example Code

### Using `insertInto`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InsertIntoPartitionedTable").getOrCreate()

data = [(1, "A", "2023-01-01"), (2, "B", "2023-01-01"), (3, "C", "2023-01-02")]
df = spark.createDataFrame(data, ["id", "value", "date_col"])

# Append to partitioned table
df.write.mode("append").insertInto("my_partitioned_table")

# Dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").insertInto("my_partitioned_table")
```

### Using `saveAsTable`

```python
data = [(1, "X", "2024-01-01"), (2, "Y", "2024-01-01"), (3, "Z", "2024-01-02")]
df = spark.createDataFrame(data, ["id", "value", "event_date"])

# Create new partitioned table
df.write.partitionBy("event_date").mode("overwrite").saveAsTable("new_partitioned_table")

# Overwrite existing partitioned table (entire table replaced)
df.write.partitionBy("event_date").mode("overwrite").saveAsTable("existing_partitioned_table")
```

---

**Summary**:  
- `saveAsTable(overwrite)` recreates tables if schema differs (dangerous but useful for schema evolution).  
- `insertInto(overwrite)` + dynamic partitioning is safer for incremental partition updates.  
- Always manage schemas carefully to avoid data loss or corruption.

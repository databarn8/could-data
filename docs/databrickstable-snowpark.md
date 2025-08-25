## snowpark dataframe save to snowflake table

Got it 👍 — you want to take a **Snowpark DataFrame** and persist it into a **Snowflake table**.  

Here’s how you can do it in Python with **Snowpark**:

---

### **1. Setup Session**
```python
from snowflake.snowpark import Session

connection_parameters = {
    "account": "<your_account>",
    "user": "<your_user>",
    "password": "<your_password>",
    "role": "<your_role>",
    "warehouse": "<your_wh>",
    "database": "<your_db>",
    "schema": "<your_schema>"
}

session = Session.builder.configs(connection_parameters).create()
```

---

### **2. Create or Load a DataFrame**
Example: create one from `values`:
```python
from snowflake.snowpark.functions import col

df = session.create_dataframe(
    [[1, "Alice"], [2, "Bob"], [3, "Charlie"]],
    schema=["id", "name"]
)
```

---

### **3. Save DataFrame to a Snowflake Table**

#### a) **Create a new table (overwrite if exists)**
```python
df.write.save_as_table("MY_TABLE", mode="overwrite")
```

#### b) **Append to an existing table**
```python
df.write.save_as_table("MY_TABLE", mode="append")
```

#### c) **Create a temporary table**
```python
df.write.save_as_table("TEMP_TABLE", mode="overwrite", table_type="temporary")
```

---

### **4. Verify**
```python
session.table("MY_TABLE").show()
```

---

✅ **Key Notes**
- `mode="overwrite"` → drops & recreates the table  
- `mode="append"` → adds rows without replacing  
- `table_type` can be `"temporary"`, `"transient"`, or `"permanent"` (default permanent)  

---

If your **data starts in a Databricks table**, there are a couple of ways to move it into Snowflake via **Snowpark**.  
It depends on scale (small vs. large datasets).

---

## 🔹 Option 1: Small/Medium Dataset (via Pandas → Snowpark)
If the Databricks table fits in memory:

```python
# In Databricks
import pandas as pd

# Load from Databricks table into Pandas
df_pandas = spark.table("my_databricks_table").toPandas()

# Now use Snowpark
from snowflake.snowpark import Session
session = Session.builder.configs(connection_parameters).create()

# Convert Pandas → Snowpark DataFrame
df_snow = session.create_dataframe(df_pandas)

# Save to Snowflake table
df_snow.write.save_as_table("MY_SNOWFLAKE_TABLE", mode="overwrite")
```

✅ Good for dev/testing or when tables are not huge.  

---

## 🔹 Option 2: Large Dataset (Use External Stage or COPY INTO)
For **large data**, the recommended pattern is:
1. **Export from Databricks** to a cloud storage bucket (S3, ADLS, GCS).  
2. **Create a Snowflake external stage** pointing to that bucket.  
3. **Use Snowflake `COPY INTO`** (or Snowpark `copy_into`) to load efficiently.

Example (Databricks → S3):
```python
# In Databricks
spark.table("my_databricks_table") \
    .write \
    .mode("overwrite") \
    .parquet("s3://my-bucket/export/my_table/")
```

Then in Snowflake:
```sql
CREATE OR REPLACE STAGE my_stage
  URL='s3://my-bucket/export/'
  STORAGE_INTEGRATION = my_integration;

COPY INTO MY_SNOWFLAKE_TABLE
FROM @my_stage/my_table/
FILE_FORMAT = (TYPE = PARQUET);
```

Or via **Snowpark Python**:
```python
session.sql("""
COPY INTO MY_SNOWFLAKE_TABLE
FROM @my_stage/my_table/
FILE_FORMAT = (TYPE = PARQUET)
""").collect()
```

✅ Best for production pipelines, large data, or recurring transfers.  

---

## 🔹 Option 3: Databricks Snowflake Connector
You can also skip Snowpark and push data **directly from Databricks to Snowflake**:

```python
df = spark.table("my_databricks_table")

df.write \
  .format("snowflake") \
  .option("sfURL", "<account>.snowflakecomputing.com") \
  .option("sfDatabase", "<db>") \
  .option("sfSchema", "<schema>") \
  .option("sfWarehouse", "<wh>") \
  .option("sfRole", "<role>") \
  .option("dbtable", "MY_SNOWFLAKE_TABLE") \
  .option("sfUser", "<user>") \
  .option("sfPassword", "<password>") \
  .mode("overwrite") \
  .save()
```

---

✅ Summary:
- **Option 1** → Pandas → Snowpark (quick + small datasets)  
- **Option 2** → Stage + COPY (scalable + production)  
- **Option 3** → Databricks Snowflake Connector (direct write, often easiest)  

---

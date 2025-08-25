---

## **1. Create a Stage for Parquet Files**
Suppose you have data stored like this in S3:

```
s3://my-bucket/sales_data/year=2024/month=08/day=23/part-0001.parquet
s3://my-bucket/sales_data/year=2024/month=08/day=24/part-0002.parquet
s3://my-bucket/sales_data/year=2025/month=01/day=01/part-0003.parquet
```

Create an external stage:

```sql
CREATE OR REPLACE STAGE demo_schema.sales_stage
  URL = 's3://my-bucket/sales_data/'
  STORAGE_INTEGRATION = my_s3_int
  FILE_FORMAT = (TYPE = PARQUET);
```

---

## **2. Create External Table with Partition Columns**
```sql
CREATE OR REPLACE EXTERNAL TABLE demo_schema.sales_ext_table
  (
    -- Columns inferred from parquet schema
    order_id STRING,
    amount NUMBER,
    customer_id STRING,

    -- Extract "partitions" from file path
    year STRING AS (REGEXP_SUBSTR(METADATA$FILE_PATH, 'year=([0-9]+)', 1, 1, 'e', 1)),
    month STRING AS (REGEXP_SUBSTR(METADATA$FILE_PATH, 'month=([0-9]+)', 1, 1, 'e', 1)),
    day STRING AS (REGEXP_SUBSTR(METADATA$FILE_PATH, 'day=([0-9]+)', 1, 1, 'e', 1))
  )
  LOCATION=@demo_schema.sales_stage
  AUTO_REFRESH = TRUE
  FILE_FORMAT = (TYPE = PARQUET);
```

👉 Here, Snowflake extracts `year`, `month`, `day` from directory paths and makes them queryable **as if they are partition columns**.

---

## **3. Query the External Table**
```sql
-- Query all rows
SELECT * FROM demo_schema.sales_ext_table;

-- Partition prune by year
SELECT * FROM demo_schema.sales_ext_table WHERE year = '2024';

-- Partition prune by year/month/day
SELECT * FROM demo_schema.sales_ext_table 
WHERE year='2025' AND month='01' AND day='01';
```

Snowflake will automatically **prune partitions (directories)** so it won’t scan unnecessary files 🚀.

---

## **4. Refresh Metadata (if auto not enabled)**
If you didn’t set `AUTO_REFRESH = TRUE`:
```sql
ALTER EXTERNAL TABLE demo_schema.sales_ext_table REFRESH;
```

---

✅ That’s the Snowflake way of handling **partitioned external tables on Parquet** — instead of native Hive-style partitions, you **extract directory values into computed columns**.  

Do you want me to also show you the **same setup but with Glue-style partitioned folders (`/region=us/state=NY/`)** as another example?

> can you create sample that is not year.month,day partition rather something else and make business sense.  I want parquet files again

Perfect 👍 — instead of the usual `year/month/day` partitions, let’s make something more **business-oriented**.  

Example business case: **Retail Sales Data** partitioned by **region** and **product category**.  
This makes sense because analysts often filter by geography and product type, so those are useful “partitions.”  

---

## **1. Example Directory Layout in S3**
```
s3://my-bucket/retail_sales/region=US/category=Electronics/sales_us_electronics_01.parquet
s3://my-bucket/retail_sales/region=US/category=Clothing/sales_us_clothing_01.parquet
s3://my-bucket/retail_sales/region=EU/category=Electronics/sales_eu_electronics_01.parquet
s3://my-bucket/retail_sales/region=EU/category=Clothing/sales_eu_clothing_01.parquet
s3://my-bucket/retail_sales/region=ASIA/category=Food/sales_asia_food_01.parquet
```

---

## **2. Sample Parquet Data (per file, ~5 rows each)**

**Example: `sales_us_electronics_01.parquet`**
```json
[
  {"order_id": "E1001", "customer_id": "C001", "amount": 499.99, "order_date": "2025-08-01"},
  {"order_id": "E1002", "customer_id": "C002", "amount": 1299.00, "order_date": "2025-08-02"},
  {"order_id": "E1003", "customer_id": "C003", "amount": 79.99, "order_date": "2025-08-02"},
  {"order_id": "E1004", "customer_id": "C004", "amount": 250.50, "order_date": "2025-08-03"},
  {"order_id": "E1005", "customer_id": "C005", "amount": 1800.00, "order_date": "2025-08-03"}
]
```

(Other files follow same schema but different region/category + data.)

---

## **3. Create Stage for Parquet Files**
```sql
CREATE OR REPLACE STAGE demo_schema.retail_stage
  URL = 's3://my-bucket/retail_sales/'
  STORAGE_INTEGRATION = my_s3_int
  FILE_FORMAT = (TYPE = PARQUET);
```

---

## **4. External Table with Business Partitions**
```sql
CREATE OR REPLACE EXTERNAL TABLE demo_schema.retail_sales_ext
(
  order_id STRING,
  customer_id STRING,
  amount NUMBER,
  order_date DATE,

  -- Partition columns extracted from path
  region STRING AS (REGEXP_SUBSTR(METADATA$FILE_PATH, 'region=([^/]+)', 1, 1, 'e', 1)),
  category STRING AS (REGEXP_SUBSTR(METADATA$FILE_PATH, 'category=([^/]+)', 1, 1, 'e', 1))
)
LOCATION=@demo_schema.retail_stage
AUTO_REFRESH = TRUE
FILE_FORMAT = (TYPE = PARQUET);
```

---

## **5. Query Examples**
```sql
-- All sales
SELECT * FROM demo_schema.retail_sales_ext;

-- Only Electronics in US
SELECT * 
FROM demo_schema.retail_sales_ext
WHERE region = 'US' AND category = 'Electronics';

-- Clothing sales across all regions
SELECT region, SUM(amount) AS total_sales
FROM demo_schema.retail_sales_ext
WHERE category = 'Clothing'
GROUP BY region;
```

Snowflake will **prune partitions** so only relevant Parquet files are scanned. 🚀  

---

⚡ Question for you:  
Do you want me to **generate actual Parquet files** (5 files, 5 rows each like the CSV example) so you can upload to S3 and test this setup, or is the SQL + directory structure enough?

> can create some parquet files to reflect the schema/table definition you did above, if you can not generate files easily, please provide python script that I can tweak to generate different set of data for test

Snowflake 👍 — my environment here doesn’t have a Parquet writer (`pyarrow` / `fastparquet`) installed, so I couldn’t actually write `.parquet` files directly.  

But ✅ I can give you a **ready-to-run Python script** that you can run locally (with `pyarrow` or `fastparquet` installed) to generate the partitioned Parquet files.  

---

## **Python Script to Generate Business-Partitioned Parquet Files**

```python
import pandas as pd
import os

# Install one of these if not present:
# pip install pyarrow
# or
# pip install fastparquet

# Base directory (local folder; later you can upload to S3)
base_dir = "retail_sales"
os.makedirs(base_dir, exist_ok=True)

# Partition definitions
partitions = {
    ("US", "Electronics"): [
        {"order_id": "E1001", "customer_id": "C001", "amount": 499.99, "order_date": "2025-08-01"},
        {"order_id": "E1002", "customer_id": "C002", "amount": 1299.00, "order_date": "2025-08-02"},
        {"order_id": "E1003", "customer_id": "C003", "amount": 79.99, "order_date": "2025-08-02"},
        {"order_id": "E1004", "customer_id": "C004", "amount": 250.50, "order_date": "2025-08-03"},
        {"order_id": "E1005", "customer_id": "C005", "amount": 1800.00, "order_date": "2025-08-03"},
    ],
    ("US", "Clothing"): [
        {"order_id": "C2001", "customer_id": "C006", "amount": 59.99, "order_date": "2025-08-01"},
        {"order_id": "C2002", "customer_id": "C007", "amount": 89.99, "order_date": "2025-08-02"},
        {"order_id": "C2003", "customer_id": "C008", "amount": 120.00, "order_date": "2025-08-02"},
        {"order_id": "C2004", "customer_id": "C009", "amount": 200.00, "order_date": "2025-08-03"},
        {"order_id": "C2005", "customer_id": "C010", "amount": 75.00, "order_date": "2025-08-03"},
    ],
    ("EU", "Electronics"): [
        {"order_id": "E3001", "customer_id": "C011", "amount": 699.00, "order_date": "2025-08-05"},
        {"order_id": "E3002", "customer_id": "C012", "amount": 150.00, "order_date": "2025-08-05"},
        {"order_id": "E3003", "customer_id": "C013", "amount": 999.99, "order_date": "2025-08-06"},
        {"order_id": "E3004", "customer_id": "C014", "amount": 450.00, "order_date": "2025-08-06"},
        {"order_id": "E3005", "customer_id": "C015", "amount": 300.00, "order_date": "2025-08-07"},
    ],
    ("EU", "Clothing"): [
        {"order_id": "C4001", "customer_id": "C016", "amount": 49.99, "order_date": "2025-08-05"},
        {"order_id": "C4002", "customer_id": "C017", "amount": 89.99, "order_date": "2025-08-06"},
        {"order_id": "C4003", "customer_id": "C018", "amount": 130.00, "order_date": "2025-08-06"},
        {"order_id": "C4004", "customer_id": "C019", "amount": 300.00, "order_date": "2025-08-07"},
        {"order_id": "C4005", "customer_id": "C020", "amount": 110.00, "order_date": "2025-08-07"},
    ],
    ("ASIA", "Food"): [
        {"order_id": "F5001", "customer_id": "C021", "amount": 10.99, "order_date": "2025-08-10"},
        {"order_id": "F5002", "customer_id": "C022", "amount": 25.50, "order_date": "2025-08-10"},
        {"order_id": "F5003", "customer_id": "C023", "amount": 18.75, "order_date": "2025-08-11"},
        {"order_id": "F5004", "customer_id": "C024", "amount": 40.00, "order_date": "2025-08-11"},
        {"order_id": "F5005", "customer_id": "C025", "amount": 75.25, "order_date": "2025-08-12"},
    ],
}

# Write Parquet files with Hive-style partitioning
for (region, category), rows in partitions.items():
    df = pd.DataFrame(rows)
    path = os.path.join(base_dir, f"region={region}", f"category={category}")
    os.makedirs(path, exist_ok=True)
    filename = os.path.join(path, f"sales_{region.lower()}_{category.lower()}.parquet")
    df.to_parquet(filename, index=False)
    print(f"✅ Wrote {filename}")

print("All parquet files created!")
```

---

### **Usage**
1. Save script as `generate_retail_sales_parquet.py`.  
2. Install parquet support:  
   ```bash
   pip install pyarrow
   ```
   or  
   ```bash
   pip install fastparquet
   ```
3. Run:
   ```bash
   python generate_retail_sales_parquet.py
   ```
4. You’ll get a folder structure like:
   ```
   retail_sales/
     region=US/category=Electronics/sales_us_electronics.parquet
     region=US/category=Clothing/sales_us_clothing.parquet
     region=EU/category=Electronics/sales_eu_electronics.parquet
     region=EU/category=Clothing/sales_eu_clothing.parquet
     region=ASIA/category=Food/sales_asia_food.parquet
   ```
5. Upload to S3:
   ```bash
   aws s3 sync retail_sales/ s3://my-bucket/retail_sales/
   ```

---

Do you want me to also include a **Snowflake `COPY INTO` example** (load from parquet → internal table) in addition to the **external table approach**? That way you can test both ingestion styles.
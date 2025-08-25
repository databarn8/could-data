# PySpark Nested JSON Aggregation & Flattening Examples

This Markdown file contains the full conversation details, sample JSON, and self-contained PySpark scripts that generate 10-20 rows of data, parse nested JSON, aggregate unique items, and explode arrays.

---

## 1. Sample JSON Template

```json
{
  "device": "iOS",
  "event_name": "shipping_info",
  "event_previous_timestamp": 1593872284589490,
  "event_timestamp": 1593880751614429,
  "geo": {"city": "El Paso de Robles", "state": "CA"},
  "items": [
    {"coupon": "NEWBED10", "item_id": "M_STAN_T", "item_name": "Standard Twin Mattress", "item_revenue_in_usd": 535.5, "price_in_usd": 595.0, "quantity": 2},
    {"coupon": "NEWBED20", "item_id": "S_STAN_T", "item_name": "Styled Twin Mattress", "item_revenue_in_usd": 700.5, "price_in_usd": 595.0, "quantity": 4}
  ],
  "traffic_source": "search",
  "user_first_touch_timestamp": 1593469999999999,
  "user_id": "UA000000106291434"
}
```

---

## 2. Full Self-Contained PySpark Script (Databricks-ready)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, lit, collect_set, flatten, array_distinct, explode, arrays_zip, expr
import json
import random

# Initialize Spark
spark = SparkSession.builder.appName("NestedJSON_FullExample").getOrCreate()

# Generate 10-20 rows of sample nested JSON data
users = [f"UA{i}" for i in range(1,6)]  # 5 users
rows = []
for user in users:
    for _ in range(random.randint(2,4)):  # multiple events per user
        row = {
            "user_id": user,
            "event_name": random.choice(["shipping_info", "purchase"]),
            "items": [
                {"item_name": random.choice(["Standard Twin Mattress","Styled Twin Mattress","Queen Mattress"]),
                 "coupon": random.choice(["NEWBED10","NEWBED20","SALE15"])},
                {"item_name": random.choice(["Standard Twin Mattress","Styled Twin Mattress","Queen Mattress"]),
                 "coupon": random.choice(["NEWBED10","NEWBED20","SALE15"])}
            ]
        }
        rows.append((json.dumps(row),))

# Create DataFrame
df = spark.createDataFrame(rows, ["json_str"])

# Infer schema and parse JSON
df_schema = schema_of_json(lit(rows[0][0]))
df_parsed = df.withColumn("data", from_json(col("json_str"), df_schema))

# Select relevant fields
df_items = df_parsed.select(
    col("data.user_id").alias("user_id"),
    col("data.event_name").alias("event_name"),
    col("data.items").alias("items")
)

print("=== Data Before Aggregation ===")
df_items.show(truncate=False)

# Aggregate unique item names and coupons per user using collect_set, flatten, array_distinct
df_agg = df_items.groupBy("user_id").agg(
    array_distinct(flatten(collect_set(expr("transform(items, x -> x.item_name)")))).alias("unique_item_names"),
    array_distinct(flatten(collect_set(expr("transform(items, x -> x.coupon)")))).alias("unique_coupons")
)

print("=== Aggregated Unique Item Names and Coupons per User ===")
df_agg.show(truncate=False)

# Zip arrays and explode to one row per item per user
df_zipped = df_agg.withColumn("items_zipped", arrays_zip(col("unique_item_names"), col("unique_coupons")))
df_exploded = df_zipped.select("user_id", explode(col("items_zipped")).alias("item"))
df_exploded_final = df_exploded.select(
    "user_id",
    col("item.unique_item_names").alias("item_name"),
    col("item.unique_coupons").alias("coupon")
)

print("=== Exploded Aggregated Items per User ===")
df_exploded_final.show(truncate=False)
```

---

## 3. Key Notes

- `from_json` + `schema_of_json` parse nested JSON into DataFrame columns.
- `collect_set` aggregates multiple rows per user into an array.
- `flatten` merges arrays of arrays into a single array.
- `array_distinct` removes duplicates.
- `arrays_zip` keeps `item_name` paired with `coupon`.
- `explode` transforms arrays into one row per item per user.
- Script generates 10-20 rows dynamically inside the script for testing.

This Markdown file now includes the **full conversation logic, data generation, and all PySpark functions discussed**.


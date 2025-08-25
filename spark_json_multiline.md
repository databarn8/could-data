# Spark Multiline JSON Processing - Conversation Export

## User Question
```
customers_df = df.selectExpr("inline(customers)")

spark load multiline jason files

Can you check if the syntax is correct and can you create a simple sample to explain how spark does it?
```

## Response

I'll help you check the syntax and create a sample to demonstrate how Spark handles multiline JSON files.

### Syntax Check
Your syntax `customers_df = df.selectExpr("inline(customers)")` looks correct, assuming:
- `df` is a DataFrame that already contains the JSON data
- `customers` is an array column in your DataFrame
- You want to flatten/explode the array into separate rows

### Complete Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("MultilineJSONExample") \
    .getOrCreate()

# Sample multiline JSON content (what you might have in a file)
sample_json_content = '''
{
  "store_id": "store_001",
  "customers": [
    {
      "customer_id": "cust_001",
      "name": "John Doe",
      "email": "john@example.com",
      "orders": 5
    },
    {
      "customer_id": "cust_002", 
      "name": "Jane Smith",
      "email": "jane@example.com",
      "orders": 3
    }
  ]
}
{
  "store_id": "store_002",
  "customers": [
    {
      "customer_id": "cust_003",
      "name": "Bob Wilson",
      "email": "bob@example.com", 
      "orders": 7
    }
  ]
}
'''

# Method 1: Reading multiline JSON from file
# If you have a file, you would read it like this:
# df = spark.read.option("multiline", "true").json("path/to/your/file.json")

# Method 2: Creating DataFrame from sample data (for demonstration)
# First, let's create sample data files
with open("/tmp/sample_multiline.json", "w") as f:
    f.write(sample_json_content)

# Read multiline JSON file
print("=== Reading Multiline JSON ===")
df = spark.read.option("multiline", "true").json("/tmp/sample_multiline.json")

print("Original DataFrame schema:")
df.printSchema()
print("\nOriginal DataFrame content:")
df.show(truncate=False)

# Method 3: Using selectExpr with inline() - Your original approach
print("\n=== Using selectExpr with inline() ===")
customers_df = df.selectExpr("store_id", "inline(customers)")

print("Flattened customers schema:")
customers_df.printSchema()
print("\nFlattened customers content:")
customers_df.show(truncate=False)

# Alternative Method: Using explode() function
print("\n=== Alternative: Using explode() ===")
customers_df_alt = df.select("store_id", explode("customers").alias("customer"))
customers_df_expanded = customers_df_alt.select("store_id", "customer.*")

print("Alternative flattened customers:")
customers_df_expanded.show(truncate=False)

# Key options for reading multiline JSON files
print("\n=== Key Options for Multiline JSON ===")
print("1. multiline=true : Essential for reading JSON objects spanning multiple lines")
print("2. mode='PERMISSIVE' : Default mode, puts corrupt records in _corrupt_record column")
print("3. mode='DROPMALFORMED' : Drops malformed records")
print("4. mode='FAILFAST' : Throws exception on malformed records")

# Example with different options
df_with_options = spark.read \
    .option("multiline", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("/tmp/sample_multiline.json")

print("\nDataFrame with additional options:")
df_with_options.show()

# Clean up
spark.stop()
```

### Key Points About Your Syntax

1. **Your syntax is correct** - `selectExpr("inline(customers)")` will work if `customers` is an array of structs
2. The `inline()` function flattens array elements into separate rows and expands struct fields into columns
3. **Critical option**: You must use `.option("multiline", "true")` when reading JSON files that span multiple lines

### Common Patterns for Multiline JSON in Spark

```python
# Reading the file
df = spark.read.option("multiline", "true").json("your_file.json")

# Your approach (flattening arrays)
customers_df = df.selectExpr("inline(customers)")

# Alternative approach
customers_df = df.select(explode("customers").alias("customer")).select("customer.*")
```

### When to Use Each Approach

- Use `inline()` when you want to flatten arrays AND expand structs in one step
- Use `explode()` + `select()` when you need more control over the flattening process
- Always use `multiline=true` for JSON objects that span multiple lines

The sample code above demonstrates both approaches and shows how they produce the same result. Your original syntax should work perfectly for flattening nested customer arrays!

---

*Conversation exported on August 23, 2025*
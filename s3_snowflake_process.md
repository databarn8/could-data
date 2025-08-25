# S3 to Snowflake Data Pipeline Process

## Step 1: Prepare and Upload JSON File to S3

### 1.1 Save JSON Data to File
```bash
# Save the JSON data to a file named sample_events.json
# (Copy the JSON array from the previous artifact)
```

### 1.2 Upload to S3 using AWS CLI
```bash
# Upload file to your S3 bucket
aws s3 cp sample_events.json s3://your-bucket-name/events/sample_events.json

# Verify upload
aws s3 ls s3://your-bucket-name/events/
```

### 1.3 Alternative: Upload via AWS Console
1. Go to AWS S3 Console
2. Navigate to your bucket
3. Create/navigate to `events/` folder
4. Upload `sample_events.json`

## Step 2: Connect to Snowflake and Set Up Environment

### 2.1 Connect to Snowflake
```sql
-- Connect using SnowSQL or Snowflake Web UI
snowsql -a your-account -u your-username

-- Or resolve your billing issue first if trial expired
-- Then connect via web UI: https://your-account.snowflakecomputing.com
```

### 2.2 Set Up Database and Schema
```sql
-- Create or use existing database
USE DATABASE your_database;
-- or
CREATE DATABASE IF NOT EXISTS ANALYTICS_DB;
USE DATABASE ANALYTICS_DB;

-- Create or use schema
USE SCHEMA PUBLIC;
-- or 
CREATE SCHEMA IF NOT EXISTS EVENTS;
USE SCHEMA EVENTS;

-- Use appropriate warehouse
USE WAREHOUSE your_warehouse;
```

## Step 3: Verify and Use S3 Stage

### 3.1 Check Existing Stage
```sql
-- List all stages
SHOW STAGES;

-- Describe your existing stage
DESC STAGE my_s3_stage;

-- Test stage connectivity
LIST @my_s3_stage;
```

### 3.2 If Stage Doesn't Exist, Create It
```sql
-- Create S3 stage (if needed)
CREATE OR REPLACE STAGE my_s3_stage
  URL = 's3://your-bucket-name/events/'
  CREDENTIALS = (
    AWS_KEY_ID = 'your-aws-access-key'
    AWS_SECRET_KEY = 'your-aws-secret-key'
  )
  FILE_FORMAT = (
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
  );
```

### 3.3 List Files in Stage
```sql
-- Check if your file is accessible through the stage
LIST @my_s3_stage;

-- Look specifically for your file
LIST @my_s3_stage/sample_events.json;
```

## Step 4: Create Tables and Load Data

### 4.1 Create Raw JSON Table
```sql
-- Create table to store raw JSON
CREATE OR REPLACE TABLE raw_events (
    raw_data VARIANT,
    file_name STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 4.2 Load Data from Stage
```sql
-- Load JSON data from S3 stage
COPY INTO raw_events (raw_data, file_name)
FROM (
    SELECT 
        $1,
        METADATA$FILENAME
    FROM @my_s3_stage/sample_events.json
);

-- Check load results
SELECT COUNT(*) as total_records FROM raw_events;
```

### 4.3 Create Structured Table (Optional)
```sql
-- Create structured table for better performance
CREATE OR REPLACE TABLE events_structured AS
SELECT 
    raw_data:device::STRING as device,
    raw_data:event_name::STRING as event_name,
    raw_data:event_previous_timestamp::BIGINT as event_previous_timestamp,
    raw_data:event_timestamp::BIGINT as event_timestamp,
    TO_TIMESTAMP(raw_data:event_timestamp::BIGINT / 1000000) as event_datetime,
    raw_data:geo.city::STRING as city,
    raw_data:geo.state::STRING as state,
    raw_data:traffic_source::STRING as traffic_source,
    raw_data:user_first_touch_timestamp::BIGINT as user_first_touch_timestamp,
    raw_data:user_id::STRING as user_id,
    raw_data:items as items_array,
    load_timestamp
FROM raw_events;
```

## Step 5: Data Exploration Queries

### 5.1 Basic Data Overview
```sql
-- Check total records
SELECT COUNT(*) as total_events FROM events_structured;

-- View sample records
SELECT * FROM events_structured LIMIT 5;

-- Check data types and structure
DESCRIBE TABLE events_structured;
```

### 5.2 Event Analysis
```sql
-- Event distribution by type
SELECT 
    event_name,
    COUNT(*) as event_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM events_structured) as percentage
FROM events_structured
GROUP BY event_name
ORDER BY event_count DESC;

-- Device distribution
SELECT 
    device,
    COUNT(*) as device_count
FROM events_structured
GROUP BY device
ORDER BY device_count DESC;

-- Traffic source analysis
SELECT 
    traffic_source,
    COUNT(*) as source_count,
    COUNT(DISTINCT user_id) as unique_users
FROM events_structured
GROUP BY traffic_source
ORDER BY source_count DESC;
```

### 5.3 Geographic Analysis
```sql
-- Events by state
SELECT 
    state,
    city,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM events_structured
GROUP BY state, city
ORDER BY event_count DESC;

-- Top cities
SELECT 
    city,
    state,
    COUNT(*) as events
FROM events_structured
GROUP BY city, state
ORDER BY events DESC;
```

### 5.4 Items and Revenue Analysis
```sql
-- Flatten items array for detailed analysis
CREATE OR REPLACE VIEW items_detail AS
SELECT 
    e.user_id,
    e.event_name,
    e.device,
    e.city,
    e.state,
    e.event_datetime,
    items.value:item_id::STRING as item_id,
    items.value:item_name::STRING as item_name,
    items.value:quantity::INTEGER as quantity,
    items.value:price_in_usd::FLOAT as price_usd,
    items.value:item_revenue_in_usd::FLOAT as revenue_usd,
    items.value:coupon::STRING as coupon
FROM events_structured e,
LATERAL FLATTEN(input => e.items_array) items;

-- Revenue analysis
SELECT 
    item_name,
    SUM(quantity) as total_quantity,
    SUM(revenue_usd) as total_revenue,
    AVG(price_usd) as avg_price,
    COUNT(DISTINCT user_id) as unique_buyers
FROM items_detail
GROUP BY item_name
ORDER BY total_revenue DESC;

-- Coupon usage analysis
SELECT 
    CASE 
        WHEN coupon IS NULL THEN 'No Coupon'
        ELSE coupon 
    END as coupon_status,
    COUNT(*) as usage_count,
    SUM(revenue_usd) as total_revenue,
    AVG(revenue_usd) as avg_revenue_per_item
FROM items_detail
GROUP BY coupon_status
ORDER BY usage_count DESC;
```

### 5.5 Time-based Analysis
```sql
-- Events over time (by hour)
SELECT 
    DATE_TRUNC('HOUR', event_datetime) as event_hour,
    COUNT(*) as event_count
FROM events_structured
GROUP BY event_hour
ORDER BY event_hour;

-- User journey analysis
SELECT 
    user_id,
    COUNT(*) as total_events,
    MIN(event_datetime) as first_event,
    MAX(event_datetime) as last_event,
    LISTAGG(DISTINCT event_name, ' -> ') as event_sequence
FROM events_structured
GROUP BY user_id
ORDER BY total_events DESC;
```

### 5.6 Advanced Analytics
```sql
-- Revenue by geographic region
SELECT 
    state,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(i.revenue_usd) as total_revenue,
    AVG(i.revenue_usd) as avg_revenue_per_item
FROM events_structured e
JOIN items_detail i ON e.user_id = i.user_id
GROUP BY state
ORDER BY total_revenue DESC;

-- Conversion funnel analysis
WITH funnel AS (
    SELECT 
        user_id,
        MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) as viewed,
        MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) as added_to_cart,
        MAX(CASE WHEN event_name = 'checkout' THEN 1 ELSE 0 END) as checkout,
        MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) as purchased
    FROM events_structured
    GROUP BY user_id
)
SELECT 
    SUM(viewed) as total_viewers,
    SUM(added_to_cart) as total_cart_additions,
    SUM(checkout) as total_checkouts,
    SUM(purchased) as total_purchases,
    
    -- Conversion rates
    SUM(added_to_cart) * 100.0 / SUM(viewed) as view_to_cart_rate,
    SUM(checkout) * 100.0 / SUM(added_to_cart) as cart_to_checkout_rate,
    SUM(purchased) * 100.0 / SUM(checkout) as checkout_to_purchase_rate
FROM funnel;
```

## Step 6: Data Quality Checks

### 6.1 Validate Data Integrity
```sql
-- Check for missing values
SELECT 
    COUNT(*) as total_records,
    COUNT(user_id) as non_null_user_ids,
    COUNT(event_name) as non_null_events,
    COUNT(CASE WHEN items_array IS NULL THEN 1 END) as null_items
FROM events_structured;

-- Validate timestamp consistency
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN event_timestamp < event_previous_timestamp THEN 1 END) as timestamp_issues
FROM events_structured;

-- Check for duplicate events
SELECT 
    user_id,
    event_timestamp,
    COUNT(*) as duplicate_count
FROM events_structured
GROUP BY user_id, event_timestamp
HAVING COUNT(*) > 1;
```

## Troubleshooting Tips

### Common Issues and Solutions

1. **Stage Access Issues**
   ```sql
   -- Test stage permissions
   LIST @my_s3_stage;
   
   -- If access denied, verify AWS credentials and S3 permissions
   ```

2. **File Format Issues**
   ```sql
   -- Check file format settings
   SHOW FILE FORMATS;
   
   -- Create specific format if needed
   CREATE FILE FORMAT json_format
     TYPE = JSON
     STRIP_OUTER_ARRAY = TRUE;
   ```

3. **Data Loading Issues**
   ```sql
   -- Check copy command history
   SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY())
   WHERE TABLE_NAME = 'RAW_EVENTS'
   ORDER BY LAST_LOAD_TIME DESC;
   ```

4. **Performance Optimization**
   ```sql
   -- Create clustering keys for better performance
   ALTER TABLE events_structured 
   CLUSTER BY (event_name, state, DATE_TRUNC('DAY', event_datetime));
   ```

Remember to replace placeholder values like `your-bucket-name`, `your-account`, etc., with your actual values!
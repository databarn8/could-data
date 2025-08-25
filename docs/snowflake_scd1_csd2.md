Snowflake itself does not automatically implement SCD (Slowly Changing Dimension) logic, but it provides the tools—SQL, streams, tasks, and merge statements—to handle both SCD1 and SCD2 efficiently. Let me break it down clearly.

1️⃣ SCD Type 1 (Overwrite / No History)

Goal: Always keep the latest value for a dimension. No history is kept.

Implementation in Snowflake: Use a MERGE statement to update existing rows and insert new rows.

Example Table
CREATE OR REPLACE TABLE customers_scd1 (
    customer_id INT PRIMARY KEY,
    name STRING,
    email STRING
);

Merge Statement

Assume staging_customers has new/updated records:

MERGE INTO customers_scd1 AS target
USING staging_customers AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.email = source.email
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email)
    VALUES (source.customer_id, source.name, source.email);


✅ This updates existing rows and inserts new rows. Old data is overwritten.

2️⃣ SCD Type 2 (Keep Full History)

Goal: Keep a full history of changes with effective dates or current flag.

Common Pattern: Add columns like:

effective_from (start date of the record)

effective_to (end date, or NULL if current)

is_current (optional flag: TRUE for current record)

Example Table
CREATE OR REPLACE TABLE customers_scd2 (
    customer_id INT,
    name STRING,
    email STRING,
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN
);

SCD2 Merge Logic
MERGE INTO customers_scd2 AS target
USING staging_customers AS source
ON target.customer_id = source.customer_id
   AND target.is_current = TRUE
WHEN MATCHED AND 
     (target.name != source.name OR target.email != source.email) THEN
    -- Close old record
    UPDATE SET 
        target.effective_to = CURRENT_DATE,
        target.is_current = FALSE
WHEN NOT MATCHED THEN
    -- Insert new record
    INSERT (customer_id, name, email, effective_from, effective_to, is_current)
    VALUES (source.customer_id, source.name, source.email, CURRENT_DATE, NULL, TRUE);


✅ This preserves old rows and inserts new versioned rows when changes occur.

3️⃣ Automation in Snowflake

Streams: Track changes in staging tables.

Tasks: Schedule merge operations to run continuously or daily.

Snowpipe + Streams: You can ingest incremental data and automatically apply SCD logic.

Workflow Example:

Stage data via Snowpipe → staging table

Use Stream to detect new/changed rows

Apply MERGE for SCD1 or SCD2

Schedule with Tasks for automation

💡 Key Points:

SCD1 = overwrite, no history

SCD2 = keep history, use effective_from, effective_to, and is_current

Snowflake does not have built-in SCD tables; you define logic via SQL and automation (streams/tasks)
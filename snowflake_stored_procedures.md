# Snowflake Stored Procedures with IF Conditions

This document contains examples of Snowflake stored procedures using both `LANGUAGE SQL` (Snowflake Scripting) and `LANGUAGE JAVASCRIPT`, including hybrid approaches.

---

## 1. Simple Stored Procedure with IF (LANGUAGE SQL)

```sql
CREATE OR REPLACE PROCEDURE update_pending_orders_sql()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_count INTEGER;
BEGIN
    -- Count pending orders
    SELECT COUNT(*) INTO v_count FROM ORDERS WHERE status = 'PENDING';

    -- Check condition
    IF v_count > 0 THEN
        UPDATE ORDERS SET status = 'PROCESSING' WHERE status = 'PENDING';
        RETURN 'Updated ' || v_count || ' pending orders to PROCESSING.';
    ELSE
        RETURN 'No pending orders found.';
    END IF;
END;
$$;
```

Usage:

```sql
CALL update_pending_orders_sql();
```

---

## 2. Returning a Result Set (LANGUAGE SQL)

```sql
CREATE OR REPLACE PROCEDURE update_pending_orders_result()
RETURNS TABLE (order_id INT, status STRING, amount NUMBER, note STRING)
LANGUAGE SQL
AS
$$
DECLARE
    v_count INTEGER;
BEGIN
    -- Count pending orders
    SELECT COUNT(*) INTO v_count FROM ORDERS WHERE status = 'PENDING';

    IF v_count > 0 THEN
        -- Update and return updated rows
        UPDATE ORDERS SET status = 'PROCESSING' WHERE status = 'PENDING';

        RETURN TABLE(
            SELECT order_id, status, amount, 'Updated to PROCESSING' AS note
            FROM ORDERS
            WHERE status = 'PROCESSING'
        );
    ELSE
        -- Return a message row
        RETURN TABLE(
            SELECT NULL AS order_id, NULL AS status, NULL AS amount, 'No pending orders found.' AS note
        );
    END IF;
END;
$$;
```

Usage:

```sql
CALL update_pending_orders_result();
```

---

## 3. Dynamic Result Set (LANGUAGE JAVASCRIPT)

```sql
CREATE OR REPLACE PROCEDURE dynamic_query_return(query STRING)
RETURNS TABLE ()
LANGUAGE JAVASCRIPT
AS
$$
    var stmt = snowflake.createStatement({sqlText: query});
    var rs = stmt.execute();
    return rs;  -- dynamic RESULTSET
$$;
```

Usage:

```sql
CALL dynamic_query_return('SELECT order_id, status, amount FROM ORDERS WHERE status = ''PENDING''');
```

---

## 4. Hybrid Approach (SQL + JavaScript)

### Step 1 – JavaScript dynamic runner

```sql
CREATE OR REPLACE PROCEDURE run_dynamic_query(query STRING)
RETURNS TABLE ()
LANGUAGE JAVASCRIPT
AS
$$
    var stmt = snowflake.createStatement({sqlText: query});
    var rs = stmt.execute();
    return rs;
$$;
```

### Step 2 – SQL procedure with IF logic

```sql
CREATE OR REPLACE PROCEDURE process_orders_hybrid()
RETURNS TABLE ()
LANGUAGE SQL
AS
$$
DECLARE
    v_count INTEGER;
    v_sql   STRING;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ORDERS WHERE status = 'PENDING';

    IF v_count > 0 THEN
        UPDATE ORDERS SET status = 'PROCESSING' WHERE status = 'PENDING';
        LET v_sql = 'SELECT order_id, status, amount FROM ORDERS WHERE status = ''PROCESSING''';
    ELSE
        LET v_sql = 'SELECT ''No pending orders found.'' AS message';
    END IF;

    RETURN TABLE(CALL run_dynamic_query(v_sql));
END;
$$;
```

Usage:

```sql
CALL process_orders_hybrid();
```

---

## 5. Generic Hybrid Procedure

```sql
CREATE OR REPLACE PROCEDURE process_table_hybrid(
    table_name STRING,
    condition  STRING,
    update_set STRING
)
RETURNS TABLE ()
LANGUAGE SQL
AS
$$
DECLARE
    v_count INTEGER;
    v_sql   STRING;
BEGIN
    -- Count rows
    LET v_sql = 'SELECT COUNT(*) FROM ' || table_name || ' WHERE ' || condition;
    EXECUTE IMMEDIATE :v_sql INTO v_count;

    IF v_count > 0 THEN
        LET v_sql = 'UPDATE ' || table_name || ' SET ' || update_set || ' WHERE ' || condition;
        EXECUTE IMMEDIATE :v_sql;

        LET v_sql = 'SELECT * FROM ' || table_name || ' WHERE ' || condition;
    ELSE
        LET v_sql = 'SELECT ''No rows found for condition: ' || condition || ''' AS message';
    END IF;

    RETURN TABLE(CALL run_dynamic_query(v_sql));
END;
$$;
```

Usage:

```sql
CALL process_table_hybrid(
    'ORDERS',
    'status = ''PENDING''',
    'status = ''PROCESSING'''
);
```

---

## Notes
- Use `LANGUAGE SQL` when schema is fixed.  
- Use `LANGUAGE JAVASCRIPT` when you need dynamic result sets.  
- Use the **hybrid pattern** to combine both: logic in SQL + dynamic query in JavaScript.  


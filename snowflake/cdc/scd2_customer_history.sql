CREATE OR REPLACE TASK task_customer_history
WAREHOUSE = COMPUTE_WH
AFTER task_customer_current
WHEN SYSTEM$STREAM_HAS_DATA('target_table_changes')
AS
-- for delete and udate case 
UPDATE customer_history h
SET
    end_time   = CURRENT_TIMESTAMP(),
    is_current = FALSE
FROM (
    SELECT *
    FROM target_table_changes
    WHERE
          METADATA$ACTION = 'DELETE'
       OR (METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY METADATA$ROW_ID
    ) = 1
) src
WHERE h.customer_id = src.customer_id
  AND h.is_current = TRUE;

  
INSERT INTO customer_history (
    customer_id,
    first_name,
    last_name,
    email,
    street,
    city,
    state,
    country,
    start_time,
    end_time,
    is_current
)
SELECT
    src.customer_id,
    src.first_name,
    src.last_name,
    src.email,
    src.street,
    src.city,
    src.state,
    src.country,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM (
    SELECT *
    FROM target_table_changes
    WHERE METADATA$ACTION = 'INSERT'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY METADATA$ROW_ID
    ) = 1
) src;

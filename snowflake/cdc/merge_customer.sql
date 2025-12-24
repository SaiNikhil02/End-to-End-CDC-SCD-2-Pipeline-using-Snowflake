CREATE OR REPLACE TASK task_customer_current
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('customer_table_changes')
AS

MERGE INTO customer t
USING SCD_DEMO.SCD2.CUSTOMER_TABLE_CHANGES  as src
ON t.customer_id = src.customer_id

--DELETE records
WHEN MATCHED
  AND src.METADATA$ACTION = 'DELETE'
THEN DELETE

--UPDATE existing records (CDC update = INSERT + ISUPDATE = TRUE)
WHEN MATCHED
  AND src.METADATA$ACTION = 'INSERT'
  AND src.METADATA$ISUPDATE = TRUE
THEN UPDATE SET
    t.first_name        = src.first_name,
    t.last_name         = src.last_name,
    t.email             = src.email,
    t.street            = src.street,
    t.city              = src.city,
    t.state             = src.state,
    t.country           = src.country,
    t.update_timestamp  = CURRENT_TIMESTAMP()

--INSERT brand new records
WHEN NOT MATCHED
  AND src.METADATA$ACTION = 'INSERT'
  AND src.METADATA$ISUPDATE = FALSE
THEN INSERT (
    customer_id,
    first_name,
    last_name,
    email,
    street,
    city,
    state,
    country,
    update_timestamp
)
VALUES (
    src.customer_id,
    src.first_name,
    src.last_name,
    src.email,
    src.street,
    src.city,
    src.state,
    src.country,
    CURRENT_TIMESTAMP()
);

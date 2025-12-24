CREATE OR REPLACE STAGE SCD_DEMO.SCD2.customer_ext_stage 
url = 's3://dw-snowflake-course/stream-data/'
credentials = (aws_key = '' , aws_secret_key = '')

CREATE OR REPLACE FILE FORMAT SCD_DEMO.SCD2.CSV
TYPE = CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

LIST @customer_ext_stage;

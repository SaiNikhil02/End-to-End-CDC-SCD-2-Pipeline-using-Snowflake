CREATE OR REPLACE PIPE customer_s3_pipe
  auto_ingest = true
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  FILE_FORMAT = CSV
  ;

show pipes;
desc pipe customer_s3_pipe;

SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMER_RAW',
    START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP)
  )
);

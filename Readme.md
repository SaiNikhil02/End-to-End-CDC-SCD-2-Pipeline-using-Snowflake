## Architecture Overview

This project implements an end-to-end CDC pipeline using Apache NiFi, AWS, and Snowflake.

Apache NiFi (running on Dockerized EC2) ingests source data and writes files to Amazon S3.
Snowpipe automatically ingests these files into a Snowflake staging table.
Snowflake Streams capture incremental changes from the staging and target tables.
Snowflake Tasks consume the streams to apply CDC logic and maintain both current-state and SCD Type-2 history tables.

The pipeline is fully event-driven, scalable, and supports exactly-once processing.



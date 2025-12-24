create or replace stream customer_table_changes on table customer;

select * from    customer_table_changes;
  
DROP STREAM IF EXISTS customer_table_changes;
create or replace stream customer_table_changes on table customer_raw;

SELECT * FROM customer_history ;
create or replace stream target_table_changes on table customer;
select * from target_table_changes;

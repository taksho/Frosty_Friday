-- Set the SYSADMIN role
use role sysadmin;

-- Create warehouse, database, schema
use warehouse ff_wh;
use schema ff_db.ff_schema;

-- Create internal stage
create or replace stage ff_int_stage;

-- Upload .parquet by UI and list a file in internal stage
list @ff_int_stage;

-- Create a file format for parquet
create or replace file format parquet_ff
    type = parquet
;

-- Check the schema of parquet using infer_schema
select * from table(
    infer_schema(
        location=>'@ff_int_stage',
        file_format=>'parquet_ff'
    )
)
;

-- Create table using infer_schema
create table week2parquet using template (
    select array_agg(object_construct(*))
    from table (
        infer_schema(
            location=>'@ff_int_stage',
            file_format=>'parquet_ff'
            )
    )
);

-- Load the parquet file using MATCH_BY_COLUMN_NAME.
copy into week2parquet from '@ff_int_stage'
file_format = (format_name = 'parquet_ff') MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;

-- Check the table
select * from week2parquet;

-- Create a view that only show us the EMPLOEE_ID and DEPT and JOB_TITLE columns
create view week2view as
select "employee_id", "dept", "job_title" from week2parquet;

-- Create a stream that tracks the view's change
create or replace stream week2view_stream on view week2view;

-- Execute the following commands
UPDATE week2parquet SET "country" = 'Japan' WHERE "employee_id" = 8;
UPDATE week2parquet SET "last_name" = 'Forester' WHERE "employee_id" = 22;
UPDATE week2parquet SET "dept" = 'Marketing' WHERE "employee_id" = 25;
UPDATE week2parquet SET "title" = 'Ms' WHERE "employee_id" = 32;
UPDATE week2parquet SET "job_title" = 'Senior Financial Analyst' WHERE "employee_id" = 68;

-- Check the result of stream
select * from week2view_stream;
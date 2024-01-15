-- Set the SYSADMIN role
use role sysadmin;

-- Create warehouse, Database, schema
create or replace warehouse ff_wh AUTO_SUSPEND=1;
create or replace database ff_db;
create or replace schema ff_db.ff_schema;

-- Create S3 External Stage
create or replace stage ff_db.ff_schema.ff_s3
    url = 's3://frostyfridaychallenges/challenge_1/'
;

-- List files in S3 bucket
list @ff_db.ff_schema.ff_s3;

-- Create a file format for csv
create or replace file format csv_ff
    type = csv
;

-- Check the files using the file format
select $1, metadata$filename, metadata$file_row_number from @ff_s3 (file_format=>'csv_ff');

-- Replace the file format
create or replace file format csv_ff
    type = csv
    skip_header = 1
    null_if = ('NULL', 'totally_empty') 
    skip_blank_lines = true
    comment = '"null_if" is used to eliminate useless values'
;

-- Create a table
create or replace table week1csv(
    result varchar,
    filename varchar,
    file_row_number int,
    loaded_at timestamp_ltz
)
;

-- Copy the files using file format and metadatas
COPY into week1csv from (
    select 
        $1,
        metadata$filename,
        metadata$file_row_number,
        metadata$start_scan_time
    from '@ff_s3')
    file_format = (format_name = 'csv_ff')
;

-- Delete the NULL rows
delete from week1csv where result is null;

-- Select the result by the right order
select * from week1csv order by filename, file_row_number;
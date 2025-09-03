create database news_api;

use news_api;

create file format parquet_format type=parquet;

create or replace storage integration news_data_storage_integration
type = external_stage
storage_provider = gcs
enabled = true
storage_allowed_locations = ('gcs://snowflake_projects_gds_test/news_data_analysis/parquet_files');

-- Retrieve the Cloud Storage Service Account for your snowflake account

desc storage integration news_data_storage_integration;

-- A stage in Snowflake refers to a location (internal or external) 
-- where data files are uploaded, stored, and prepared before being loaded into Snowflake tables.

create or replace stage gcs_raw_data_stage
    url = 'gcs://snowflake_projects_gds_test/news_data_analysis/parquet_files/'
    storage_integration = news_data_storage_integration
    file_format = (type = 'PARQUET');

show stages;

list @gcs_raw_data_stage;

select * from NEWS_API.PUBLIC.NEWS_API_DATA;
select * from NEWS_API.PUBLIC.AUTHOR_ACTIVITY;
select * from NEWS_API.PUBLIC.SUMMARY_NEWS;

drop stage gcs_raw_data_stage;
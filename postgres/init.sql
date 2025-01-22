-- Create a schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS metadata_utilities;

-- Create a tables for data storage
DROP TABLE IF EXISTS metadata_utilities.ingestion_connection_info;
CREATE TABLE metadata_utilities.ingestion_connection_info (
    connection_id SERIAL PRIMARY KEY,
    connection_name VARCHAR(255) NOT NULL,
    connection_url VARCHAR(255) NOT NULL,
    user_name VARCHAR(255) NOT NULL, 
    password_encrypted VARCHAR NOT NULL, 
    security_token VARCHAR NOT NULL, 
    created_by VARCHAR(255) NOT NULL, 
    created_date TIMESTAMP NOT NULL,
    modified_by VARCHAR(255) NOT NULL, 
    modified_date TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS metadata_utilities.ingestion_config;
CREATE TABLE IF NOT EXISTS metadata_utilities.ingestion_config (
ingestion_config_id SERIAL PRIMARY KEY,
ingestion_type VARCHAR(255) NOT NULL,
source_schema_table_name VARCHAR(255) NOT NULL, 
primary_key_column VARCHAR(255), 
incremental_date_column VARCHAR(255),
load_type VARCHAR(50) NOT NULL,  
extra_parameters VARCHAR(255), 
partitioned_by VARCHAR(255),
api_source_name VARCHAR(255),    
source_system VARCHAR(255) NOT NULL,
connection_name VARCHAR(255) NOT NULL,
project_id VARCHAR(255) NOT NULL,  
database_schema VARCHAR(255) NOT NULL, 
table_name VARCHAR(255) NOT NULL,
file_format VARCHAR(50), 
header VARCHAR(255),
delimiter VARCHAR(50),
quote_characters VARCHAR(50),
escape_characters VARCHAR(50),
accepted_encoding VARCHAR(50),
is_parquet  BOOLEAN,
to_parquet BOOLEAN NOT NULL,
bucket VARCHAR(255) NOT NULL,
bucket_destination VARCHAR(255) NOT NULL,
archive_destination VARCHAR(255) NOT NULL,
created_by VARCHAR(255) NOT NULL, 
created_date TIMESTAMP NOT NULL,
modified_by VARCHAR(255) NOT NULL, 
modified_date TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS metadata_utilities.ingestion_column_details;
CREATE TABLE IF NOT EXISTS metadata_utilities.ingestion_column_details (
  column_name VARCHAR(255) NOT NULL,
  datatype VARCHAR(255) NOT NULL,
  ordinal_position BIGINT NOT NULL,
  mapping_column VARCHAR(255),
  project_id VARCHAR(255) NOT NULL,
  database_schema VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  modified_by VARCHAR(255) NOT NULL,
  modified_date TIMESTAMP  NOT NULL,
  created_by VARCHAR(255) NOT NULL,
  created_date TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS metadata_utilities.workflow_action_history;
CREATE TABLE IF NOT EXISTS metadata_utilities.workflow_action_history (
action_id SERIAL PRIMARY KEY,
connection_name VARCHAR(255) NOT NULL,
database_schema VARCHAR(255) NOT NULL,
table_name VARCHAR(255) NOT NULL,
executed_by VARCHAR(255) NOT NULL, 
execution_start_datetime TIMESTAMP NOT NULL,
execution_end_datetime TIMESTAMP,
execution_status BIGINT
);

DROP TABLE IF EXISTS metadata_utilities.workflow_audit_details;
CREATE TABLE IF NOT EXISTS metadata_utilities.workflow_audit_details (
audit_id SERIAL PRIMARY KEY,
action_id BIGINT NOT NULL, 
connection_name VARCHAR(255) NOT NULL,
database_schema VARCHAR(255) NOT NULL,
table_name VARCHAR(255) NOT NULL,
execution_start_datetime TIMESTAMP,
execution_end_datetime TIMESTAMP,
execution_duration BIGINT,
record_cnt_before BIGINT,
record_cnt_after BIGINT,
delta FLOAT8,
delta_avg FLOAT8,
delta_variance FLOAT8,
delta_std_deviation FLOAT8,
FOREIGN KEY (action_id) REFERENCES metadata_utilities.workflow_action_history(action_id)
);  
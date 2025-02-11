DROP TABLE IF EXISTS metadata_utilities.spark_config;
CREATE TABLE IF NOT EXISTS metadata_utilities.spark_config (
spark_id SERIAL PRIMARY KEY,
application_name VARCHAR(255) NOT NULL,
executor_instances VARCHAR(10) NOT NULL,
executor_memory VARCHAR(10) NOT NULL,
executor_cores VARCHAR(10) NOT NULL,
driver_memory VARCHAR(10) NOT NULL,
driver_cores VARCHAR(10) NOT NULL,
auto_broadcast_join_threshold VARCHAR(10) NOT NULL,
shuffle_partitions VARCHAR(10) NOT NULL,
broadcast_timeout VARCHAR(10) NOT NULL,
connection_id BIGINT NOT NULL,
connection_name VARCHAR(255) NOT NULL,
bucket VARCHAR(255) NOT NULL,
source_file VARCHAR(255) NOT NULL,
created_by VARCHAR(255) NOT NULL, 
created_date TIMESTAMP NOT NULL,
modified_by VARCHAR(255) NOT NULL, 
modified_date TIMESTAMP NOT NULL,
FOREIGN KEY (connection_id) REFERENCES metadata_utilities.ingestion_connection_info(connection_id)
);  
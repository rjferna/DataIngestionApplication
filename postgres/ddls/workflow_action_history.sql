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
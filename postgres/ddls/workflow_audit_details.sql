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
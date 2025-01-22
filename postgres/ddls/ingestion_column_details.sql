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
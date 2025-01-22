DROP TABLE IF EXISTS ingestion_connection_info;
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

INSERT INTO metadata_utilities.ingestion_connection_info (connection_name, connection_url,user_name, password_encrypted,  security_token, created_by, created_date, modified_by, modified_date) VALUES
(
'TEST', 
'TEST ENTRY', 
'ADMIN', 
'<encrypted_credentials>', 
'{"token": "<security_token_if_needed>", "access": "<access_key_if_needed>"}',
'ADMIN',
CURRENT_TIMESTAMP,
'ADMIN',
CURRENT_TIMESTAMP)
;
def get_connection_details(connection_name: str, table_name: str) -> str:
    try:
        query = f""" 
                    SELECT DISTINCT 
                        a.connection_name,   
                        a.connection_url, 
                        a.user_name, 
                        a.password_encrypted, 
                        a.security_token, 
                        b.ingestion_type,
                        b.source_schema_table_name,
                        b.primary_key_column,
                        b.incremental_date_column,
                        b.load_type,
                        b.extra_parameters,
                        b.project_id,
                        b.database_schema,
                        b.table_name,
                        b.file_format,
                        b.header,
                        b.delimiter,            
                        b.quote_characters,
                        b.escape_characters,
                        b.accepted_encoding,
                        b.is_parquet,
                        b.to_parquet,
                        b.bucket,
                        b.bucket_destination,
                        b.archive_destination
                      FROM metadata_utilities.ingestion_connection_info as a
                      INNER JOIN metadata_utilities.ingestion_config as b on a.connection_name = b.connection_name
                      WHERE 
                        a.connection_name = '{connection_name}'
                      AND
                        b.table_name = '{table_name}'; 
                    """
        return query
    except Exception as e:
        return f"Error: {e}"


def get_column_details(project_id: str, database_schema: str, table_name: str) -> str:
    try:
        query = f""" 
                SELECT 
                table_name,
                STRING_AGG(LOWER(mapping_column) || ' ' || datatype, ', ') AS stg_ref_create_table_column_details,  
                STRING_AGG('SAFE_CAST(' || LOWER(column_name) || ' AS ' || datatype || ') AS ' || LOWER(mapping_column), ', ') AS source_to_stg_conversion_column_details,
                STRING_AGG(LOWER(column_name), ', ') AS source_to_stg_column_query,
                STRING_AGG(LOWER(mapping_column), ', ') AS mapping_stg_to_ref_column_query
                FROM metadata_utilities.ingestion_column_details
                WHERE table_name = '{table_name}' AND database_schema = '{database_schema}' AND project_id = '{project_id}'
                GROUP BY table_name;  """

        return query
    except Exception as e:
        return f"Error: {e}"


##-------------------------------------------##
# ----     Workflow Action Functions     ---- #
##-------------------------------------------##


def set_workflow_action_history_record(
    connection_name: str,
    database_schema: str,
    table_name: str,
    user_name: str,
    execution_start_datetime: str,
    execution_status: int,
) -> str:
    try:
        query = f""" 
                    INSERT INTO metadata_utilities.workflow_action_history (connection_name, database_schema, table_name, executed_by, execution_start_datetime, execution_end_datetime, execution_status) 
                    VALUES('{connection_name}', '{database_schema}', '{table_name}', '{user_name}', CAST('{execution_start_datetime}' AS TIMESTAMP), NULL, CAST({execution_status} AS BIGINT));
                """
        return query
    except Exception as e:
        return f"Error: {e}"


def get_workflow_action_id() -> str:
    try:
        query = f""" SELECT MAX(action_id) as action_id FROM metadata_utilities.workflow_action_history; """
        return query
    except Exception as e:
        return f"Error: {e}"


def update_workflow_action_history_record(action_id: int, execution_status: int) -> str:
    try:
        query = f""" UPDATE metadata_utilities.workflow_action_history 
                     SET execution_end_datetime = CURRENT_TIMESTAMP, execution_status = CAST({execution_status} AS BIGINT)
                     WHERE action_id = CAST({action_id} AS BIGINT); """

        return query
    except Exception as e:
        return f"Error: {e}"


##-------------------------------------------##
# ----      Workflow Audit Functions     ---- #
##-------------------------------------------##


def set_workflow_audit_details_record(
    action_id: int,
    connection_name: str,
    database_schema: str,
    table_name: str,
    execution_start_datetime: str,
    record_count: int,
) -> str:
    try:
        query = f""" 
                    INSERT INTO metadata_utilities.workflow_audit_details (
                    action_id, 
                    connection_name, 
                    database_schema, 
                    table_name, 
                    execution_start_datetime, 
                    execution_end_datetime, 
                    execution_duration, 
                    record_cnt_before, 
                    record_cnt_after, 
                    delta, 
                    delta_avg, 
                    delta_variance, 
                    delta_std_deviation
                    ) 
                    VALUES({action_id}, '{connection_name}', '{database_schema}', '{table_name}', CAST('{execution_start_datetime}' AS TIMESTAMP), NULL, NULL, {record_count}, NULL, NULL, NULL, NULL, NULL); 
                    """
        return query
    except Exception as e:
        return f"Error: {e}"


def update_workflow_audit_details_record(
    action_id: int, table_name: str, record_count: int
) -> str:
    try:
        query = f"""
                    UPDATE metadata_utilities.workflow_audit_details
                    SET execution_end_datetime = (SELECT execution_end_datetime FROM metadata_utilities.workflow_action_history WHERE action_id = {action_id}),
                    record_cnt_after= {record_count}
                    WHERE action_id= {action_id};

                    UPDATE metadata_utilities.workflow_audit_details
                    SET execution_duration = (EXTRACT(EPOCH FROM (execution_end_datetime - execution_start_datetime)) / 60),
                    delta= (record_cnt_after - record_cnt_before)
                    WHERE action_id= {action_id};

                    UPDATE metadata_utilities.workflow_audit_details
                    SET delta_avg = a.avrg, delta_variance = a.var, delta_std_deviation = a.standard_deviation
                    FROM ( 
                    WITH vals AS (
                    SELECT 
                        audit_id,
                        COALESCE(delta, 0.0) AS record_cnt_delta
                    FROM metadata_utilities.workflow_audit_details
                    WHERE table_name = UPPER('{table_name}')
                    ORDER BY audit_id DESC
                    LIMIT 7
                    ), v_avg AS (
                    SELECT
                        SUM(record_cnt_delta) / COUNT(audit_id) AS avrg
                    FROM vals 
                    ), v_var_stddev AS (
                    SELECT 
                        SUM(ABS(record_cnt_delta - avrg) ^ 2) / COUNT(audit_id) AS var, 
                        CASE
                            WHEN COUNT(audit_id) - 1 = 0 THEN SUM(ABS(record_cnt_delta - avrg) ^ 2) / COUNT(audit_id)
                            ELSE SUM(ABS(record_cnt_delta - avrg) ^ 2) / (COUNT(audit_id) - 1) 
                        END AS standard_deviation 
                    FROM vals 
                    CROSS JOIN v_avg 
                    ) 
                    SELECT
                        avrg,
                        var, 
                        standard_deviation 
                    FROM v_avg
                    CROSS JOIN v_var_stddev 
                    ) AS a
                    WHERE action_id = {action_id};
                    """
        return query
    except Exception as e:
        return f"Error: {e}"

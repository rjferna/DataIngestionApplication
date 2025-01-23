def get_incremental_date(
    date: str, project_id: str, dataset: str, table_name: str
) -> str:
    try:
        query = f""" SELECT MAX({date}) AS VALUE FROM `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`;  """
        return query
    except Exception as e:
        return f"Error: {e}"


def get_table_exists(project_id: str, dataset: str, table_name: str) -> str:
    try:
        query = f""" SELECT COUNT(1) AS VALUE FROM `{project_id.lower()}.ref_{dataset.lower()}.__TABLES_SUMMARY__` WHERE table_id = '{table_name.lower()}';  """
        return query
    except Exception as e:
        return f"Error: {e}"


def get_record_count(project_id: str, dataset: str, table_name: str):
    try:
        query = f""" SELECT COUNT(1) AS VALUE FROM `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`;  """
        return query
    except Exception as e:
        return 0


def create_external_table(
    project_id: str,
    dataset,
    table_name: str,
    bucket_destination_name: str,
    file_format: str,
) -> str:
    try:
        query = f""" CREATE OR REPLACE EXTERNAL TABLE `{project_id.lower()}.external_{dataset.lower()}.{table_name.lower()}`
                     OPTIONS (
                     format= '{file_format}', 
                     uris=["gs://{bucket_destination_name}"]
                     ); """
        return query
    except Exception as e:
        return f"Error: {e}"


def create_and_load_staging_table(
    project_id: str,
    dataset: str,
    table_name: str,
    stg_and_ref_create_table: str,
    source_to_stg_conversion: str,
) -> str:
    try:
        query = f""" 
                    DROP TABLE IF EXISTS `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}`;
                    CREATE TABLE IF NOT EXISTS `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}` ( 
                    {stg_and_ref_create_table}, IS_DELETED BOOL, IS_HARD_DELETE BOOL, DW_CREATE_DATETIME DATETIME, DW_LOAD_DATETIME DATETIME);
                      
                    INSERT INTO `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}`
                    SELECT {source_to_stg_conversion}, FALSE AS IS_DELETED, FALSE AS IS_HARD_DELETE, CURRENT_DATETIME AS DW_CREATE_DATETIME, CURRENT_DATETIME AS DW_LOAD_DATETIME 
                    FROM `{project_id.lower()}.external_{dataset.lower()}.{table_name.lower()}`; 
                """
        return query
    except Exception as e:
        return f"Error: {e}"


def set_column_alias(columns: str) -> dict:
    try:
        list_of_columns = columns.split(",")
        mydict = {}
        hold = []
        alias_T = []
        alias_S = []

        for cols in list_of_columns:
            if cols not in ("IS_DELETED", "IS_HARD_DELETE", "DW_CREATE_DATETIME"):
                hold.append("T." + cols + "=S." + cols)
                alias_T.append("T." + cols)
                alias_S.append("S." + cols)

        mydict.update({"cols": ",".join(hold)})
        mydict.update({"T": ",".join(alias_T)})
        mydict.update({"S": ",".join(alias_S)})

        return mydict
    except Exception as e:
        return f"Error"


def create_and_load_reference_table(
    flag: int,
    project_id: str,
    dataset: str,
    table_name: str,
    stg_and_ref_create_table: str,
    mapping_stg_to_ref_query: str,
    primary_key_column: str,
) -> str:
    try:

        if flag == 0:
            query = f""" 
                    DROP TABLE IF EXISTS `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`;
                    CREATE TABLE IF NOT EXISTS `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}` ( 
                    {stg_and_ref_create_table}, IS_DELETED BOOL, IS_HARD_DELETE BOOL, DW_CREATE_DATETIME DATETIME, DW_LOAD_DATETIME DATETIME);

                    INSERT INTO `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`
                    SELECT {mapping_stg_to_ref_query}, FALSE AS IS_DELETED, FALSE AS IS_HARD_DELETE, CURRENT_DATETIME AS DW_CREATE_DATETIME, CURRENT_DATETIME AS DW_LOAD_DATETIME 
                    FROM `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}`;
                    """
        elif flag == 1:

            dict_statements = set_column_alias(columns=mapping_stg_to_ref_query)

            query = f""" 
                        MERGE `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}` as T
                        USING `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}` as S
                        ON T.{primary_key_column} = S.{primary_key_column}
                        WHEN MATCHED THEN 
                        UPDATE SET {dict_statements['cols']}
                        WHEN NOT MATCHED THEN
                        INSERT ({mapping_stg_to_ref_query}, IS_DELETED, IS_HARD_DELETE, DW_CREATE_DATETIME, DW_LOAD_DATETIME)
                        VALUES ({dict_statements['S']}, FALSE, FALSE, CURRENT_DATETIME, CURRENT_DATETIME);
                    """
        return query
    except Exception as e:
        return f"Error: {e}"

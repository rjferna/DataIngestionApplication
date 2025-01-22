from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta


def list_files_in_bucket(
    bucket_name: str, bucket_destination: str, keyfile_path: str
) -> list[str]:
    # Initialize a client with the service account keyfile
    storage_client = storage.Client.from_service_account_json(keyfile_path)

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # List all objects in the bucket
    blobs = bucket.list_blobs()

    bucket_items = []
    files = []

    # print(f"Files in bucket {bucket_name}:")
    for blob in blobs:
        bucket_items.append(blob.name)

    # Clean Name
    for val in range(0, len(bucket_items)):
        if len(bucket_items[val]) >= 21:
            # files.append(bucket_items[val].split('input/nhl-game-data/')[1].split('.')[0]) # Removes bucket path and file type extension
            files.append(bucket_items[val].split(bucket_destination)[1])

    return files


def get_gcp_storage(
    bucket_name: str,
    prefix_path: str,
    import_path: str,
    import_file: str,
    keyfile: dict,
) -> str:
    try:
        FULL_IMPORT_PATH = "{}{}".format(import_path, import_file)

        credentials = Credentials.from_service_account_info(keyfile)

        # Initialize a client with the service account keyfile
        storage_client = storage.Client(credentials=credentials)
        # storage_client = storage.Client.from_service_account_json(keyfile)

        # Get the bucket
        bucket = storage_client.get_bucket(bucket_name)

        # Get the file
        file = bucket.blob(prefix_path)

        file.download_to_filename(FULL_IMPORT_PATH)

        return f"{FULL_IMPORT_PATH}"
    except Exception as e:
        return f"Error: {e}"


def upload_to_bucket(
    bucket_name: str,
    source_file_name: str,
    destination_blob_name: str,
    keyfile_path: str,
) -> str:
    try:
        # Initialize a client with the service account keyfile
        storage_client = storage.Client.from_service_account_json(keyfile_path)

        # Get the bucket
        bucket = storage_client.bucket(bucket_name)

        # Create a blob and upload the file to the bucket
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        # print(f"File {source_file_name} uploaded to {destination_blob_name}.")
        return "SUCCESS"
    except Exception as e:
        return f"Error: {e}."


def archive_file(
    source_bucket_name: str,
    source_file_name: str,
    archive_bucket_name: str,
    archive_destination: str,
    archive_file_name: str,
    keyfile_path: str,
) -> str:
    try:
        # Initialize a client with the service account keyfile
        storage_client = storage.Client.from_service_account_json(keyfile_path)

        # Get the source bucket
        source_bucket = storage_client.bucket(source_bucket_name)
        source_file = source_bucket.blob(source_file_name)

        # Get the destination bucket
        archive_destination_bucket = storage_client.bucket(archive_bucket_name)

        # Generate the new filename with current date-time
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_name = f"{timestamp}_{archive_file_name}"  # archive_file_name.format(timestamp=timestamp)

        # Copy the blob to the new location with the new name
        source_bucket.copy_blob(
            source_file, archive_destination_bucket, archive_destination + new_name
        )

        source_file.delete()

        return "SUCCESS"
    except Exception as e:
        return f"Error: {e}"


def get_incremental_date(
    date: str, project_id: str, dataset: str, table_name: str, keyfile_path: str
):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        query = f""" SELECT MAX({date}) as MX_DATE FROM `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`;  """

        # Execute the query
        query_job = client.query(query)

        results = query_job.result()

        result_dict = {}
        for row in results:
            row_dict = {key: row[key] for key in row.keys()}
            result_dict[row[0]] = (
                row_dict  # Using the first column's value as the dictionary key # Print the results
            )

        # Identify Data Ingestion Workflow
        for key, value in result_dict.items():
            date_value = value["MX_DATE"]

        return date_value
    except Exception as e:
        return f"Error: {e}"  # (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_table_exists(
    project_id: str, dataset: str, table_name: str, keyfile_path: str
) -> str:
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        query = f""" SELECT COUNT(1) AS flag FROM `{project_id.lower()}.ref_{dataset.lower()}.__TABLES_SUMMARY__` WHERE table_id = '{table_name.lower()}';  """

        query_job = client.query(query)

        # Fetch the results
        results = query_job.result()

        # Store results in a dictionary
        result_dict = {}
        for row in results:
            row_dict = {key: row[key] for key in row.keys()}
            result_dict[row[0]] = row_dict

        # Identify Data Ingestion Workflow
        for key, value in result_dict.items():
            flag = value["flag"]

        return flag
    except Exception as e:
        return f"Error: {e}"


def get_record_count(project_id: str, dataset: str, table_name: str, keyfile_path: str):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        query = f""" SELECT COUNT(1) AS cnt FROM `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`;  """

        # Execute the query
        query_job = client.query(query)

        results = query_job.result()

        result_dict = {}
        for row in results:
            row_dict = {key: row[key] for key in row.keys()}
            result_dict[row[0]] = row_dict

        # Identify Data Ingestion Workflow
        for key, value in result_dict.items():
            count = value["cnt"]

        return count
    except Exception as e:
        return 0


def create_external_table(
    project_id: str,
    dataset,
    table_name: str,
    bucket_destination_name: str,
    file_format: str,
    keyfile_path: str,
) -> str:
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        query = f""" CREATE OR REPLACE EXTERNAL TABLE `{project_id.lower()}.external_{dataset.lower()}.{table_name.lower()}`
                     OPTIONS (
                     format= '{file_format}', 
                     uris=["gs://{bucket_destination_name}"]
                     ); """

        query_job = client.query(query)

        results = query_job.result()

        return "SUCCESS"
    except Exception as e:
        return f"Error: {e}"


def create_and_load_staging_table(
    project_id: str,
    dataset: str,
    table_name: str,
    stg_and_ref_create_table: str,
    source_to_stg_conversion: str,
    keyfile_path: str,
) -> str:
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        query = f""" 
                    DROP TABLE IF EXISTS `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}`;
                    CREATE TABLE IF NOT EXISTS `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}` ( 
                    {stg_and_ref_create_table}, IS_DELETED BOOL, IS_HARD_DELETE BOOL, DW_CREATE_DATETIME DATETIME, DW_LOAD_DATETIME DATETIME);
                      
                    INSERT INTO `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}`
                    SELECT {source_to_stg_conversion}, FALSE AS IS_DELETED, FALSE AS IS_HARD_DELETE, CURRENT_DATETIME AS DW_CREATE_DATETIME, CURRENT_DATETIME AS DW_LOAD_DATETIME 
                    FROM `{project_id.lower()}.external_{dataset.lower()}.{table_name.lower()}`; 
                """

        query_job = client.query(query)

        results = query_job.result()

        return f"SUCCESS: {results}"
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
    load_type: str,
    stg_and_ref_create_table: str,
    mapping_stg_to_ref_query: str,
    primary_key_column: str,
    keyfile_path: str,
) -> str:
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        # flag: 0 table does not exists, 1 table exists

        if flag == 0:
            query = f""" 
                    DROP TABLE IF EXISTS `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`;
                    CREATE TABLE IF NOT EXISTS `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}` ( 
                    {stg_and_ref_create_table}, IS_DELETED BOOL, IS_HARD_DELETE BOOL, DW_CREATE_DATETIME DATETIME, DW_LOAD_DATETIME DATETIME);

                    INSERT INTO `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`
                    SELECT {mapping_stg_to_ref_query}, FALSE AS IS_DELETED, FALSE AS IS_HARD_DELETE, CURRENT_DATETIME AS DW_CREATE_DATETIME, CURRENT_DATETIME AS DW_LOAD_DATETIME 
                    FROM `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}`;
                    """
        elif flag == 1 and load_type == "FULL":
            query = f""" 
                    DROP TABLE IF EXISTS `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`;
                    CREATE TABLE IF NOT EXISTS `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}` ( 
                    {stg_and_ref_create_table}, IS_DELETED BOOL, IS_HARD_DELETE BOOL, DW_CREATE_DATETIME DATETIME, DW_LOAD_DATETIME DATETIME);

                    INSERT INTO `{project_id.lower()}.ref_{dataset.lower()}.{table_name.lower()}`
                    SELECT {mapping_stg_to_ref_query}, FALSE AS IS_DELETED, FALSE AS IS_HARD_DELETE, CURRENT_DATETIME AS DW_CREATE_DATETIME, CURRENT_DATETIME AS DW_LOAD_DATETIME 
                    FROM `{project_id.lower()}.stg_{dataset.lower()}.{table_name.lower()}`;
                    """
        elif flag == 1 and load_type == "INCR":

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

        query_job = client.query(query)

        results = query_job.result()

        return f"SUCCESS: {results}"
    except Exception as e:
        return f"Error: {e}"

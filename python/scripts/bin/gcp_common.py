from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta


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


def gcp_execute_query(query: str, return_response: int, keyfile_path: str):
    try:
        keyfile = keyfile_path

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_file(keyfile)
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        # Execute the query
        query_job = client.query(query)

        if return_response == 0:
            results = query_job.result()

            return f"SUCCESS: {results}"
        elif return_response == 1:
            results = query_job.result()

            result_dict = {}

            for row in results:
                row_dict = {key: row[key] for key in row.keys()}
                result_dict[row[0]] = row_dict

            # Identify Data Ingestion Workflow
            for key, value in result_dict.items():
                output = value["VALUE"]

            return output
    except Exception as e:
        return f"Error: {e}"

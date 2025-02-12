from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
import pandas as pd


def get_gcp_storage(
    bucket_name: str,
    prefix_path: str,
    keyfile_contents: dict
) -> str:
    try:
        credentials_dict = keyfile_contents

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)

        # Initialize a client with the service account keyfile
        storage_client = storage.Client(credentials=credentials)

        # Get the bucket
        bucket = storage_client.get_bucket(bucket_name)

        # Get the file
        file = bucket.blob(prefix_path)

        file_contents = file.download_as_string()

        return file_contents.decode("utf-8")
    except Exception as e:
        return f"ERROR: {e}"


def gcp_execute_query(
    query: str, keyfile_contents: dict
):
    try:
        credentials_dict = keyfile_contents

        # Query Check
        if "DELETE" in query.upper(): 
            return "ERROR: DELETE STATEMENTS NOT ALLOWED"

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)

        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        # Execute the query
        query_job = client.query(query)
        results = query_job.result()

        if "SELECT" in query.upper():
            # Extract the data into a list of dictionaries
            rows = [dict(row) for row in results]

            # Convert to a Pandas DataFrame
            data_df = pd.DataFrame(rows)

            return data_df
        elif ("INSERT", "UPDATE") in query.upper():
            return "SUCCESS"
    except Exception as e:
        return f"ERROR: {e}"
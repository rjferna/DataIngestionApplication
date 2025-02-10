from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
import pandas as pd

def gcp_execute_query(
    query: str, return_response: int, keyfile_contents: dict
):
    try:
        credentials_dict = keyfile_contents 

        # Create credentials & Initialize Client
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)

        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        # Execute the query
        query_job = client.query(query)
        results = query_job.result()

        # Extract the data into a list of dictionaries
        rows = [dict(row) for row in results]

        # Convert to a Pandas DataFrame
        data_df = pd.DataFrame(rows)

        return data_df
    except Exception as e:
        return f"Error: {e}"
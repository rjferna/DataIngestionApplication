import requests
import time


def get_unix_timestamp(date_str):
    try:
        timestamp = (
            int(time.mktime(time.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ"))) * 1000
        )

        return timestamp
    except Exception as e:
        return None


def get_request(
    key: str,
    url: str,
    encoding: str,
    incremental_start_date: int,
    incremental_end_date: str,
    interval: str,
) -> dict:
    api_key = key
    base_url = url
    accepted_encoding = encoding

    if incremental_start_date == None:
        headers = {
            "Accept-Encoding": accepted_encoding,
            "Authorization": f"bearer {api_key}",
        }
        response = requests.get(base_url, headers=headers)
    else:
        interval = interval

        if type(incremental_start_date) == str and type(incremental_end_date) == str:
            start_date = get_unix_timestamp(incremental_start_date)
            end_date = get_unix_timestamp(incremental_end_date)
        elif type(incremental_start_date) == int and type(incremental_end_date) == str:
            start_date = incremental_start_date
            end_date = get_unix_timestamp(incremental_end_date)
        else:
            start_date = incremental_start_date
            end_date = incremental_end_date

        if start_date is None or end_date is None:
            return "Error: Date Conversion"

        headers = {
            "Accept-Encoding": accepted_encoding,
            "Authorization": f"bearer {api_key}",
        }

        params = {"start": start_date, "end": end_date, "interval": interval}

        response = requests.get(base_url, headers=headers, params=params)

    response_json = response.json()

    if response.status_code == 200:
        return response_json
    elif response_json == None:
        return f"Error: None Object Returned."
    else:
        return f"Error: {response_json.get('error', 'Unknown error')}"

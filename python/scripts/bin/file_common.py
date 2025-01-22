import csv
import json
import pandas as pd


def csv_to_parquet(
    file_path: str,
    header: str,
    seperator: str,
    quotation: str,
    parquet_filename: str,
    compression: str,
) -> str:
    try:
        if header == None:
            header = "infer"

        csv = pd.read_csv(
            filepath_or_buffer=file_path,
            header=header,
            sep=seperator,
            quotechar=quotation,
            engine="python",
            encoding="utf-8",
        )

        # Write the DataFrame to Parquet
        csv.to_parquet(
            f"{parquet_filename}.parquet", engine="pyarrow", compression=compression
        )  # You can choose 'fastparquet' if preferred

        return f"{parquet_filename}.parquet"
    except Exception as e:
        return f"Error: {e}"


def response_to_parquet(
    response_data: dict, parquet_filename: str, compression: str
) -> str:
    try:
        # Load response data to DataFrame
        df = pd.DataFrame(response_data["data"])

        # Write the DataFrame to Parquet
        df.to_parquet(
            f"{parquet_filename}.parquet", engine="pyarrow", compression=compression
        )  # You can choose 'fastparquet' if preferred

        return f"{parquet_filename}.parquet"
    except Exception as e:
        return f"Error: {e}"

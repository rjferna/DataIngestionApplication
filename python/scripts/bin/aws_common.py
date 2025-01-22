import boto3


def get_aws_s3(
    aws_access_key: str,
    aws_security_token: str,
    bucket_path: str,
    prefix_path: str,
    import_path: str,
    import_file: str,
) -> str:
    try:
        FULL_IMPORT_PATH = "{}{}".format(import_path, import_file)

        # Initialize a client with AWS
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_security_token,
        )

        # Downloading file from S3 Bucket
        s3.download_file(bucket_path, prefix_path, FULL_IMPORT_PATH)

        # Close session
        s3.close()

        return f"{FULL_IMPORT_PATH}"
    except Exception as e:
        return f"Error: {e}"

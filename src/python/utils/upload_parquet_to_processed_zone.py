def upload_parquet_to_processed_zone(parquet_file, bucket_name, key, s3_client):
    """
    This function uploads a Parquet file to a specified location in an S3 bucket.

    Parameters
    ----------
    parquet_file : str
        The local path to the Parquet file to be uploaded.
    bucket_name : str
        The name of the target S3 bucket.
    key : str
        The S3 key (path) where the file should be uploaded.
    s3_client : boto3.client
        A Boto3 S3 client object used to perform the upload.

    Returns
    -------
    str
        A success message indicating the uploaded file path in S3.

    Raises
    ------
    Exception
    Propagates any exception raised during the upload process.

    """

    try:
        s3_client.upload_file(parquet_file, bucket_name, key)
        print(f"Successfully uploaded {key} to s3://{bucket_name}/{key}")
        return f"Successfully uploaded {key} to s3://{bucket_name}/{key}"
    except Exception as e:
        print(f"Failed to up: {e}")
        raise e

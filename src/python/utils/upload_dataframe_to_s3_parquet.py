import boto3
from datetime import datetime
from io import BytesIO


def upload_dataframe_to_s3_parquet(
    df,
    table_name,
    bucket_name,
    key_prefix,
    timestamp,
    compression="snappy",
    s3_client=None,
):
    """Saves a dataframe to parquet file and stores it into s3 bucket

    Args:
    - df: dataframe of transformed table
    - table_name: name of the dimensions /fact table
    - bucket_name:The name of the target S3 bucket
    - compression: One of ["snappy", "gzip", "brotli", "none"] user choice"""
    if compression not in ["snappy", "gzip", "brotli", "none"]:
        raise ValueError(f"Invalid compression: {compression}")
    """compression can be "snappy" => Fast compression, moderate size reduction
                        "gzip"  => Higher compression ratio, slower compression/decompression
                        "brotli"=> Very good compression ratio, slower, newer
                        "none"=> No Compression, larger file size but fastest to read/ write  """
    s3_client = boto3.client("s3")

    # timestamp = datetime.now().isoformat()
    filename = f"{table_name}-{timestamp}.parquet"
    s3_key = f"{key_prefix.rstrip('/')}/{filename}"

    # Write DataFrame to in-memory buffer
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression=compression)
    buffer.seek(0)

    # Upload in-memory buffer to S3
    try:
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
        print(f"Uploaded to s3://{bucket_name}/{s3_key}")
        return f"s3://{bucket_name}/{s3_key}"
    except Exception as e:
        print(f"Upload failed: {e}")
        raise e

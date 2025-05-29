from botocore.exceptions import ClientError
import logging


def upload_json_to_s3(json_file, bucket_name, key, s3_client):
    """
    Uploads a JSON string to an s3 bucket

    Args:
        - json_file (json str): a json-formatted string
        - bucket_name (str): the name of the bucket you're putting the JSON data into
        - key (str): the location inside the s3 bucket that you want to put the data into
        - s3_client

    Output:
        - Places the json data in the specified key in the specified bucket
    """
    try:
        s3_client.put_object(Body=json_file, Bucket=bucket_name, Key=key)
        print(f"Successfully uploaded {json_file} to s3://{bucket_name}/{key}")
    except ClientError as e:
        logging.error(e)
        raise e

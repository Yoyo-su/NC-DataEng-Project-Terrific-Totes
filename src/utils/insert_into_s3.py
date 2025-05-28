from botocore.exceptions import ClientError
import logging


def upload_json_to_s3(json_file, bucket_name, key, s3_client):
    try:
        s3_client.put_object(Body=json_file, Bucket=bucket_name, Key=key)
        print(f"Successfully uploaded {json_file} to s3://{bucket_name}/{key}")
    except ClientError as e:
        logging.error(e)
        # raise e

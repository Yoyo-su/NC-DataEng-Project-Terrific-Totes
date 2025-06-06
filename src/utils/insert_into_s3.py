from botocore.exceptions import ClientError


def upload_json_to_s3(json_file, bucket_name, key, s3_client):
    try:
        s3_client.put_object(Body=json_file, Bucket=bucket_name, Key=key)
        print(f"Successfully uploaded {key} to s3://{bucket_name}/{key}")

    except ClientError as e:
        print(f"Failed to put object: {e}")
        raise e

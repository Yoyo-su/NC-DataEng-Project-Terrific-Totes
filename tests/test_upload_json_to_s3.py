from src.utils.insert_into_s3 import upload_json_to_s3
from moto import mock_aws
from src.utils.json_dumps import dump_to_json
import boto3 
import pytest
import sys
import os
import json
from datetime import datetime

@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"]='Test'
    os.environ["AWS_SECRET_ACCESS_KEY"]='Test'
    os.environ["AWS_SESSION_TOKEN"]='Test'
    os.environ["AWS_SECURITY_TOKEN"]='Test'
    os.environ["AWS_DEFAULT_REGION"]='eu-west-2'

@mock_aws
def test_upload_json_to_s3(aws_creds):
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket="test-bucket",CreateBucketConfiguration = {"LocationConstraint": 'eu-west-2'})
    table_name = 'test-table'
    sample_data = {"foo": "bar"}
    json_data = json.dumps(sample_data)
    timestamp = datetime.utcnow().strftime('%B%d%Y-%H:%M:%S')
    test_key = f'{table_name}/{timestamp}.json'
    upload_json_to_s3(json_data, "test-bucket", test_key, s3_client)
    response = s3_client.get_object(Bucket="test-bucket", Key=test_key)
    body = response["Body"].read().decode("utf-8")
    assert json.loads(body) == sample_data

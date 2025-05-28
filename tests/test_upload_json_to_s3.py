from src.utils.insert_into_s3 import upload_json_to_s3
from moto import mock_aws
import boto3
import pytest
from botocore.exceptions import ClientError

import os
import json
from datetime import datetime


@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "Test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "Test"
    os.environ["AWS_SESSION_TOKEN"] = "Test"
    os.environ["AWS_SECURITY_TOKEN"] = "Test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


class TestInsertIntoS3:
    @pytest.mark.it(
        "Testing that our function uploads a complete, simple json file to an s3 bucket."
    )
    @mock_aws
    def test_upload_json_to_s3(aws_creds):
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        table_name = "test-table"
        sample_data = {"foo": "bar"}
        json_data = json.dumps(sample_data)
        timestamp = datetime.utcnow().strftime("%B%d%Y-%H:%M:%S")
        test_key = f"{table_name}/{timestamp}.json"
        upload_json_to_s3(json_data, "test-bucket", test_key, s3_client)
        response = s3_client.get_object(Bucket="test-bucket", Key=test_key)
        body = response["Body"].read().decode("utf-8")
        assert json.loads(body) == sample_data

    @pytest.mark.it("Testing that an empty file is stored if the input json is empty")
    @mock_aws
    def test_uploaded_file_is_empty(aws_creds):
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        table_name = "test-table"
        sample_data = {}
        json_data = json.dumps(sample_data)
        timestamp = datetime.utcnow().strftime("%B%d%Y-%H:%M:%S")
        test_key = f"{table_name}/{timestamp}.json"
        upload_json_to_s3(json_data, "test-bucket", test_key, s3_client)
        response = s3_client.get_object(Bucket="test-bucket", Key=test_key)
        body = response["Body"].read().decode("utf-8")
        assert json.loads(body) == {}

    @pytest.mark.it("If the s3 bucket does not exist, a ClientError should be raised")
    @mock_aws
    def test_s3_bucket_does_not_exist_exception_raised(aws_creds):
        s3_client = boto3.client("s3")
        # s3_client.create_bucket(Bucket="test-bucket",CreateBucketConfiguration = {"LocationConstraint": 'eu-west-2'})
        table_name = "test-table"
        sample_data = {}
        json_data = json.dumps(sample_data)
        timestamp = datetime.utcnow().strftime("%B%d%Y-%H:%M:%S")
        test_key = f"{table_name}/{timestamp}.json"
        with pytest.raises(ClientError):
            upload_json_to_s3(json_data, "test-bucket", test_key, s3_client)

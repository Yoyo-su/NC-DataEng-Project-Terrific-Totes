from src.extract_lambda import lambda_handler
import pytest
import os
from moto import mock_aws
import boto3


@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "Test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "Test"
    os.environ["AWS_SESSION_TOKEN"] = "Test"
    os.environ["AWS_SECURITY_TOKEN"] = "Test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


class TestExtractLambda:
    @pytest.mark.it(
        "Testing that our lambda function successfully runs with all util functions integrated"
    )
    @mock_aws
    def test_lambda_function_returns_success_when_invoked(aws_creds):
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="fscifa-raw-data",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = lambda_handler({}, {})
        assert result == {"result": "success"}

    @pytest.mark.it(
        "Testing that our lambda function returns error if bucket doesn't exist"
    )
    @mock_aws
    def test_lambda_function_returns_error_when_expected(aws_creds):
        with pytest.raises(Exception):
            lambda_handler({}, {})

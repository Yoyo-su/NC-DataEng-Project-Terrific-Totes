import sys
import os
import pytest
from unittest.mock import Mock, patch
from moto import mock_aws
import boto3
import pandas as pd

# import sqlite3

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/python"))
)
from src.load_lambda import lambda_handler


""" Tests for Extract_lambda funtion"""


@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "Test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "Test"
    os.environ["AWS_SESSION_TOKEN"] = "Test"
    os.environ["AWS_SECURITY_TOKEN"] = "Test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


class TestLoadLambda:
    @pytest.mark.it(
        "Testing that when there are no new files to load, an exception is raised"
    )
    @mock_aws
    @patch("src.extract_lambda.extract_db")
    @patch("src.python.utils.extract_db.connect_to_db")
    def test_lambda_function_loads_nothing_when_there_are_no_new_files(
        self, mock_connect_to_db, aws_creds
    ):
        mock_conn = Mock()
        mock_conn.run.return_value = []
        mock_conn.columns = []
        mock_connect_to_db.return_value = mock_conn
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="fscifa-processed-data",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with pytest.raises(Exception):
            lambda_handler({}, {})

    @pytest.mark.it(
        "Testing that our transform lambda function successfully runs with all util functions integrated"
    )
    @mock_aws
    @patch("src.load_lambda.fetch_parquet")
    @patch("src.load_lambda.parquet_to_sql")
    def test_lambda_function_successfully_uploads_new_file(
        self, mock_parquet_to_sql, mock_fetch_parquet, aws_creds
    ):
        dummy_df = pd.DataFrame()
        mock_fetch_parquet.return_value = dummy_df.to_parquet()
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="fscifa-processed-data",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = lambda_handler({}, {})
        assert result == {"result": "success"}

import sys
import boto3
from datetime import datetime
import os
import pytest
from unittest.mock import Mock, patch
from moto import mock_aws
import boto3
import pandas as pd

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/python"))
)

from src.transform_lambda import lambda_handler

""" Tests for transform_lambda funtion"""


@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "Test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "Test"
    os.environ["AWS_SESSION_TOKEN"] = "Test"
    os.environ["AWS_SECURITY_TOKEN"] = "Test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


class TestTransformLambda:
    @pytest.mark.it(
        "Testing that our transform lambda function successfully runs with all util functions integrated"
    )
    @mock_aws
    @patch("src.transform_lambda.transform_dim_staff")
    @patch("src.transform_lambda.transform_dim_location")
    @patch("src.transform_lambda.transform_dim_design")
    @patch("src.transform_lambda.transform_dim_currency")
    @patch("src.transform_lambda.transform_dim_counterparty")
    @patch("src.transform_lambda.transform_fact_sales_order")
    @patch("src.transform_lambda.transform_fact_sales_order")
    @patch("src.transform_lambda.transform_dim_date")
    @patch("src.transform_lambda.upload_json_to_s3")
    def test_lambda_function_returns_success_when_invoked(
        self,
        aws_creds,
        mock_upload_json,
        mock_transform_dim_date,
        mock_transform_fact_sales,
        mock_dim_counterparty,
        mock_transform_dim_currency,
        mock_transform_dim_design,
        mock_transform_dim_location,
        mock_transform_dim_staff,
    ):
        dummy_df = pd.DataFrame()
        mock_transform_dim_staff.return_value = dummy_df
        mock_transform_dim_location.return_value = dummy_df
        mock_transform_dim_design.return_value = dummy_df
        mock_transform_dim_currency.return_value = dummy_df
        mock_dim_counterparty.return_value = dummy_df
        mock_transform_fact_sales.return_value = dummy_df
        mock_transform_dim_date.return_value = dummy_df
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="fscifa-processed-data",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = lambda_handler({}, {})
        assert result == {"result": "success"}

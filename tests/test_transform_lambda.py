import sys
import boto3
from datetime import datetime
import os
import pytest
from unittest.mock import Mock, patch
from moto import mock_aws
import boto3
import pandas as pd

table_list = [
    "dim_staff",
    "dim_location",
    "dim_design",
    "dim_date",
    "dim_currency",
    "dim_counterparty",
    "fact_sales_order",
]

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
    def test_s3_has_the_uploaded_file_inside(
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
        uploaded_objects_list = s3_client.list_objects_v2(Bucket = 'fscifa-processed-data')

        bucket_contents = [uploaded_objects_list['Contents'][i]['Key'] for i in range(6)]
        table_in_bucket_contents = False
        for table in table_list:
            for content in bucket_contents:
                if table in content:
                    table_in_bucket_contents = True
            assert table_in_bucket_contents 


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
    def test_s3_skips_empty_files(
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
        mock_transform_dim_design.return_value = None
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
        uploaded_objects_list = s3_client.list_objects_v2(Bucket = 'fscifa-processed-data')

        bucket_contents = [uploaded_objects_list['Contents'][i]['Key'] for i in range(6)]
        timestamp2 = datetime.now().isoformat(timespec="minutes")
        filename = f"dim_design/dim_design-{timestamp2}.parquet"
        assert filename not in bucket_contents

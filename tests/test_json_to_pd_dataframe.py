import pytest
import pandas as pd
from src.utils.json_to_pd_dataframe import json_to_pd_dataframe
from moto import mock_aws
import boto3
import os


@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "Test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "Test"
    os.environ["AWS_SECURITY_TOKEN"] = "Test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture()
def s3_resource(aws_creds):
    with mock_aws():
        yield boto3.resource("s3", region_name="eu-west-2")


@pytest.fixture()
def bucket(aws_creds, s3_resource):
    with mock_aws():
        s3_resource.create_bucket(
            Bucket="test_ingest_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        bucket = s3_resource.Bucket("test_ingest_bucket")
        return bucket


class TestJsonToPDdataframe:
    @pytest.mark.it("when passed a file not of type json, should raise exception")
    def test_incorrect_file_type_raises_exception(self, bucket):
        test_file_1 = "address-2025-06-29T11:06:18.399084.txt"
        with pytest.raises(
            Exception,
            match=r"Error when converting file to dataframe: most_recent_file should be of type json",
        ):
            json_to_pd_dataframe(test_file_1, "address", "test_ingest_bucket")

    @pytest.mark.it("when passed incorrect table name, should raise exception")
    def test_incorrect_table_name_raises_exception(self, bucket):
        test_file_1 = "address-2025-06-29T11:06:18.399084.txt"
        with pytest.raises(
            Exception,
            match=r"Error when converting file to dataframe: incorrect table_name",
        ):
            json_to_pd_dataframe(test_file_1, "payments", "test_ingest_bucket")

    @pytest.mark.it(
        "when passed an incorrect bucket_name, raises ClientError with appropriate message"
    )
    def test_incorrect_bucket_name_raises_exception(self, bucket):
        test_file_1 = "address-2025-06-29T11:06:18.399084.txt"
        with pytest.raises(
            Exception,
            match=r"Error when converting file to dataframe: most_recent_file should be of type json",
        ):
            json_to_pd_dataframe(test_file_1, "address", "test_wrong_bucket")

    @pytest.mark.it(
        "when passed a json file with one record returns dataframe with one row"
    )
    def test_returns_dataframe_with_one_row(self, bucket):
        test_file_1 = "address-2025-06-29T11:06:18.399084.json"
        with open(test_file_1, "w") as file:
            file.write(
                '{"address": [{"address_id": 1, "address_line_1": "6826 Herzog Via"}]}'
            )
        bucket.upload_file(test_file_1, test_file_1)
        result = json_to_pd_dataframe(test_file_1, "address", "test_ingest_bucket")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["address_id"][0] == 1
        assert result["address_line_1"][0] == "6826 Herzog Via"

    @pytest.mark.it(
        "when passed a json file with two records returns dataframe with two rows"
    )
    def test_returns_dataframe_with_multiple_rows(self, bucket):
        test_file_1 = "address-2025-06-29T11:06:18.399084.json"
        with open(test_file_1, "w") as file:
            file.write(
                '{"address": [{"address_id": 1, "address_line_1": "6826 Herzog Via"},{"address_id": 2, "address_line_1": "93 High Street"}]}'
            )
        bucket.upload_file(test_file_1, test_file_1)
        result = json_to_pd_dataframe(test_file_1, "address", "test_ingest_bucket")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result["address_id"][0] == 1
        assert result["address_line_1"][0] == "6826 Herzog Via"
        assert result["address_id"][1] == 2
        assert result["address_line_1"][1] == "93 High Street"

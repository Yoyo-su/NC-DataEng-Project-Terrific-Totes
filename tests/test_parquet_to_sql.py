from src.python.utils.parquet_to_sql import fetch_parquet, parquet_to_sql
import pytest
import os
import boto3
from moto import mock_aws
import pandas as pd
from unittest.mock import Mock, patch



test_df = pd.DataFrame(
    {
        "sales_order_id": [7, 1],
        "staff": ['hello', 'wo\'rld'],
        "counterparty_id": [420, 13],
        "currency_id": [1, 3],
    }
)


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
            Bucket="fscifa-processed-data",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        bucket = s3_resource.Bucket("fscifa-processed-data")

        # add last_updated txt to the mock bucket:
        with open("tests/data/last_updated.txt", "w") as file:
            file.write("2025-05-29T11:06:18.399084")
        bucket.upload_file("tests/data/last_updated.txt", "last_updated.txt")

        # add parquet file to mock bucket
        test_df.to_parquet(
            "tests/data/dim_staff-2025-05-29T11:06:18.399084.parquet",
            engine="pyarrow",
            compression="snappy",
        )
        bucket.upload_file(
            "tests/data/dim_staff-2025-05-29T11:06:18.399084.parquet",
            "dim_staff/dim_staff-2025-05-29T11:06:18.399084.parquet",
        )

        return bucket


class TestFetchParquet:
    @pytest.mark.it("should return a parquet file as a dataframe as expected")
    def test_fetch_parquet_returns_parquet_dataframe(self, bucket):
        table_name = "dim_staff"
        expected_df = pd.read_parquet(
            "tests/data/dim_staff-2025-05-29T11:06:18.399084.parquet", engine="pyarrow"
        )
        result = fetch_parquet(table_name, "fscifa-processed-data")
        pd.testing.assert_frame_equal(result, expected_df)

    @pytest.mark.it("should raise an error if provided with an invalid table")
    def test_fetch_parquet_returns_error_if_given_bad_table(self, bucket):
        table_name = "dummy"
        with pytest.raises(Exception):
            fetch_parquet(table_name, "fscifa-processed-data")

    @pytest.mark.it("should raise an error if provided with an invalid bucket name")
    def test_fetch_parquet_returns_error_if_given_bad_bucket(self, bucket):
        table_name = "dim_staff"
        with pytest.raises(Exception):
            fetch_parquet(table_name, "fscifa-bad_data")

    @pytest.mark.it("should raise an error if no s3 connection")
    def test_fetch_parquet_returns_error_if_no_s3_connection(self):
        table_name = "dim_staff"
        with pytest.raises(Exception):
            fetch_parquet(table_name, "fscifa-processed-data")


class TestParquetToSQL:
    @pytest.mark.it("Should return dictionary indicating success")
    @patch("src.python.utils.parquet_to_sql.connect_to_db")
    def test_parquet_to_sql_returns_dict_if_succeful(self, mock_connect_to_db):
        mock_conn = Mock()
        mock_conn.run.return_value = []
        mock_connect_to_db.return_value = mock_conn
        table_name = "dim_staff"
        result = parquet_to_sql(table_name, test_df)
        assert result == {"result": "success"}

    @pytest.mark.it("Should run expected insert query on db connection ")
    @patch("src.python.utils.parquet_to_sql.connect_to_db")
    def test_parquet_to_sql_runs_expected_query(self, mock_connect_to_db):
        mock_conn = Mock()
        mock_conn.run.return_value = []
        mock_connect_to_db.return_value = mock_conn
        table_name = "dim_staff"
        parquet_to_sql(table_name, test_df)
        assert mock_conn.run.called  # Ensure .run() was called
        mock_conn.run.assert_called_with(
            "INSERT INTO dim_staff (sales_order_id, staff, counterparty_id, currency_id) VALUES(7, hello, 420, 1), (1, wo''rld, 13, 3);"
        )  # Ensure expected query was run

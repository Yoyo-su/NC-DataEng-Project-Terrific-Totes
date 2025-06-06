import pytest
import pandas as pd
from moto import mock_aws
import boto3
from src.utils.upload_dataframe_to_s3_parquet import upload_dataframe_to_s3_parquet


@pytest.fixture
def sample_df():
    return pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})


@pytest.fixture
def mock_s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        yield s3


def test_successful_upload(sample_df, mock_s3_client):
    result = upload_dataframe_to_s3_parquet(
        df=sample_df,
        table_name="test_table",
        bucket_name="test-bucket",
        key_prefix="test-folder",
        s3_client=mock_s3_client,
    )

    assert result.startswith("s3://test-bucket/test-folder/test_table-")
    # Get the key from the returned path
    s3_key = result.replace("s3://test-bucket/", "")
    obj = mock_s3_client.get_object(Bucket="test-bucket", Key=s3_key)
    assert obj["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_invalid_compression(sample_df):
    with pytest.raises(ValueError, match="Invalid compression: invalid_codec"):
        upload_dataframe_to_s3_parquet(
            df=sample_df,
            table_name="test_table",
            bucket_name="test-bucket",
            key_prefix="test-folder",
            compression="invalid_codec",
        )


def test_correct_key_format(sample_df, mock_s3_client):
    result = upload_dataframe_to_s3_parquet(
        df=sample_df,
        table_name="some_table",
        bucket_name="test-bucket",
        key_prefix="some-folder/",
        s3_client=mock_s3_client,
    )
    assert "some-folder/some_table-" in result


def test_upload_failure(sample_df):
    bad_client = boto3.client("s3", region_name="us-east-1")  # not mocked
    with pytest.raises(Exception):
        upload_dataframe_to_s3_parquet(
            df=sample_df,
            table_name="fail_table",
            bucket_name="nonexistent-bucket",
            key_prefix="folder",
            s3_client=bad_client,
        )

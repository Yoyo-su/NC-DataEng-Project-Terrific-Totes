import pytest
from moto import mock_aws
import boto3
from src.utils.upload_parquet_to_processed_zone import upload_parquet_to_processed_zone


@pytest.fixture
def s3_client():
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.mark.it("Upload parquet files to s3 successfully")
def test_upload_parquet_to_s3(s3_client):
    table_name = "test_staff"
    path = f"{table_name}.parquet"
    s3_client.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    key = f"processed_zone/{path}"
    result = upload_parquet_to_processed_zone(
        path, "test-bucket", key=key, s3_client=s3_client
    )
    expected = f"Successfully uploaded {key} to s3://test-bucket/{key}"

    assert result == expected


@pytest.mark.it("Testing error handling when upload parquet files to s3 ")
def test_upload_parquet_to_s3_error_handling_without_creating_bucket(s3_client):
    table_name = "test_staff"
    path = f"{table_name}.parquet"
    key = f"processed_zone/{path}"
    with pytest.raises(Exception):
        upload_parquet_to_processed_zone(
            path, "test-bucket", key=key, s3_client=s3_client
        )


@pytest.mark.it(
    "Checks if the uploaded Parquet file exists in the S3 bucket with the correct key"
)
def test_upload_parquet_to_s3_using_list_objects(s3_client):
    table_name = "test_staff"
    path = f"{table_name}.parquet"
    s3_client.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    key = f"processed_zone/{path}"
    upload_parquet_to_processed_zone(path, "test-bucket", key=key, s3_client=s3_client)
    objects = s3_client.list_objects_v2(Bucket="test-bucket")["Contents"]
    Keys = [obj["Key"] for obj in objects]
    assert key in Keys

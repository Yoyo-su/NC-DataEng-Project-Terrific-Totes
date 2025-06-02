import os
import pytest
from moto import mock_aws
import boto3
from src.utils.load_data_from_most_recent_json import (
    find_files_with_specified_table_name,
    find_most_recent_file,
    load_data_from_most_recent_json,
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
            Bucket="test_ingest_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        bucket = s3_resource.Bucket("test_ingest_bucket")
        with open("last_updated.txt", "w") as file:
            file.write("2025-05-29T11:06:18.399084")
        bucket.upload_file(
            "last_updated.txt",
            "last_updated.txt",
        )
        return bucket


class TestFindFilesWithSpecifiedTableName:

    @pytest.mark.it(
        "when passed a bucket containing one file with the specified table name, returns a list containing that filename"
    )
    def test_returns_list_of_one_file(self, bucket):
        with open("tests/data/address-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        bucket.upload_file(
            "tests/data/address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-29T11:06:18.399084.json",
        )

        result = find_files_with_specified_table_name("address", "test_ingest_bucket")
        assert len(result) == 1
        assert result[0] == "address-2025-05-29T11:06:18.399084.json"

    @pytest.mark.it(
        "when passed a bucket containing two files, one with the specified table name, returns a list containing only the specified filename"
    )
    def test_finds_correct_file_in_bucket_with_multiple_files_of_different_table_names(
        self, bucket
    ):
        with open("tests/data/address-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        with open("tests/data/payments-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        bucket.upload_file(
            "tests/data/address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-29T11:06:18.399084.json",
        )
        bucket.upload_file(
            "tests/data/payments-2025-05-29T11:06:18.399084.json",
            "payments-2025-05-29T11:06:18.399084.json",
        )
        result = find_files_with_specified_table_name("address", "test_ingest_bucket")
        assert len(result) == 1
        assert result[0] == "address-2025-05-29T11:06:18.399084.json"

    @pytest.mark.it(
        """when passed a bucket containing two files, both with the specified table name, returns
    a list containing both filenames, with the most recent first in the list"""
    )
    def test_finds_correct_file_in_bucket_with_multiple_files_of_same_table_name(
        self, bucket
    ):
        with open("tests/data/address-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        with open("tests/data/address-2025-06-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        bucket.upload_file(
            "tests/data/address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-29T11:06:18.399084.json",
        )
        bucket.upload_file(
            "tests/data/address-2025-06-29T11:06:18.399084.json",
            "address-2025-06-29T11:06:18.399084.json",
        )
        result = find_files_with_specified_table_name("address", "test_ingest_bucket")
        assert len(result) == 2
        assert "address-2025-06-29T11:06:18.399084.json" in result
        assert "address-2025-05-29T11:06:18.399084.json" in result

    @pytest.mark.it(
        """when passed a bucket that does not contain a file with the table_name passed as an argument,
                    returns an empty list"""
    )
    def test_returns_empty_list_when_passed_with_table_name_not_in_bucket(self, bucket):
        with open("tests/data/address-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        bucket.upload_file(
            "tests/data/address-2025-05-29T11:06:18.399084.json",
            "address-2025-06-29T11:06:18.399084.json",
        )
        result = find_files_with_specified_table_name("payments", "test_ingest_bucket")
        assert result == []


class TestFindMostRecentFile:

    @pytest.mark.it(
        """when passed a bucket that contains a file with the specified table name, but with a different date/time to
        the last date/time in last_updated.txt, raises exception with appropriate message"""
    )
    def test_no_updated_data_for_specified_table(self, bucket):
        test_list = ["address-2025-05-28T11:06:18.399084.json"]
        with pytest.raises(Exception, match=r"No new data for table, address"):
            find_most_recent_file(test_list, "address", "test_ingest_bucket")

    @pytest.mark.it(
        "when passed an empty list, raises IndexError with appropriate message"
    )
    def test_empty_list(self):
        test_list = []
        with pytest.raises(
            IndexError,
            match=r"No file containing table, address is found in the s3 bucket",
        ):
            find_most_recent_file(test_list, "address", "test_ingest_bucket")

    @pytest.mark.it(
        """when passed a bucket that does not contain a last_updated.txt file, raises
        FileNotFoundError with appropriate message"""
    )
    def test_raises_exception_when_no_last_updated_txt_file_in_bucket(self, bucket):
        last_updated = bucket.Object("last_updated.txt")
        last_updated.delete()
        test_list = ["address-2025-05-29T11:06:18.399084.json"]
        with pytest.raises(
            FileNotFoundError,
            match=r"File not found 404: there is no last_updated.txt file saved in the s3 bucket 'fscifa-raw-data'",
        ):
            find_most_recent_file(test_list, "address", "test_ingest_bucket")

    @pytest.mark.it(
        """"when there is only one json file in the bucket, that has the table name being searched for, and the same
        date/time as the last updated date/time specified in the last_updated.txt, find_most_recent_file returns that
        json file name"""
    )
    def test_date_time_last_updated_same_as_json_filename(self, bucket):
        test_files = ["address-2025-05-29T11:06:18.399084.json"]
        result = find_most_recent_file(test_files, "address", "test_ingest_bucket")
        assert result == "address-2025-05-29T11:06:18.399084.json"

    @pytest.mark.it(
        """when passed a list containing two filenames, and the most
                    recent file has the same date/time as in the last line of
                    last_updated.txt, returns that file name"""
    )
    def test_two_filenames(self, bucket):
        test_list = [
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-03-29T11:06:18.399084.json",
        ]
        assert (
            find_most_recent_file(test_list, "address", "test_ingest_bucket")
            == "address-2025-05-29T11:06:18.399084.json"
        )

    @pytest.mark.it(
        """when passed a list containing multiple of the same filenames, all with the same
          date/time as in the last line of last_updated.txt, returns this file name once"""
    )
    def test_two_of_the_same_filenames(self, bucket):
        test_list = [
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-29T11:06:18.399084.json",
        ]
        assert (
            find_most_recent_file(test_list, "address", "test_ingest_bucket")
            == "address-2025-05-29T11:06:18.399084.json"
        )

    @pytest.mark.it(
        """when passed a list containing many filenames, on same date, at different times,
        returns one with most recent time, where this is the same as the date/time on
        the last line of last_updated.txt"""
    )
    def test_two_filenames_same_date_different_time(self, bucket):
        test_list = [
            "address-2025-05-29T10:06:18.399084.json",
            "address-2025-05-29T11:06:18.399084.json",
        ]
        assert (
            find_most_recent_file(test_list, "address", "test_ingest_bucket")
            == "address-2025-05-29T11:06:18.399084.json"
        )

    @pytest.mark.it(
        """when passed a list containing multiple filenames on different days in the same month, returns most recent,
        where this most recent date/time is the same as the date/time on the last line of last_updated.txt"""
    )
    def test_multiple_filenames_different_days_in_same_month(self, bucket):
        test_list = [
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-26T11:06:18.399084.json",
            "address-2025-05-14T11:06:18.399084.json",
            "address-2025-05-09T11:06:18.399084.json",
        ]
        assert (
            find_most_recent_file(test_list, "address", "test_ingest_bucket")
            == "address-2025-05-29T11:06:18.399084.json"
        )


class TestLoadMostRecentDataFromJson:
    @pytest.mark.it(
        """test that previously tested dependency injection functions work when put together"""
    )
    def test_load_data_from_most_recent_json(self, bucket):
        test_file_1 = "tests/data/address-2025-05-29T11:06:18.399084.json"
        test_file_2 = "tests/data/address-2025-06-29T11:06:18.399084.json"
        with open(test_file_1, "w") as file:
            file.write(
                '{"address": [{"address_id": 1, "address_line_1": "6826 Herzog Via"}]}'
            )
        with open(test_file_2, "w") as file:
            file.write(
                '{"address": [{"address_id": 2, "address_line_1": "93 High Street"}]}'
            )
        bucket.upload_file(test_file_1, "address-2025-05-29T11:06:18.399084.json")
        bucket.upload_file(test_file_2, "address-2025-01-29T11:06:18.399084.json")

        assert (
            load_data_from_most_recent_json("address", "test_ingest_bucket")
            == "address-2025-05-29T11:06:18.399084.json"
        )

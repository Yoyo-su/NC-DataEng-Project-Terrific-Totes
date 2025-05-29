import os
import pytest
from moto import mock_aws
import boto3
from utils.load_data_from_most_recent_json import (
    find_files_with_specified_table_name,
    find_most_recent_file,
    read_file_and_turn_it_to_list_dict,
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
        return s3_resource.Bucket("test_ingest_bucket")


class TestFindFilesWithSpecifiedTableName:
    @pytest.mark.it(
        "when passed a bucket containing one file with the specified table name, returns a list containing that filename"
    )
    def test_returns_list_of_one_file(self, bucket):
        bucket.upload_file(
            "address-2025-05-29T11:06:18.399084.json",
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
        with open("address-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        with open("payments-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        bucket.upload_file(
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-29T11:06:18.399084.json",
        )
        bucket.upload_file(
            "payments-2025-05-29T11:06:18.399084.json",
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
        with open("address-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        with open("address-2025-06-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        bucket.upload_file(
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-29T11:06:18.399084.json",
        )
        bucket.upload_file(
            "address-2025-06-29T11:06:18.399084.json",
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
        with open("address-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"example": "data"}')
        bucket.upload_file(
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-06-29T11:06:18.399084.json",
        )
        result = find_files_with_specified_table_name("payments", "test_ingest_bucket")
        assert result == []


class TestFindMostRecentFile:
    @pytest.mark.it("when passed an empty list, returns None")
    def test_empty_list(self):
        test_list = []
        assert find_most_recent_file(test_list) is None

    @pytest.mark.it(
        "when passed a list containing only one filename, returns that filename"
    )
    def test_single_filename(self):
        test_list = ["address-2025-05-29T11:06:18.399084.json"]
        assert (
            find_most_recent_file(test_list)
            == "address-2025-05-29T11:06:18.399084.json"
        )

    @pytest.mark.it("when passed a list containing two filenames, returns most recent")
    def test_two_filenames(self):
        test_list = [
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-06-29T11:06:18.399084.json",
        ]
        assert (
            find_most_recent_file(test_list)
            == "address-2025-06-29T11:06:18.399084.json"
        )

    @pytest.mark.it(
        "when passed a list containing multiple of the same filenames, returns only one"
    )
    def test_two_of_the_same_filenames(self):
        test_list = [
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-29T11:06:18.399084.json",
        ]
        assert (
            find_most_recent_file(test_list)
            == "address-2025-05-29T11:06:18.399084.json"
        )

    @pytest.mark.it(
        "when passed a list containing many filenames, on same date, returns one with most recent time"
    )
    def test_two_filenames_same_date_different_time(self):
        test_list = [
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-29T11:08:18.399084.json",
        ]
        assert (
            find_most_recent_file(test_list)
            == "address-2025-05-29T11:08:18.399084.json"
        )

    @pytest.mark.it(
        "when passed a list containing multiple filenames on different days in the same month, returns most recent"
    )
    def test_multiple_filenames_different_days_in_same_month(self):
        test_list = [
            "address-2025-05-29T11:06:18.399084.json",
            "address-2025-05-26T11:06:18.399084.json",
            "address-2025-05-30T11:06:18.399084.json",
            "address-2025-05-09T11:06:18.399084.json",
        ]
        assert (
            find_most_recent_file(test_list)
            == "address-2025-05-30T11:06:18.399084.json"
        )


class TestReadFileAndTurnToListDict:
    @pytest.mark.it(
        "when most recent file is None (i.e. there isn't a file with that table name in the bucket) returns empty dict in a list"
    )
    def test_no_file_containing_specified_table(self, bucket):
        assert read_file_and_turn_it_to_list_dict(
            "address", "test_ingest_bucket", None
        ) == [{}]

    @pytest.mark.it(
        "when most recent file is a json that contains one record, return a list with one dict"
    )
    def test_file_with_one_record(self, bucket):
        most_recent_file = "address-2025-05-29T11:06:18.399084.json"
        with open(most_recent_file, "w") as file:
            file.write(
                '{"address": [{"address_id": 1, "address_line_1": "6826 Herzog Via"}]}'
            )
        bucket.upload_file(most_recent_file, most_recent_file)
        assert read_file_and_turn_it_to_list_dict(
            "address", "test_ingest_bucket", most_recent_file
        ) == [{"address_id": 1, "address_line_1": "6826 Herzog Via"}]

    @pytest.mark.it(
        "when most recent file is a json that contains two records, return a list with two dicts"
    )
    def test_file_with_two_records(self, bucket):
        most_recent_file = "address-2025-05-29T11:06:18.399084.json"
        with open(most_recent_file, "w") as file:
            file.write(
                '{"address": [{"address_id": 1, "address_line_1": "6826 Herzog Via"},{"address_id": 2, "address_line_1": "93 High Street"}]}'
            )
        bucket.upload_file(most_recent_file, most_recent_file)
        assert read_file_and_turn_it_to_list_dict(
            "address", "test_ingest_bucket", most_recent_file
        ) == [
            {"address_id": 1, "address_line_1": "6826 Herzog Via"},
            {"address_id": 2, "address_line_1": "93 High Street"},
        ]

    @pytest.mark.it(
        "when most recent file is an empty json, returns list of empty dictionary"
    )
    def test_single_item_json(self, bucket):
        with open("address-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write('{"addresses": [{}]}')
        assert read_file_and_turn_it_to_list_dict(
            "address", "test_ingest_bucket", "address-2025-05-29T11:06:18.399084.json"
        ) == [{}]


class TestLoadMostRecentDataFromJson:
    @pytest.mark.it(
        """test that previously tested dependency injection functions work when put together"""
    )
    def test_load_data_from_json(self, bucket):
        test_file_1 = "address-2025-05-29T11:06:18.399084.json"
        test_file_2 = "address-2025-06-29T11:06:18.399084.json"
        with open(test_file_1, "w") as file:
            file.write(
                '{"address": [{"address_id": 1, "address_line_1": "6826 Herzog Via"}]}'
            )
        with open(test_file_2, "w") as file:
            file.write(
                '{"address": [{"address_id": 2, "address_line_1": "93 High Street"}]}'
            )
        bucket.upload_file(test_file_1, "address-2025-05-29T11:06:18.399084.json")
        bucket.upload_file(test_file_2, "address-2025-06-29T11:06:18.399084.json")

        assert load_data_from_most_recent_json("address", "test_ingest_bucket") == [
            {"address_id": 2, "address_line_1": "93 High Street"}
        ]

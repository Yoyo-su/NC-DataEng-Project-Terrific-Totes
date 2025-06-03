import pytest
from src.utils.transform_dimension_tables import (
    transform_dim_location,
    transform_dim_counterparty,
    transform_dim_currency,
)
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
            Bucket="fscifa-raw-data",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        bucket = s3_resource.Bucket("fscifa-raw-data")
        # add folder structure to mock bucket to imitate actual bucket folder structure:
        bucket.put_object(Key="address/")
        bucket.put_object(Key="counterparty/")
        bucket.put_object(Key="currency/")
        bucket.put_object(Key="staff/")
        bucket.put_object(Key="design/")
        # add last_updated txt to the mock bucket:
        with open("tests/data/last_updated.txt", "w") as file:
            file.write("2025-05-29T11:06:18.399084")
        bucket.upload_file("tests/data/last_updated.txt", "last_updated.txt")
        # add test address data to the mock bucket:
        test_address_data = """{"address": [
                        {"address_id": 1,
                        "address_line_1": "2 High Street",
                        "address_line_2": "Hackney",
                        "district": "Greater London",
                        "city": "London",
                        "postal_code": "N4 5HP",
                        "country": "UK",
                        "phone": "07933457899",
                        "created_at": "2025-05-29T11:05:00.399084",
                        "last_updated": "2025-05-29T11:05:30.399084"}
                        ]
                    }"""
        with open("tests/data/address-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write(test_address_data)
        bucket.upload_file(
            "tests/data/address-2025-05-29T11:06:18.399084.json",
            "address/address-2025-05-29T11:06:18.399084.json",
        )
        # add test counterparty data to the mock bucket:
        test_counterparty_data = """{"counterparty": [
                        {"counterparty_id": 1,
                        "counterparty_legal_name": "Alison Limited",
                        "legal_address_id": 1,
                        "commercial_contact": "Alison",
                        "delivery_contact": "Chris",
                        "created_at": "2025-05-29T11:05:00.399084",
                        "last_updated": "2025-05-29T11:05:30.399084"}
                        ]
                    }"""
        with open(
            "tests/data/counterparty-2025-05-29T11:06:18.399084.json", "w"
        ) as file:
            file.write(test_counterparty_data)
        bucket.upload_file(
            "tests/data/counterparty-2025-05-29T11:06:18.399084.json",
            "counterparty/counterparty-2025-05-29T11:06:18.399084.json",
        )
        # add test currency data to the mock bucket:
        test_currency_data = """{"currency": [
                        {"currency_id": 1,
                        "currency_code": "GBP",
                        "created_at": "2025-05-29T11:05:00.399084",
                        "last_updated": "2025-05-29T11:05:30.399084"},
                        {"currency_id": 2,
                        "currency_code": "EUR",
                        "created_at": "2025-05-29T11:05:00.399084",
                        "last_updated": "2025-05-29T11:05:30.399084"},
                        {"currency_id": 3,
                        "currency_code": "USD",
                        "created_at": "2025-05-29T11:05:00.399084",
                        "last_updated": "2025-05-29T11:05:30.399084"}
                        ]
                    }"""
        with open("tests/data/currency-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write(test_currency_data)
        bucket.upload_file(
            "tests/data/currency-2025-05-29T11:06:18.399084.json",
            "currency/currency-2025-05-29T11:06:18.399084.json",
        )
        return bucket


class TestTransformDimLocation:
    @pytest.mark.it(
        "test that transform_dim_location returns a dataframe with correct columns"
    )
    def test_returns_location_df_with_correct_columns(self, bucket):
        result = transform_dim_location()
        assert len(result.columns) == 8
        assert "location_id" in list(result.columns)
        assert "address_line_1" in list(result.columns)
        assert "address_line_2" in list(result.columns)
        assert "district" in list(result.columns)
        assert "city" in list(result.columns)
        assert "postal_code" in list(result.columns)
        assert "country" in list(result.columns)
        assert "phone" in list(result.columns)

    @pytest.mark.it(
        "test that transform_dim_location returns a dataframe containing data"
    )
    def test_location_df_not_empty(self, bucket):
        assert not transform_dim_location().empty


class TestTransformDimCounterparty:
    @pytest.mark.it(
        "test that transform_dim_counterparty returns a dataframe with correct columns"
    )
    def test_returns_counterparty_df_with_correct_columns(self, bucket):
        result = transform_dim_counterparty()
        assert len(result.columns) == 9
        assert "counterparty_id" in list(result.columns)
        assert "counterparty_legal_name" in list(result.columns)
        assert "counterparty_legal_address_line_1" in list(result.columns)
        assert "counterparty_legal_address_line_2" in list(result.columns)
        assert "counterparty_legal_district" in list(result.columns)
        assert "counterparty_legal_postal_code" in list(result.columns)
        assert "counterparty_legal_country" in list(result.columns)
        assert "counterparty_legal_phone_number" in list(result.columns)

    @pytest.mark.it(
        "test that transform_dim_counterparty returns a dataframe containing data"
    )
    def test_counterparty_df_not_empty(self, bucket):
        assert not transform_dim_counterparty().empty


class TestTransformDimCurrency:
    @pytest.mark.xfail(
        "test that transform_dim_currency returns a dataframe with correct columns"
    )
    def test_returns_currency_df_with_correct_columns(self, bucket):
        result = transform_dim_currency()
        assert len(list(result.columns)) == 3
        assert "currency_id" in list(result.columns)
        assert "currency_code" in list(result.columns)
        assert "currency_name" in list(result.columns)

    @pytest.mark.xfail(
        "test that transform_dim_currency contains correct currency_name"
    )
    def test_returns_correct_currency_name(self, bucket):
        result = transform_dim_currency()
        for i, row in enumerate(result["currency_code"], 1):
            if row == "GBP":
                assert result["currency_name"][i] == "British pound"

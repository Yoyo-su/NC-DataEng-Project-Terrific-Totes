import pytest
from src.python.utils.transform_dimension_tables import (
    transform_dim_location,
    transform_dim_counterparty,
    transform_dim_currency,
    transform_dim_staff,
    transform_dim_design,
    get_department_data,
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
        bucket.put_object(Key="department/")
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

        # add test staff data to the mock bucket:
        test_staff_data = """{"staff": [
                        {"staff_id": 1,
                        "first_name": "Amy",
                        "last_name": "Smith",
                        "department_id": 1,
                        "email_address": "amy@gmail.com",
                        "created_at": "2025-05-29T11:05:00.399084",
                        "last_updated": "2025-05-29T11:05:30.399084"}
                       ]
                    }"""
        with open("tests/data/staff-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write(test_staff_data)
        bucket.upload_file(
            "tests/data/staff-2025-05-29T11:06:18.399084.json",
            "staff/staff-2025-05-29T11:06:18.399084.json",
        )

        # add test department data to the mock bucket:
        test_department_data_1 = """{"department": [
                        {"department_id": 1,
                        "department_name": "Sales",
                        "location": "London",
                        "manager": "John Carter",
                        "created_at": "2025-05-29T11:05:00.399084",
                        "last_updated": "2025-05-29T11:05:30.399084"}
                       ]
                    }"""
        with open("tests/data/department-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write(test_department_data_1)
        bucket.upload_file(
            "tests/data/department-2025-05-29T11:06:18.399084.json",
            "department/department-2025-05-29T11:06:18.399084.json",
        )

        test_department_data_2 = """{"department": [
                        {"department_id": 2,
                        "department_name": "Logistics",
                        "location": "Newport",
                        "manager": "Rose Dawson",
                        "created_at": "2025-05-29T10:05:00.399084",
                        "last_updated": "2025-05-29T10:05:30.399084"}
                       ]
                    }"""
        with open("tests/data/department-2025-05-29T10:06:18.399084.json", "w") as file:
            file.write(test_department_data_2)
        bucket.upload_file(
            "tests/data/department-2025-05-29T10:06:18.399084.json",
            "department/department-2025-05-29T10:06:18.399084.json",
        )

        # add test design data to the mock bucket:
        test_design_data = """{"design": [
                        {"design_id": 1,
                        "created_at": "2025-05-29T11:05:00.399084",
                        "last_updated": "2025-05-29T11:05:30.399084",
                        "design_name": "Summer sunset",
                        "file_location": "Desktop/Design",
                        "file_name": "Summer_sunset.jpg"}
                       ]
                    }"""
        with open("tests/data/design-2025-05-29T11:06:18.399084.json", "w") as file:
            file.write(test_design_data)
        bucket.upload_file(
            "tests/data/design-2025-05-29T11:06:18.399084.json",
            "design/design-2025-05-29T11:06:18.399084.json",
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
    @pytest.mark.it(
        "test that transform_dim_currency returns a dataframe with correct columns"
    )
    def test_returns_currency_df_with_correct_columns(self, bucket):
        result = transform_dim_currency()
        assert len(list(result.columns)) == 3
        assert "currency_id" in list(result.columns)
        assert "currency_code" in list(result.columns)
        assert "currency_name" in list(result.columns)

    @pytest.mark.it("test that transform_dim_currency contains correct currency_name")
    def test_returns_correct_currency_name(self, bucket):
        result = transform_dim_currency()
        assert result["currency_name"][0] == "British pound"
        assert result["currency_name"][1] == "European Euro"
        assert result["currency_name"][2] == "United States dollar"


class TestGetDepartmentData:
    @pytest.mark.it(
        "test that get_department_data returns a dataframe with correct columns"
    )
    def test_returns_department_df_with_correct_columns(self, bucket):
        result = get_department_data()
        assert len(list(result.columns)) == 6
        assert "department_id" in list(result.columns)
        assert "department_name" in list(result.columns)
        assert "location" in list(result.columns)
        assert "manager" in list(result.columns)
        assert "created_at" in list(result.columns)
        assert "last_updated" in list(result.columns)

    @pytest.mark.it(
        "test that get_department_data returns a non-empty dataframe with 2 rows"
    )
    def test_department_df_not_empty(self, bucket):
        result = get_department_data()
        assert not result.empty
        assert len(result) == 2


class TestTransformDimStaff:
    @pytest.mark.it(
        "test that transform_dim_staff returns a dataframe with correct columns"
    )
    def test_returns_staff_df_with_correct_columns(self, bucket):
        result = transform_dim_staff()
        assert len(list(result.columns)) == 6
        assert "staff_id" in list(result.columns)
        assert "first_name" in list(result.columns)
        assert "last_name" in list(result.columns)
        assert "department_name" in list(result.columns)
        assert "location" in list(result.columns)
        assert "email_address" in list(result.columns)

    @pytest.mark.it("test that transform_dim_staff returns a dataframe containing data")
    def test_staff_df_not_empty(self, bucket):
        assert not transform_dim_staff().empty


class TestTransformDimDesign:
    @pytest.mark.it(
        "test that transform_dim_design returns a dataframe with correct columns"
    )
    def test_returns_staff_df_with_correct_columns(self, bucket):
        result = transform_dim_design()
        assert len(list(result.columns)) == 4
        assert "design_id" in list(result.columns)
        assert "design_name" in list(result.columns)
        assert "file_location" in list(result.columns)
        assert "file_name" in list(result.columns)

    @pytest.mark.it(
        "test that transform_dim_design returns a dataframe containing data"
    )
    def test_design_df_not_empty(self, bucket):
        assert not transform_dim_design().empty

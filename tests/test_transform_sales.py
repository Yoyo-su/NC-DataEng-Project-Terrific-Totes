import pytest
import pandas as pd
from src.python.utils.transform_sales import (
    transform_fact_sales_order,
    transform_dim_date,
)
import os
from moto import mock_aws
import boto3
from unittest.mock import patch


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

        # add last_updated txt to the mock bucket:
        with open("tests/data/last_updated.txt", "w") as file:
            file.write("2025-05-29T11:06:18.399084")
        bucket.upload_file("tests/data/last_updated.txt", "last_updated.txt")

        # add test sales data to the mock bucket:
        test_sales_data = """{
            "sales_order": [
                {
                "sales_order_id": 7,
                "created_at": "1904-05-20 11:12:12.115290",
                "last_updated": "2022-02-21 10:02:12.115290",
                "design_id": 32,
                "staff_id": 666,
                "counterparty_id": 420,
                "units_sold": 42972,
                "unit_price": 3.36,
                "currency_id": 1,
                "agreed_delivery_date": "1905-06-22",
                "agreed_payment_date": "1914-05-29",
                "agreed_delivery_location_id": 12
                },
                {
                "sales_order_id": 1,
                "created_at": "1910-05-21 11:40:12.115290",
                "last_updated": "2021-02-20 08:58:12.115290",
                "design_id": 45,
                "staff_id": 69,
                "counterparty_id": 13,
                "units_sold": 69939,
                "unit_price": 7.79,
                "currency_id": 3,
                "agreed_delivery_date": "1910-05-21",
                "agreed_payment_date": "1910-05-21",
                "agreed_delivery_location_id": 3
                }
            ]
            }"""
        with open(
            "tests/data/sales_order-2025-05-29T11:06:18.399084.json", "w"
        ) as file:
            file.write(test_sales_data)
        bucket.upload_file(
            "tests/data/sales_order-2025-05-29T11:06:18.399084.json",
            "sales_order/sales_order-2025-05-29T11:06:18.399084.json",
        )
        return bucket


class TestMakeFactSalesOrderTable:
    @pytest.mark.it(
        "Tests that the transform_fact_sales_order function makes a fact table correctly"
    )
    def test_transform_fact_sales_order_reformats_as_expected(self, bucket):
        fact_table = transform_fact_sales_order()
        expected_fact_table = pd.DataFrame(
            {
                "sales_order_id": [7, 1],
                "created_date": ["1904-05-20", "1910-05-21"],
                "created_time": ["11:12:12.115290", "11:40:12.115290"],
                "last_updated_date": ["2022-02-21", "2021-02-20"],
                "last_updated_time": ["10:02:12.115290", "08:58:12.115290"],
                "sales_staff_id": [666, 69],
                "counterparty_id": [420, 13],
                "units_sold": [42972, 69939],
                "unit_price": [3.36, 7.79],
                "currency_id": [1, 3],
                "design_id": [32, 45],
                "agreed_payment_date": ["1914-05-29", "1910-05-21"],
                "agreed_delivery_date": ["1905-06-22", "1910-05-21"],
                "agreed_delivery_location_id": [12, 3],
            }
        )
        pd.testing.assert_frame_equal(fact_table, expected_fact_table)

    @pytest.mark.it(
        "Tests that transform_fact_sales_order raises an exception when given an empty or malformed dataframe"
    )
    @patch("src.python.utils.transform_sales.json_to_pd_dataframe")
    def test_raises_appropriate_exception_for_malformed_dataframe(
        self, json_to_pd_dataframe, bucket
    ):
        json_to_pd_dataframe.return_value = pd.DataFrame({})
        with pytest.raises(Exception):
            transform_fact_sales_order()


class TestMakeDimDate:
    @pytest.mark.it(
        "Tests that the transform_dim_date function outputs the expected dim table"
    )
    def test_transform_dim_date_creates_correct_dim_table(self):
        test_fact_table = pd.DataFrame(
            {
                "sales_order_id": [7, 1],
                "created_date": ["1904-05-20", "1910-05-21"],
                "created_time": ["11:12:12.115290", "11:40:12.115290"],
                "last_updated_date": ["2022-02-21", "2021-02-20"],
                "last_updated_time": ["10:02:12.115290", "08:58:12.115290"],
                "sales_staff_id": [666, 69],
                "counterparty_id": [420, 13],
                "units_sold": [42972, 69939],
                "unit_price": [3.36, 7.79],
                "currency_id": [1, 3],
                "design_id": [32, 45],
                "agreed_payment_date": ["1914-05-29", "1910-05-21"],
                "agreed_delivery_date": ["1905-06-22", "1910-05-21"],
                "agreed_delivery_location_id": [12, 3],
            }
        )
        dim_table = transform_dim_date(test_fact_table)
        print(dim_table)
        expected_dim_table = pd.DataFrame(
            {
                "date_id": pd.to_datetime(
                    [
                        "1904-05-20",
                        "1905-06-22",
                        "1910-05-21",
                        "1914-05-29",
                        "2021-02-20",
                        "2022-02-21",
                    ]
                ),
                "year": [1904, 1905, 1910, 1914, 2021, 2022],
                "month": [5, 6, 5, 5, 2, 2],
                "day": [20, 22, 21, 29, 20, 21],
                "day_of_week": [4, 3, 5, 4, 5, 0],
                "day_name": [
                    "Friday",
                    "Thursday",
                    "Saturday",
                    "Friday",
                    "Saturday",
                    "Monday",
                ],
                "month_name": ["May", "June", "May", "May", "February", "February"],
                "quarter": [2, 2, 2, 2, 1, 1],
            }
        )
        pd.testing.assert_frame_equal(
            dim_table.sort_values(by="date_id")
            .reset_index(drop=True)
            .sort_index(axis=1),
            expected_dim_table.sort_values(by="date_id")
            .reset_index(drop=True)
            .sort_index(axis=1),
            check_like=True,
            check_dtype=False,
        )

    @pytest.mark.it(
        "Tests that transform_fact_sales_order raises an exception when given an empty or malformed dataframe"
    )
    def test_dim_maker_raises_appropriate_exception_for_malformed_dataframe(self):
        bad_dataframe = pd.DataFrame({})
        with pytest.raises(Exception):
            transform_dim_date(bad_dataframe)

import boto3
from utils.transform_sales import transform_dim_date, transform_fact_sales_order
from utils.transform_dimension_tables import (
    transform_dim_counterparty,
    transform_dim_currency,
    transform_dim_design,
    transform_dim_location,
    transform_dim_staff,
)
from utils.upload_dataframe_to_s3_parquet import upload_dataframe_to_s3_parquet


def lambda_handler(event, context):
    """
    When invoked, this lambda handler will:
    - invoke util functions to create dataframes for dimension tables, and fact table, sales_order
    - creates parquet files containing these dataframes, by invoking function dataframe_to_parquet
    - uploads these parquet files to s3 bucket, "fscifa-processed-data"

    """

    table_list = [
        "fact_sales_order",
        "dim_staff",
        "dim_location",
        "dim_design",
        "dim_date",
        "dim_currency",
        "dim_counterparty",
    ]

    s3_client = boto3.client("s3")
    for table in table_list:
        table_name = table
        if table == "dim_location":
            df = transform_dim_location()
        elif table == "dim_staff":
            df = transform_dim_staff()
        elif table == "dim_currency":
            df = transform_dim_currency()
        elif table == "dim_counterparty":
            df = transform_dim_counterparty()
        elif table == "dim_design":
            df = transform_dim_design()
        elif table == "dim_date":
            fact_sales = transform_fact_sales_order()
            if fact_sales:
                df = transform_dim_date(fact_sales)
        elif table == "fact_sales_order":
            df = transform_fact_sales_order()
        else:
            print(f"No transformation function found for: {table}")
            continue
        if df:
            key_prefix = f"{table_name}"
            upload_dataframe_to_s3_parquet(
                df, table_name, "fscifa-processed-data", key_prefix, s3_client=s3_client
            )

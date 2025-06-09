import pandas as pd
from utils.find_most_recent_filename import find_most_recent_filename
from utils.json_to_pd_dataframe import json_to_pd_dataframe


def transform_fact_sales_order():
    """
    This function takes in an OLTP-style dataframe describing company sales.
    It outputs a fact table, which will occupy the centre of an OLAP-style star-schema database,
    ready to be converted to parquet and sent to a 'processed' S3 bucket.

    Returns:
        - pd.DataFrame: a Pandas dataframe in star schema.

    Raises:
        - Exception: a generic exception if an error occurs.
    """
    global fact_sales_order
    try:
        most_recent_file = find_most_recent_filename("sales_order", "fscifa-raw-data")
        if not most_recent_file:
            return None
        fact_sales_order = json_to_pd_dataframe(
            most_recent_file, "sales_order", "fscifa-raw-data"
        )

        fact_sales_order["created_date"] = pd.to_datetime(
            fact_sales_order["created_at"], format="mixed", errors="raise"
        ).dt.date.astype(str)
        fact_sales_order["created_time"] = pd.to_datetime(
            fact_sales_order["created_at"], format="mixed", errors="raise"
        ).dt.time.astype(str)

        fact_sales_order["last_updated_date"] = pd.to_datetime(
            fact_sales_order["last_updated"], format="mixed", errors="raise"
        ).dt.date.astype(str)
        fact_sales_order["last_updated_time"] = pd.to_datetime(
            fact_sales_order["last_updated"], format="mixed", errors="raise"
        ).dt.time.astype(str)
        fact_sales_order["sales_staff_id"] = fact_sales_order["staff_id"]

        fact_sales_order.drop(
            columns=["created_at", "last_updated", "staff_id"], inplace=True
        )

        new_column_order = [
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ]

        fact_sales_order = fact_sales_order[new_column_order]

        return fact_sales_order
    except Exception as err:
        print(f"Unable to make facts table: {err}.")
        raise err


def transform_dim_date(fact_sales_order):
    """
    This function takes in THE RESULT OF make_fact_sales_order_table(df_sales), i.e., a fact table for sales data
    It outputs a dimension table of date data

    Arguments:
        - fact_sales_order (pd.DataFrame): a fact table representing sales data, to occupy the centre of a star schema

    Returns:
        - pd.DataFrame: a Pandas dataframe, which is a dimensions table

    Raises:
        - Exception: a generic exception if an error occurs.

    """
    global dim_date
    try:
        if fact_sales_order is not None:
            created_date = pd.Series(fact_sales_order["created_date"], name="date_id")
            last_updated_date = pd.Series(
                fact_sales_order["last_updated_date"], name="date_id"
            )
            agreed_payment_date = pd.to_datetime(
                fact_sales_order["agreed_payment_date"], errors="coerce"
            )
            agreed_delivery_date = pd.to_datetime(
                fact_sales_order["agreed_delivery_date"], errors="coerce"
            )

            all_the_dates = (
                pd.concat(
                    [
                        created_date.astype(str),
                        last_updated_date.astype(str),
                        pd.Series(agreed_payment_date, name="date_id").astype(str),
                        pd.Series(agreed_delivery_date, name="date_id").astype(str),
                    ],
                    ignore_index=True,
                )
                .drop_duplicates()
                .dropna()
                .reset_index(drop=True)
            )

            dim_date = pd.DataFrame()
            dim_date["date_id"] = pd.to_datetime(all_the_dates, format="%Y-%m-%d")
            dim_date["year"] = dim_date["date_id"].dt.year
            dim_date["month"] = dim_date["date_id"].dt.month
            dim_date["day"] = dim_date["date_id"].dt.day
            dim_date["day_of_week"] = dim_date["date_id"].dt.dayofweek
            dim_date["day_name"] = dim_date["date_id"].dt.day_name()
            dim_date["month_name"] = dim_date["date_id"].dt.month_name()
            dim_date["quarter"] = dim_date["date_id"].dt.quarter

            return dim_date
        return None
    except Exception as err:
        print(f"Unable to make dimensions table: {err}.")
        raise err

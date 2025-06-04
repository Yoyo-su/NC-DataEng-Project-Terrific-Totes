import pandas as pd
from src.utils.find_most_recent_json_filename import find_most_recent_json_filename
from src.utils.json_to_pd_dataframe import json_to_pd_dataframe
from forex_python.converter import CurrencyCodes


def transform_dim_location():
    most_recent_file = find_most_recent_json_filename("address", "fscifa-raw-data")
    if most_recent_file:
        location_df = json_to_pd_dataframe(
            most_recent_file, "address", "fscifa-raw-data"
        )
        location_df.rename(columns={"address_id": "location_id"}, inplace=True)
        location_df.drop(["created_at", "last_updated"], axis=1, inplace=True)
        return location_df


def transform_dim_counterparty():
    most_recent_file = find_most_recent_json_filename("counterparty", "fscifa-raw-data")
    if most_recent_file:
        counterparty_df = json_to_pd_dataframe(
            most_recent_file, "counterparty", "fscifa-raw-data"
        )
        counterparty_df.drop(
            ["created_at", "last_updated", "delivery_contact", "commercial_contact"],
            axis=1,
            inplace=True,
        )
        merge_location_to_counterparty_df = pd.merge(
            counterparty_df,
            transform_dim_location(),
            left_on="legal_address_id",
            right_on="location_id",
            how="left",
        )
        merge_location_to_counterparty_df.drop(
            ["location_id", "legal_address_id"], axis=1, inplace=True
        )
        for column_name in list(merge_location_to_counterparty_df.columns):
            if column_name == "phone":
                merge_location_to_counterparty_df.rename(
                    columns={"phone": "counterparty_legal_phone_number"}, inplace=True
                )
            if (
                column_name != "counterparty_id"
                and column_name != "counterparty_legal_name"
            ):
                merge_location_to_counterparty_df.rename(
                    columns={column_name: f"counterparty_legal_{column_name}"},
                    inplace=True,
                )
        return merge_location_to_counterparty_df


def find_currency_name_by_currency_code(code):
    c = CurrencyCodes()
    return c.get_currency_name(code)


def transform_dim_currency():
    most_recent_file = find_most_recent_json_filename("currency", "fscifa-raw-data")
    if most_recent_file:
        currency_df = json_to_pd_dataframe(
            most_recent_file, "currency", "fscifa-raw-data"
        )
        currency_df.drop(["last_updated", "created_at"], axis=1, inplace=True)
        currency_df["currency_name"] = currency_df["currency_code"].apply(
            find_currency_name_by_currency_code
        )
        return currency_df

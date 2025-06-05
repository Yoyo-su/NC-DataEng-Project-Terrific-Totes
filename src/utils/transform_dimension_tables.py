import pandas as pd
from src.utils.find_most_recent_filename import (
    find_most_recent_filename,
    find_files_with_specified_table_name,
)
from src.utils.json_to_pd_dataframe import json_to_pd_dataframe
from forex_python.converter import CurrencyCodes


def transform_dim_location():
    """
    This function:
    - calls find_most_recent_filename
    - check whether this returns a file name string (which will be the case if new data has been added to the address table in the totesys database)
    - if an exception is raised, transform_dim_location returns nothing (because there is no new data to be transformed)
    - otherwise, json_to_pd_dataframe is invoked, which returns a dataframe for new address data, location_df
    - address_id column of location_df is renamed to location_id, to match specification
    - columns, "created_at" and "last_updated" are dropped from location_df, to match specification
    - transformed location_df (dataframe) is returned

    Arguments: no arguments.

    Returns: nothing (if no new location data), or a dataframe containing new, transformed location data.

    """
    most_recent_file = find_most_recent_filename("address", "fscifa-raw-data")
    if most_recent_file:
        location_df = json_to_pd_dataframe(
            most_recent_file, "address", "fscifa-raw-data"
        )
        location_df.rename(columns={"address_id": "location_id"}, inplace=True)
        location_df.drop(["created_at", "last_updated"], axis=1, inplace=True)
        return location_df


def transform_dim_counterparty():
    """
    This function:
    - calls find_most_recent_filename
    - check whether this returns a file name string (will be the case if new data has been added to the counterparty table in the totesys database)
    - if an exception is raised, transform_dim_counterparty returns nothing (because there is no new data to be transformed)
    - otherwise, json_to_pd_dataframe is invoked, which returns a dataframe for new counterparty data, counterparty_df
    - columns, "created_at", "last_updated", "delivery_contact", "commercial_contact" dropped from counterparty_df, to match specification
    - counterparty_df is left merged with location_df (created by invoking transform_dim_location), to add location data for the counterparty
    - columns, "location_id", "legal_address_id", dropped from counterparty_df, to match specification
    - for loop is used to rename particular columns to match specification
    - transformed counterparty_df (dataframe) is returned

    Arguments: no arguments.

    Returns: nothing (if no new counterparty data), or a dataframe containing new, transformed counterparty data.

    """
    most_recent_file = find_most_recent_filename("counterparty", "fscifa-raw-data")
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
    """
    This function uses the forex-python.converter module to return the correct currency name, when passed with a given currency code.

    """
    c = CurrencyCodes()
    return c.get_currency_name(code)


def transform_dim_currency():
    """
    This function:
    - calls find_most_recent_filename
    - checks whether this returns a file name string (which will be the case if new data has been added to the currency table in the totesys database)
    - if an exception is raised, transform_dim_currency returns nothing (because there is no new data to be transformed)
    - otherwise, json_to_pd_dataframe is invoked, which returns a dataframe for new currency data, currency_df
    - columns, "last_updated" and "created_at" dropped from currency_df, to match specification
    - new column, "currency_name" created, filled by calling the function, find_currency_name_by_currency_code, for each row
    - transformed currency_df (dataframe) is returned

    Arguments: no arguments.

    Returns: nothing (if no new currency data), or a dataframe containing new, transformed currency data.

    """
    most_recent_file = find_most_recent_filename("currency", "fscifa-raw-data")
    if most_recent_file:
        currency_df = json_to_pd_dataframe(
            most_recent_file, "currency", "fscifa-raw-data"
        )
        currency_df.drop(["last_updated", "created_at"], axis=1, inplace=True)
        currency_df["currency_name"] = currency_df["currency_code"].apply(
            find_currency_name_by_currency_code
        )
        return currency_df


def get_department_data():
    """
    This function:
    - calls find_files_with_specified_table_name, which returns a list of json files from department folder of s3 bucket, fscifa-raw-data
    - assigns this list of json files to variable, files
    - creates a dataframe (department_df) out of the first json file in the list, by calling json_to_pd_dataframe
    - loops through remaining json files in list, and creates a dataframe for each (additional_df), and appends these dataframes to department_df
    - returns department_df

    Arguments: no arguments.

    Returns: a dataframe (department_df) containing all department data to date.

    """
    files = find_files_with_specified_table_name("department", "fscifa-raw-data")
    department_df = json_to_pd_dataframe(files[0], "department", "fscifa-raw-data")
    for i in range(1, len(files)):
        additional_df = json_to_pd_dataframe(files[i], "department", "fscifa-raw-data")
        department_df = pd.concat([department_df, additional_df], axis=0)
    return department_df


def transform_dim_staff():
    """
    This function:
    - calls find_most_recent_filename
    - check whether this returns a file name string (will be the case if new data has been added to the staff table in the totesys database)
    - if an exception is raised, transform_dim_staff returns nothing (because there is no new data to be transformed)
    - otherwise, json_to_pd_dataframe is invoked, which returns a dataframe for new staff data, staff_df
    - create department_df by invoking get_department_data
    - columns "manager", "created_at" and "last_updated" are dropped from department_df, to match specification
    - columns "created_at" and "last_updated" are dropped from staff_df, to match specification
    - staff_df is left merged with department_df on depatment_id, to add location data for each staff member
    - column, "department_id" is dropped from staff_df, to match specification
    - transformed staff_df (dataframe) is returned

    Arguments: no arguments.

    Returns: nothing (if no new staff data), or a dataframe containing new, transformed staff data.

    """

    most_recent_file = find_most_recent_filename("staff", "fscifa-raw-data")
    if most_recent_file:
        staff_df = json_to_pd_dataframe(most_recent_file, "staff", "fscifa-raw-data")
    department_df = get_department_data()
    department_df.drop(
        ["manager", "created_at", "last_updated"],
        axis=1,
        inplace=True,
    )
    staff_df.drop(
        ["created_at", "last_updated"],
        axis=1,
        inplace=True,
    )
    merge_staff_to_department_df = pd.merge(
        staff_df, department_df, on="department_id", how="left"
    )
    merge_staff_to_department_df.drop(
        ["department_id"],
        axis=1,
        inplace=True,
    )
    return merge_staff_to_department_df


def transform_dim_design():
    """
    This function:
    - calls find_most_recent_filename
    - check whether this returns a file name string (which will be the case if new data has been added to the design table in the totesys database)
    - if an exception is raised, transform_dim_design returns nothing (because there is no new data to be transformed)
    - otherwise, json_to_pd_dataframe is invoked, which returns a dataframe for new design data, design_df
    - columns "created_at" and "last_updated" are dropped from design_df, to match specification
    - transformed design_df (dataframe) is returned

    Arguments: no arguments.

    Returns: nothing (if no new design data), or a dataframe containing new, transformed design data.

    """
    most_recent_file = find_most_recent_filename("design", "fscifa-raw-data")
    if most_recent_file:
        design_df = json_to_pd_dataframe(most_recent_file, "design", "fscifa-raw-data")
    design_df.drop(
        ["created_at", "last_updated"],
        axis=1,
        inplace=True,
    )
    return design_df

import pandas as pd
from utils.find_most_recent_filename import (
    find_most_recent_filename,
    find_files_with_specified_table_name,
)
from utils.json_to_pd_dataframe import json_to_pd_dataframe


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
    global location_df

    most_recent_file = find_most_recent_filename("address", "fscifa-raw-data")
    if not most_recent_file:
        return None
    location_df = json_to_pd_dataframe(most_recent_file, "address", "fscifa-raw-data")
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
    global counterparty_df
    most_recent_file = find_most_recent_filename("counterparty", "fscifa-raw-data")
    if not most_recent_file:
        return None
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
    This function takes a currency code (e.g. "USD"), and returns the currency's name (e.g. "United States dollar").
    If the currency code is not recognised, it raises a KeyError exception, informing the user the currency code is not found.

    """

    currency_codes_to_names = {
        "JPY": "Japanese yen",
        "BGN": "Bulgarian lev",
        "CZK": "Czech koruna",
        "DKK": "Danish krone",
        "GBP": "British pound",
        "HUF": "Hungarian forint",
        "PLN": "Polish zloty",
        "RON": "Romanian leu",
        "SEK": "Swedish krona",
        "CHF": "Swiss franc",
        "ISK": "Icelandic króna",
        "NOK": "Norwegian krone",
        "TRY": "Turkish new lira",
        "AUD": "Australian dollar",
        "BRL": "Brazilian real",
        "CAD": "Canadian dollar",
        "CNY": "Chinese/Yuan renminbi",
        "HKD": "Hong Kong dollar",
        "IDR": "Indonesian rupiah",
        "ILS": "Israeli new sheqel",
        "INR": "Indian rupee",
        "KRW": "South Korean won",
        "MXN": "Mexican peso",
        "MYR": "Malaysian ringgit",
        "NZD": "New Zealand dollar",
        "PHP": "Philippine peso",
        "SGD": "Singapore dollar",
        "THB": "Thai baht",
        "ZAR": "South African rand",
        "EUR": "European Euro",
        "USD": "United States dollar",
        "KWD": "Kuwaiti dinar",
        "BHD": "Bahraini dinar",
        "OMR": "Omani rial",
        "JOD": "Jordanian dinar",
        "GIP": "Gibraltar pound",
        "KYD": "Cayman Islands dollar",
        "GEL": "Georgian lari",
        "GHS": "Ghanaian cedi",
        "GYD": "Guyanese dollar",
        "JMD": "Jamaican dollar",
        "KZT": "Kazakhstani tenge",
        "KES": "Kenyan shilling",
        "KGS": "Kyrgyzstani som",
        "LAK": "Laotian kip",
        "LBP": "Lebanese pound",
        "LRD": "Liberian dollar",
        "LYD": "Libyan dinar",
        "MGA": "Malagasy ariary",
        "MWK": "Malawian kwacha",
        "MVR": "Maldivian rufiyaa",
        "MUR": "Mauritian rupee",
        "MNT": "Mongolian tugrik",
        "MZN": "Mozambican metical",
        "NAD": "Namibian dollar",
        "NPR": "Nepalese rupee",
        "NIO": "Nicaraguan córdoba",
        "NGN": "Nigerian naira",
        "PKR": "Pakistani rupee",
        "PAB": "Panamanian balboa",
        "PYG": "Paraguayan guarani",
        "PEN": "Peruvian sol",
        "QAR": "Qatari riyal",
        "RUB": "Russian ruble",
        "SHP": "Saint Helena pound",
        "SCR": "Seychelles rupee",
        "SBD": "Solomon Islands dollar",
        "LKR": "Sri Lankan rupee",
        "SDG": "Sudanese pound",
        "SRD": "Surinamese dollar",
        "SYP": "Syrian pound",
        "TZS": "Tanzanian shilling",
        "TOP": "Tongan paanga",
        "TTD": "Trinidad and Tobago dollar",
    }
    try:
        return currency_codes_to_names[code]
    except KeyError:
        raise KeyError("Currency code not found")


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
    global currency_df
    most_recent_file = find_most_recent_filename("currency", "fscifa-raw-data")
    if not most_recent_file:
        return None
    currency_df = json_to_pd_dataframe(most_recent_file, "currency", "fscifa-raw-data")
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
    global department_df
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
    global staff_df
    most_recent_file = find_most_recent_filename("staff", "fscifa-raw-data")
    if not most_recent_file:
        return None
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
    global design_df
    most_recent_file = find_most_recent_filename("design", "fscifa-raw-data")
    if not most_recent_file:
        return None
    design_df = json_to_pd_dataframe(most_recent_file, "design", "fscifa-raw-data")
    design_df.drop(
        ["created_at", "last_updated"],
        axis=1,
        inplace=True,
    )
    return design_df

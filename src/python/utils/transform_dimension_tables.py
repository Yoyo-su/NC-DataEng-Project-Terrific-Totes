import pandas as pd
from utils.find_most_recent_filename import (
    find_most_recent_filename,
    find_files_with_specified_table_name,
)
from utils.json_to_pd_dataframe import json_to_pd_dataframe


def get_sales_delivery_location_data():
    """
    This function:
    - calls find_files_with_specified_table_name, which returns a list of json files from sales_order folder of s3 bucket, fscifa-raw-data
    - assigns this list of json files to variable, files
    - creates a dataframe (sales_location_df) out of the first json file in the list, by calling json_to_pd_dataframe
    - drops all columns of the sales_location_df except for agreed_delivery_location_id
    - loops through remaining json files in list, creates a dataframe for each (additional_df), drops all columns except agreed_delivery_location_id,
      appends additional dataframes to sales_location_df
    - returns sales_location_df

    Arguments: no arguments.

    Returns: a single column dataframe (sales_location_df) containing all agreed_delivery_location_ids.

    """
    files = find_files_with_specified_table_name("sales_order", "fscifa-raw-data")
    if files:
        sales_location_df = json_to_pd_dataframe(
            files[0], "sales_order", "fscifa-raw-data"
        )
        for i in range(1, len(files)):
            additional_df = json_to_pd_dataframe(
                files[i], "sales_order", "fscifa-raw-data"
            )
            sales_location_df = pd.concat([sales_location_df, additional_df], axis=0)
        sales_location_df.drop(
            [
                "sales_order_id",
                "created_at",
                "last_updated",
                "design_id",
                "staff_id",
                "counterparty_id",
                "units_sold",
                "unit_price",
                "currency_id",
                "agreed_delivery_date",
                "agreed_payment_date",
            ],
            axis=1,
            inplace=True,
        )
        sales_location_df.drop_duplicates(subset=None, keep="first", inplace=True)
        return sales_location_df


def transform_dim_location():
    """
    This function:
    - calls find_most_recent_filename
    - check whether this returns a file name string (which will be the case if new data has been added to the address table in the totesys database)
    - if an exception is raised, transform_dim_location returns nothing (because there is no new data to be transformed)
    - otherwise, json_to_pd_dataframe is invoked, which returns a dataframe for new address data, address_df
    - left merge address data into sales_location_df (obtained by invoking get_sales_delivery_location_data) to create dim_location_df
    - address_id column of dim_location_df is renamed to location_id, to match specification
    - columns, "created_at" and "last_updated" are dropped from dim_location_df, to match specification
    - drops duplicate rows from dim_location_df (which may appear if there are multiple sales delivered to the same address)
    - transformed dim_location_df (dataframe) is returned

    Arguments: no arguments.

    Returns: nothing (if no new location data), or a dataframe containing new, transformed location data.

    """
    global address_df

    most_recent_file = find_most_recent_filename("address", "fscifa-raw-data")
    if most_recent_file:
        address_df = json_to_pd_dataframe(
            most_recent_file, "address", "fscifa-raw-data"
        )
        dim_location_df = pd.merge(
            get_sales_delivery_location_data(),
            address_df,
            left_on="agreed_delivery_location_id",
            right_on="address_id",
        )
        dim_location_df.rename(
            columns={"agreed_delivery_location_id": "location_id"}, inplace=True
        )
        dim_location_df.drop(
            ["created_at", "last_updated", "address_id"], axis=1, inplace=True
        )
        dim_location_df.drop_duplicates(subset=None, keep="first", inplace=True)
        return dim_location_df


def transform_dim_counterparty():
    """
    This function:
    - calls find_most_recent_filename
    - check whether this returns a file name string (will be the case if new data has been added to the counterparty table in the totesys database)
    - if an exception is raised, transform_dim_counterparty returns nothing (because there is no new data to be transformed)
    - otherwise, json_to_pd_dataframe is invoked, which returns a dataframe for new counterparty data, counterparty_df
    - columns, "created_at", "last_updated", "delivery_contact", "commercial_contact" dropped from counterparty_df, to match specification
    - json_to_pd_dataframe is invoked, which returns a dataframe for new address data, location_df
    - all columns of the location_df except for address_id are dropped, to match specification
    - counterparty_df is left merged with location_df, to add location data for the counterparty
    - columns, "address_id", "legal_address_id", dropped from counterparty_df, to match specification
    - for loop is used to rename particular columns to match specification
    - transformed counterparty_df (dataframe) is returned

    Arguments: no arguments.

    Returns: nothing (if no new counterparty data), or a dataframe containing new, transformed counterparty data.

    """
    global counterparty_df
    most_recent_counterparty_file = find_most_recent_filename(
        "counterparty", "fscifa-raw-data"
    )
    most_recent_address_file = find_most_recent_filename("address", "fscifa-raw-data")
    if most_recent_counterparty_file:
        counterparty_df = json_to_pd_dataframe(
            most_recent_counterparty_file, "counterparty", "fscifa-raw-data"
        )
        counterparty_df.drop(
            ["created_at", "last_updated", "delivery_contact", "commercial_contact"],
            axis=1,
            inplace=True,
        )
        location_df = json_to_pd_dataframe(
            most_recent_address_file, "address", "fscifa-raw-data"
        )
        location_df.drop(
            ["created_at", "last_updated"],
            axis=1,
            inplace=True,
        )
        merge_location_to_counterparty_df = pd.merge(
            counterparty_df,
            location_df,
            left_on="legal_address_id",
            right_on="address_id",
            how="left",
        )
        merge_location_to_counterparty_df.drop(
            ["address_id", "legal_address_id"], axis=1, inplace=True
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
    - columns "manager", "created_at" and "last_updated" are dropped from department_df, to match specification
    - drop duplicate rows from department_df (so we only have one row for each department_id)
    - returns department_df

    Arguments: no arguments.

    Returns: a dataframe (department_df) containing all department data to date.

    """
    global department_df
    files = find_files_with_specified_table_name("department", "fscifa-raw-data")
    if files:
        department_df = json_to_pd_dataframe(files[0], "department", "fscifa-raw-data")
        for i in range(1, len(files)):
            additional_df = json_to_pd_dataframe(
                files[i], "department", "fscifa-raw-data"
            )
            department_df = pd.concat([department_df, additional_df], axis=0)
        department_df.drop(
            ["manager", "created_at", "last_updated"],
            axis=1,
            inplace=True,
        )
        department_df.drop_duplicates(subset=None, keep="first", inplace=True)
        return department_df


def transform_dim_staff():
    """
    This function:
    - calls find_most_recent_filename
    - check whether this returns a file name string (will be the case if new data has been added to the staff table in the totesys database)
    - if an exception is raised, transform_dim_staff returns nothing (because there is no new data to be transformed)
    - otherwise, json_to_pd_dataframe is invoked, which returns a dataframe for new staff data, staff_df
    - columns "created_at" and "last_updated" are dropped from staff_df, to match specification
    - staff_df is left merged with department_df (created by invoking get_department_data) on depatment_id, to add location data for each staff member
    - column, "department_id" is dropped from staff_df, to match specification
    - transformed staff_df (dataframe) is returned

    Arguments: no arguments.

    Returns: nothing (if no new staff data), or a dataframe containing new, transformed staff data.

    """
    global staff_df
    most_recent_file = find_most_recent_filename("staff", "fscifa-raw-data")
    if most_recent_file:
        staff_df = json_to_pd_dataframe(most_recent_file, "staff", "fscifa-raw-data")
    staff_df.drop(
        ["created_at", "last_updated"],
        axis=1,
        inplace=True,
    )
    merge_staff_to_department_df = pd.merge(
        staff_df, get_department_data(), on="department_id", how="left"
    )
    merge_staff_to_department_df.drop(
        ["department_id"],
        axis=1,
        inplace=True,
    )
    new_column_order = [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
    ]

    merge_staff_to_department_df = merge_staff_to_department_df[new_column_order]
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
    if most_recent_file:
        design_df = json_to_pd_dataframe(most_recent_file, "design", "fscifa-raw-data")
        design_df.drop(
            ["created_at", "last_updated"],
            axis=1,
            inplace=True,
        )
        return design_df

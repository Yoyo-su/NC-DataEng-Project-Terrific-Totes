import boto3
from utils.transform_sales import transform_dim_date, transform_fact_sales_order
from utils.transform_dimension_tables import transform_dim_counterparty, transform_dim_currency, transform_dim_design, transform_dim_location, transform_dim_staff
from utils.upload_dataframe_to_s3_parquet import upload_dataframe_to_s3_parquet
from botocore.exceptions import ClientError
import pandas as pd
from datetime import datetime
import json
from io import BytesIO


def find_most_recent_filename(table_name, bucket_name):
    """
    This function:
    - looks through files (of default json type, or parquet type if specified) in a specified s3 bucket, with a specified table name
    - selects the most recent file starting with this table name
    - compares the date/time in this file with the date/time in the last_updated.txt
    - if date/time are the same, returns string of the filename
    - else raises Exception informing the user that there is no new data for this table since the last update
    These functionalities are implemented using dependency injection.

    Arguments: table_name (str): a specified table name from the original OLTP database

    Returns: if a new file is found in the specified bucket containing the specified table_name,
    returns a string containing that file's name, otherwise raises an appropriate exception.

    """
    files = find_files_with_specified_table_name(table_name, bucket_name)
    most_recent_file = find_most_recent_file(files, table_name, bucket_name)
    return most_recent_file


"""The below functions are used as dependencies, injected within find_most_recent_filename:"""


def find_files_with_specified_table_name(table_name, bucket_name):
    """
    This function:
    - Retrieves a list of files in a specified s3 bucket (bucket_name) whose names contain the specified table_name

    Arguments:
    - table_name (str): name contained within the name of the file you're searching for
    - bucket_name (str): the name of the s3 bucket you're searching in

    Returns:
    - list[str]: A list of filenames in the s3 bucket containing table_name
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    files = []
    for obj in bucket.objects.filter(Prefix=table_name):
        if "" not in obj.key.split("/"):
            files.append(obj.key.split("/")[1])
    return files


def find_most_recent_file(files, table_name, bucket_name):
    """
    This function:
    - returns the name of the most recent file in a given s3 bucket whose name contains table_name

    Arguments:
    - files (List[str]): list of filenames
    - table_name (str): name contained within the name of the file you're searching for
    - bucket_name (str): the name of the s3 bucket you're searching in

    Returns:
    - str: The name of the most recent file whose name contains table_name

    """
    try:
        most_recent_file = sorted(files, reverse=True)[0]
        file_date_time = most_recent_file[len(table_name) + 1 : -5]
        s3 = boto3.resource("s3")
        last_updated_file = s3.Object(bucket_name, "last_updated.txt")
        last_update = last_updated_file.get()["Body"].read().decode("utf-8")
        if last_update == file_date_time:
            return most_recent_file
        else:
            raise Exception(f"No new data for table, {table_name}")
    except IndexError:
        raise IndexError(
            f"No file containing table, {table_name} is found in the s3 bucket"
        )
    except ClientError as err:
        if (
            err.response["Error"]["Code"] == "404"
            or err.response["Error"]["Code"] == "NoSuchKey"
        ):
            raise FileNotFoundError(
                "File not found 404: there is no last_updated.txt file saved in the s3 bucket 'fscifa-raw-data'"
            )
        else:
            raise ClientError


def json_to_pd_dataframe(most_recent_file: str, table_name, bucket_name):
    """
        This function:
               - downloads most_recent_file containing table_name in its name, from specified s3 bucket
               - loads the data from this json file into a pandas dataframe, which is returned

    Arguments: - most_recent_file, which is the most recent file in the s3 bucket, "fscifa-raw-data", with the specified table_name
               - table_name, which is a table name from the original OLTP database.
               - bucket_name, which should be set to "fscifa-raw-data" in production.

    Returns: a pandas dataframe of the data from the specified file.

    """
    try:
        s3 = boto3.resource("s3")
        s3_file_path = f"{table_name}/{most_recent_file}"
        last_updated_file = s3.Object(bucket_name, s3_file_path)
        updated_data = last_updated_file.get()["Body"].read().decode("utf-8")
        data = json.loads(updated_data)
        data_df = pd.json_normalize(data[table_name])
        return data_df
    except Exception:
        if not most_recent_file.startswith(table_name):
            raise Exception(
                "Error when converting file to dataframe: incorrect table_name"
            )
        elif not most_recent_file.endswith(".json"):
            raise Exception(
                "Error when converting file to dataframe: most_recent_file should be of type json"
            )
        else:
            raise ClientError(
                "Error retrieving data from specified bucket, check bucket_name is correct"
            )


def transform_dim_location():
    """
    This function:
    - calls find_most_recent_filename
    - check whether this returns a file name string (which will be the case if new data has been added to the address table in the totesys database)
    - if an exception is raised, transform_dim_location returns nothing (because there is no new data to be transformed)
    - otherwise, json_to_pd_dataframe is invoked, which returns a dataframe for new address data, location_df
    - within location_df, the address_id column is renamed to location_id, to match specification
    - within location_df, columns, "created_at" and "last_updated" are dropped, to match specification
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
    - counterparty_df is left merged with location_df (by invoking transform_dim_location), to obtain location data for the counterparty
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
    - check whether this returns a file name string (which will be the case if new data has been added to the currency table in the totesys database)
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
    print(files)
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


def upload_dataframe_to_s3_parquet(
    df, table_name, bucket_name, key_prefix, compression="snappy", s3_client=None
):
    """
    Converts a DataFrame to a Parquet file in-memory and uploads it directly to S3.

    Args:
    - df: The pandas DataFrame to upload.
    - table_name: Logical name of the table (used in filename).
    - bucket_name: Target S3 bucket.
    - key_prefix: S3 folder/prefix.
    - compression: One of ["snappy", "gzip", "brotli", "none"].
    - s3_client: boto3 S3 client (optional).

    Returns:
    - S3 path of the uploaded object.
    """

    if compression not in ["snappy", "gzip", "brotli", "none"]:
        raise ValueError(f"Invalid compression: {compression}")

    s3_client = boto3.client("s3")

    timestamp = datetime.now().isoformat()
    filename = f"{table_name}-{timestamp}.parquet"
    s3_key = f"{key_prefix.rstrip('/')}/{filename}"

    # Write DataFrame to in-memory buffer
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression=compression)
    buffer.seek(0)

    # Upload in-memory buffer to S3
    try:
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
        print(f"Uploaded to s3://{bucket_name}/{s3_key}")
        return f"s3://{bucket_name}/{s3_key}"
    except Exception as e:
        print(f"Upload failed: {e}")
        raise e


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
    try:
        most_recent_file = find_most_recent_filename("sales_order", "fscifa-raw-data")
        if most_recent_file:
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
    try:
        if not fact_sales_order.empty:
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
            dim_date["date_id"] = pd.to_datetime(all_the_dates)
            dim_date["year"] = dim_date["date_id"].dt.year
            dim_date["month"] = dim_date["date_id"].dt.month
            dim_date["day"] = dim_date["date_id"].dt.day
            dim_date["day_of_week"] = dim_date["date_id"].dt.dayofweek
            dim_date["day_name"] = dim_date["date_id"].dt.day_name()
            dim_date["month_name"] = dim_date["date_id"].dt.month_name()
            dim_date["quarter"] = dim_date["date_id"].dt.quarter

        return dim_date
    except Exception as err:
        print(f"Unable to make dimensions table: {err}.")
        raise err


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
            df = transform_dim_date(fact_sales)
        elif table == "fact_sales_order":
            df = transform_fact_sales_order()
        else:
            print(f"No transformation function found for: {table}")
            continue

        key_prefix = f"{table_name}"
        upload_dataframe_to_s3_parquet(
            df, table_name, "fscifa-processed-data", key_prefix, s3_client=s3_client
        )

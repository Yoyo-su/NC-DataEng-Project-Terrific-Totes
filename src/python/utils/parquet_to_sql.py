from utils.find_most_recent_filename import find_most_recent_filename
import pandas as pd
import boto3
from db.connection import connect_to_db, close_db
import io


def fetch_parquet(table_name, bucket):
    """
    This function:
    - invokes find_most_recent_filename, to find the most recent parquet file for a given table_name, in the specified s3 bucket
    - gets the s3 file path for the parquet file
    - reads the parquet file, and puts its contents into a dataframe
    - returns this dataframe

    Parameters
    ----------
    table_name : str
        Name of the dimensions or fact table
    bucket : str
        Name of the target S3 bucket.

    Returns
    ----------
    A dataframe containing the contents of the latest parquet file for a given dimensions or fact table.

    Raises
    ----------
    An Exception if any error was encountered when trying to read the parquet file from the s3 bucket, and return it as a dataframe.

    """

    try:
        most_recent_file = find_most_recent_filename(table_name, bucket, "parquet")
        s3 = boto3.client("s3")
        s3_file_path = f"{table_name}/{most_recent_file}"
        obj = s3.get_object(Bucket=bucket, Key=s3_file_path)
        parquet_df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        return parquet_df
    except Exception as err:
        print(f"Unable to retrieve parquet file from bucket: {err}")
        raise err


def parquet_to_sql(table_name, df):
    """
    This function:
    - takes a dataframe for a given dimension or fact table
    - puts the columns in that dataframe into a list
    - puts the rows in that dataframe into a list
    - constructs a SQL query that will insert values into a given dimension or fact table
    - connects to the data warehouse (a postgres database)
    - runs the query within the database
    - returns a success message to confirm successful insertion into the data warehouse

    Parameters
    ----------
    table_name : str
        Name of a dimension or fact table
    df : dataframe
        Dataframe of the dimension or fact table, table_name

    Returns
    ----------
    A success message to confirm successful insertion into the data warehouse.

    Raises
    ----------
    An Exception if any error was encountered when trying to insert into the data warehouse.

    """
    try:
        # put dataframe column names into a list:
        columns = df.columns.tolist()

        # put dataframe values into a list:
        values = df.values.tolist()

        # construct SQL query
        query = f"INSERT INTO {table_name} ("
        for column in columns:
            query += f"{column}, "
        query = query[:-2] + ") VALUES("
        for row in values:
            for value in row:
                if type(value) is str:
                    value = value.replace("'", "''")
                query += f"{value}, "
            query = query[:-2] + "), ("
        query = query[:-3] + ";"

        # connect to the data warehouse, and run the query
        conn = connect_to_db()
        conn.run(query)
        return {"result": "success"}
    except Exception as err:
        print(f"Unable to run sql query: {err}")
        raise err
    finally:
        close_db(conn)

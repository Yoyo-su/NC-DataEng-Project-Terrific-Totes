from src.utils.find_most_recent_json_filename import find_most_recent_json_filename
import pandas as pd
import boto3
from db.connection import connect_to_db, close_db
import io


def fetch_parquet(table_name, bucket):
    try:
        # get most recent parquet from bucket with table name in filename
        most_recent_file = find_most_recent_json_filename(table_name, bucket, "parquet")
        s3 = boto3.client("s3")
        s3_file_path = f"{table_name}/{most_recent_file}"
        obj = s3.get_object(Bucket=bucket, Key=s3_file_path)
        parquet_df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        return parquet_df
    except Exception as err:
        print(f"Unable to retrieve parquet file from bucket: {err}")
        raise err


def parquet_to_sql(table_name, df):
    try:
        # make parquetfile a dataframe and seperate columns/values
        columns = df.columns.tolist()
        values = df.values.tolist()

        # construct sql query
        query = f"INSERT INTO {table_name} ("
        for column in columns:
            query += f"{column}, "
        query = query[:-2] + ") VALUES("
        for row in values:
            for value in row:
                query += f"{value}, "
            query = query[:-2] + "), ("
        query = query[:-3] + ";"
        conn = connect_to_db()
        conn.run(query)
        return {"result": "success"}
    except Exception as err:
        print(f"Unable to run sql query: {err}")
        raise err
    finally:
        close_db(conn)

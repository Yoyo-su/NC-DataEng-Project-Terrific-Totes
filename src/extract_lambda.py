from pg8000.native import Connection
from dotenv import load_dotenv
import os
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import json

""" EXTRACT LAMBDA: Includes extract lambda handler and util functions """


load_dotenv()


def connect_to_db():
    """Open connection to DB"""
    try:

        db = Connection(
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            database=os.getenv("PG_DATABASE"),
            host=os.getenv("PG_HOST"),
            port=int(os.getenv("PG_PORT")),
        )
        return db
    except Exception:
        print("Error connecting to database.")


def close_db(db):
    """Close connection to db"""
    try:
        db.close()
    except Exception:
        print("Error closing database connection.")


def lambda_handler(event, context):
    """Lambda handler that extracts data from a list of database tables (dbtables) and puts each table as a json inside an S3 bucket

    Args:
        event (dict): an event given by AWS (unused but required by AWS)
        context (dict): an AWS Lambda context object (unused but required by AWS)

    Return:
        dict: A dictionary to indicate successful completion, of the form {"result": "success"}

    Raises:
        Exception: if the extraction or the upload fails
    """

    try:
        s3_client = boto3.client("s3")
        timestamp = datetime.now().isoformat()
        table_list = [
            "address",
            "counterparty",
            "currency",
            "department",
            "design",
            "payment",
            "payment_type",
            "purchase_order",
            "sales_order",
            "staff",
            "transaction",
        ]

        for table in table_list:
            try:
                """Run query with last updated if last_updated.txt exists"""
                response = s3_client.get_object(
                    Bucket="fscifa-raw-data", Key="last_updated.txt"
                )
                last_updated = response["Body"].read().decode("utf-8")
                check_time = last_updated.replace("T", " ")
                table_dict = extract_db(table, check_time)
            except Exception:
                """Run full select * query if not"""
                table_dict = extract_db(table)
            key = f"{table}/{table}-{timestamp}.json"
            if table_dict[table]:
                json_data = dump_to_json(table_dict)
                upload_json_to_s3(json_data, "fscifa-raw-data", key, s3_client)
        upload_json_to_s3(timestamp, "fscifa-raw-data", "last_updated.txt", s3_client)
        return {"result": "success"}
    except Exception as error:
        print(f"Failed to extract data from database: {error}")
        raise Exception


def extract_db(table_name, last_updated=None):
    """Lambda function that extracts data from a database table and returns it as a formatted dictionary

    Args:
        table_name (string): name of the dbtable to extract data from

    Returns:
        dict: dictionary with table name as the key and a list  value containing each table row as a dict
    """

    try:
        conn = connect_to_db()
        if not last_updated:
            query = f"SELECT * FROM {table_name};"
        else:
            query = f"SELECT * FROM {table_name} WHERE last_updated > '{last_updated}';"
        response = conn.run(query)
        columns = [column["name"] for column in conn.columns]
        table_dict = {table_name: [dict(zip(columns, row)) for row in response]}
        return table_dict

    except Exception as error:
        print(f"Failed to extract from DB: {error}")
        raise error
    finally:
        if conn:
            close_db(conn)


def upload_json_to_s3(json_file, bucket_name, key, s3_client):
    """
    Util function that uploads a json file to an s3 bucket

    Args:
        json_file (json): json to be uploaded
        bucket_name (string): bucket to upload to
        key (string): filepath to save to
        s3_client (boto): boto3 s3 client connection
    """

    try:
        s3_client.put_object(Body=json_file, Bucket=bucket_name, Key=key)
        print(f"Successfully uploaded {key} to s3://{bucket_name}/{key}")

    except ClientError as e:
        print(f"Failed to put object: {e}")
        raise e


def dump_to_json(data):
    """
    Util function that takes in a dictionary and converts it to json format for storage
    Args:
        data (dictionary): a dictionary representing data from a table
    Returns:
        a json file
    """

    return json.dumps(data, default=str)


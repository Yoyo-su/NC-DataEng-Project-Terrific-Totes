from dotenv import load_dotenv
import boto3
from datetime import datetime
from utils.extract_db import extract_db
from utils.json_dumps import dump_to_json
from utils.insert_into_s3 import upload_json_to_s3
from db.connection import connect_to_db, close_db



""" EXTRACT LAMBDA: Includes extract lambda handler and util functions """


load_dotenv()

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
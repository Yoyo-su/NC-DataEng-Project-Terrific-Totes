import boto3
from src.utils.extract_db import extract_db
from src.utils.json_dumps import dump_to_json
from src.utils.insert_into_s3 import upload_json_to_s3
from datetime import datetime


def lambda_handler(event, context):
    """Lambda handler that extracts data from dbtables and puts each table as a json inside an S3 bucket

    Args:
        event (_type_): _description_ ##TODO
        context (_type_): _description_ ##TODO
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

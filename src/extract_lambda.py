from utils.extract_db import extract_db
import json
import boto3
from utils.json_dumps import dump_to_json
from utils.insert_into_s3 import upload_json_to_s3
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """ Lambda handler that extracts data from dbtables and puts each table as a json inside an S3 bucket

    Args:
        event (_type_): _description_ ##TODO
        context (_type_): _description_ ##TODO
    """
    
    
    try:
        timestamp = datetime.utcnow().strftime('%B%d%Y-%H:%M:%S')
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
            table_dict = extract_db(table)
            key = f'{table}/{timestamp}.json'
            # INSERT JSON DUMPS
            json_data = dump_to_json(table_dict)
            # INSERT PUT OBJECT IN S3
            upload_json_to_s3(json_data, 'fscifa-raw-data',table,s3_client)
# def upload_json_to_s3(json_file, bucket_name, table_name, s3_client):

    except Exception as error:
        print(f"Failed to extract data from database: {error}")

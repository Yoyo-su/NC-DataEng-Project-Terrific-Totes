from utils.extract_db import extract_db


def lambda_handler(event, context):
    try:
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
            # INSERT JSON DUMPS
            # INSERT PUT OBJECT IN S3
    except Exception as error:
        print(f"Failed to extract data from database: {error}")

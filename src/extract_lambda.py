from utils.extract_db import extract_db


def lambda_handler(event, context):
    """ Lambda handler that extracts data from dbtables and puts each table as a json inside an S3 bucket

    Args:
        event (_type_): _description_ ##TODO
        context (_type_): _description_ ##TODO
    """
    
    
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

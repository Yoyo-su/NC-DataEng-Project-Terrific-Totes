from utils.parquet_to_sql import fetch_parquet, parquet_to_sql


def lambda_handler(event, context):
    """
    Lambda handler that loads the most recently processed parquet files from s3 into a data warehouse (PostgreSQL)

    It will read the files in fscifa-processed-data
    check the most recent file was created on the last iteration
    if it was:
        - it will upload this file to the data warehouse

    else:
        - it will do nothing

    Args:
        event (dict): an event given by AWS (unused but required by AWS)
        context (dict): an AWS Lambda context object (unused but required by AWS)

    Return:
        dict: A dictionary to indicate successful completion, of the form {"result": "success"}

    Raises:
        Exception: if the extraction or the upload fails

    """
    bucket = "fscifa-processed-data"
    table_list = [
        "dim_staff",
        "dim_date",
        "dim_counterparty",
        "dim_currency",
        "dim_design",
        "dim_location",
        "fact_sales_order",
    ]
    errors = []
    for table in table_list:
        try:

            parquet_df = fetch_parquet(table, bucket)
            if parquet_df is not None:
                parquet_to_sql(table, parquet_df)
                print(f"{table} table updated in OLAP warehouse")
            else:
                print(f"No data to load for {table}")
        except Exception as error:
            print(f"Failed to update {table} in database: {error}")
            errors.append(error)
            continue

    if len(errors) > 0:
        for error in errors:
            raise error
    else:
        return {"result": "success"}

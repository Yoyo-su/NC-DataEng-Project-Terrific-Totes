import boto3
from datetime import datetime


def load_data_from_most_recent_json(table_name, bucket_name):
    """
    This function:
    - looks through the json files in the ingestion s3 bucket with a given table name
    - selects the most recent file starting with this table name

    These functionalities are implemented using dependency injection.

    Arguments: table_name which is a table name from the original OLTP database

    Returns: a string containing the name of the most recent file with specified table_name
    """
    files = find_files_with_specified_table_name(table_name, bucket_name)
    most_recent_file = find_most_recent_file(files, table_name)
    return most_recent_file


"""The below functions are used as dependencies, injected within load_data_from_most_recent_json:"""


def find_files_with_specified_table_name(table_name, bucket_name):
    resource = boto3.resource("s3")
    bucket = resource.Bucket(bucket_name)
    files = [obj.key for obj in bucket.objects.all() if obj.key.startswith(table_name)]
    return files


def find_most_recent_file(files, table_name,bucket_name):
    try:
        most_recent_file = sorted(files, reverse=True)[0]
        file_date_time = most_recent_file[len(table_name)+1:-5]
        resource = boto3.resource("s3")
        bucket = resource.Bucket(bucket_name)
        bucket.download_file("last_updated.txt", "last_updated.txt")
        with open("last_updated.txt", "r") as file:
            last_update = file.readline()
        if last_update == file_date_time:
            return most_recent_file
        else:
            raise Exception(f"No new data for table, {table_name}")
    except IndexError:
        raise IndexError(
            f"No file containing table, {table_name} is found in the s3 bucket"
        )
    
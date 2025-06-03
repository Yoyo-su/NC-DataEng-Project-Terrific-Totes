import boto3
from botocore.exceptions import ClientError


def find_most_recent_json_filename(table_name, bucket_name):
    """
    This function:
    - looks through the json files in the ingestion s3 bucket with a given table name
    - selects the most recent file starting with this table name
    - compares the date/time in this file with the date/time in the last_updated.txt
    - if date/time are the same, returns string of the filename
    - else raises Exception informing the user that there is no new data for this table since the last update
    These functionalities are implemented using dependency injection.

    Arguments: table_name which is a table name from the original OLTP database

    Returns: a string containing the name of the most recent file with specified table_name
    """
    files = find_files_with_specified_table_name(table_name, bucket_name)
    most_recent_file = find_most_recent_file(files, table_name, bucket_name)
    return most_recent_file


"""The below functions are used as dependencies, injected within load_data_from_most_recent_json:"""


def find_files_with_specified_table_name(table_name, bucket_name):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    files = [obj.key for obj in bucket.objects.all() if obj.key.startswith(table_name)]
    return files


def find_most_recent_file(files, table_name, bucket_name):
    try:
        most_recent_file = sorted(files, reverse=True)[0]
        file_date_time = most_recent_file[len(table_name) + 1 : -5]
        s3 = boto3.resource("s3")
        last_updated_file = s3.Object(bucket_name,"last_updated.txt")
        last_update = last_updated_file.get()["Body"].read().decode("utf-8")
        if last_update == file_date_time:
            return most_recent_file
        else:
            raise Exception(f"No new data for table, {table_name}")
    except IndexError:
        raise IndexError(
            f"No file containing table, {table_name} is found in the s3 bucket"
        )
    except ClientError as err:
        if (
            err.response["Error"]["Code"] == "404"
            or err.response["Error"]["Code"] == "NoSuchKey"
        ):
            raise FileNotFoundError(
                "File not found 404: there is no last_updated.txt file saved in the s3 bucket 'fscifa-raw-data'"
            )
        else:
            raise ClientError

import json
import boto3
from botocore.exceptions import ClientError

def load_data_from_most_recent_json(table_name, bucket_name):
    """
    This function:
    - looks through the json files in the ingestion s3 bucket with a given table name
    - selects the most recent file starting with this table name
    - reads the data from this json file and loads it to a list of Python dictionaries, and returns this list
    These functionalities are implemented using dependency injection.

    Arguments: table_name which is a table name from the original OLTP database

    Returns: list of dictionaries, with each dictionary representing a record from a given table of the OLTP database

    """
    files = find_files_with_specified_table_name(table_name,bucket_name)
    most_recent_file = find_most_recent_file(files)
    data = read_file_and_turn_it_to_list_dict(table_name,bucket_name,most_recent_file)
    return data

"""The below functions are used as dependencies, injectected within load_data_from_most_recent_json:"""

def find_files_with_specified_table_name(table_name,bucket_name):
    resource = boto3.resource('s3')
    bucket = resource.Bucket(bucket_name) 
    files = [obj.key for obj in bucket.objects.all() if obj.key.startswith(table_name)]
    return files

def find_most_recent_file(files):
    if len(files) > 0:
        most_recent_file = sorted(files,reverse=True)[0]
        return most_recent_file
    else:
        return None

def read_file_and_turn_it_to_list_dict(table_name,bucket_name,most_recent_file):
    resource = boto3.resource('s3')
    bucket = resource.Bucket(bucket_name) 
    try:
        bucket.download_file(most_recent_file,most_recent_file)
        with open(most_recent_file,'r') as file:
            data = json.load(file)[table_name]
            data_dict =[item for item in data]
        return data_dict 
    except ValueError:
        return [{}]
    except ClientError:
        return [{}]

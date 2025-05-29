import json
import boto3

def files_with_specified_table_name(table_name):
    client = boto3.client('s3')
    bucket = client.Bucket('fscifa-raw-data')
    files = [obj.key for obj in bucket.objects.all() if obj.key.startswith(table_name)]
    return files

def find_most_recent_file(files):
    most_recent_file = files.sort(reverse=True)[0]
    return most_recent_file

def read_file_and_turn_it_to_list_dict(most_recent_file):
    with open(most_recent_file,'r') as file:
        data = json.load(file)
        data_dict =[item for item in data[table_name]]
    return data_dict 





def get_json_from_s3_raw_data(table_name, find_the_right_Bucket):
    """
    This function does:
    - looks through the json files in the ingestion s3 bucket with a given table name
    - selects the most recent file starting with this table name
    - reads the data from this json file and loads it to a list of Python dictionaries, and returns this list


    Arguments: table_name which is a table name from the original OLTP database

    Returns: list of dictionaries, with each dictionary representing a record from a given table of the OLTP database

    """
    # client = boto3.client('s3')
    # bucket = s3.Bucket('fscifa-raw-data')
    files = [obj.key for obj in bucket.objects.all() if file.startswith(table_name)]
    most_recent_file = files.sort(reverse=True)[0]
    with open(most_recent_file,'r') as file:
        data = json.load(file)
        data_dict =[item for item in data[table_name]]
    return data_dict 


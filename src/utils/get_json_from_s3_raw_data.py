import json
import boto3

def get_json_from_s3_raw_data(table_name):
    client = boto3.client('s3')
    bucket = s3.Bucket('fscifa-raw-data')
    files = [obj.key for obj in bucket.objects.all() if file.startswith(table_name)]
    most_recent_file = files.sort(reverse=True)[0]
    with open(most_recent_file,'r') as file:
        data = json.load(file)
    return data

import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError


def json_to_pd_dataframe(most_recent_file: str, table_name, bucket_name):
    """
        This function:
               - downloads most_recent_file containing table_name in its name, from specified s3 bucket
               - loads the data from this json file into a pandas dataframe, which is returned

    Arguments: - most_recent_file, which is the most recent file in the s3 bucket, "fscifa-raw-data", with the specified table_name
               - table_name, which is a table name from the original OLTP database.
               - bucket_name, which should be set to "fscifa-raw-data" in production.

    Returns: a pandas dataframe of the data from the specified file.

    """
    try:
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(bucket_name)
        s3_file_path = f"{table_name}/{most_recent_file}"
        last_updated_file = s3.Object(bucket_name, s3_file_path)
        updated_data = last_updated_file.get()["Body"].read().decode("utf-8")
        data = json.loads(updated_data)
        data_df = pd.json_normalize(data[table_name])
        return data_df
    except Exception:
        if not most_recent_file.startswith(table_name):
            raise Exception(
                "Error when converting file to dataframe: incorrect table_name"
            )
        elif not most_recent_file.endswith(".json"):
            raise Exception(
                "Error when converting file to dataframe: most_recent_file should be of type json"
            )
        else:
            raise ClientError(
                "Error retrieving data from specified bucket, check bucket_name is correct"
            )

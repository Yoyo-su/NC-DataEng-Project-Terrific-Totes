import pandas as pd
from utils.load_data_from_most_recent_json import load_data_from_most_recent_json
from utils.json_to_pd_dataframe import json_to_pd_dataframe

def dimension_tables_to_pd_dataframe(table_name):
    bucket_name = 'fscifa-raw-data'
    most_recent_file = load_data_from_most_recent_json(table_name, bucket_name)
    table_df = json_to_pd_dataframe(most_recent_file, table_name, bucket_name)
    if table_name == 'address':
       table_df.rename(columns = {"address_id" : "location_id"}, inplace = True)
       table_df.drop(["created_at","last_updated"], axis=1)
    return table_df   

       
       



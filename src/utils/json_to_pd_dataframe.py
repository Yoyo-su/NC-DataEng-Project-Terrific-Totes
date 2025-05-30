import pandas as pd
import json

def json_to_pd_dataframe(most_recent_file: str, table_name):
    if most_recent_file:
        with open(most_recent_file,'r') as file:
            data = json.load(file)
            result = pd.DataFrame(data=data[table_name])
        print(result)
        return result
    else:
        raise Exception("Table is not found")
    








# def read_file_and_turn_it_to_list_dict(table_name, bucket_name, most_recent_file):
#     resource = boto3.resource("s3")
#     bucket = resource.Bucket(bucket_name)
#     try:
#         bucket.download_file(most_recent_file, most_recent_file)
#         with open(most_recent_file, "r") as file:
#             data = json.load(file)[table_name]
#             data_records = [item for item in data]
#         return data_records
#     except ValueError:
#         return [{}]
#     except ClientError:
#         return [{}]



# [{},{}]

# import pandas as pd 
  
# # Initialise data to lists. 
# data = [{'Geeks': 'dataframe', 'For': 'using', 'geeks': 'list'},
#         {'Geeks':10, 'For': 20, 'geeks': 30}] 

# df = pd.DataFrame.from_dict(data)
# print(df)



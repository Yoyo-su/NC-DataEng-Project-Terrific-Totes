# - read the parquet
# - create the query from the parquet

import pandas as pd
import pyarrow
from db.connection import connect_to_db, close_db

print(pd.read_parquet('/home/frederick/FSCIFA-project/test_staff.parquet',engine='pyarrow'))

def parquet_to_sql(parquet_filename, parquet_filepath, bucket):
    df = pd.read_parquet('/home/frederick/FSCIFA-project/test_staff.parquet',engine='pyarrow')

# df = pd.read_parquet(parquet_path)
# column_name = df.columns
# INSERT INTO {table_name} ({column_name}) VALUES
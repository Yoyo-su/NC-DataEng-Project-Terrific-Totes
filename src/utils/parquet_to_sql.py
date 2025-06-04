# - read the parquet
# - create the query from the parquet

import pandas
import pyarrow

print(pandas.read_parquet('/home/frederick/FSCIFA-project/test_staff.parquet',engine='pyarrow'))

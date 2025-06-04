"""
Will load the most recently processed parquet files from s3 into a data warehouse (PostgreSQL)

It will read the files in fscifa-processed-data
check the most recent file was created on the last iteration
if it was:
    - it will upload this file to the data warehouse

else:
    - it will do nothing

"ready_bucket" 
"fscifa-processed-data"

 "lambda_bucket" 
"fscifa-lamdba"



once the most recent file found
take that file name 
puts that file in an SQL insert query
run the SQL query on the database

we can make a util function to:
- put the file contents into an SQL query 
- run the SQL query on the database

"""


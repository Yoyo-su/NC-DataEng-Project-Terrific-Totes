import os
import pytest
import json
from moto import mock_aws
import boto3
from src.utils.get_json_from_s3_raw_data import files_with_specified_table_name

@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"]='Test'
    os.environ["AWS_SECRET_ACCESS_KEY"]='Test'
    os.environ["AWS_SECURITY_TOKEN"]='Test'
    os.environ["AWS_DEFAULT_REGION"]='eu-west-2'

@pytest.fixture()
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client('s3')


class TestFindFilesWithSpecifiedTableName:
    @pytest.mark.it("""Returns a list with one file name when s3 bucket 
                        has only one file with specified table name""")
    def test_returns_list_of_one_file(self,s3_client):
        s3_client.create_bucket(Bucket='test_ingest_bucket',
                                CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as 'address-2025-05-29T11:06:18.399084.json':
            'address-2025-05-29T11:06:18.399084.json'.write("Hello, world!")
            tmp_path = tmp.name

        s3_client.upload_file('address-2025-05-29T11:06:18.399084.json', 'test_ingest_bucket','address')
        result = files_with_specified_table_name('address')
        assert len(result) == 1
        assert result[0]=='address-2025-05-29T11:06:18.399084.json'

with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
        tmp.write("Hello, world!")
        tmp_path = tmp.name


# import sys
# for item in sys.path:
#     print(item)


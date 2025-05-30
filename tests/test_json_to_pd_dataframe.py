import pytest
import pandas as pd
from src.utils.json_to_pd_dataframe import json_to_pd_dataframe


class TestDictTransformedToDataframe:
    @pytest.mark.it(
        "when passed a json file with one record returns dataframe with one row" 
    )
    def test_returns_dataframe_with_one_row(self):
        test_file_1 = "address-2025-06-29T11:06:18.399084.json"
        with open(test_file_1, "w") as file:
            file.write(
                '{"address": [{"address_id": 1, "address_line_1": "6826 Herzog Via"}]}'
            )
        result = json_to_pd_dataframe(test_file_1,'address') 
        assert isinstance(result,pd.DataFrame) 
        assert len(result) == 1
        assert result["address_id"][0] == 1
        assert result["address_line_1"][0] == "6826 Herzog Via"

    @pytest.mark.it(
        "when passed a json file with two records returns dataframe with two rows" 
    )
    def test_returns_dataframe_with_multiple_rows(self):
        test_file_1 = "address-2025-06-29T11:06:18.399084.json"
        with open(test_file_1, "w") as file:
            file.write(
                '{"address": [{"address_id": 1, "address_line_1": "6826 Herzog Via"},{"address_id": 2, "address_line_1": "93 High Street"}]}'
            )
        result = json_to_pd_dataframe(test_file_1,'address') 
        assert isinstance(result,pd.DataFrame) 
        assert len(result) == 2
        assert result["address_id"][0] == 1
        assert result["address_line_1"][0] == "6826 Herzog Via"
        assert result["address_id"][1] == 2
        assert result["address_line_1"][1] == "93 High Street"
        


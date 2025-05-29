import pytest
from src.utils.json_dumps import dump_to_json
import json


class TestJsonDumps:
    @pytest.mark.it(
        "Testing an empty dictionary returns a json string of an empty dictionary"
    )
    def test_empty_dictionary(self):
        test_data = {}
        result = dump_to_json(test_data)
        assert result == json.dumps({})

    @pytest.mark.it(
        "Testing that a nested dictionary returns a json string of a nested dictionary"
    )
    def test_nested_dictionary(self):
        test_data_2 = {"foo": "bar", "lorem": {"foo2": "bar2"}}
        result2 = dump_to_json(test_data_2)
        assert result2 == json.dumps({"foo": "bar", "lorem": {"foo2": "bar2"}})

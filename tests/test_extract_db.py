from unittest.mock import Mock, patch
import pytest
from src.utils.extract_db import extract_db
import datetime

"""Tests for the extract_db util function"""


class TestExtractDB:
    @pytest.mark.it("Testing returns dictionary with expected key and value as list")
    @patch("src.utils.extract_db.connect_to_db")
    def test_extract_db_returns_dictionary_with_expected_key_and_list_value(
        self, mock_connect_to_db
    ):
        mock_conn = Mock()
        mock_conn.run.return_value = []
        mock_conn.columns = []
        mock_connect_to_db.return_value = mock_conn
        result = extract_db("staff")
        assert isinstance(result, dict)
        assert "staff" in result.keys()
        assert isinstance(result["staff"], list)

    @pytest.mark.it(
        "Testing extract db with empty table returns dictionary with empty list"
    )
    @patch("src.utils.extract_db.connect_to_db")
    def test_extract_db_returns_empty_list_inside_dictionary(self, mock_connect_to_db):
        mock_conn = Mock()
        mock_conn.run.return_value = []
        mock_conn.columns = []
        mock_connect_to_db.return_value = mock_conn
        expected = {"staff": []}
        result = extract_db("staff")
        assert expected == result

    @pytest.mark.it("Testing extract db returns data as formatted dictionary")
    @patch("src.utils.extract_db.connect_to_db")
    def test_extract_db_returns_expected_values(self, mock_connect_to_db):
        mock_conn = Mock()
        mock_conn.run.return_value = [
            [
                1,
                "person",
                "one",
                2,
                "person1@terrifictotes.com",
                datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
            ],
            [
                2,
                "person",
                "two",
                6,
                "person2@terrifictotes.com",
                datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
            ],
        ]

        mock_conn.columns = [
            {
                "table_oid": 16466,
                "column_attrnum": 1,
                "type_oid": 23,
                "type_size": 4,
                "type_modifier": -1,
                "format": 0,
                "name": "staff_id",
            },
            {
                "table_oid": 16466,
                "column_attrnum": 2,
                "type_oid": 25,
                "type_size": -1,
                "type_modifier": -1,
                "format": 0,
                "name": "first_name",
            },
            {
                "table_oid": 16466,
                "column_attrnum": 3,
                "type_oid": 25,
                "type_size": -1,
                "type_modifier": -1,
                "format": 0,
                "name": "last_name",
            },
            {
                "table_oid": 16466,
                "column_attrnum": 4,
                "type_oid": 23,
                "type_size": 4,
                "type_modifier": -1,
                "format": 0,
                "name": "department_id",
            },
            {
                "table_oid": 16466,
                "column_attrnum": 5,
                "type_oid": 25,
                "type_size": -1,
                "type_modifier": -1,
                "format": 0,
                "name": "email_address",
            },
            {
                "table_oid": 16466,
                "column_attrnum": 6,
                "type_oid": 1114,
                "type_size": 8,
                "type_modifier": 3,
                "format": 0,
                "name": "created_at",
            },
            {
                "table_oid": 16466,
                "column_attrnum": 7,
                "type_oid": 1114,
                "type_size": 8,
                "type_modifier": 3,
                "format": 0,
                "name": "last_updated",
            },
        ]
        mock_connect_to_db.return_value = mock_conn

        expected = {
            "staff": [
                {
                    "staff_id": 1,
                    "first_name": "person",
                    "last_name": "one",
                    "department_id": 2,
                    "email_address": "person1@terrifictotes.com",
                    "created_at": datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                    "last_updated": datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                },
                {
                    "staff_id": 2,
                    "first_name": "person",
                    "last_name": "two",
                    "department_id": 6,
                    "email_address": "person2@terrifictotes.com",
                    "created_at": datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                    "last_updated": datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                },
            ]
        }
        result = extract_db("staff")
        assert expected == result
    @pytest.mark.it("Testing raises exception")
    @patch("src.utils.extract_db.connect_to_db")
    def test_extract_db_raises_exception_if_table_doesnt_exist(
        self, mock_connect_to_db
    ):
        mock_conn = Mock()
        mock_connect_to_db.return_value = mock_conn
        with pytest.raises(Exception):
            result = extract_db("fail")
        
from unittest.mock import MagicMock, Mock, patch
import pytest
from src.utils.extract_db import extract_db
import datetime

@pytest.mark.it("Testing extract db")
@patch("src.utils.extract_db.connect_to_db")
def test_extract_db(mock_connect_to_db):
    mock_conn = Mock()
    mock_conn.run.return_value = [[1, 'Jeremie', 'Franey', 2, 'jeremie.franey@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 
                                datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)],
                                [2, 'Deron', 'Beier', 6, 'deron.beier@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]]

    mock_conn.columns = [{'table_oid': 16466, 'column_attrnum': 1, 'type_oid': 23, 'type_size': 4, 'type_modifier': -1, 'format': 0, 'name': 'staff_id'}, {'table_oid': 16466, 'column_attrnum': 2, 'type_oid': 25, 'type_size': -1, 'type_modifier': -1, 'format': 0, 'name': 'first_name'}, {'table_oid': 16466, 'column_attrnum': 3, 'type_oid': 25, 'type_size': -1, 'type_modifier': -1, 'format': 0, 'name': 'last_name'}, {'table_oid': 16466, 'column_attrnum': 4, 'type_oid': 23, 'type_size': 4, 'type_modifier': -1, 'format': 0, 'name': 'department_id'}, {'table_oid': 16466, 'column_attrnum': 5, 'type_oid': 25, 'type_size': -1, 'type_modifier': -1, 'format': 0, 'name': 'email_address'}, {'table_oid': 16466, 'column_attrnum': 6, 'type_oid': 1114, 'type_size': 8, 'type_modifier': 3, 'format': 0, 'name': 'created_at'}, {'table_oid': 16466, 'column_attrnum': 7, 'type_oid': 1114, 'type_size': 8, 'type_modifier': 3, 'format': 0, 'name': 'last_updated'}]
    mock_connect_to_db.return_value = mock_conn

    expected = {"staff":[{'staff_id': 1, 'first_name': 'Jeremie', 'last_name': 'Franey', 'department_id': 2, 'email_address': 'jeremie.franey@terrifictotes.com', 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)}, {'staff_id': 2, 'first_name': 'Deron', 'last_name': 'Beier', 'department_id': 6, 'email_address': 'deron.beier@terrifictotes.com', 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)}]}
    result = extract_db("staff")
    assert expected == result
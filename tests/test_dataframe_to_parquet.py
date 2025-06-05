import pytest
import pandas as pd
from src.utils.dataframe_to_parquet import dataframe_to_parquet
from unittest.mock import Mock, patch
from datetime import datetime


@pytest.fixture
def mock_df():
    user_data = {
        "test_staff": [
            {"department_id": 2, "first_name": "Jeremie", "last_name": "Franey"},
            {"department_id": 6, "first_name": "Deron", "last_name": "Beier"},
        ]
    }
    return pd.DataFrame(user_data["test_staff"])


@pytest.mark.it(
    "Checks whether dataframe to parquet file function returns correct path"
)
@patch("pandas.DataFrame.to_parquet")
@patch("src.utils.dataframe_to_parquet.datetime")
def test_df_to_parquet_successful_creation_asserting_by_path(mock_datetime,mock_to_parquet, mock_df):
    fixed_time = datetime(2025, 6, 4, 10,13,0)
    mock_datetime.now.return_value = fixed_time
    table_name = "test_staff"
    path = f"{table_name}-{fixed_time.isoformat()}.parquet"

    result = dataframe_to_parquet(mock_df, table_name)  # returning path from util func
    mock_to_parquet.assert_called_once_with(
        path, engine="fastparquet", compression="gzip"
    )
    assert result == path


@pytest.mark.it("writes DataFrame to Parquet and reads it back without data loss")
@patch("src.utils.dataframe_to_parquet.datetime")
def test_df_to_parquet_successful_creation_by_reading_parquet_file(mock_datetime, mock_df):
    fixed_time = datetime(2025, 6, 4, 10,13,0)
    mock_datetime.now.return_value = fixed_time
    table_name = "test_staff"
    path = f"{table_name}-{fixed_time.isoformat()}.parquet"
    dataframe_to_parquet(mock_df, table_name)
    df_parquet = pd.read_parquet(path)
    pd.testing.assert_frame_equal(mock_df, df_parquet)


@pytest.mark.it("raises ValueError for unsupported compression type")
def test_df_to_parquet_invalid_compression(mock_df):
    table_name = "test_staff"
    with pytest.raises(ValueError) as err:
        dataframe_to_parquet(mock_df, table_name, compression="gjkkj")

    assert str(err.value) == "Invalid compression: gjkkj"


@pytest.mark.it("raises an exception when DataFrame.to_parquet fails")
def test_df_to_parquet_exception_handling():
    table_name = "test_staff"
    mock_df = Mock()
    mock_df.to_parquet.side_effect = Exception("raised exception through test")

    result = dataframe_to_parquet(mock_df, table_name)
    assert result is None

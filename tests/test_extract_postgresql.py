import io
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.etl.extract_postgresql import get_dataframe_from_rds
from src.utils.exception import CustomException

# Test: Successful Data Fetch from PostgreSQL
@patch('src.etl.extract_postgresql.psycopg2.connect', autospec=True)
def test_fetch_from_postgresql_success(mock_connect):
    mock_data = io.StringIO("column1,column2\ndata1,data2\ndata3,data4")
    mock_df = pd.read_csv(mock_data)

    with patch("pandas.read_sql_query", return_value=mock_df):
        result = get_dataframe_from_rds()
    
    assert isinstance(result, pd.DataFrame) and result.shape == (2, 2)
    assert 'column1' in result.columns

# Test: Query Returns No Data
@patch('src.etl.extract_postgresql.psycopg2.connect', autospec=True)
def test_fetch_from_postgresql_empty(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []

    mock_conn = mock_connect.return_value
    mock_conn.cursor.return_value = mock_cursor

    with pytest.raises(CustomException, match="PostgreSQL: Query returned no data."):
        get_dataframe_from_rds()

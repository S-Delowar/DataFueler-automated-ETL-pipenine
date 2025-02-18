import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.etl.extract_mongodb import get_mongodb_connection, get_data_from_mongodb, get_validated_mongodb_data

# Test 1: MongoDB connection
@patch('src.etl.extract_mongodb.MongoClient', autospec=True)
def test_get_mongodb_connection(mock_mongo_client):
    mock_mongo_client.return_value = MagicMock()
    assert get_mongodb_connection() is not None
    mock_mongo_client.assert_called_once()

# Test 2: Fetching data from MongoDB
@patch('src.etl.extract_mongodb.get_mongodb_connection', autospec=True)
def test_get_data_from_mongodb(mock_get_connection):
    mock_get_connection.return_value.find.return_value = [{'column1': 'value1', 'column2': 'value2'}]
    df = get_data_from_mongodb()
    assert isinstance(df, pd.DataFrame) and not df.empty

# Test 3: Data validation
@patch.multiple(
    'src.etl.extract_mongodb',
    get_data_from_mongodb=MagicMock(return_value=pd.DataFrame({'column1': ['value1'], 'column2': ['value2']})),
    load_schema=MagicMock(return_value={'columns': [{'name': 'column1'}, {'name': 'column2'}]}),
    get_dataframe_validation_errors=MagicMock(return_value=[]),
)
def test_get_validated_mongodb_data():
    df = get_validated_mongodb_data()
    assert isinstance(df, pd.DataFrame) and not df.empty

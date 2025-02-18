# tests/test_transform.py
import pytest
import pandas as pd
from src.etl.transform import get_final_combined_data
from src.utils.exception import CustomException



def test_combine_data_success():
    mongo_data = pd.DataFrame({"column1": ["data1", "data3"], "column2": ["data2", "data4"]})
    postgres_data = pd.DataFrame({"column1": ["data5", "data7"], "column2": ["data6", "data8"]})
    final_columns = ["column1", "column2"]

    combined_data = get_final_combined_data(mongo_data, postgres_data, final_fields=final_columns)
    print(combined_data)
    print(combined_data.shape)

    assert isinstance(combined_data, pd.DataFrame)
    assert combined_data.shape == (4, 2)  
    assert 'column1' in combined_data.columns



def test_combine_data_no_empty_dfs():
    mongo_data = pd.DataFrame({"column1": [], "column2": []})
    postgres_data = pd.DataFrame({"column1": [], "column2": []})
    final_columns = ["column1", "column2"]

    with pytest.raises(CustomException, match="No data found to combine"):
        get_final_combined_data(mongo_data, postgres_data, final_columns)

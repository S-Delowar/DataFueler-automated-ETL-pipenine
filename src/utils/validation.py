from src.utils.logger import logging
from src.utils.exception import CustomException
import pandas as pd


def get_dataframe_validation_errors(df, schema):
    schema = schema.get("columns")
    
    errors = []
    
    for col in schema:
        col_name = col.get("name")
        expected_type = col.get("type")
        
        if not col_name in df.columns:
            errors.append(f"Missing column: {col_name}")
            continue
        
        if expected_type=="int":
            if not pd.api.types.is_integer_dtype(df[col_name]):
                errors.append(f"Column {col_name} is not of type int")
            
        elif expected_type=="float":
            if not pd.api.types.is_float_dtype(df[col_name]):
                errors.append(f"Column {col_name} is not of type float")
            
        elif expected_type=="object":
            if not pd.api.types.is_object_dtype(df[col_name]):
                errors.append(f"Column {col_name} is not of type object")
        else:
            errors.append(f"Unsupported expected type '{expected_type}' for column {col_name}")
            
    return errors
        
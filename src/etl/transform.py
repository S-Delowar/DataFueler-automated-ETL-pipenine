import sys
import pandas as pd

from src.etl.extract_mongodb import get_validated_mongodb_data
from src.etl.extract_postgresql import get_validated_postgres_data
from src.utils.common import load_schema
from src.utils.exception import CustomException
from src.utils.logger import logging


def get_final_df_on_mongodb_and_postgres_data():
    try:
        logging.info(f"Loading validated mongodb and validated postgres data")
        mongodb_df = get_validated_mongodb_data()
        postgres_df = get_validated_postgres_data()
        
        logging.info(f"Defining final columns")
        combined_columns = load_schema("config/schema.yml").get("columns")
        final_col_names = [col.get("name") for col in combined_columns]
        
        logging.info(f"Merging mongodb and postgres data on the defined final columns")
        final_df = pd.concat([mongodb_df[final_col_names], postgres_df[final_col_names]])
        
        logging.info(f"Final data is ready now with shape {final_df.shape}")
        
        return final_df
    
    except Exception as e:
        raise CustomException(e, sys)
 



if __name__=="__main__":
    final_df = get_final_df_on_mongodb_and_postgres_data()
    print(f"Created final data. Shape {final_df.shape}")
    print(final_df.columns)
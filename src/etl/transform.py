import sys
import pandas as pd

from src.utils.common import load_schema
from src.utils.exception import CustomException
from src.utils.logger import logging


def get_final_df_on_mongodb_and_postgres_data():
    try:
        logging.info(f"Loading validated mongodb and validated postgres data")
        mongodb_df = pd.read_csv("validated_mongodb_data.csv")
        postgres_df = pd.read_csv("validated_postgres_data.csv")
        
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
    get_final_df_on_mongodb_and_postgres_data()
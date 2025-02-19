import os
import sys
import pandas as pd
import numpy as np
from pymongo.mongo_client import MongoClient
import yaml
from src.utils.common import load_schema
from src.utils.logger import logging
from src.utils.exception import CustomException
from dotenv import load_dotenv

from src.utils.validation import get_dataframe_validation_errors

load_dotenv()



def convert_numpy_arrays(record):
    """
    Recursively convert numpy arrays in a dictionary to lists.
    """
    if isinstance(record, dict):
        return {k: convert_numpy_arrays(v) for k, v in record.items()}
    elif isinstance(record, list):
        return [convert_numpy_arrays(item) for item in record]
    elif isinstance(record, np.ndarray):
        return record.tolist()
    else:
        return record


# get mongodb connection
def get_mongodb_connection():
    mongodb_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DBNAME")
    collection_name = os.getenv("MONGODB_COLLECTION")
    try:
        logging.info("Connecting MongoDB server")
        client = MongoClient(mongodb_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        logging.info("MongoDB server connected")
        return collection
    except Exception as e:
        logging.info(f"Error connecting to MongoDB: {str(e)}")
        raise CustomException(e, sys)


def get_data_from_mongodb():
    try:       
        collection = get_mongodb_connection()
        
        logging.info(f"Loading data from mongodb")     
        data = collection.find()
        
        # Convert cursor to list and then convert any numpy arrays in each document
        data_list = [convert_numpy_arrays(doc) for doc in list(data)]
        
        logging.info("Converting fetched data to Dataframe")
        df = pd.DataFrame(data_list)
        
        if "_id" in df.columns:
            df.drop(columns=["_id"], axis=1, inplace=True)
            logging.info(f"Drop unnecessary column '_id'")

        logging.info(f"Loaded {df.shape[0]} records from mongodb database and converted into pandas DataFrame")
                
        return df
    except Exception as e:
        raise CustomException(e, sys)


# get_and_validated mongodb data
def get_validated_mongodb_data():
    df = get_data_from_mongodb()
    if not df.empty:
        validation_schema = load_schema("config/schema.yml")
        validation_errors = get_dataframe_validation_errors(df, validation_schema)
        if validation_errors:
            for error in validation_errors:
                logging.info(error)
        else:
            logging.info(f"MongoDB Data Validated Successfully")
            mongodb_data_save_path = "Processed_Data/valid_mongodb_data.csv"
            df.to_csv(mongodb_data_save_path, index=False)
            print(f"MongoDB data extracted and saved to path '{mongodb_data_save_path}'")
            return df
            
    else: 
        logging.info("Empty Dataframe from Postgres Data")        
        



if __name__ == "__main__":
    try:
        df = get_validated_mongodb_data()
        print(df.shape)
    except Exception as e:
        raise CustomException(e, sys)
        
    
    

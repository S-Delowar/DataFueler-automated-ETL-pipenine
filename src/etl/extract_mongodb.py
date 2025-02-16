import os
import sys
import pandas as pd
from pymongo.mongo_client import MongoClient
import yaml
from src.utils.common import load_schema
from src.utils.logger import logging
from src.utils.exception import CustomException
from dotenv import load_dotenv

from src.utils.validation import get_dataframe_validation_errors

load_dotenv()


def get_data_from_mongodb(mongodb_uri, db_name, collection_name):
    try:
        logging.info("Connecting MongoDB server")
        client = MongoClient(mongodb_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        logging.info("MongoDB server connected")   
        logging.info(f"Loading data from mongodb collection: {collection_name}") 
        
        data = collection.find()
        df = pd.DataFrame(list(data))
        logging.info(f"Loaded {df.shape[0]} records from db and converted into pandas DataFrame")
        df.drop(columns=["_id"], axis=1, inplace=True)
        logging.info(f"Drop unnecessary column '_id'")
        
        return df
    except Exception as e:
        raise CustomException(e, sys)
        
        

if __name__ == "__main__":
    MONGODB_URI = os.getenv("MONGODB_URI")
    MONGODB_DBNAME = os.getenv("MONGODB_DBNAME")
    MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")
    
    df = get_data_from_mongodb(MONGODB_URI, MONGODB_DBNAME, MONGODB_COLLECTION)
    
    if not df.empty:
        logging.info(f"validating dataframe")
        
        validation_schema = load_schema("config/schema.yml")
        validation_errors = get_dataframe_validation_errors(df, validation_schema)
        
        if validation_errors:
            logging.info(f"Errors found")
            for error in validation_errors:
                logging.info(error)
        else:
            logging.info(f"Validation is successful")
            df.to_csv("validated_mongodb_data.csv", index=False)
    else:
        print("Empty Dataframe from Postgres Data")

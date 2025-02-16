import os
from src.etl.extract_mongodb import get_data_from_mongodb


if __name__=="__main__":
    MONGODB_URI = os.getenv("MONGODB_URI")
    MONGODB_DBNAME = os.getenv("MONGODB_DBNAME")
    MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")
    
    df = get_data_from_mongodb(MONGODB_URI, MONGODB_DBNAME, MONGODB_COLLECTION)
    
    print(f" from main.py file: shape {df.shape}")
    
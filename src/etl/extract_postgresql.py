import os
import sys
from dotenv import load_dotenv
import psycopg2
import pandas as pd

from src.utils.common import load_schema
from src.utils.exception import CustomException
from src.utils.logger import logging
from src.utils.validation import get_dataframe_validation_errors

load_dotenv()

# create connection
def get_postgres_connection():
    rds_host = os.getenv("RDS_HOST")
    rds_user = os.getenv("RDS_USER")
    rds_password = os.getenv("RDS_PASSWORD")
    rds_db = os.getenv("RDS_DB")
    rds_port = os.getenv("RDS_PORT")
    
    try:
        logging.info(f"Creating connection to RDS Postgresql Database")
        conn = psycopg2.connect(
            host = rds_host,
            user = rds_user,
            password = rds_password,
            dbname = rds_db,
            port = rds_port
            )

        logging.info(f"Connection established")
        return conn
    except Exception as e:
        logging.info(f"Error connecting to PostgresQL: {str(e)}")
        raise CustomException(e, sys)

# function to get data from postgresql rds
def fetch_data_from_rds():
    conn = get_postgres_connection()
    try:    
        cur = conn.cursor()
        
        table_name = "fuel_consumption_2015_25"
        get_data_query = f"SELECT * from {table_name};"
        
        logging.info(f"Loading postgres data")
        df = pd.read_sql_query(get_data_query, conn)
        if df.empty:
            raise ValueError("PostgreSQL: Query returned no data.")
        logging.info(f"Loaded {df.shape[0]} records from the database")
        
        conn.close()
        logging.info(f"PostgreSQL connection closed.")
        
        return df
    except Exception as e:
        logging.info("Connection invalid")
        raise CustomException(e, sys)


# get_and_validated postgres data
def get_validated_postgres_data():
    df = fetch_data_from_rds()
    validation_schema = load_schema("config/schema.yml")
    validation_errors = get_dataframe_validation_errors(df, validation_schema)
    if validation_errors:
        for error in validation_errors:
            logging.info(error)
    else:
        logging.info(f"Postgres Data Validated Successfully")
        postgres_data_save_path = "Processed_Data/valid_postgres_data.csv"
        df.to_csv(postgres_data_save_path, index=False)
        logging.info(f"Postgres data extracted and saved to path '{postgres_data_save_path}'")
        return df
        

if __name__=="__main__":
    try:
        get_validated_postgres_data()
    except Exception as e:
        raise CustomException(e, sys)
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


# function to get data from postgresql rds
def get_df_from_rds(rds_host, rds_user, rds_password, rds_db, rds_port):
    conn = None
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
        
        cur = conn.cursor()
        
        table_name = "fuel_consumption_2015_25"
        get_data_query = f"SELECT * from {table_name};"
        
        logging.info(f"Getting all data from the table {table_name} of databse {rds_db}")
        df = pd.read_sql_query(get_data_query, conn)
        logging.info(f"Loaded {df.shape[0]} records from the database")
        
        conn.close()
        
        return df
    except Exception as e:
        logging.info("Connection invalid")
        raise CustomException(e, sys)


if __name__=="__main__":
    rds_host = os.getenv("RDS_HOST")
    rds_user = os.getenv("RDS_USER")
    rds_password = os.getenv("RDS_PASSWORD")
    rds_db = os.getenv("RDS_DB")
    rds_port = os.getenv("RDS_PORT")
    
    df = get_df_from_rds(rds_host, rds_user, rds_password, rds_db, rds_port)
    
    if not df.empty:
        validation_schema = load_schema("config/schema.yml")
        validation_errors = get_dataframe_validation_errors(df, validation_schema)
        if validation_errors:
            for error in validation_errors:
                logging.info(error)
        else:
            logging.info(f"Postgres Data Validated Successfully")
            df.to_csv("validated_postgres_data.csv")
            
    else: 
        print("Empty Dataframe from Postgres Data")
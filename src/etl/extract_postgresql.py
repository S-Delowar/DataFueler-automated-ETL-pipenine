import os
import sys
from dotenv import load_dotenv
import psycopg2
import pandas as pd
# from sqlalchemy import create_engine

from src.utils.common import load_schema
from src.utils.exception import CustomException
from src.utils.logger import logging
from src.utils.validation import get_dataframe_validation_errors

load_dotenv()


# def get_engine():
#     """
#     Create and return a SQLAlchemy engine using credentials from the .env file.
#     """
#     try:
#         rds_host = os.getenv("RDS_HOST")
#         rds_user = os.getenv("RDS_USER")
#         rds_password = os.getenv("RDS_PASSWORD")
#         rds_db = os.getenv("RDS_DB")
#         rds_port = os.getenv("RDS_PORT")

#         if not all([rds_host, rds_user, rds_password, rds_db, rds_port]):
#             logging.info("One or more RDS environment variables are missing.")
#             raise EnvironmentError("Missing RDS connection parameters in .env file.")

#         # Construct the SQLAlchemy connection URL
#         DATABASE_URL = f"postgresql://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{rds_db}"
#         engine = create_engine(DATABASE_URL)
#         logging.info("SQLAlchemy engine created successfully.")
        
#         return engine
#     except Exception as e:
#         raise CustomException(e, sys)



# def get_dataframe_from_rds():
#     """
#     Fetch data from the given table using the provided SQLAlchemy engine and return as a DataFrame.
#     """
#     engine = get_engine()
#     table_name = "fuel_consumption_2015_25"
#     get_data_query = f"SELECT * from {table_name};"
    
#     try:
#         logging.info(f"Executing query to fetch data from table {table_name}.")
        
#         with engine.connect() as conn:
#             df = pd.read_sql_query(sql=get_data_query, con=conn)
        
#         if df.empty:
#             raise ValueError("PostgreSQL: Query returned no data.")
        
#         logging.info(f"Loaded {len(df)} records from table.")
#         return df
#     except Exception as e:
#         logging.error(f"Error fetching data from table {table_name}: {e}")
#         raise CustomException(e, sys)


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
def get_dataframe_from_rds():
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
    df = get_dataframe_from_rds()
    validation_schema = load_schema("config/schema.yml")
    validation_errors = get_dataframe_validation_errors(df, validation_schema)
    if validation_errors:
        for error in validation_errors:
            logging.info(error)
    else:
        logging.info(f"Postgres Data Validated Successfully")
        postgres_data_save_path = "Processed_Data/valid_postgres_data.csv"
        # df.to_csv(postgres_data_save_path, index=False)
        # logging.info(f"Postgres data extracted and saved to path '{postgres_data_save_path}'")
        return df
        

if __name__=="__main__":
    try:
        get_validated_postgres_data()
    except Exception as e:
        raise CustomException(e, sys)
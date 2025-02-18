import os
import sys
from src.etl.extract_mongodb import get_validated_mongodb_data
from src.etl.extract_postgresql import get_validated_postgres_data
from src.etl.load_s3 import upload_dataframe_to_s3
from src.etl.transform import get_final_combined_data
from src.utils.exception import CustomException




def etl_process():
    try:
        mongo_df = get_validated_mongodb_data()
        postgres_df = get_validated_postgres_data()
        final_df = get_final_combined_data(mongo_df, postgres_df)
        
        # get bucket name from environment
        bucket_name = os.getenv("S3_BUCKET_NAME")
        s3_key = "Processed_Data/fuel_consumption_data.csv"
        
        # uploading to s3
        upload_dataframe_to_s3(final_df, bucket_name, s3_key)
        
    except Exception as e:
        raise CustomException(e, sys)





if __name__ == "__main__":
    etl_process()
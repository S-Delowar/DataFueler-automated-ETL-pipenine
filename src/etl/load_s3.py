import io
from operator import index
import os
import sys
import pandas as pd
from dotenv import load_dotenv

from src.utils.exception import CustomException
from src.utils.logger import logging

import boto3
from botocore.exceptions import ClientError


load_dotenv()

# Define client for s3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)



def upload_dataframe_to_s3(df, bucket_name, s3_key):
    """Uploads a pandas dataframe to S3 bucket as a csv file"""
    
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
         
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
        logging.info(f"DataFrame successfully uploaded to s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        raise CustomException(e, sys)
        return False











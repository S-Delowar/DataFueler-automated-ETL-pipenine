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



def upload_file_to_s3(local_filepath, bucket_name, s3_key=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if s3_key is None:
        s3_key = os.path.basename(local_filepath)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(local_filepath, bucket_name, s3_key)
        logging.info(f"File successfully uploaded from '{local_filepath}' to s3 bucket '{bucket_name}'")
    except ClientError as e:
        logging.error(e)
        return False
    return True



def initiate_uploading_final_data_to_s3():
    try:
        # Define final data path
        final_data_path = "Processed_Data/final_combined_data.csv"
        # get bucket name from environment
        bucket_name = os.getenv("S3_BUCKET_NAME")
        # uploading to s3
        upload_file_to_s3(final_data_path, bucket_name)
    except Exception as e:
        raise CustomException(e, sys)




if __name__=="__main__":
    initiate_uploading_final_data_to_s3()

import pytest
import boto3
import pandas as pd
from moto import mock_aws
from src.etl.load_s3 import upload_dataframe_to_s3


@pytest.fixture
def mock_s3_bucket():
    """Creating a mock s3 bucket using moto"""
    with mock_aws():
        s3 = boto3.client("s3", region_name ="us-east-1")
        bucket_name = "test_bucket"
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name
        

def test_upload_dataframe_to_s3(mock_s3_bucket):
    """Test for uploading a pandas dataframe to s3 bucket"""
    data_dict = {
        "name": ["sahed", "delowar"],
        "age": [27, 28]
    }
    
    df = pd.DataFrame(data_dict)
    s3_key = "data/final.csv"
    result = upload_dataframe_to_s3(df, mock_s3_bucket, s3_key)
    
    # Check result returns True or not
    assert result == True
    
    # Verify the object was uploaded correctly
    s3 = boto3.client("s3", region_name="us-east-1")
    response = s3.get_object(Bucket=mock_s3_bucket, Key=s3_key)
    
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    
    # Verify the content of the file
    content = response["Body"].read().decode("utf-8")
    
    assert "name,age" in content
    assert "sahed,27" in content
    assert "delowar,28" in content
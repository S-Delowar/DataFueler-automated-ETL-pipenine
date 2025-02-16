import sys
from src.utils.exception import CustomException
from src.utils.logger import logging

if __name__=="__main__":
    # logging.info("New log is here")
    
    try:
        a = 4/0
        print(a)
    except Exception as e:
        raise CustomException(e,sys)
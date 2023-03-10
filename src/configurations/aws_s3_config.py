import os,sys
from src.exception import DataException
from src.logger import logging
from boto3 import Session
from from_root import from_root
from src.constants.cloud_constants import AWS_ACCESS_KEY_ID,AWS_REGION_NAME,AWS_SECRET_ACCESS_KEY,S3_DATA_BUCKET_NAME
class AwsStorage:
    def __init__(self):
        try:
            logging.info("Reading the Environment Variables From The System and Other Constants")
            self.ACCESS_KEY_ID = os.getenv(AWS_ACCESS_KEY_ID)
            self.SECRET_KEY = os.getenv(AWS_SECRET_ACCESS_KEY)
            self.REGION_NAME = os.getenv(AWS_REGION_NAME)
            self.BUCKET_NAME = S3_DATA_BUCKET_NAME

            logging.info("Environment Variables and Other Constants Loaded Successfully")

        except Exception as e:
            raise DataException(e,sys)
        
    def get_aws_storage_config(self):
        try:
            logging.info("Returning the Environment Variables and Constants as a Dictionary")
            return self.__dict__
        
        except Exception as e:
            raise DataException(e,sys)
    
class StorageConnection:
    """
    Created connection with S3 bucket using boto3 api to fetch the model from Repository.
    """
    def __init__(self):
        try:
            logging.info("Creating Boto3 Session Configurations")
            self.config = AwsStorage()
            self.session = Session(aws_access_key_id=self.config.ACCESS_KEY_ID,
                                   aws_secret_access_key=self.config.SECRET_KEY,
                                   region_name=self.config.REGION_NAME)
            self.s3 = self.session.resource("s3")
            self.bucket = self.s3.Bucket(self.config.BUCKET_NAME)

            logging.info("Created Boto3 Session Configurations")

        except Exception as e:
            raise DataException(e,sys)

    def download_data_from_s3(self, s3_folder_name, local_dir_name):
        """
        Download the contents of a folder directory
        Args:
            bucket_name: the name of the s3 bucket
            s3_folder: the folder path in the s3 bucket
            local_dir: a relative or absolute directory path in the local file system
        """
        try:
            logging.info("Downloading Data From Bucket")
            s3_folder = s3_folder_name
            local_dir = local_dir_name
            bucket = self.bucket
            for obj in bucket.objects.filter(Prefix=s3_folder):
                target = obj.key if local_dir is None \
                    else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
                if not os.path.exists(os.path.dirname(target)):
                    os.makedirs(os.path.dirname(target))
                if obj.key[-1] == '/':
                    continue
                bucket.download_file(obj.key, target)
            logging.info("Downloading Complete")

        except Exception as e:
            raise DataException(e,sys)

from src.constants.cloud_constants import S3_FEATURE_REGISTRY_BUCKET_NAME
from src.constants.file_constants import FEATURE_STORE_FOLDER_NAME,FEATURE_STORE_FILE_PATH
from src.exception import DataException
from src.logger import logging
import os,sys


class S3Sync:
    '''
    Helper Class for Syncing to AWS Using AWS CLI
    '''
    def sync_folder_to_s3(self,folder,aws_buket_url):
        try:
            command = f"aws s3 sync {folder} {aws_buket_url} "
            os.system(command)
        except Exception as e:
            raise DataException(e,sys)
        
    def sync_folder_from_s3(self,folder,aws_bucket_url):
        try:
            command = f"aws s3 sync  {aws_bucket_url} {folder} "
            os.system(command)
        except Exception as e:
            raise DataException(e,sys)

class DataSync:
    '''
    Sync the Feast Feature Store Registry + Files to AWS S3
    '''    
    def __init__(self):
        try:
            logging.info("Getting the connections for Syncing Feature Store")
            self.s3_sync = S3Sync()
            self.feature_store_folder_name = FEATURE_STORE_FOLDER_NAME
            self.feature_registry_folder = FEATURE_STORE_FILE_PATH

            logging.info("Connecting Successful")
        except Exception as e:
            raise DataException(e,sys)
    
    def sync_feature_registries_to_s3(self):
        try:
            logging.info("Syncing the feast feature store registry and files to AWS S3 Bucket")
            aws_bucket_url = f"s3://{S3_FEATURE_REGISTRY_BUCKET_NAME}/{self.feature_store_folder_name}"
            self.s3_sync.sync_folder_to_s3(folder = self.feature_registry_folder,aws_buket_url=aws_bucket_url)

            logging.info("Syncing of Feature Store Successful")

        except Exception as e:
            raise DataException(e,sys)

if __name__ == "__main__":
    #Dummy Main Function To Test The Class
    getdataobj = DataSync()
    #getdataobj.sync_feature_registries_to_s3()
    
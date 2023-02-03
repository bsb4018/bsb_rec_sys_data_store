
from src.constants.cloud_constants import S3_FEATURE_REGISTRY_BUCKET_NAME,S3_ENTITY_DATA_BUCKET_NAME
from src.constants.file_constants import ENTITY_ROOT_DATA_DIR,FEATURE_STORE_FILE_PATH
from src.exception import DataException
from src.logger import logging
import os,sys


class S3Sync:
    def sync_folder_to_s3(self,folder,aws_buket_url):
        command = f"aws s3 sync {folder} {aws_buket_url} "
        os.system(command)
        
    def sync_folder_from_s3(self,folder,aws_bucket_url):
        command = f"aws s3 sync  {aws_bucket_url} {folder} "
        os.system(command)

class DataSync:    
    def __init__(self):
        try:
            self.s3_sync = S3Sync()
            self.feature_registry_folder = FEATURE_STORE_FILE_PATH
        except Exception as e:
            raise DataException(e,sys)
    
    def sync_feature_registries_to_s3(self):
        try:
            logging.info("Entered the sync_feature_registries_to_s3 method of DataSync class")
            aws_bucket_url = f"s3://{S3_FEATURE_REGISTRY_BUCKET_NAME}/"
            self.s3_sync.sync_folder_to_s3(folder = self.feature_registry_folder,aws_buket_url=aws_bucket_url)
            logging.info("Performed Syncing of artifact to S3 bucket")

        except Exception as e:
            raise DataException(e,sys)

if __name__ == "__main__":
    getdataobj = DataSync()
    getdataobj.sync_feature_registries_to_s3()
    
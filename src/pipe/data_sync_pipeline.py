import os, sys
from src.exception import DataException
from src.logger import logging
from src.components.data_storing_main import StoreDataCourse
from src.components.data_sync import DataSync
class DataSyncPipeline:
    '''
    Defines the Data Sync Pipelien for Syncing Feature Store to AWS S3
    '''
    def __init__(self,):
        try:
            self.sync_data = DataSync()
        except Exception as e:
            raise DataException(e,sys)
        
    def run_data_sync_pipeline(self):
        try:
            logging.info("Starting the Feature Store Sync Pipeline")
            self.sync_data.sync_feature_registries_to_s3()

            logging.info("Feature Store Sync Successful")
        except Exception as e:
            raise DataException(e,sys)

if __name__ == "__main__":
    #Call the Pipeline
    data_sync_pipeline  = DataSyncPipeline()
    data_sync_pipeline.run_data_sync_pipeline()
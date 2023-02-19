import os,sys
from src.exception import DataException
from src.logger import logging
from src.components.data_storing_main import StoreDataCourse
class DataStorePipeline:
    '''
    Defines the Data Storing Pipeline
    '''
    def __init__(self,):
        try:
            self.store_data = StoreDataCourse()
        except Exception as e:
            raise DataException(e,sys)
        
    def run_data_pipeline(self):
        try:
            logging.info("Starting the Data Storing and Feature Store Pipeline")
            self.store_data.download_data_from_s3()
            self.store_data.create_and_store_interactions_data_and_features()
            self.store_data.create_and_store_users_data_and_features()
            self.store_data.create_and_store_courses_data_and_features()

            logging.info("Data Download and Features Creation Complete")
        except Exception as e:
            raise DataException(e,sys)
        

if __name__ == "__main__":
    #Call the Pipeline
    data_store_pipeline  =  DataStorePipeline()
    data_store_pipeline.run_data_pipeline()
import sys
from src.exception import DataException
from src.components.data_storing_main import StoreDataCourse
class DataStorePipeline:
    def __init__(self,):
        try:
            self.store_data = StoreDataCourse()
        except Exception as e:
            raise DataException(e,sys)
        
    def run_data_pipeline(self):
        try:
            self.store_data.download_data_from_s3()
            self.store_data.create_and_store_interactions_data_and_features()
            self.store_data.create_and_store_users_data_and_features()
            self.store_data.create_and_store_courses_data_and_features()
        except Exception as e:
            raise DataException(e,sys)
        

if __name__ == "__main__":
    data_store_pipeline  =  DataStorePipeline()
    data_store_pipeline.run_data_pipeline()
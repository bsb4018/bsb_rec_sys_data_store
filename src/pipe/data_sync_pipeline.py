import sys
from src.exception import DataException
from src.components.data_storing_main import StoreDataCourse
from src.components.data_sync import DataSync
class DataSyncPipeline:
    def __init__(self,):
        try:
            self.sync_data = DataSync()
        except Exception as e:
            raise DataException(e,sys)
        
    def run_data_sync_pipeline(self):
        try:
            self.sync_data.sync_feature_registries_to_s3()
        except Exception as e:
            raise DataException(e,sys)

if __name__ == "__main__":
    data_sync_pipeline  = DataSyncPipeline()
    data_sync_pipeline.run_data_sync_pipeline()
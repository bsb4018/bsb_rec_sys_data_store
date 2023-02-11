import os, sys
from src.exception import DataException
import pandas as pd
from src.logger import logging
from src.constants.file_constants import INTERACTIONS_PARQUET_FILEPATH,COURSES_PARQUET_FILEPATH,USERS_PARQUET_FILEPATH,INITIAL_DATA_S3_FOLDER_NAME,INITIAL_DATA_LOCAL_FOLDER_NAME
#from src.constants.cloud_constants import S3_DATA_BUCKET_NAME
from csv import writer
from src.configurations.mongo_config import MongoDBClient
from src.configurations.aws_s3_config import StorageConnection


class StoreDataCourse:
    '''
    We take courses data, users data, user-course interactions data from apis, put it in proper and required format
    (added -> id and timestamp) for storing in our data-warehouse 
    '''
    def __init__(self):
        try:
            self.mongo_client = MongoDBClient()
            self.connection = self.mongo_client.dbcollection
            #self.index_connection = self.mongo_client.index_collection
            self.course_connection = self.mongo_client.course_collection
            self.storage_connection = StorageConnection()
            pass
        except Exception as e:
            raise DataException(e,sys)

    def download_data_from_s3(self):
        try:
            self.storage_connection.download_data_from_s3(INITIAL_DATA_S3_FOLDER_NAME,INITIAL_DATA_LOCAL_FOLDER_NAME)
        except Exception as e:
            raise DataException(e,sys)
    

    def create_and_store_interactions_data_and_features(self):
        try:
            path_to_data = os.path.join(INITIAL_DATA_LOCAL_FOLDER_NAME, "events-data-v1.0.0.csv")
            interaction_data = pd.read_csv(path_to_data)
            interaction_ids = [i for i in range(1,len(interaction_data)+1)]
            interaction_data["interaction_id"] = interaction_ids
            interaction_data['event'] = interaction_data['event'].replace(['viewed'], 1)
            interaction_data['event'] = interaction_data['event'].replace(['wishlisted'], 2)
            interaction_data['event'] = interaction_data['event'].replace(['enrolled'], 3)

            interaction_data["event_timestamp"] = pd.to_datetime(interaction_data["event_timestamp"])
            interaction_data["user_id"] = interaction_data["user_id"].astype('int64')
            interaction_data["event"] = interaction_data["event"].astype('int64')
            interaction_data["course_id"] = interaction_data["course_id"].astype('int64')
            interaction_data["event_timestamp"] = interaction_data["event_timestamp"].astype('datetime64[ns]')
            interaction_data["interaction_id"] = interaction_data["interaction_id"].astype('int64')

            interaction_data.to_parquet(INTERACTIONS_PARQUET_FILEPATH, index=False)
            #interaction_data.to_parquet("C:/Users/shiv1/OneDrive/Desktop/nmn/events-from-s3.parquet", index=False)

            #number_of_records = len(interaction_data)
            #data_index = { "id_name":"latest_interaction_id", "value": number_of_records}
            #self.index_connection.insert_one(data_index)
            
        except Exception as e:
            raise DataException(e,sys)

    def create_and_store_users_data_and_features(self):
        try:
            path_to_data = os.path.join(INITIAL_DATA_LOCAL_FOLDER_NAME, "users-data-v1.0.0.csv")
            user_data = pd.read_csv(path_to_data)
            user_feature_ids = [i for i in range(1,len(user_data)+1)]
            user_data["user_feature_id"] = user_feature_ids
          
            
            user_data["user_id"] = user_data["user"].astype('int64')
            user_data.drop(["user"], axis=1, inplace=True)
            user_data["prev_web_dev"] = user_data["prev_web_dev"].astype('int64')
            user_data["prev_data_sc"] = user_data["prev_data_sc"].astype('int64')
            user_data["prev_data_an"] = user_data["prev_data_an"].astype('int64')
            user_data["prev_game_dev"] = user_data["prev_game_dev"].astype('int64')
            user_data["prev_mob_dev"] = user_data["prev_mob_dev"].astype('int64')
            user_data["prev_program"] = user_data["prev_program"].astype('int64')
            user_data["prev_cloud"] = user_data["prev_cloud"].astype('int64')
            user_data["yrs_of_exp"] = user_data["yrs_of_exp"].astype('int64')
            user_data["no_certifications"] = user_data["no_certifications"].astype('int64')
            user_data["event_timestamp"] = pd.to_datetime(user_data["event_timestamp"])
            user_data["event_timestamp"] = user_data["event_timestamp"].astype('datetime64[ns]')

            user_data.to_parquet(USERS_PARQUET_FILEPATH, index=False)
            #interaction_data.to_parquet("C:/Users/shiv1/OneDrive/Desktop/nmn/events-from-s3.parquet", index=False)

            #number_of_records = len(interaction_data)
            #data_index = { "id_name":"latest_interaction_id", "value": number_of_records}
            #self.index_connection.insert_one(data_index)
            
        except Exception as e:
            raise DataException(e,sys)
          

    def create_and_store_courses_data_and_features(self,):
        
        #Putting the incoming courses data from app through api into our data warehouse
        try:
            logging.info("Into the store_courses_data_tagwise function of StoreData class")
            path_to_data = os.path.join(INITIAL_DATA_LOCAL_FOLDER_NAME, "courses-data-v1.0.0.csv")
            courses_features = pd.read_csv(path_to_data)
            #number_of_records = len(courses_features)
            #data_index = { "id_name":"latest_course_feature_id", "value": number_of_records}
            #self.index_connection.insert_one(data_index)
            
            for index,row in courses_features.iterrows():
                course_tags = row["course_tags"]
                course_tags_list = str(course_tags).split()
                if "WebDevelopment" in course_tags_list:
                    self.connection.insert_one({"category": "web_dev", "course_name": row["course_name"]})

                if "DataScience" in course_tags_list:
                    self.connection.insert_one({"category": "data_sc", "course_name": row["course_name"]})
 
                if "DataAnalysis" in course_tags_list:
                    self.connection.insert_one({"category": "data_an", "course_name": row["course_name"]})
                    
                if "GameDevelopment" in course_tags_list:
                    self.connection.insert_one({"category": "game_dev", "course_name": row["course_name"]})

                if "MobileDevelopment" in course_tags_list:
                    self.connection.insert_one({"category": "mob_dev", "course_name": row["course_name"]})

                if "Programming" in course_tags_list:
                    self.connection.insert_one({"category": "program", "course_name": row["course_name"]})

                if "Cloud" in course_tags_list:
                    self.connection.insert_one({"category": "cloud", "course_name": row["course_name"]})
                
                self.course_connection.insert_one({"course_name": row["course_name"], "course_id": row["course_id"]})
            

            feature_ids = [i for i in range(1,len(courses_features)+1)]

            courses_data = pd.DataFrame()
            courses_data["event_timestamp"] = courses_features["event_timestamp"]
            courses_data["event_timestamp"] = pd.to_datetime(courses_data["event_timestamp"])
            
            courses_data["course_feature_id"] = feature_ids
            #courses_data["course_feature_id"] = courses_data["course_feature_id"].astype('int64')
            
            courses_data["course_id"] = courses_features["course_id"].astype('int64')
            courses_data["course_name"] = courses_features["course_name"]
            courses_data["course_tags"] = courses_features["course_tags"]

            courses_data["event_timestamp"] = courses_data["event_timestamp"].astype('datetime64[ns]')
            courses_data["course_feature_id"] = courses_data["course_feature_id"].astype('int64')
            courses_data["course_id"] = courses_data["course_id"].astype('int64')
            courses_data["course_name"] = courses_data["course_name"].astype('object')
            courses_data["course_tags"] = courses_data["course_tags"].astype('object')


            courses_data.to_parquet(COURSES_PARQUET_FILEPATH, index=False)
            #courses_data.to_parquet("C:/Users/shiv1/OneDrive/Desktop/nmn/courses-from-s3.parquet", index=False)

            
        except Exception as e:
            raise DataException(e,sys)
        

if __name__ == "__main__":
    getdataobj  =  StoreDataCourse()
    getdataobj.download_data_from_s3()
    getdataobj.create_and_store_interactions_data_and_features()
    #getdataobj.create_and_store_courses_data_and_features()
    #getdataobj.create_and_store_users_data_and_features()
   
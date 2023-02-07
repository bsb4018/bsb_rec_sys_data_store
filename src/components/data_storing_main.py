import os, sys
from src.exception import DataException
import pandas as pd
from src.logger import logging
from src.constants.file_constants import INTERACTIONS_PARQUET_FILEPATH,COURSES_PARQUET_FILEPATH,INITIAL_DATA_S3_FOLDER_NAME,INITIAL_DATA_LOCAL_FOLDER_NAME
from src.constants.cloud_constants import S3_DATA_BUCKET_NAME
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
            interaction_data["event_timestamp"] = pd.to_datetime(interaction_data["event_timestamp"])
            interaction_ids = [i for i in range(0,len(interaction_data))]
            interaction_data["interaction_id"] = interaction_ids
            interaction_data['event'] = interaction_data['event'].replace(['viewed'], 1)
            interaction_data['event'] = interaction_data['event'].replace(['wishlisted'], 2)
            interaction_data['event'] = interaction_data['event'].replace(['enrolled'], 3)
            
                 

            interaction_data.to_parquet(INTERACTIONS_PARQUET_FILEPATH, index=False)
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


            tags_list = []
            #course_ids = []
            #feature_ids = []
            #course_id_counter = 0
            for index,row in courses_features.iterrows():
                tags = ""
                if row["web_dev"] == 1:
                    tags += "Web Development "
                    #self.connection.insert_one({"category": "web_dev", "course_name": row["course_name"]})

                if row["data_sc"] == 1:
                    tags += "Data Science "
                    #self.connection.insert_one({"category": "data_sc", "course_name": row["course_name"]})
 
                if row['data_an'] == 1:
                    tags += "Data Analysis "
                    #self.connection.insert_one({"category": "data_an", "course_name": row["course_name"]})
                    
                if row['game_dev'] == 1:
                    tags += "Game Development "
                    #self.connection.insert_one({"category": "game_dev", "course_name": row["course_name"]})

                if row['mob_dev'] == 1:
                    tags += "Mobile Development "
                    #self.connection.insert_one({"category": "mob_dev", "course_name": row["course_name"]})

                if row['program'] == 1:
                    tags += "Programming "
                    #self.connection.insert_one({"category": "program", "course_name": row["course_name"]})

                if row['cloud'] == 1:
                    tags += "Cloud "
                    #self.connection.insert_one({"category": "cloud", "course_name": row["course_name"]})

                tags_list.append(tags)
                #course_ids.append(course_id_counter)
                #feature_ids.append(course_id_counter + 1)
                
                #self.course_connection.insert_one({"course_name": row["course_name"], "course_id": course_id_counter})
                
                #course_id_counter = course_id_counter + 1

            course_ids = [i for i in range(0,len(courses_features))]
            feature_ids = [i for i in range(1,len(courses_features)+1)]

            courses_data = pd.DataFrame()
            courses_data["event_timestamp"] = courses_features["event_timestamp"]
            courses_data["course_feature_id"] = feature_ids
            courses_data["course_id"] = course_ids
            courses_data["course_name"] = courses_features["course_name"]
            courses_data["course_tags"] = tags_list
            courses_data["event_timestamp"] = pd.to_datetime(courses_data["event_timestamp"])

            
            courses_data.to_parquet(COURSES_PARQUET_FILEPATH, index=False)
            #courses_data.to_parquet("C:/Users/shiv1/OneDrive/Desktop/nmn/courses-from-s3.parquet", index=False)

            
        except Exception as e:
            raise DataException(e,sys)
        

if __name__ == "__main__":
    getdataobj  =  StoreDataCourse()
    #getdataobj.download_data_from_s3()
    #getdataobj.create_and_store_interactions_data_and_features()
    #getdataobj.create_and_store_courses_data_and_features()
   
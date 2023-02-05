import os, sys
from src.exception import DataException
import pandas as pd
from src.logger import logging
from src.constants.file_constants import INTERACTIONS_PARQUET_FILEPATH,COURSES_PARQUET_FILEPATH,COURSES_S3_FOLDER,COURSES_S3_FOLDER_LOCAL,INTERACTIONS_S3_FOLDER,INTERACTIONS_S3_FOLDER_LOCAL
from csv import writer
from src.configurations.mongo_config import MongoDBClient
from src.components.data_validation import DataValidation
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
            self.index_connection = self.mongo_client.index_collection
            self.course_connection = self.mongo_client.course_collection
            self.data_validation = DataValidation()
            self.storage_connection = StorageConnection()
            pass
        except Exception as e:
            raise DataException(e,sys)


    def download_interactions_data_from_s3(self):
        try:
            pass
            self.storage_connection.download_data_from_s3(INTERACTIONS_S3_FOLDER,INTERACTIONS_S3_FOLDER_LOCAL)

            courses_features = pd.read_parquet(INTERACTIONS_S3_FOLDER_LOCAL)
            courses_data.to_parquet(INTERACTIONS_PARQUET_FILEPATH, index=False)

            number_of_records = len(courses_features)
            data_index = { "id_name":"latest_interaction_id", "value": number_of_records}
            self.index_connection.insert_one(data_index)

        except Exception as e:
            raise DataException(e,sys)
          

    def create_course_features(self,):
        
        #Putting the incoming courses data from app through api into our data warehouse
        try:
            logging.info("Into the store_courses_data_tagwise function of StoreData class")
            self.storage_connection.download_data_from_s3(COURSES_S3_FOLDER,COURSES_S3_FOLDER_LOCAL)

            courses_features = pd.read_parquet(COURSES_S3_FOLDER_LOCAL)
            number_of_records = len(courses_features)
            data_index = { "id_name":"latest_course_feature_id", "value": number_of_records}
            self.index_connection.insert_one(data_index)


            tags_list = []
            course_ids = []
            feature_ids = []
            course_id_counter = 0
            for index,row in courses_features.iterrows():
                tags = ""
                if row["web_dev"] == 1:
                    tags += "Web Development "
                    self.connection.insert_one({"category": "web_dev", "course_name": row["course_name"]})

                if row["data_sc"] == 1:
                    tags += "Data Science "
                    self.connection.insert_one({"category": "data_sc", "course_name": row["course_name"]})
 
                if row['data_an'] == 1:
                    tags += "Data Analysis "
                    self.connection.insert_one({"category": "data_an", "course_name": row["course_name"]})
                    
                if row['game_dev'] == 1:
                    tags += "Game Development "
                    self.connection.insert_one({"category": "game_dev", "course_name": row["course_name"]})

                if row['mob_dev'] == 1:
                    tags += "Mobile Development "
                    self.connection.insert_one({"category": "mob_dev", "course_name": row["course_name"]})

                if row['program'] == 1:
                    tags += "Programming "
                    self.connection.insert_one({"category": "program", "course_name": row["course_name"]})

                if row['cloud'] == 1:
                    tags += "Cloud "
                    self.connection.insert_one({"category": "cloud", "course_name": row["course_name"]})

                tags_list.append(tags)
                course_ids.append(course_id_counter)
                feature_ids.append(course_id_counter + 1)
                
                self.course_connection.insert_one({"course_name": row["course_name"], "course_id": course_id_counter})
                
                course_id_counter = course_id_counter + 1

            courses_data = pd.DataFrame()
            courses_data["event_timestamp"] = courses_features["timestamp"]
            courses_data["course_feature_id"] = feature_ids
            courses_data["course_id"] = course_ids
            courses_data["course_name"] = courses_features["course_name"]
            courses_data["course_tags"] = tags_list
            courses_data["event_timestamp"] = pd.to_datetime(courses_data["event_timestamp"])

            #current_timestamp = pd.Timestamp.now()
            #current_timestamp = current_timestamp.strftime("%Y%m%d%H%M%S")
            courses_data.to_parquet(COURSES_PARQUET_FILEPATH, index=False)

            
        except Exception as e:
            raise DataException(e,sys)
        

if __name__ == "__main__":
    getdataobj  =  StoreDataCourse()

    courses_data = {
        
    }

    interaction_data = {

    }

    #print(getdataobj.store_courses_data(courses_data))
    #print(getdataobj.store_user_course_interactions(interaction_data))
   
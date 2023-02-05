import os, sys
from src.exception import DataException
import pandas as pd
from src.logger import logging
from src.constants.file_constants import INTERACTIONS_PARQUET_FILEPATH , COURSES_PARQUET_FILEPATH, ENTITY_COURSES_PARQUET_FILEPATH, ENTITY_INTERACTIONS_PARQUET_FILEPATH
from csv import writer
from src.configurations.mongo_config import MongoDBClient
from src.components.data_validation import DataValidation
from src.configurations.aws_s3_config import StorageConnection
class StoreData:
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
            #self.reverse_course_connection = self.mongo_client.reverse_courses_collection_name
            self.data_validation = DataValidation()
            self.storage_connection = StorageConnection()
            pass
        except Exception as e:
            raise DataException(e,sys)
        

    def store_courses_data(self, item_dict: dict):
        
        #Putting the incoming courses data from app through api into our data warehouse
        try:
            logging.info("Into the store_courses_data function of StoreData class")

            #Create current timestamp for the interaction
            logging.info("Getting current UTC Timestamp")
            current_timestamp = pd.Timestamp.now()
            current_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")

        
            data_validation_status = False

            data_validation_status = self.data_validation.check_courses_validation(item_dict) #return true if everything is valid

            if data_validation_status == False:
                return False
                            
            data_validation_status = True

            logging.info("Getting latest course feature id from mongo db")
            latest_course_id = dict(self.index_connection.find({"id_name":"latest_course_feature_id"},{'_id': 0, 'value':1}).next()).get('value')
            new_course_feature_id = latest_course_id + 1
            data_index = { "id_name":"latest_course_feature_id", "value": new_course_feature_id}
            self.index_connection.delete_one({"id_name":"latest_course_feature_id"})
            self.index_connection.insert_one(data_index)

            tags = ""
            if item_dict["web_dev"] == 1:
                tags += "Web Development "
                self.connection.insert_one({"category": "web_dev", "course_name": item_dict["course_name"]})
            if item_dict["data_sc"] == 1:
                tags += "Data Science "
                self.connection.insert_one({"category": "data_sc", "course_name": item_dict["course_name"]})
            if item_dict['data_an'] == 1:
                tags += "Data Analysis "
                self.connection.insert_one({"category": "data_an", "course_name": item_dict["course_name"]})
            if item_dict['game_dev'] == 1:
                tags += "Game Development "
                self.connection.insert_one({"category": "game_dev", "course_name": item_dict["course_name"]})
            if item_dict['mob_dev'] == 1:
                tags += "Mobile Development "
                self.connection.insert_one({"category": "mob_dev", "course_name": item_dict["course_name"]})
            if item_dict['program'] == 1:
                tags += "Programming "
                self.connection.insert_one({"category": "program", "course_name": item_dict["course_name"]})
            if item_dict['cloud'] == 1:
                tags += "Cloud "
                self.connection.insert_one({"category": "cloud", "course_name": item_dict["course_name"]})

            new_df = { 
                "event_timestamp" : [current_timestamp],
                "course_feature_id" : [new_course_feature_id],
                "course_id" : [new_course_feature_id - 1],
                "course_name" : [str(item_dict["course_name"])],
                "course_tags" : [tags],
            }
            new_course_df = pd.DataFrame(new_df)
            new_course_df["event_timestamp"] = pd.to_datetime(new_course_df["event_timestamp"])


            new_df_entity = { 
                "event_timestamp" : current_timestamp,
                "course_feature_id" : [new_course_feature_id],
             }
            new_course_df_entity = pd.DataFrame(new_df_entity)
            new_course_df_entity["event_timestamp"] = pd.to_datetime(new_course_df_entity["event_timestamp"])

            logging.info("Storing new feature data in parquet")
            dfd = pd.read_parquet(COURSES_PARQUET_FILEPATH)
            dfd = dfd.append(new_course_df, ignore_index=True)
            dfd.to_parquet(COURSES_PARQUET_FILEPATH, index=False)


            logging.info("Storing new entity data in parquet")
            dfd = pd.read_parquet(ENTITY_COURSES_PARQUET_FILEPATH)
            dfd = dfd.append(new_course_df_entity, ignore_index=True)
            dfd.to_parquet(ENTITY_COURSES_PARQUET_FILEPATH, index=False)        

            logging.info("Exiting the store_courses_data function of StoreData class")

            return data_validation_status
           
        except Exception as e:
            raise DataException(e,sys)

    def store_user_course_interactions(self,item_dict: dict):
        
        #Putting the incoming user interactions data from app through api into our data warehouse
        
        try:
            logging.info("Into the store_user_course_interactions function of StoreData class")

            #Create current timestamp for the interaction
            logging.info("Getting current UTC Timestamp")
            current_timestamp = pd.Timestamp.now()
            current_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")

            data_validation_status = False

            data_validation_status = self.data_validation.check_interactions_validation(item_dict) #return true if everything is valid

            if data_validation_status == False:
                return False
            
            data_validation_status = True

            #Capture user details
            user_id = item_dict["user_id"]
            course_taken = item_dict["course_taken"]
            event_viewed = item_dict["event_viewed"]
            event_wishlisted = item_dict["event_wishlisted"]
            event_enrolled = item_dict["event_enrolled"]

           
            complete_df = pd.DataFrame()
            complete_df_entity = pd.DataFrame()

            logging.info("Finding the course id")
            #Find Course ID of the course taken
            find_course_id = dict(self.course_connection.find({"course_name":course_taken},{'_id':0, 'course_id':1}).next()).get('course_id')

            if event_viewed:
                logging.info("Getting latest interaction id from mongo db")
                latest_interaction_id = dict(self.index_connection.find({"id_name":"latest_interaction_id"},{'_id': 0, 'value':1}).next()).get('value')
                new_interaction_id = latest_interaction_id + 1
                data_index = { "id_name":"latest_interaction_id", "value": new_interaction_id}
                self.index_connection.delete_one({"id_name":"latest_interaction_id"})
                self.index_connection.insert_one(data_index)    
                
                new_df_viewed = { 
                "user_id" : [user_id],
                "event" : 1,
                "course_id" : [find_course_id],
                "event_timestamp" : [current_timestamp],
                "interaction_id" : [new_interaction_id],
                }
                view_df = pd.DataFrame(new_df_viewed)
                view_df["event_timestamp"] = pd.to_datetime(view_df["event_timestamp"])
                complete_df = pd.concat([complete_df,view_df], axis=0)

                entity_df_viewed = { 
                "event_timestamp" : [current_timestamp],
                "interaction_id" : [new_interaction_id],
                }
                view_df_entity = pd.DataFrame(entity_df_viewed)
                view_df_entity["event_timestamp"] = pd.to_datetime(view_df_entity["event_timestamp"])
                complete_df_entity = pd.concat([complete_df_entity,view_df_entity], axis=0)

            if event_wishlisted:
                logging.info("Getting latest interaction id from mongo db")
                latest_interaction_id = dict(self.index_connection.find({"id_name":"latest_interaction_id"},{'_id': 0, 'value':1}).next()).get('value')
                new_interaction_id = latest_interaction_id + 1
                data_index = { "id_name":"latest_interaction_id", "value": new_interaction_id}
                self.index_connection.delete_one({"id_name":"latest_interaction_id"})
                self.index_connection.insert_one(data_index)    
                
                new_df_wishlisted = { 
                "user_id" : [user_id],
                "event" : 2,
                "course_id" : [find_course_id],
                "event_timestamp" : [current_timestamp],
                "interaction_id" : [new_interaction_id],
                }
                wishlisted_df = pd.DataFrame(new_df_wishlisted)
                wishlisted_df["event_timestamp"] = pd.to_datetime(wishlisted_df["event_timestamp"])
                complete_df = pd.concat([complete_df,wishlisted_df], axis=0)

                entity_df_wishlisted = { 
                "event_timestamp" : [current_timestamp],
                "interaction_id" : [new_interaction_id],
                }
                wish_df_entity = pd.DataFrame(entity_df_wishlisted)
                wish_df_entity["event_timestamp"] = pd.to_datetime(wish_df_entity["event_timestamp"])
                complete_df_entity = pd.concat([complete_df_entity,wish_df_entity], axis=0)
                
            if event_enrolled:
                logging.info("Getting latest interaction id from mongo db")
                latest_interaction_id = dict(self.index_connection.find({"id_name":"latest_interaction_id"},{'_id': 0, 'value':1}).next()).get('value')
                new_interaction_id = latest_interaction_id + 1
                data_index = { "id_name":"latest_interaction_id", "value": new_interaction_id}
                self.index_connection.delete_one({"id_name":"latest_interaction_id"})
                self.index_connection.insert_one(data_index)    
                
                new_df_enrolled = { 
                "user_id" : [user_id],
                "event" : 3,
                "course_id" : [find_course_id],
                "event_timestamp" : [current_timestamp],
                "interaction_id" : [new_interaction_id],
                }
                enrolled_df = pd.DataFrame(new_df_enrolled)
                enrolled_df["event_timestamp"] = pd.to_datetime(enrolled_df["event_timestamp"])
                complete_df = pd.concat([complete_df,enrolled_df], axis=0)

                entity_df_enrolled = { 
                "event_timestamp" : [current_timestamp],
                "interaction_id" : [new_interaction_id],
                }
                enrolled_df_entity = pd.DataFrame(entity_df_enrolled)
                enrolled_df_entity["event_timestamp"] = pd.to_datetime(enrolled_df_entity["event_timestamp"])
                complete_df_entity = pd.concat([complete_df_entity,enrolled_df_entity], axis=0)


            logging.info("Storing new feature data in parquet")
            dfd = pd.read_parquet(INTERACTIONS_PARQUET_FILEPATH)
            dfd = dfd.append(complete_df, ignore_index=True)
            dfd.to_parquet(INTERACTIONS_PARQUET_FILEPATH, index=False)

            logging.info("Storing new entity data in parquet")
            dfd = pd.read_parquet(ENTITY_INTERACTIONS_PARQUET_FILEPATH)
            dfd = dfd.append(complete_df_entity, ignore_index=True)
            dfd.to_parquet(ENTITY_INTERACTIONS_PARQUET_FILEPATH, index=False)

            logging.info("Exiting the store_user_course_interactions function of StoreData class")
            
            return data_validation_status

        except Exception as e:
            raise DataException(e,sys)
        

if __name__ == "__main__":
    getdataobj  =  StoreData()

    courses_data = {
        
    }

    interaction_data = {

    }

    print(getdataobj.store_courses_data(courses_data))
    print(getdataobj.store_user_course_interactions(interaction_data))
   
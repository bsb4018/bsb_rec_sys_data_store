import os, sys
from src.exception.custom_exception import DataException
import pandas as pd
from src.logger.logger import logging
from src.constants.file_constants import INTERACTIONS_CSV_FILEPATH, INTERACTIONS_PARQUET_FILEPATH, COURSES_CSV_FILEPATH 

class StoreData:
    '''
    We take courses data, users data, user-course interactions data from apis, put it in proper and required format
    (added -> id and timestamp) for storing in our data-warehouse 
    '''
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise DataException(e,sys)
           
    def store_user(self, df:pd.DataFrame):
        '''
        Putting the incoming user data from app through api into our data warehouse
        '''
        try:
            pass
        except Exception as e:
            raise DataException(e,sys)

    def store_courses(self, df:pd.DataFrame):
        '''
        Putting the incoming courses data from app through api into our data warehouse
        '''
        try:
            #df.head(5)
            #Create current timestamp for the interaction
            #current_timestamp = pd.Timestamp.utcnow() 

            #Load old courses data and create new id
            #old_courses_data = pd.read_csv("courses_data.csv")
            #lastest_course_id = old_courses_data["interaction_id"].iat[-1]
            #new_course_id = lastest_course_id + 1

            #Insert ID and timestamp Information
            #new_courses_df = df
            #new_courses_df.insert(loc = 0,column = 'course_id',value = new_course_id)
            #new_courses_df.insert(loc = 2,column = 'timestamp',value = current_timestamp)

            #Create new data information and put in in our table        
            #old_courses_data = pd.concat([old_courses_data,new_courses_df], axis=0)
            #old_courses_data.to_parquet(path="D:/work2/course_recommend_app/cr_data_collection/data/old_courses_data.parquet")
            pass
        except Exception as e:
            raise DataException(e,sys)
    

    def store_user_course_interactions(self,item_dict: dict):
        '''
        Putting the incoming user interactions data from app through api into our data warehouse
        '''
        try:
            logging.info("Into the store_user_course_interactions function of StoreData class")

            #Create current timestamp for the interaction
            logging.info("Getting current UTC Timestamp")
            current_timestamp = pd.to_datetime('now',utc=True)

            logging.info("Loading Old File")
            old_interactions_data = pd.read_csv(INTERACTIONS_CSV_FILEPATH)
            lastest_interaction_id =  old_interactions_data["interaction_id"].iat[-1]
            new_interaction_id = lastest_interaction_id + 1
          
            #Capture user details
            user_id = item_dict["user_id"]
            course_taken = item_dict["course_taken"]
            course_progress = item_dict["course_progress"]
            course_like = item_dict["course_like"]
            time_spent = item_dict["time_spent"]
            course_rating = item_dict["rating"]

            logging.info("Finding the course id")
            #Find Course ID of the course taken
            courses_df = pd.read_csv(COURSES_CSV_FILEPATH)
            find_course_id = courses_df[courses_df["course_name"] == course_taken]["course_id"].item()
            
            #Find the weighted rating using the implicit variables
            if time_spent < 3:
                w1 = 0.1
            elif time_spent >=4 and time_spent <=10:
                w1 = 0.3
            elif time_spent >=11 and time_spent<=20:
                w1 = 0.5
            elif time_spent >=21 :
                w1 = 1.0

            if course_progress < 10:
                w2 = 0.1
            elif course_progress >=10 and course_progress<=40:
                w2 = 0.3
            elif course_progress >=41 and course_progress<=80:
                w2 = 0.5
            elif course_progress >=81 and course_progress<=100:
                w2 = 1.0

            if course_like == 0:
                w3 = 0
            elif course_like == 1:
                w3 = 1

            if course_rating == 0:
                w4 = -1.0
            elif course_rating == 1:
                w4 = -0.5
            elif course_rating == 2:
                w4 = 0.3
            elif course_rating == 3:
                w4 = 0.5
            elif course_rating == 4:
                w4 = 0.8
            elif course_rating == 5:
                w4 = 1.0

            logging.info("Calculating the weighted rating")
            weighted_rating = w1 + w2 + w3 + w4 + 1

            weighted_rating = round(weighted_rating, 4)

            new_interaction = {
                "interaction_id": [new_interaction_id],
                "user_id": [user_id],
                "course_id": [find_course_id],
                "rating": [weighted_rating],
                "event_timestamp": [current_timestamp]
            }
            new_interaction_df = pd.DataFrame(new_interaction)   

            logging.info("Creating the new data")
            old_interactions_data = pd.concat([old_interactions_data,new_interaction_df], axis=0)

            logging.info("Saving the new data in csv")
            old_interactions_data.to_csv(INTERACTIONS_CSV_FILEPATH, index=False)

            logging.info("Storing new data in parquet")
            dfd = pd.read_csv(INTERACTIONS_CSV_FILEPATH)
            dfd["event_timestamp"] = pd.to_datetime(dfd["event_timestamp"], utc=True)
            dfd.to_parquet(INTERACTIONS_PARQUET_FILEPATH, index=False)

            logging.info("Exiting the store_user_course_interactions function of StoreData class")

        except Exception as e:
            raise DataException(e,sys)
    
    
    
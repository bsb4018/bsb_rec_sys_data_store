import os, sys
from src.exception import DataException
import pandas as pd
from src.logger import logging
from src.constants.file_constants import INTERACTIONS_CSV_FILEPATH, INTERACTIONS_PARQUET_FILEPATH, COURSES_CSV_FILEPATH,COURSES_PARQUET_FILEPATH
from csv import writer
from src.configurations.mongo_config import MongoDBClient
from src.configurations.redshift_config import RedshiftConnection
class StoreData:
    '''
    We take courses data, users data, user-course interactions data from apis, put it in proper and required format
    (added -> id and timestamp) for storing in our data-warehouse 
    '''
    def __init__(self):
        try:
            self.mongo_client = MongoDBClient()
            self.connection = self.mongo_client.dbcollection
            self.redshift = RedshiftConnection()
            self.redshift_connection = self.redshift.redshift_connection

            #self.data_validation = DataValidation()
            pass
        except Exception as e:
            raise DataException(e,sys)
          

    def store_courses_data_redshift(self, item_dict: dict):
        '''
        Putting the incoming courses data from app through api into our data warehouse
        '''
        
        try:
            logging.info("Into the store_courses_data_redshift function of StoreData class")

            #Create current timestamp for the interaction
            #logging.info("Getting current UTC Timestamp")
            current_timestamp = pd.to_datetime('now', utc=True)
            current_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            current_timestamp = pd.date_range(start=current_timestamp,periods=1, freq='S')
            current_timestamp = current_timestamp.tz_localize(tz="Asia/Calcutta")
                            
            data_validation_status = True

            #logging.info("Loading Old File")
             
            #Create / borrow redshift cursor
            self.redshift_connect.set_session(autocommit=True)
            redshift_cursor = self.redshift_connection.cursor()

            #Query the courses database from redshift to get the latest course id
            redshift_cursor.execute("""select max(course_feature_id) from courses""")
            latest_course_feature_id = redshift_cursor.fetchone()[0]

            #Create the new course id
            new_course_feature_id = latest_course_feature_id + 1

            #Forming the data row
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

            self.mongo_client.close()

            #Put the new row into redshift
            redshift_cursor.execute("""INSERT INTO courses (event_timestamp, course_feature_id, course_id,course_name,course_tags)
            VALUES (%s, %s, %s, %s, %s);""",(current_timestamp[0], new_course_feature_id, new_course_feature_id, item_dict["course_name"], tags))
            
            logging.info("Exiting the store_courses_data_redshift function of StoreData class")
            return data_validation_status
           
        except Exception as e:
            raise DataException(e,sys)

    def store_interactions_data_redshift(self, item_dict: dict):
        try:
            logging.info("Enter the store_interactions_data_redshift of DataStore class")
            #Create current timestamp for the interaction
            logging.info("Getting current UTC Timestamp")
            current_timestamp = pd.to_datetime('now', utc=True)
            current_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            current_timestamp = pd.date_range(start=current_timestamp,periods=1, freq='S')
            current_timestamp = current_timestamp.tz_localize(tz="Asia/Calcutta")
                            
            data_validation_status = True

            #Create / borrow redshift cursor
            self.redshift_connect.set_session(autocommit=True)
            redshift_cursor = self.redshift_connection.cursor()

            #Query the courses database from redshift to get the latest course id
            redshift_cursor.execute("""select max(interaction_id) from courses""")
            latest_interaction_id = redshift_cursor.fetchone()[0]

            #Create the new course id
            new_interaction_id = latest_interaction_id + 1

            #Capture user details
            user_id = item_dict["user_id"]
            course_taken = item_dict["course_taken"]
            course_progress = item_dict["course_progress"]
            course_like = item_dict["course_like"]
            time_spent = item_dict["time_spent"]
            course_rating = item_dict["rating"]

            logging.info("Finding the course id")
            #Find Course ID of the course taken
            redshift_cursor.execute("""select course_id from courses where course_name = (%s);""",(course_taken))
            find_courese_id = redshift_cursor.fetchone()[0]


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

            #Put the new row into redshift
            redshift_cursor.execute("""INSERT INTO interactions (interaction_id, user_id, course_id, rating, event_timestamp)
            VALUES (%s, %s, %s, %s, %s);""",(new_interaction_id, user_id, find_courese_id, weighted_rating,current_timestamp[0]))
            
            logging.info("Exiting the store_interactions_data_redshift function of StoreData class")
            return data_validation_status

        except Exception as e:
            raise DataException(e,sys)


        
    
    '''
    def store_courses_data(self, item_dict: dict):
        
        #Putting the incoming courses data from app through api into our data warehouse
        
        
        try:
            logging.info("Into the store_courses_data function of StoreData class")

            #Create current timestamp for the interaction
            logging.info("Getting current UTC Timestamp")
            current_timestamp = pd.to_datetime('now',utc=True)

            #data_validation_status = False

            #data_validation_status = self.data_validation.check_courses_validation(item_dict) #return true if everything is valid

            #if data_validation_status == False:
            #    return False
                            
            data_validation_status = True

            logging.info("Loading Old File")
            count_rows=len(open(COURSES_CSV_FILEPATH).readlines()) 
            old_courses_data = pd.read_csv(COURSES_CSV_FILEPATH, skiprows=range(1,count_rows-5), header=0)
            lastest_course_id =  old_courses_data["course_id"].iat[-1]
            new_course_id = lastest_course_id + 1

            lastest_course_feature_id =  old_courses_data["course_feature_id"].iat[-1]
            new_course_feature_id = lastest_course_feature_id + 1

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

            self.mongo_client.close()

            new_course = [new_course_feature_id,new_course_id, str(item_dict["course_name"]),tags,current_timestamp]
            
            with open(COURSES_CSV_FILEPATH, 'a') as f_object:
                writer_object = writer(f_object)
                writer_object.writerow(new_course)
                f_object.close()

            logging.info("Storing new data in parquet")
            dfd = pd.read_csv(COURSES_CSV_FILEPATH)
            dfd["timestamp"] = pd.to_datetime(dfd["timestamp"], utc=True)
            dfd.to_parquet(COURSES_PARQUET_FILEPATH, index=False)

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
            current_timestamp = pd.to_datetime('now',utc=True)

            #data_validation_status = False

            #data_validation_status = self.data_validation.check_interactions_validation(item_dict) #return true if everything is valid

            #if data_validation_status == False:
            #    return False
            
            data_validation_status = True

            logging.info("Loading Old File")
            count_rows=len(open(INTERACTIONS_CSV_FILEPATH).readlines()) 
            old_interactions_data = pd.read_csv(INTERACTIONS_CSV_FILEPATH, skiprows=range(1,count_rows-5), header=0)
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

            new_interaction = [new_interaction_id,user_id,find_course_id,weighted_rating,current_timestamp]

            with open(INTERACTIONS_CSV_FILEPATH, 'a') as f_object:
                writer_object = writer(f_object)
                writer_object.writerow(new_interaction)
                f_object.close()

            logging.info("Storing new data in parquet")
            dfd = pd.read_csv(INTERACTIONS_CSV_FILEPATH)
            dfd["event_timestamp"] = pd.to_datetime(dfd["event_timestamp"], utc=True)
            dfd.to_parquet(INTERACTIONS_PARQUET_FILEPATH, index=False)

            logging.info("Exiting the store_user_course_interactions function of StoreData class")
            
            return data_validation_status

        except Exception as e:
            raise DataException(e,sys)
    '''
    
    #def store_user(self, item_dict: dict):
        
        #Putting the incoming user data from app through api into our data warehouse
        
        #save username, genrate user id by checking database, create interest_tags based on entered data, 
        # create courses taken tags from interactions data** 
    #    try:
    #        pass
    #    except Exception as e:
    #        raise DataException(e,sys)
    
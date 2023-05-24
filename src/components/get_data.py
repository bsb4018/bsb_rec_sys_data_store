from feast import FeatureStore
import pandas as pd
from src.exception import DataException
from src.logger import logging
import os, sys
import json
from typing import List

class GetData:
    def __init__(self):
        try:
            self.store = FeatureStore(repo_path="feature_repo")

        except Exception as e:
            raise DataException(e,sys)
   
    
    def get_user_interactions_all_data(self):
        #logging.info("Into the get_user_interactions_data function of GetData class")

        interaction_entity_sql = f"""
                SELECT event_timestamp,interaction_id
                FROM {self.store.get_data_source("rs_source_interactions").get_table_query_string()}
                WHERE event_timestamp BETWEEN '2019-01-01' and '2024-01-31'
            """

        #logging.info("Getting Features from Feast")
        interaction_data = self.store.get_historical_features(entity_df = interaction_entity_sql, features = \
            ["interaction_features:user_id",
            "interaction_features:course_id",\
                "interaction_features:event"]).to_df()
        
        #logging.info("Forming the response")
        response_data = interaction_data[["user_id", "course_id", "event"]]

        return response_data

    def get_users_all_data(self):
        #logging.info("Into the get_user_interactions_data function of GetData class")

        user_entity_sql = f"""
                SELECT user_feature_id,event_timestamp
                FROM {self.store.get_data_source("rs_source_users").get_table_query_string()} 
                WHERE event_timestamp BETWEEN '2019-01-01' and '2024-01-31'
            """
       
        #logging.info("Getting Features from Feast")
        users_data = self.store.get_historical_features(entity_df = user_entity_sql, \
            features = ["user_features:prev_web_dev","user_features:prev_data_sc","user_features:prev_data_an",\
                        "user_features:prev_game_dev","user_features:prev_mob_dev","user_features:prev_program",\
                            "user_features:prev_cloud","user_features:yrs_of_exp","user_features:no_certifications",\
                                "user_features:user_id"]).to_df()
        
        #logging.info("Forming the response")
        response_data = users_data[["user_id","prev_web_dev","prev_data_sc","prev_data_an",\
                        "prev_game_dev","prev_mob_dev","prev_program",\
                            "prev_cloud","yrs_of_exp","no_certifications",]]
        return response_data

if __name__ == "__main__":
    getdataobj  =  GetData()
    #print(getdataobj.get_user_interactions_all_data())
    print(getdataobj.get_users_all_data())



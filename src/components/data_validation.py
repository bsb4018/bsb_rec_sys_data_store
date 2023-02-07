
''''
import os, sys
from src.exception import DataException
import pandas as pd
from src.logger import logging
from csv import writer


class DataValidation:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise DataException(e,sys)

    def check_courses_validation(self, item_dict: dict):
        try:

            if item_dict["web_dev"] != 0 and item_dict["web_dev"] != 1:
                return False
            if item_dict["data_sc"] != 0 and item_dict["data_sc"] != 1:
                return False
            if item_dict["data_an"] != 0 and item_dict["data_an"] != 1:
                return False
            if item_dict["game_dev"] != 0 and item_dict["game_dev"] != 1:
                return False
            if item_dict["mob_dev"] != 0 and item_dict["mob_dev"] != 1:
                return False
            if item_dict["program"] != 0 and item_dict["program"] != 1:
                return False
            if item_dict["cloud"] != 0 and item_dict["cloud"] != 1:
                return False
    
            return True
        except Exception as e:
            raise DataException(e,sys)

    def check_interactions_validation(self, item_dict: dict):
        try:

            if item_dict["event_viewed"] != 0 and item_dict["event_viewed"] != 1:
                return False
            if item_dict["event_wishlisted"] != 0 and item_dict["event_wishlisted"] != 1:
                return False
            if item_dict["event_enrolled"] != 0 and item_dict["event_enrolled"] != 1:
                return False
            
            #if item_dict["course_taken"] is in list_of_courses_in_our_db
            #if item_dict["user_id"] is in list_of_users_in_our_db
            
            return True
        except Exception as e:
            raise DataException(e,sys)
'''
import os, sys
from src.exception import DataException
import pandas as pd
from src.logger import logging
from src.constants.file_constants import INTERACTIONS_CSV_FILEPATH, INTERACTIONS_PARQUET_FILEPATH, COURSES_CSV_FILEPATH,COURSES_PARQUET_FILEPATH
from src.components.data_validation import DataValidation
from csv import writer


class DataValidation:
    def __init__(self):
        try:
            self.data_validation = DataValidation()
        except Exception as e:
            raise DataException(e,sys)

    def check_courses_validation(self, item_dict: dict):
        try:
            pass
        except Exception as e:
            raise DataException(e,sys)

    def check_interactions_validation(self, item_dict: dict):
        try:
            pass
        except Exception as e:
            raise DataException(e,sys)

    
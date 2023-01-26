#Use psycopg2 library to create a connection to redshift and return the connection / cursor
import os,sys
from src.exception import DataException
import pandas as pd
from src.constants.cloud_constants import REDSHIFT_HOST,REDSHIFT_DATABASE_NAME,REDSHIFT_USER,REDSHIFT_PASSWORD
import psycopg2

class RedshiftConnection():
    def __init__(self):
        try:
            self.redshift_connection = psycopg2.connect(
                host=os.getenv(REDSHIFT_HOST),
                dbname=os.getenv(REDSHIFT_DATABASE_NAME),
                user=os.getenv(REDSHIFT_USER),
                password=os.getenv(REDSHIFT_PASSWORD),
                port='5439'
            )
        except Exception as e:
            raise DataException(e,sys)
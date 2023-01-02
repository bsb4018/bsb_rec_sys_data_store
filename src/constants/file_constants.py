import os
ROOT_DATA_DIR = "data"


INTERACTIONS_CSV_FILE_NAME = "user_course_interactions.csv"
INTERACTIONS_PARQUET_FILE_NAME = "old_interactions_data.parquet"
COURSES_CSV_FILE_NAME = "tagged_courses_data.csv"
COURSES_PARQUET_FILE_NAME = "tagged_courses_data.parquet"


INTERACTIONS_CSV_FILEPATH = os.path.join(ROOT_DATA_DIR,INTERACTIONS_CSV_FILE_NAME)
INTERACTIONS_PARQUET_FILEPATH = os.path.join(ROOT_DATA_DIR,INTERACTIONS_PARQUET_FILE_NAME)
COURSES_CSV_FILEPATH = os.path.join(ROOT_DATA_DIR,COURSES_CSV_FILE_NAME)
COURSES_PARQUET_FILEPATH = os.path.join(ROOT_DATA_DIR,COURSES_PARQUET_FILE_NAME)
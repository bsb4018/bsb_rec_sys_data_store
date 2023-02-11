import os
FEATURE_ROOT_DATA_DIR = "data-feature"
INTERACTIONS_PARQUET_FILE_NAME = "events_all.parquet"
COURSES_PARQUET_FILE_NAME = "courses_data.parquet"
USERS_PARQUET_FILE_NAME = "users_data.parquet"
INTERACTIONS_PARQUET_FILEPATH = os.path.join(FEATURE_ROOT_DATA_DIR,INTERACTIONS_PARQUET_FILE_NAME)
COURSES_PARQUET_FILEPATH = os.path.join(FEATURE_ROOT_DATA_DIR,COURSES_PARQUET_FILE_NAME)
USERS_PARQUET_FILEPATH = os.path.join(FEATURE_ROOT_DATA_DIR,USERS_PARQUET_FILE_NAME)


ENTITY_ROOT_DATA_DIR = "data-entity"
EINTERACTIONS_PARQUET_FILE_NAME = "data-interactions-entity.parquet"
ECOURSES_PARQUET_FILE_NAME = "data-courses-entity.parquet"

ENTITY_COURSES_PARQUET_FILEPATH = os.path.join(ENTITY_ROOT_DATA_DIR,ECOURSES_PARQUET_FILE_NAME)
ENTITY_INTERACTIONS_PARQUET_FILEPATH = os.path.join(ENTITY_ROOT_DATA_DIR,EINTERACTIONS_PARQUET_FILE_NAME)


FEATURE_STORE_FOLDER_NAME = "feature_repo"
FEATURE_STORE_FILE_PATH = os.path.join(FEATURE_STORE_FOLDER_NAME)


INITIAL_DATA_S3_FOLDER_NAME = "data-v1.0.0"
INITIAL_DATA_LOCAL_FOLDER_NAME = os.path.join("data-download")


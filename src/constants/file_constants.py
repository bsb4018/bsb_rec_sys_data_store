import os

#Constants for location and file names for downloading and storing the downloaded data
INITIAL_DATA_S3_FOLDER_NAME = "data-v1.0.0"
INITIAL_DATA_LOCAL_FOLDER_NAME = os.path.join("data-download")


#Constants for location and file names for storing the created features from the downloaded data
FEATURE_ROOT_DATA_DIR = "data-feature"
INTERACTIONS_PARQUET_FILE_NAME = "events_all.parquet"
COURSES_PARQUET_FILE_NAME = "courses_data.parquet"
USERS_PARQUET_FILE_NAME = "users_data.parquet"
INTERACTIONS_PARQUET_FILEPATH = os.path.join(FEATURE_ROOT_DATA_DIR,INTERACTIONS_PARQUET_FILE_NAME)
COURSES_PARQUET_FILEPATH = os.path.join(FEATURE_ROOT_DATA_DIR,COURSES_PARQUET_FILE_NAME)
USERS_PARQUET_FILEPATH = os.path.join(FEATURE_ROOT_DATA_DIR,USERS_PARQUET_FILE_NAME)

#Constants for location and file names for creating the feature store in feast
FEATURE_STORE_FOLDER_NAME = "feature_repo"
FEATURE_STORE_FILE_PATH = os.path.join(FEATURE_STORE_FOLDER_NAME)




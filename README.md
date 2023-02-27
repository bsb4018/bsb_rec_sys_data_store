# Course Recommender System - Data and Feature Store

### Problem Statement
Creating a Feature Store For Course Recommender System using Users, Courses, User-Course Interactions Data 

### Solution Proposed 
The solution creates a feature store using feast utilizing AWS Infrastructure

## Tech Stack Used
1. Python 
2. Feast Feature Store
3. AWS
4. Docker
5. MongoDB

## Infrastructure Required.

1. AWS S3
2. Terraform


## How to run?
Before we run the project, make sure that you are having MongoDB in your local system, with Compass since we are using MongoDB for some data storage. You also need AWS account to access S3, Redshift, Glue, DynamoDB Services. You also need to have terraform installed and configured


## Project Architecture
![image](https://github.com/bsb4018/bsb_rec_sys_data_store/blob/main/images/dcv22.drawio.png)


### Step 1: Clone the repository
```bash
git clone https://github.com/bsb4018/bsb_rec_sys_data_store.git
```

### Step 2- Create a conda environment after opening the repository

```bash
conda create -p venv python=3.8 -y
```

```bash
conda activate venv/
```

### Step 3 - Install the requirements
```bash
pip install -r requirements.txt
```

### Step 4 - Create AWS Account and do the following get the following ids
```bash
Create three S3 bucket with unique names 
Replace the names accordingly in src/constants/cloud_constants.py 

Create another S3 bucket with with name bsb-4018-rec-sys-app-proj-<any-unique-key>
Goto infra/main.tf and replace the name under "aws_s3_bucket_acl" resource
Create a database name dev under AWS GLUE CATALOG
Get a note of the following
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION_NAME
```

### Step 5 - Create Mongo DB Atlas Cluster and Create the following 
```bash
Database with name "recsysdb"
collection with name "courses_tagwise"         
collection with name "course_name_id"
Get the MONGODB_URL
```

### Step 6 - Export the environment variable(LINUX) or Put in System Environments(WINDOWS)
```bash
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>

export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>

export AWS_REGION_NAME=<AWS_REGION_NAME>

export MONGODB_URL="mongodb+srv://<username>:<password>@cluster.3gdw4s.mongodb.net/?retryWrites=true&w=majority"
```


### Step 7 - Start locally
```bash
/bin/bash -c ./start.sh
```

### Step 8 - Stop locally
```bash
/bin/bash -c ./stop.sh
```

## Runing Through Docker

1. Check if the Dockerfile is available in the project directory

2. Build the Docker image
```
docker build --build-arg AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID> --build-arg AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY> --build-arg AWS_REGION_NAME=<AWS_REGION_NAME> --build-arg MONGODB_URL=<MONGODB_URL> . 

```

3. Run the Docker image
```
docker run -d -p 8090:8090 <IMAGE_NAME>
```

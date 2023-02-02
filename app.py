from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
from typing import List, Union, Any
import uvicorn
from fastapi import FastAPI, File, UploadFile
from starlette.responses import RedirectResponse
from uvicorn import run as app_run
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import json
from src.components.data_storing import StoreData
from pydantic import BaseModel
from src.constants.app_constants import APP_HOST,APP_PORT

app = FastAPI(title="RecommenderAppDataCollection-Server")
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["authentication"])
async def index():
    return RedirectResponse(url="/docs")

class Interaction_Item(BaseModel):
    user_id: int
    course_taken: str
    event_viewed: int
    event_wishlisted: int
    event_enrolled: int

class Course_Item(BaseModel):
    course_name: str
    web_dev: int
    data_sc: int
    data_an: int
    game_dev: int
    mob_dev: int
    program: int
    cloud: int

# User Details Post Api
@app.post("/add_user_details")
def add_user_details(item: Interaction_Item):
    try:
        
        item_dict = item.dict()
        add_user_details = StoreData()
        status = add_user_details.store_user_course_interactions(item_dict)
        if status == True:
            return {"User_Course Interaction Added Successfully"}
        else:
            return {"Invalid Data Entered"}
        
    except Exception as e:
        raise Response(f"Error Occured! {e}")


# User Details Post Api
@app.post("/add_course_details")
def add_course_details(item: Course_Item):
    try:
        
        item_dict = item.dict()
        add_user_details = StoreData()
        status = add_user_details.store_courses_data(item_dict)
        if status == True:
            return {"Course Data Added Successfully"}
        else:
            return {"Invalid Data Entered"}
        
    except Exception as e:
        raise Response(f"Error Occured! {e}")


if __name__ == "__main__":
    uvicorn.run(app, host=APP_HOST, port=APP_PORT)
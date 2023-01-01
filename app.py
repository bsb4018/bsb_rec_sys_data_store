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
from src.components.store_data import StoreData
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
    course_progress: int
    course_like: int
    time_spent: int
    rating: int

# User Details Post Api
@app.post("/add_user_details")
def add_user_details(item: Interaction_Item):
    try:
        
        item_dict = item.dict()
        add_user_details = StoreData()
        add_user_details.store_user_course_interactions(item_dict)
        return {"User_Course Interaction Added Successfully"}
        
    except Exception as e:
        raise Response(f"Error Occured! {e}")



if __name__ == "__main__":
    uvicorn.run(app, host=APP_HOST, port=APP_PORT)
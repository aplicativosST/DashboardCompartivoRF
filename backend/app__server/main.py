''' 
Backend server for the application. The server runs using FastAPI, 
and provides endpoints for the frontend to interact with the functions stored in helper.py functions
'''

#%% Library imports
import fastapi
import json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pydantic
from pydantic import BaseModel,Field
import typing 
from typing import List, Dict, Any, Optional
import datetime
from datetime import datetime
import numpy as np
from backend.helpers.helper import extract_sheet_names,extract_data,spot_monthly_data_extraction,variable_monthly_data_extraction,spot_yearly_data_extraction,variable_yearly_data_extraction
import uuid
import time

# In-memory session storage
DATA_STORE: Dict[str, Dict] = {}
# Session expiration (seconds)
SESSION_TTL = 3600  # 1 hour

def cleanup_sessions():
    """Remove expired sessions from memory"""
    now = time.time()

    expired_sessions = [
        session_id
        for session_id, data in DATA_STORE.items()
        if now - data["timestamp"] > SESSION_TTL
    ]

    for session_id in expired_sessions:
        del DATA_STORE[session_id]


#%%Starting the app
app=FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # later it can be restricted
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# %%Setting up base clase models for the API

#************** Payloads structure from frontend to backend **************

class ScenarioItem(BaseModel):
    ''' 
        Payload structure for the RF scenario item. Contains the scenario_name, rout, and flag of wheter it is the case to compare
    '''
    route:str
    compare_case:bool=False

class RfScenariosPayload(BaseModel):
    ''' 
        Payload, contains a list of RF scenario items. 
        This is the payload that will be consumed by the endpoint and sent to the helper functions to extract the sheet names to compare
    '''
    scenarios:Dict[str,ScenarioItem]

class ExtractSheetDataPayload(BaseModel):
    ''' 
        Payload structure to extract data from excel files, contains a list of RF scenario items and the sheet name to extract. 
        This is the payload that will be consumed by the endpoint and sent to the helper functions to extract the data of the given sheet name for each scenario
    '''
    scenarios:Dict[str,ScenarioItem]
    sheet_names:List[str]


class ExtractVariableDataPayload(BaseModel):
    '''
        Payload structure to extract data for a given variable from the extacted data of the json information.
        This is the payload that will be consumed by the endpoint and set to the helper functions to extract the data of the given variable.
    '''
    results:Dict[str,Any]
    scenario:str
    var_name:str
    plant:str

class ExtractSpotDataPayLoad(BaseModel):
    '''  
        Payload to extract the monthly spot price data from the extracted data of the json information
    '''
    results:Dict[str,Any]
    scenario:str

#************** Payloads structure from backend to frontend **************

class SpotResultItem(BaseModel):
    ''' 
        Payload structure for the spot results of a single scenario. 
        Contains the date and the value of the spot price.
    '''
    date:List[datetime]
    spot:List[float]

class SpotResultPayload(BaseModel):
    '''
        Payload structure concatenating spot result items
    '''
    result:Dict[str,SpotResultItem]

class VarResultItem(BaseModel):
    '''
        Payload Structure for one item of a given variable of Gx, Volume, Inflows . 
        Contains the date and the value of the variable per plant for a given scenario
    '''
    date:List[datetime]
    value:List[float]

class VarResultPayload(BaseModel):
    '''
        Payload consumed by the frontend, contains a dictionary of variable results for each scnenario
    '''
    result:Dict[str,VarResultItem]

#%% Endpoint 
@app.get("/")
def root():
    '''
    Root endpoint to test the server is running. Returns a simple message.
    '''
    return{"message":"Server is alive!"}

@app.post("/get_sheet_names") #Checked!
def get_sheet_names(payload:RfScenariosPayload): 
    ''' 
        Endpoint to get the sheet names valid for comparison from the provided RF scenarios. 
        The endpoint receives a payload of the scenarios routes and flags, and returns a list of the valid sheets for comparison.
    '''
    payload_dict=payload.model_dump()
    
   
    sheet_names_input=payload_dict["scenarios"]
    print(sheet_names_input)
    sheet_names=extract_sheet_names(sheet_names_input)


    return {"sheet_names":sheet_names}

@app.post("/extract_data_excel")
def extract_sheet_data(payload: ExtractSheetDataPayload):

    payload_dict = payload.model_dump()
    rf_scenarios_input = payload_dict["scenarios"]
    sheet_names_input = payload_dict["sheet_names"]

    monthly_results, yearly_results = extract_data(
        rf_scenarios_input,
        sheet_names_input
    )

    # Clean expired sessions
    cleanup_sessions()

    # Create session
    session_id = str(uuid.uuid4())

    DATA_STORE[session_id] = {
        "monthly": monthly_results,
        "yearly": yearly_results,
        "timestamp": time.time()
    }

    return {"session_id": session_id}

#****************************** Monthly data extraction endpoints ******************************
@app.post("/extract_monthly_spot_data",response_model=SpotResultPayload) 
def extract_monthly_spot_data(payload:ExtractSpotDataPayLoad): #Checked!
    ''' 
        Endpoint to extract the monthly spot price data from the json information
    '''
    payload_dict=payload.model_dump()
    monthly_results_input=payload_dict["results"]
    scenario_input=payload_dict["scenario"]

    if not scenario_input:
        raise HTTPException(status_code=400,detail="No Scenario Selected")

    spot_monthly_data=spot_monthly_data_extraction(monthly_results_input,scenario_input)
    return{"result":spot_monthly_data}

@app.post("/extract_monthly_var_data",response_model=VarResultPayload) 
def extract_monthly_var_data(payload:ExtractVariableDataPayload): #Checked!
    ''' 
        Endpoint to extract the monthly data from a variable in the json information
    '''
    payload_dict=payload.model_dump()
    monthly_results_input=payload_dict["results"]
    scenario_input=payload_dict["scenario"]

    if not scenario_input:
        raise HTTPException(400,detail="Scenario not found")

    var_name_input=payload_dict["var_name"]
    plant_input=payload_dict["plant"]
    
    var_monthly_data=variable_monthly_data_extraction(monthly_results_input,scenario_input,var_name_input,plant_input)
    return{"result":var_monthly_data}


#*********************************** Yearly data endpoints ***********************************

@app.post("/extract_yearly_spot_data",response_model=SpotResultPayload) 
def extract_yearly_spot_data(payload:ExtractSpotDataPayLoad):
    ''' 
        Endpoint to extract the yearly spot price data from the json information
    '''
    payload_dict=payload.model_dump()
    yearly_results_input=payload_dict["results"]
    scenario_input=payload_dict["scenario"]

    if not scenario_input:
        raise HTTPException(400,detail="Scenario not found")


    print("Scenario received:", scenario_input)

    spot_yearly_data=spot_yearly_data_extraction(yearly_results_input,scenario_input)
    return{"result":spot_yearly_data}

@app.post("/extract_yearly_var_data",response_model=VarResultPayload) 
def extract_yearly_var_data(payload:ExtractVariableDataPayload): 
    ''' 
        Endpoint to extract the yearly data from a variable in the json information
    '''
    payload_dict=payload.model_dump()
    yearly_results_input=payload_dict["results"]
    scenario_input=payload_dict["scenario"]

    if not scenario_input:
        raise HTTPException(400,detail="Scenario not found")

    var_name_input=payload_dict["var_name"]
    plant_input=payload_dict["plant"]
    
    var_yearly_data=variable_yearly_data_extraction(yearly_results_input,scenario_input,var_name_input,plant_input)
    return{"result":var_yearly_data}
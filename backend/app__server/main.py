import os
import uuid
import time
import tempfile
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime

from helpers.helper import (
    extract_sheet_names,
    extract_data,
    spot_monthly_data_extraction,
    variable_monthly_data_extraction,
    spot_yearly_data_extraction,
    variable_yearly_data_extraction,
)

DATA_STORE: Dict[str, Dict[str, Any]] = {}
SESSION_TTL = 3600


def cleanup_sessions():
    now = time.time()
    expired_sessions = [
        session_id
        for session_id, data in DATA_STORE.items()
        if now - data["timestamp"] > SESSION_TTL
    ]

    for session_id in expired_sessions:
        session = DATA_STORE[session_id]
        temp_files = session.get("temp_files", [])
        for path in temp_files:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
        del DATA_STORE[session_id]


app = FastAPI(
    root_path="/api",
    docs_url="/docs",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SessionPayload(BaseModel):
    session_id: str


class ExtractSheetDataPayload(BaseModel):
    session_id: str
    sheet_names: List[str]


class ExtractVariableDataPayload(BaseModel):
    session_id: str
    scenario: str
    var_name: str
    plant: str


class ExtractSpotDataPayload(BaseModel):
    session_id: str
    scenario: str


class SpotResultItem(BaseModel):
    date: List[datetime]
    spot: List[float]


class SpotResultPayload(BaseModel):
    result: Dict[str, SpotResultItem]


class VarResultItem(BaseModel):
    date: List[datetime]
    value: List[float]


class VarResultPayload(BaseModel):
    result: Dict[str, VarResultItem]


@app.get("/")
def root():
    return {"message": "Server is alive!"}


@app.post("/upload_rf_files")
async def upload_rf_files(
    compare_case: str = Form(...),
    case_a: Optional[UploadFile] = File(None),
    case_b: Optional[UploadFile] = File(None),
    case_c: Optional[UploadFile] = File(None),
):
    cleanup_sessions()

    uploaded = {
        "case_a": case_a,
        "case_b": case_b,
        "case_c": case_c,
    }

    if compare_case not in uploaded:
        raise HTTPException(status_code=400, detail="Invalid compare_case")

    provided_files = {k: v for k, v in uploaded.items() if v is not None}
    if not provided_files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    if compare_case not in provided_files:
        raise HTTPException(status_code=400, detail="Compare case must have a file")

    scenario_dict = {}
    temp_files = []

    for scenario_name, file_obj in provided_files.items():
        suffix = os.path.splitext(file_obj.filename)[1] or ".xlsx"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            content = await file_obj.read()
            tmp.write(content)
            temp_path = tmp.name

        temp_files.append(temp_path)

        scenario_dict[scenario_name] = {
            "route": temp_path,
            "compare_case": scenario_name == compare_case,
        }

    session_id = str(uuid.uuid4())
    DATA_STORE[session_id] = {
        "scenarios": scenario_dict,
        "monthly": None,
        "yearly": None,
        "timestamp": time.time(),
        "temp_files": temp_files,
    }

    return {
        "session_id": session_id,
        "uploaded_cases": list(provided_files.keys()),
        "compare_case": compare_case,
    }


@app.post("/get_sheet_names")
def get_sheet_names(payload: SessionPayload):
    cleanup_sessions()

    session = DATA_STORE.get(payload.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    sheet_names = extract_sheet_names(session["scenarios"])
    session["timestamp"] = time.time()

    return {"sheet_names": sheet_names}


@app.post("/extract_data_excel")
def extract_sheet_data(payload: ExtractSheetDataPayload):
    cleanup_sessions()

    session = DATA_STORE.get(payload.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    monthly_results, yearly_results = extract_data(
        session["scenarios"],
        payload.sheet_names
    )

    session["monthly"] = monthly_results
    session["yearly"] = yearly_results
    session["timestamp"] = time.time()

    return {"message": "Data extracted successfully"}


@app.post("/extract_monthly_spot_data", response_model=SpotResultPayload)
def extract_monthly_spot_data(payload: ExtractSpotDataPayload):
    session = DATA_STORE.get(payload.session_id)
    if not session or session["monthly"] is None:
        raise HTTPException(status_code=404, detail="Monthly results not found")

    if not payload.scenario:
        raise HTTPException(status_code=400, detail="No scenario selected")

    spot_monthly_data = spot_monthly_data_extraction(session["monthly"], payload.scenario)
    session["timestamp"] = time.time()
    return {"result": spot_monthly_data}


@app.post("/extract_monthly_var_data", response_model=VarResultPayload)
def extract_monthly_var_data(payload: ExtractVariableDataPayload):
    session = DATA_STORE.get(payload.session_id)
    if not session or session["monthly"] is None:
        raise HTTPException(status_code=404, detail="Monthly results not found")

    if not payload.scenario:
        raise HTTPException(status_code=400, detail="Scenario not found")

    var_monthly_data = variable_monthly_data_extraction(
        session["monthly"],
        payload.scenario,
        payload.var_name,
        payload.plant,
    )
    session["timestamp"] = time.time()
    return {"result": var_monthly_data}


@app.post("/extract_yearly_spot_data", response_model=SpotResultPayload)
def extract_yearly_spot_data(payload: ExtractSpotDataPayload):
    session = DATA_STORE.get(payload.session_id)
    if not session or session["yearly"] is None:
        raise HTTPException(status_code=404, detail="Yearly results not found")

    if not payload.scenario:
        raise HTTPException(status_code=400, detail="Scenario not found")

    spot_yearly_data = spot_yearly_data_extraction(session["yearly"], payload.scenario)
    session["timestamp"] = time.time()
    return {"result": spot_yearly_data}


@app.post("/extract_yearly_var_data", response_model=VarResultPayload)
def extract_yearly_var_data(payload: ExtractVariableDataPayload):
    session = DATA_STORE.get(payload.session_id)
    if not session or session["yearly"] is None:
        raise HTTPException(status_code=404, detail="Yearly results not found")

    if not payload.scenario:
        raise HTTPException(status_code=400, detail="Scenario not found")

    var_yearly_data = variable_yearly_data_extraction(
        session["yearly"],
        payload.scenario,
        payload.var_name,
        payload.plant,
    )
    session["timestamp"] = time.time()
    return {"result": var_yearly_data}

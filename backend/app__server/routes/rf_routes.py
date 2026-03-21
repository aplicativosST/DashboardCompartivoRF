from typing import Optional

from fastapi import APIRouter, File, Form, UploadFile

from app__server.core.session_store import session_store
from app__server.schemas.rf_schemas import (
    SessionPayload,
    ExtractSheetDataPayload,
    ExtractVariableDataPayload,
    ExtractSpotDataPayload,
    SpotResultPayload,
    VarResultPayload,
)
from app__server.services.rf_service import (
    get_sheet_names_service,
    extract_excel_data_service,
    extract_monthly_spot_service,
    extract_monthly_var_service,
    extract_yearly_spot_service,
    extract_yearly_var_service,
)

router = APIRouter(tags=["RF Dashboard"])


@router.get("/")
def root():
    return {"message": "Server is alive!"}


@router.post("/upload_rf_files")
async def upload_rf_files(
    compare_case: str = Form(...),
    case_a: Optional[UploadFile] = File(None),
    case_b: Optional[UploadFile] = File(None),
    case_c: Optional[UploadFile] = File(None),
):
    return await session_store.save_uploaded_files(
        compare_case=compare_case,
        case_a=case_a,
        case_b=case_b,
        case_c=case_c,
    )


@router.post("/get_sheet_names")
def get_sheet_names(payload: SessionPayload):
    session = session_store.get_session(payload.session_id)
    sheet_names = get_sheet_names_service(session)
    session_store.touch_session(payload.session_id)
    return {"sheet_names": sheet_names}


@router.post("/extract_data_excel")
def extract_data_excel(payload: ExtractSheetDataPayload):
    session = session_store.get_session(payload.session_id)
    result = extract_excel_data_service(session, payload.sheet_names)
    session_store.touch_session(payload.session_id)
    return result


@router.post("/extract_monthly_spot_data", response_model=SpotResultPayload)
def extract_monthly_spot_data(payload: ExtractSpotDataPayload):
    session = session_store.get_session(payload.session_id)
    result = extract_monthly_spot_service(session, payload.scenario)
    session_store.touch_session(payload.session_id)
    return {"result": result}


@router.post("/extract_monthly_var_data", response_model=VarResultPayload)
def extract_monthly_var_data(payload: ExtractVariableDataPayload):
    session = session_store.get_session(payload.session_id)
    result = extract_monthly_var_service(
        session,
        payload.scenario,
        payload.var_name,
        payload.plant,
    )
    session_store.touch_session(payload.session_id)
    return {"result": result}


@router.post("/extract_yearly_spot_data", response_model=SpotResultPayload)
def extract_yearly_spot_data(payload: ExtractSpotDataPayload):
    session = session_store.get_session(payload.session_id)
    result = extract_yearly_spot_service(session, payload.scenario)
    session_store.touch_session(payload.session_id)
    return {"result": result}


@router.post("/extract_yearly_var_data", response_model=VarResultPayload)
def extract_yearly_var_data(payload: ExtractVariableDataPayload):
    session = session_store.get_session(payload.session_id)
    result = extract_yearly_var_service(
        session,
        payload.scenario,
        payload.var_name,
        payload.plant,
    )
    session_store.touch_session(payload.session_id)
    return {"result": result}
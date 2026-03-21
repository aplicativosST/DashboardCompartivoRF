from datetime import datetime
from typing import Dict, List
from pydantic import BaseModel


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
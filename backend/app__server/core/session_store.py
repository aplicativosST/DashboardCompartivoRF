import os
import time
import uuid
import tempfile
from typing import Dict, Any, Optional
from fastapi import UploadFile, HTTPException

from app__server.core.config import SESSION_TTL


class SessionStore:
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}

    def cleanup_sessions(self) -> None:
        now = time.time()
        expired_sessions = [
            session_id
            for session_id, data in self.data.items()
            if now - data["timestamp"] > SESSION_TTL
        ]

        for session_id in expired_sessions:
            session = self.data[session_id]
            for path in session.get("temp_files", []):
                if os.path.exists(path):
                    try:
                        os.remove(path)
                    except OSError:
                        pass
            del self.data[session_id]

    def create_session(self, scenario_dict: Dict[str, Any], temp_files: list[str]) -> str:
        session_id = str(uuid.uuid4())
        self.data[session_id] = {
            "scenarios": scenario_dict,
            "monthly": None,
            "yearly": None,
            "timestamp": time.time(),
            "temp_files": temp_files,
        }
        return session_id

    def get_session(self, session_id: str) -> Dict[str, Any]:
        self.cleanup_sessions()
        session = self.data.get(session_id)

        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        return session

    def touch_session(self, session_id: str) -> None:
        if session_id in self.data:
            self.data[session_id]["timestamp"] = time.time()

    async def save_uploaded_files(
        self,
        compare_case: str,
        case_a: Optional[UploadFile],
        case_b: Optional[UploadFile],
        case_c: Optional[UploadFile],
    ) -> Dict[str, Any]:
        self.cleanup_sessions()

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

        session_id = self.create_session(scenario_dict, temp_files)

        return {
            "session_id": session_id,
            "uploaded_cases": list(provided_files.keys()),
            "compare_case": compare_case,
        }


session_store = SessionStore()
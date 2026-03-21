from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app__server.routes.rf_routes import router as rf_router
from app__server.core.config import API_ROOT_PATH

app = FastAPI(
    root_path=API_ROOT_PATH,
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

app.include_router(rf_router)
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app__server.api.routes.rf_routes import router as rf_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rf_router)
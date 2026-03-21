from fastapi import FastAPI

app = FastAPI()

@app.get("/")
@app.get("/api/")
def root():
    return {"message": "index alive"}
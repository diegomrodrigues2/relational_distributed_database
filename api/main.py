from fastapi import FastAPI
from database.lsm.lsm_db import SimpleLSMDB

app = FastAPI()

db = SimpleLSMDB(db_path="/tmp/api_db")

@app.get("/get/{key}")
def get_value(key: str):
    return {"value": db.get(key)}

@app.post("/put/{key}")
def put_value(key: str, value: str):
    db.put(key, value)
    return {"status": "ok"}

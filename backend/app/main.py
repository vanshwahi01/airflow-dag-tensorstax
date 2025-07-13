from fastapi import FastAPI, HTTPException
from .services.airflow_client import list_dags

app = FastAPI()

@app.get("/dags")
async def get_dags():
    try:
        return await list_dags()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

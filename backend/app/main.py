from fastapi import FastAPI, HTTPException, Path
from .services.airflow_client import list_dags, list_dag_runs
import httpx


app = FastAPI()

@app.get("/dags")
async def get_dags():
    try:
        return await list_dags()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.get("/dags/{dag_id}/runs")
async def get_dag_runs(
    dag_id: str = Path(..., description="The ID of the DAG"),
    limit: int = 10
):
    """
    Returns up to `limit` recent DagRun records for the given DAG.
    """
    try:
        return await list_dag_runs(dag_id, limit)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="DAG not found")
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
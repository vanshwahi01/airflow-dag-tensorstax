from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import PlainTextResponse
from .services.airflow_client import list_dags, list_dag_runs, get_task_logs
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
    
@app.get(
    "/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/logs/{try_number}",
    response_class=PlainTextResponse,
    description="Fetch logs for a specific task instance"
)
async def fetch_task_logs(
    dag_id: str = Path(...),
    run_id: str = Path(..., alias="run_id"),
    task_id: str = Path(...),
    try_number: int = Path(..., ge=1)
):
    """
    Proxy endpoint to get the logs of a specific task instance.
    """
    try:
        logs = await get_task_logs(dag_id, run_id, task_id, try_number)
        return logs
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Log not found")
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
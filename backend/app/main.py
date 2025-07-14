from fastapi import FastAPI, HTTPException, Path, Query, Request, Form, BackgroundTasks
from fastapi.responses import PlainTextResponse
from slack_sdk.signature import SignatureVerifier
from .services.airflow_client import list_dags, list_dag_runs, get_task_logs, list_tasks
from .services.sla_monitor import compute_sla
from .services.lineage import get_task_lineage, get_dag_lineage
from .services.alerting import handle_airflow_failure, handle_auto_fix
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import httpx
import asyncio
import os
import json
import certifi

app = FastAPI(title="Airflow Observability")

load_dotenv() # for env variables

os.environ["SSL_CERT_FILE"] = certifi.where() # ensures every library uses the same CA bundle

# CORS policy to allow requests from the frontend to localhost:3000
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    Returns up to limit recent DagRun records for the given DAG.
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
    
    
@app.get("/sla/{dag_id}")
async def get_sla(
    dag_id: str = Path(..., description="DAG ID"),
    interval: str = Query("daily", regex="^(daily|weekly|monthly)$")
):
    """
    Returns SLA compliance for the given DAG over the specified interval.
    """
    try:
        data = await compute_sla(dag_id, interval)
        return data
    except StopIteration:
        # DAG not found so empty SLA
        return {
            "dag_id": dag_id,
            "interval": interval,
            "expected": 0,
            "successes": 0,
            "misses": 0,
            "compliance_pct": 0.0
        }
    except ValueError:
        return {
            "dag_id": dag_id,
            "interval": interval,
            "expected": 0,
            "successes": 0,
            "misses": 0,
            "compliance_pct": 0.0
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    
    
@app.get("/sla")
async def get_all_sla(
    interval: str = Query("daily", regex="^(daily|weekly|monthly)$")
):
    """
    Returns SLA compliance for all DAGs over the specified interval.
    """
    try:
        # Fetch all DAG IDs
        dags_resp = await list_dags()
        dag_ids = [d["dag_id"] for d in dags_resp["dags"]]

        tasks = [compute_sla(dag_id, interval) for dag_id in dag_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any failures per-DAG
        output = []
        for dag_id, res in zip(dag_ids, results):
            if isinstance(res, Exception):
                output.append({
                    "dag_id": dag_id,
                    "error": str(res)
                })
            else:
                output.append(res)

        return {"interval": interval, "results": output}

    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    
    
@app.get("/lineage/{dag_id}")
async def lineage_for_dag(
    dag_id: str = Path(..., description="The DAG ID to inspect")
):
    """
    Task level lineage for a single DAG.
    """
    try:
        return await get_task_lineage(dag_id)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="DAG not found")
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/lineage")
async def lineage_all_dags():
    """
    DAG level lineage
    """
    try:
        return await get_dag_lineage()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    
    
@app.post("/alerts/webhook")
async def airflow_webhook(request: Request):
    """
    Endpoint for Airflow to POST on DAG failure
    """
    payload = await request.json()
    try:
        await handle_airflow_failure(payload)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
# Initialize Slack signature 
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
verifier = SignatureVerifier(signing_secret=SLACK_SIGNING_SECRET)

@app.post("/slack/actions")
async def slack_actions(
    background_tasks: BackgroundTasks,
    payload: str = Form(...)
):
    try:
        print("Received payload:", payload)
        data = json.loads(payload)
        action = data.get("actions", [])[0]
        if action["action_id"] != "auto_fix":
            return {"status": "ignored"}

        dag_id, run_id, task_id, error = action["value"].split("|", 3)
        background_tasks.add_task(handle_auto_fix, dag_id, run_id, task_id, error) # need this due to slack 3s rule for operational call
        return {"text": ":gear: Auto-fix in progress!"}

    except Exception as e:
        print("Error in /slack/actions handler:\n")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/dags/{dag_id}/tasks")
async def get_tasks(dag_id: str):
    """
    Proxy to Airflow REST API: list all tasks in a DAG.
    """
    try:
        tasks = await list_tasks(dag_id)
        return {"tasks": tasks}
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="DAG not found")
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
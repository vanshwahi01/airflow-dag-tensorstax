import os
from dotenv import load_dotenv
import httpx

# Load env variables
load_dotenv()

AIRFLOW_BASE = os.getenv("AIRFLOW_BASE_URL")
AIRFLOW_USER = os.getenv("AIRFLOW_USER")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS")

async def list_dags():
    """
    Fetches the list of DAGs from REST API.
    Returns parsed JSON
    """
    async with httpx.AsyncClient(auth=(AIRFLOW_USER, AIRFLOW_PASS)) as client:
        response = await client.get(f"{AIRFLOW_BASE}/dags")
        response.raise_for_status()
        return response.json()

async def list_dag_runs(dag_id: str, limit: int = 10):
    """
    Fetches the most recent DagRuns for a given DAG with limit
    """
    url = f"{AIRFLOW_BASE}/dags/{dag_id}/dagRuns?order_by=-execution_date&limit={limit}"
    async with httpx.AsyncClient(auth=(AIRFLOW_USER, AIRFLOW_PASS)) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()
    
async def get_task_logs(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = 1
) -> str:
    """
    Fetch the logs for a specific task instance.
    Returns the raw log text.
    """
    url = (
        f"{AIRFLOW_BASE}/dags/{dag_id}"
        f"/dagRuns/{dag_run_id}"
        f"/taskInstances/{task_id}"
        f"/logs/{try_number}"
    )
    async with httpx.AsyncClient(auth=(AIRFLOW_USER, AIRFLOW_PASS)) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text

async def list_tasks(dag_id: str):
    """
    List all tasks in a DAG, including their upstream/downstream links.
    """
    url = f"{AIRFLOW_BASE}/dags/{dag_id}/tasks"
    async with httpx.AsyncClient(auth=(AIRFLOW_USER, AIRFLOW_PASS)) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()["tasks"]

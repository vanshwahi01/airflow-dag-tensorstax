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

from .airflow_client import list_dags, list_tasks
from typing import List, Dict

async def get_task_lineage(dag_id: str) -> Dict:
    """
    Returns task‑level lineage for a single DAG.
    {
      "dag_id": str,
      "nodes": [ { "id": task_id } ... ],
      "edges": [ { "from": src, "to": dst } ... ]
    }
    """
    tasks = await list_tasks(dag_id)
    nodes = [{"id": t["task_id"]} for t in tasks]
    edges = []
    for t in tasks:
        src = t["task_id"]
        for dst in t.get("downstream_task_ids", []):
            edges.append({"from": src, "to": dst})
    return {"dag_id": dag_id, "nodes": nodes, "edges": edges}

async def get_dag_lineage() -> Dict:
    """
    Returns DAG level lineage: nodes are DAGs, edges are empty
    (stub for future cross-DAG flows).
    """
    dags = await list_dags()
    dag_ids = [d["dag_id"] for d in dags["dags"]]
    nodes = [{"id": dag_id} for dag_id in dag_ids]
    edges: List[Dict] = []
    return {"nodes": nodes, "edges": edges}

import os
import httpx
import subprocess, tempfile
from openai import OpenAI
from slack_sdk.webhook import WebhookClient
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
AIRFLOW_BASE = os.getenv("AIRFLOW_BASE_URL")
AIRFLOW_USER = os.getenv("AIRFLOW_USER")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS")

# LLM client
llm = OpenAI(
    api_key=OPENAI_API_KEY
)
# Slack webhook
slack = WebhookClient(
    SLACK_WEBHOOK_URL
)

async def handle_airflow_failure(payload: dict):
    """
    payload is the JSON Airflow will POST when a DAG run fails.
    """
    dag_id = payload["dag_id"]
    run_id = payload["dag_run_id"]
    task_id = payload["task_id"]
    error = payload.get("exception", "<no error message>")

    # 1) root cause & suggest fixes
    prompt = (
        f"A DAG in Airflow failed.\n\n"
        f"DAG: {dag_id}\n"
        f"Run ID: {run_id}\n"
        f"Task: {task_id}\n"
        f"Error:\n```\n{error}\n```\n\n"
        "Explain concisely the specific root cause, "
        "and suggest two possible fixes or improvements."
    )
    resp = llm.chat.completions.create(
        model="gpt-4o-mini",
        store=True,
        messages=[
            {"role" : "user", "content" : prompt}
        ]
    )
    
    analysis = resp.choices[0].message.content

    # 2) Post to Slack
    slack_payload = {
        "text": f"*Airflow DAG Failure* `{dag_id}` run `{run_id}` failed on task `{task_id}`",
        "blocks": [
            {"type":"section","text":{"type":"mrkdwn","text":f"*DAG*: `{dag_id}`\n*Run*: `{run_id}`\n*Task*: `{task_id}`"}},
            {"type":"section","text":{"type":"mrkdwn","text":f"*Error*:\n```{error}```"}},
            {"type":"section","text":{"type":"mrkdwn","text":f"*LLM Analysis & Fixes*:\n{analysis}"}},
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Auto‑Fix?"},
                        "action_id": "auto_fix", 
                        "value": f"{dag_id}|{run_id}|{task_id}|{error}"
                    }
                ]
            }
        ]
    }
    slack_resp = slack.send(**slack_payload)
    if not slack_resp.ok:
        raise RuntimeError(f"Slack connection failed: {slack_resp.status_code} {slack_resp.body}")
    

async def handle_auto_fix(dag_id, run_id, task_id, error):
    """
    1) Pull the DAG file from airflow_home/dags
    2) Ask the LLM to generate a patch
    3) Apply the patch
    4) Trigger a new DAG run via Airflow API
    """
    # Load the DAG source file
    dag_path = os.path.join(os.getenv("AIRFLOW_HOME"), "dags", f"{dag_id}.py")
    with open(dag_path) as f:
        original_code = f.read()

    prompt = (
        f"The following Airflow DAG file failed on task {task_id} with error:\n"
        f"```\n{error}\n```\n"
        "Here is the file contents:\n"
        f"```\n{original_code}\n```\n"
        "Output a unified diff that fixes the root cause."
    )
    resp = llm.chat.completions.create(
        model="gpt-4o-mini",
        store=True,
        messages=[{"role":"user","content":prompt}]
    )
    diff = resp.choices[0].message.content

    # Apply the patch and diff
    patch_file = tempfile.NamedTemporaryFile(delete=False)
    patch_file.write(diff.encode())
    patch_file.flush()
    subprocess.run(["patch", "-p0", "-i", patch_file.name], check=True)

    # Triggering new DAG run
    trigger_url = f"{AIRFLOW_BASE}/dags/{dag_id}/dagRuns"
    trigger_resp = httpx.post(
        trigger_url,
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        json={"conf": {}}
    )
    trigger_resp.raise_for_status()


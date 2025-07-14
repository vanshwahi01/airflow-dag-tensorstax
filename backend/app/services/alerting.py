import os
import httpx
import subprocess, tempfile
import time
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
    if slack_resp.status_code != 200:
        body = getattr(slack_resp, "body", slack_resp.text)
        raise RuntimeError(f"Slack failed: {slack_resp.status_code} {body}")
    

async def handle_auto_fix(dag_id, run_id, task_id, error):
    """
    1) Pull the DAG file from airflow_home/dags
    2) Ask the LLM to generate a patch
    3) Apply the patch
    4) Trigger a new DAG run via Airflow API
    """
    # Load the DAG source file
    dag_path = os.path.join(os.getenv("AIRFLOW_HOME"), "dags", f"{dag_id}.py")
    print("File to patch:", dag_path)
    with open(dag_path) as f:
        original_code = f.read()

    prompt = (
        f"The following Airflow DAG file (at `{dag_path}`) failed on task `{task_id}`\n"
        f"with error:\n```\n{error}\n```\n\n"
        f"Here is the file contents:\n```\n{original_code}\n```\n\n"
        "Return the complete, corrected Python file contents, without markdown fences or any commentary."
    )
    resp = llm.chat.completions.create(
        model="gpt-4o-mini",
        store=True,
        messages=[{"role": "user", "content": prompt}]
    )
    new_source = resp.choices[0].message.content

    # Overwrite the DAG file
    with open(dag_path, "w") as f:
        f.write(new_source)

    # Trigger a new DAG run
    trigger_resp = httpx.post(
        f"{AIRFLOW_BASE}/dags/{dag_id}/dagRuns",
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        json={"conf": {}}
    )
    trigger_resp.raise_for_status()
    new_run_id = trigger_resp.json()["dag_run_id"]
    
    # Retry logic
    status = None
    attempt = 1
    max_attempts = 3
    for _ in range(5): # 5 attempts with 10s sleep, we poll the status for completion
        time.sleep(10)
        jr = httpx.get(
            f"{AIRFLOW_BASE}/dags/{dag_id}/dagRuns/{new_run_id}",
            auth=(AIRFLOW_USER, AIRFLOW_PASS)
        ).json()
        status = jr["state"].lower()
        if status in ("success", "failed"):
            break

    slack = WebhookClient(os.getenv("SLACK_WEBHOOK_URL"))
    if status == "success":
        slack.send(text=f"Auto-fix succeeded on attempt {attempt} for `{dag_id}` run `{new_run_id}`.")
        return

    if status == "failed" and attempt < max_attempts:
        # Extract the new error from the run’s logs or resp
        new_error = jr.get("state") + ": " + jr.get("error", "<no error text>")
        slack.send(text=f"Auto-fix attempt {attempt} for `{dag_id}` run `{new_run_id}` failed. Retrying…")
        attempt += 1
        await handle_auto_fix(dag_id, new_run_id, task_id, new_error)
    else:
        slack.send(text=f"Auto-fix failed after {attempt} attempts for `{dag_id}`. Manual intervention needed.")

    # Testing Purposes : IGNORE
    
    # raw = resp.choices[0].message.content
    # lines = raw.splitlines()
    # clean_lines = [l for l in lines if not l.strip().startswith("```")]
    # diff_body = "\n".join(clean_lines)
    
    # # prepend a proper unified-diff header
    # filename = os.path.basename(dag_path)
    # header = f"--- {filename}\n+++ {filename}\n"
    # full_diff = header + diff_body
    
    # with tempfile.NamedTemporaryFile(mode="w+", delete=False) as patch_file:
    #     patch_file.write(full_diff)
    #     patch_file.flush()
        
    # try:
    #     subprocess.run([
    #         "patch", "-p0", "--batch", "--silent",
    #         "-d", os.path.dirname(dag_path),
    #         "-i", patch_file.name
    #     ], check=True)
    # except subprocess.CalledProcessError:
    #     rej_path = dag_path + ".rej"
    #     reject_content = ""
    #     if os.path.exists(rej_path):
    #         with open(rej_path) as rej:
    #             reject_content = rej.read()
    #     # Notify Slack of the failure
    #     slack = WebhookClient(os.getenv("SLACK_WEBHOOK_URL"))
    #     text = (
    #         f":x: *Auto‑Fix Failed* for `{dag_id}`\n"
    #         f"> *Task*: `{task_id}`\n"
    #         f"> *Error*: ```{error}```\n"
    #         f"_Patch rejects attached below:_"
    #     )
    #     slack.send(
    #         text=text,
    #         attachments=[{"text": f"```{reject_content}```"}]
    #     )
    #     return 

    # # patch_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
    # # patch_file.write(diff)
    # # patch_file.flush()
    # # subprocess.run(["patch", "-p0", "-i", patch_file.name], check=True)

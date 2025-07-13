import os
from openai import OpenAI
from slack_sdk.webhook import WebhookClient
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

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
        "text": f":*Airflow DAG Failure* `{dag_id}` run `{run_id}` failed on task `{task_id}`",
        "blocks": [
            {"type":"section","text":{"type":"mrkdwn","text":f"*DAG*: `{dag_id}`\n*Run*: `{run_id}`\n*Task*: `{task_id}`"}},
            {"type":"section","text":{"type":"mrkdwn","text":f"*Error*:\n```{error}```"}},
            {"type":"section","text":{"type":"mrkdwn","text":f"*LLM Analysis & Fixes*:\n{analysis}"}}
        ]
    }
    slack_resp = slack.send(**slack_payload)
    if not slack_resp.ok:
        raise RuntimeError(f"Slack connection failed: {slack_resp.status_code} {slack_resp.body}")

# Airflow Dashboard Application

## Airflow Observability and Automation Dashboard

## DAG Overview Dashboard

Build a dashboard UI that displays the following information for all DAGs

- **Basic DAG Info**:
  - DAG Name
  - Owner
  - Schedule (CRON or interval)

- **Task Run Summary**:
  - Visualization (e.g. horizontal bar or timeline) of recent task runs
  - Success vs failure indicators (e.g. green/red status)

- **Log Access**:
  - Ability to view logs for each task instance directly from the dashboard

---

## SLA Monitoring

Implement SLA monitoring logic for all DAGs with tracking at different time intervals:

- Daily SLA hit/miss rate
- Weekly SLA hit/miss rate
- Monthly SLA hit/miss rate

The dashboard should show SLA compliance trends and optionally export this data as JSON or CSV.

---

## DAG & Task Lineage Visualization

Create an interactive lineage view that captures both DAG-level and task-level lineage.

- **Task-level Lineage**:
  - Visualize task dependencies within a DAG

- **DAG-level Lineage**:
  - Show upstream and downstream data flow across DAGs
  - Example: Data coming from **Snowflake** and going to **Postgres**

---

## Monitoring & Alerts with LLM Integration

Add real-time error monitoring and auto alerting.

- When a DAG fails:
  - Extract the error
  - Use an LLM to **explain the root cause**
  - Suggest possible **fixes or improvements**
  
- Send **alerts via Slack**

---

## Auto-Fix with Human-in-the-Loop

Implement an auto-fix flow using an llm agent:

- Prompt user via slack for approval to auto-fix a failed DAG
- Upon approval:
  - Use the LLM to generate a fix
  - Update the DAG file
  - Re-trigger the DAG run
- auto fix should happen autonomously - for instace, if the re-trigger fails, it should validate again and if there is another error the same process repeats (extracting error to re-triggring dag)

---

## Tech Stack

- **Backend**: FastAPI
- **Frontend**: React or any
- **Visualization**: react-flow or any
- **LLM Integration**: any
- **Airflow**: local airflow instance with some sample dags
- **Slack API**: Real webhook integration

---

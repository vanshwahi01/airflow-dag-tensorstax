import os
import datetime
from datetime import timezone
from croniter import croniter
from .airflow_client import list_dag_runs, list_dags

# Map interval names to timedelta lengths
INTERVALS = {
    "daily": datetime.timedelta(days=1),
    "weekly": datetime.timedelta(weeks=1),
    "monthly": datetime.timedelta(days=30),
}

async def compute_sla(dag_id: str, interval: str = "daily"):
    """
    Returns SLA stats for the given DAG over the past `interval`.
    """
    if interval not in INTERVALS:
        raise ValueError(f"Unknown interval {interval}")

    now = datetime.datetime.now(timezone.utc)
    window = INTERVALS[interval]
    start = now - window

    # 1. Fetch DAG metadata to know its schedule
    dags_resp = await list_dags()
    dag = next(d for d in dags_resp["dags"] if d["dag_id"] == dag_id)
    sched = dag["schedule_interval"]

    # 2. Compute expected runs
    if sched is None:
        expected = 0
    elif sched.get("__type") == "TimeDelta":
        delta = datetime.timedelta(
            days=sched["days"],
            seconds=sched["seconds"],
            microseconds=sched["microseconds"],
        )
        expected = max(0, int(window.total_seconds() // delta.total_seconds()))
    elif sched.get("__type") == "CronExpression":
        base = sched["value"]
        ci = croniter(base, start)
        # count how many cron ticks between start and now
        count = 0
        while True:
            nxt = ci.get_next(datetime.datetime)
            if nxt > now:
                break
            count += 1
        expected = count
    else:
        expected = 0

    # 3. Fetch all runs in that window 
    runs_resp = await list_dag_runs(dag_id, limit=1000)
    runs = runs_resp["dag_runs"]

    # 4. Count successes within window
    successes = sum(
        1
        for r in runs
        if r["state"].lower() == "success"
        and start <= datetime.datetime.fromisoformat(r["execution_date"]) <= now
    )

    # 5. Misses = expected - successes (positive only)
    misses = max(0, expected - successes)

    pct = (successes / expected * 100.0) if expected > 0 else 0.0

    return {
        "dag_id": dag_id,
        "interval": interval,
        "expected": expected,
        "successes": successes,
        "misses": misses,
        "compliance_pct": round(pct, 1),
    }
